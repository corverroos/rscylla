package rscylla

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"golang.org/x/sync/errgroup"
)

// GetCursor returns a cursor for streaming from the first event after the provided timestamp.
//
// This is especially handy when streaming from the tail of a log. Since rscylla queries
// time windows sequentially, streaming from an zero cursor (DB create time) results in
// initial delays.
func GetCursor(ctx context.Context, session *gocql.Session, keyspace, table string, from time.Time, opts ...option) (string, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	if from.IsZero() {
		// Zero times not supported
		from = time.Unix(0, 0)
	}

	// TODO(corver): Support empty generations.
	gen, err := getGeneration(ctx, session, from)
	if err != nil {
		return "", err
	}

	shards, err := getShards(ctx, session, o.Consistency, gen)
	if err != nil {
		return "", err
	}

	kmeta, err := session.KeyspaceMetadata(keyspace)
	if err != nil {
		return "", err
	} else if _, ok := kmeta.Tables[table]; !ok {
		return "", errors.New("table not found")
	}

	ch := make(chan *reflex.Event, len(shards))
	req := queryReq{
		keyspace:    keyspace,
		table:       table,
		meta:        kmeta.Tables[table],
		gen:         gen,
		from:        from,
		to:          time.Now(),
		limit:       1,
		consistency: o.Consistency,
	}

	err = queryShards(ctx, session, shards, req, ch)
	if err != nil {
		return "", err
	}

	close(ch)
	var first *reflex.Event
	for e := range ch {
		if first == nil || e.Timestamp.Before(first.Timestamp) {
			first = e
		}
	}

	if first == nil {
		return "", errors.New("no event found")
	}

	b, err := json.Marshal(eventID{
		CursorGen:  gen,
		CursorTime: first.Timestamp.Truncate(time.Second),
	})

	return string(b), err
}

// queryReq contains the arguments for a time window query of the CDC log.
type queryReq struct {
	keyspace, table string
	meta            *gocql.TableMetadata
	gen, from, to   time.Time
	limit           int
	consistency     gocql.Consistency
}

// queryShards executes the query request against all the shards concurrently.
// It blocks until all results have been pushed back on the channel.
func queryShards(ctx context.Context, session *gocql.Session, shards [][]streamID,
	req queryReq, ch chan<- *reflex.Event) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var eg errgroup.Group
	for _, shard := range shards {
		shard := shard
		eg.Go(func() error {
			return queryShard(ctx, session, shard, req, ch)
		})
	}

	return eg.Wait()
}

// queryShard executes the query request against the provided shard.
func queryShard(ctx context.Context, session *gocql.Session, shard []streamID, req queryReq, ch chan<- *reflex.Event) error {
	if req.from.IsZero() || req.to.IsZero() {
		return errors.New("zero to/from not supported")
	}

	// Build up query and args
	var (
		q    string
		args []interface{}
	)

	// Start by selecting everything from the table
	q += fmt.Sprintf("SELECT * FROM %s.%s WHERE", req.keyspace, req.table+cdcTableSuffix)

	// Add streams
	if len(shard) == 1 {
		q += " \"cdc$stream_id\" = ? "
	} else {
		q += " \"cdc$stream_id\" IN (?" + strings.Repeat(", ?", len(shard)-1) + ")"
	}
	for _, streamID := range shard {
		args = append(args, streamID)
	}

	// Add window
	q += ` AND "cdc$time" >= ? AND "cdc$time" < ?`
	args = append(args, gocql.MinTimeUUID(req.from))
	args = append(args, gocql.MinTimeUUID(req.to))

	// Add limit
	if req.limit > 0 {
		q += " LIMIT ?"
		args = append(args, req.limit)
	}

	// End by bypassing cache
	q += " BYPASS CACHE;"

	// Do query
	iter := session.Query(q, args...).WithContext(ctx).Consistency(req.consistency).Iter()

	// Scan results
	for {
		vals := make(map[string]interface{})
		if !iter.MapScan(vals) {
			break
		}

		foreignID, err := makeForeignID(req.meta, vals)
		if err != nil {
			return err
		}

		id, err := makeEventID(req.gen, req.from, vals)
		if err != nil {
			return err
		}

		e := &reflex.Event{
			ID:        id,
			Timestamp: vals["cdc$time"].(gocql.UUID).Time(),
			Type:      OperationType(vals["cdc$operation"].(int8)),
			ForeignID: foreignID,
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- e:
		}
	}

	if err := iter.Close(); err != nil {
		return errors.Wrap(err, "query shard")
	}

	return nil
}

// makeForeignID returns the json encoded row primary key as
// the reflex event foreign ID.
func makeForeignID(meta *gocql.TableMetadata, vals map[string]interface{}) (string, error) {
	res := make(map[string]interface{})
	for _, metadata := range meta.PartitionKey {
		res[metadata.Name] = vals[metadata.Name]
	}
	for _, metadata := range meta.ClusteringColumns {
		res[metadata.Name] = vals[metadata.Name]
	}

	b, err := json.Marshal(res)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// makeEventID returns the json encoded cursor and CDC log primary key as
// the reflex event ID.
func makeEventID(gen time.Time, from time.Time, vals map[string]interface{}) (string, error) {
	e := eventID{
		CursorGen:  gen,
		CursorTime: from,
		StreamID:   hex.EncodeToString(vals["cdc$stream_id"].([]byte)),
		StreamTime: vals["cdc$time"].(gocql.UUID).String(),
		StreamSeq:  vals["cdc$batch_seq_no"].(int),
	}

	b, err := json.Marshal(e)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// getGeneration returns the generation timestamp containing t0 or the first generation.
func getGeneration(ctx context.Context, session *gocql.Session, t0 time.Time) (time.Time, error) {
	iter := session.Query("SELECT time FROM " + generationsTableName).WithContext(ctx).Consistency(gocql.One).Iter()

	var (
		prev time.Time
		gen  time.Time
	)
	for {
		if !iter.Scan(&gen) {
			break
		}
		if prev.IsZero() && t0.Before(gen) {
			// Return first generation if t0 before first generation.
			return gen, nil
		}
		if t0.Before(gen) {
			// Return previous generation if gen after t0.
			return prev, nil
		}
		prev = gen
	}

	if err := iter.Close(); err != nil {
		return time.Time{}, errors.Wrap(err, "get generation")
	}

	if prev.IsZero() {
		return time.Time{}, errors.New("no generations found")
	}

	return prev, nil
}
