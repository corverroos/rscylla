package rscylla

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gocql/gocql"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
)

const (
	safeLag           = time.Second * 30 //  safeLag is the delay after which all eventual consistencies have converged.
	generationPoll    = time.Second * 15 //  generationPoll is the period at which new generations are detected.
	generationOverlap = time.Second * 30 //  generationOverlap allows for events outside the generation boundaries.

	cdcTableSuffix       = "_scylla_cdc_log"
	generationsTableName = "system_distributed.cdc_streams_descriptions"
)

// NewStream returns a new Stream.
func NewStream(session *gocql.Session, keyspace, table string, opts ...option) *Stream {
	s := Stream{
		options:  defaultOptions(),
		keyspace: keyspace,
		table:    table,
		session:  session,
	}

	for _, opt := range opts {
		opt(&s.options)
	}

	return &s
}

// Stream represents a scyllaDB CDC table stream.
type Stream struct {
	options

	//  keyspace is the name of the CQL keyspace containing the table.
	keyspace string

	//  table is the root name of the CDC table (excluding suffix).
	table string

	//  session is the scyllaDB sessions.
	session *gocql.Session
}

// Stream implements reflex.StreamFunc and returns a StreamClient that
// streams events from a scyllaDB CDC table from the provided cursor.
//
// Stream is safe to call from multiple goroutines, but the returned
// StreamClient is only safe for a single goroutine to use.
//
// The cursor is a previously streamed event ID or the result from GetCursor.
func (s *Stream) Stream(ctx context.Context, cursorStr string,
	opts ...reflex.StreamOption) (reflex.StreamClient, error) {

	o := reflex.StreamOptions{
		Lag: safeLag,
	}
	for _, opt := range opts {
		opt(&o)
	}

	if o.StreamFromHead {
		return nil, errors.New("stream to head not supported yet")
	}

	var cur cursor
	if cursorStr != "" {
		c, err := parseCursor(cursorStr)
		if err != nil {
			return nil, err
		}
		cur = c
	}

	// TODO(corver): Add support for schema changes.
	kmeta, err := s.session.KeyspaceMetadata(s.keyspace)
	if err != nil {
		return nil, err
	} else if _, ok := kmeta.Tables[s.table]; !ok {
		return nil, errors.New("table metadata not found")
	}

	sc := &streamclient{
		eventChan: make(chan *reflex.Event),
		errChan:   make(chan error, 1),
	}

	run := func() error {
		var (
			shards [][]streamID
			ok     bool
			err    error
		)
		for {
			cur, ok, err = maybeBumpGeneration(ctx, s.session, s.Consistency, cur)
			if err != nil {
				// NoReturnErr:
				return errors.Wrap(err, "bump generation")
			} else if ok || len(shards) == 0 {
				shards, err = getShards(ctx, s.session, s.Consistency, cur.Generation, s.ShardM, s.ShardN)
				if err != nil {
					return errors.Wrap(err, "get shards")
				}
			}

			window := calcWindow(o.Lag)
			to := cur.Time.Add(window)

			toLag := time.Since(to)
			if toLag < o.Lag {
				if o.StreamToHead {
					return reflex.ErrHeadReached
				}

				// We will query past lag. Back off a bit.
				time.Sleep(o.Lag - toLag)
			}

			req := queryReq{
				keyspace:    s.keyspace,
				table:       s.table,
				meta:        kmeta.Tables[s.table],
				gen:         cur.Generation,
				from:        cur.Time,
				to:          to,
				consistency: s.Consistency,
			}

			err := queryShards(ctx, s.session, shards, req, sc.eventChan)
			if err != nil {
				return errors.Wrap(err, "query shards")
			}

			cur.Time = to
		}
	}

	go func() {
		sc.errChan <- run()
	}()

	return sc, nil
}

// sliceShards returns a consistent mth-of-n subset of shards.
func sliceShards(shards [][]streamID, m int, n int) [][]streamID {
	if n < 2 {
		return shards
	}

	var res [][]streamID
	for i, shard := range shards {
		if i%n == m {
			res = append(res, shard)
		}
	}
	return res
}

// maybeBumpGeneration returns either the provided cursor and false or
// the next generation's cursor and true.
func maybeBumpGeneration(ctx context.Context, session *gocql.Session, consistency gocql.Consistency, cur cursor) (cursor, bool, error) {
	if time.Since(cur.UpdatedAt) < generationPoll {
		return cur, false, nil
	}

	cur.UpdatedAt = time.Now()

	iter := session.Query("SELECT time FROM " + generationsTableName).WithContext(ctx).Consistency(consistency).Iter()

	var (
		nextGen time.Time
		found   bool
	)
	for {
		if !iter.Scan(&nextGen) {
			break
		}
		if nextGen.After(cur.Generation) {
			found = true
			break
		}
	}

	if err := iter.Close(); err != nil {
		return cursor{}, false, errors.Wrap(err, "list generations")
	}

	if !found {
		if cur.Generation.IsZero() {
			return cursor{}, false, errors.New("no generations found, new cluster?")
		}
		// NoReturnErr: No new generation found, return previous.
		return cur, false, nil
	}

	if !cur.Generation.IsZero() && cur.Time.Add(-generationOverlap).Before(nextGen) {
		// Current cursor before next generation.
		return cur, false, nil
	}

	return cursor{
		Generation: nextGen,
		UpdatedAt:  time.Now(),
		Time:       nextGen.Add(-generationOverlap),
	}, true, nil
}

// calcWindow returns a dynamic window size based on the lag.
// The bigger the lag, the bigger the window.
func calcWindow(lag time.Duration) time.Duration {
	if lag > time.Minute {
		return time.Minute // If lagging by a lot, window is 1min.
	} else if lag > safeLag {
		return time.Second * 10 // If lagging safe lag, window is 10s.
	}

	return time.Second // If unsafe short lag, window is 1s
}

type streamclient struct {
	eventChan chan *reflex.Event
	errChan   chan error
	err       error
}

func (s *streamclient) Recv() (*reflex.Event, error) {
	if s.err != nil {
		return nil, s.err
	}

	select {
	case e := <-s.eventChan:
		return e, nil
	case err := <-s.errChan:
		s.err = err
		return nil, err
	}
}

type cursor struct {
	Generation time.Time
	Time       time.Time

	UpdatedAt time.Time
}

func parseCursor(s string) (cursor, error) {
	var eid eventID
	err := json.Unmarshal([]byte(s), &eid)
	if err != nil {
		return cursor{}, errors.Wrap(err, "unmarshal eventid")
	}

	return cursor{
		Time:       eid.CursorTime,
		Generation: eid.CursorGen,
		UpdatedAt:  time.Now(),
	}, nil
}
