package rscylla

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/stretchr/testify/require"
)

const (
	keyspace = "test"
	table    = "t"
)

var z time.Time

func setup(t *testing.T) *gocql.Session {
	cluster := gocql.NewCluster("localhost")
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy("local-dc"))
	cluster.Timeout = time.Second * 2
	s, err := cluster.CreateSession()
	jtest.RequireNil(t, err)
	t.Cleanup(s.Close)

	exec(t, s, z, "drop keyspace if exists test")
	exec(t, s, z, "create keyspace test with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
	exec(t, s, z, "CREATE TABLE test.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};")

	return s
}

func TestStream(t *testing.T) {
	s := setup(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		for j := 0; j < 2; j++ {
			exec(t, s, time.Now().Add(-time.Second*2), "INSERT INTO test.t (pk,ck,v) values (?,?,?);", i+10, j+100, i*j)
		}
	}

	cur, err := GetCursor(ctx, s, keyspace, table, time.Time{}, WithConsistency(gocql.One))
	jtest.RequireNil(t, err)

	stream := NewStream(s, keyspace, table, WithConsistency(gocql.One))
	sc, err := stream.Stream(ctx, cur, reflex.WithStreamLag(time.Second))
	jtest.RequireNil(t, err)

	uniq := make(map[string]bool)
	for i := 0; i < 6; i++ {
		e, err := sc.Recv()
		jtest.RequireNil(t, err)

		uniq[e.ForeignID] = true
	}

	require.Len(t, uniq, 6)
}

func TestShard(t *testing.T) {
	s := setup(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		for j := 0; j < 2; j++ {
			exec(t, s, time.Now().Add(-time.Second*2), "INSERT INTO test.t (pk,ck,v) values (?,?,?);", i, j, i*j)
		}
	}

	cur, err := GetCursor(ctx, s, keyspace, table, time.Time{}, WithConsistency(gocql.One))
	jtest.RequireNil(t, err)

	streamShard := func(t *testing.T, m, n int) map[string]bool {
		stream := NewStream(s, keyspace, table, WithConsistency(gocql.One), WithShard(m, n))
		sc, err := stream.Stream(ctx, cur, reflex.WithStreamLag(time.Second), reflex.WithStreamToHead())
		jtest.RequireNil(t, err)

		uniq := make(map[string]bool)
		for {
			e, err := sc.Recv()
			if errors.Is(err, reflex.ErrHeadReached) {
				return uniq
			}
			jtest.RequireNil(t, err)

			uniq[e.ForeignID] = true
		}
	}

	uniq0 := streamShard(t, 0, 3)
	uniq1 := streamShard(t, 1, 3)
	uniq2 := streamShard(t, 2, 3)

	require.Equal(t, len(uniq0)+len(uniq1)+len(uniq2), 6)

	merge := make(map[string]bool)
	for k, v := range uniq0 {
		merge[k] = v
	}
	for k, v := range uniq1 {
		merge[k] = v
	}
	for k, v := range uniq2 {
		merge[k] = v
	}
	require.Len(t, merge, 6)
}

var load = flag.Bool("load", false, "Enable slow load tests")

func TestLoad(t *testing.T) {
	if !*load {
		t.Skip("Skipping slow load tests. Use -load flag to enable")
		return
	}

	tests := []struct {
		Name       string
		Insert     int
		Inserters  int
		WaitFor    int64
		ReadShards int
	}{
		{
			Name:      "small_no_wait",
			Insert:    10000,
			Inserters: 100,
			WaitFor:   0,
		}, {
			Name:      "small_wait",
			Insert:    10000,
			Inserters: 100,
			WaitFor:   10000,
		}, {
			Name:      "medium_wait",
			Insert:    100000,
			Inserters: 1000,
			WaitFor:   100000,
		}, {
			Name:       "medium_wait_4",
			Insert:     100000,
			Inserters:  1000,
			WaitFor:    100000,
			ReadShards: 4,
		}, {
			Name:      "large_wait",
			Insert:    1000000,
			Inserters: 1000,
			WaitFor:   1000000,
		}, {
			Name:       "large_wait_4",
			Insert:     1000000,
			Inserters:  1000,
			WaitFor:    1000000,
			ReadShards: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			s := setup(t)
			ctx := context.Background()

			var inserted int64
			insert := func(pk, cv, v int) {
				exec(t, s, z, "INSERT INTO test.t (pk,ck,v) values (?,?,?);", pk, cv, v)
				atomic.AddInt64(&inserted, 1)
			}

			mark := func() func(string, int64) {
				t0 := time.Now()
				return func(msg string, count int64) {
					duration := time.Since(t0)
					rate := float64(count) / duration.Seconds()
					fmt.Printf("%s, count=%v, duration=%v, rate=%.2f\n", msg, count, duration, rate)
				}
			}

			// Insert one row to get a cursor
			insert(0, 0, 0)

			cur, err := GetCursor(ctx, s, keyspace, table, time.Time{}, WithConsistency(gocql.One))
			jtest.RequireNil(t, err)

			// Insert data: Max 10000 partitions, all unique rows
			log := mark()
			for i := 0; i < test.Inserters; i++ {
				go func(i int) {
					for j := 0; j < test.Insert/test.Inserters; j++ {
						insert(i*j%10000, i*j, j*j)
					}
				}(i)
			}

			for atomic.LoadInt64(&inserted) < test.WaitFor {
				time.Sleep(time.Millisecond * 10)
			}
			log("Starting read", atomic.LoadInt64(&inserted))

			n := test.ReadShards
			if n == 0 {
				n = 1
			}

			var (
				wg   sync.WaitGroup
				read int64
			)
			wg.Add(n)
			log = mark()
			ctx, cancel := context.WithCancel(ctx)
			for m := 0; m < n; m++ {
				go func(m int) {
					stream := NewStream(s, keyspace, table, WithConsistency(gocql.One), WithShard(m, n))
					sc, err := stream.Stream(ctx, cur, reflex.WithStreamLag(time.Second))
					jtest.RequireNil(t, err)

					// Read all the rows.
					log := mark()
					var c int64
					for {
						_, err := sc.Recv()
						if errors.Is(err, context.Canceled) {
							break
						}
						jtest.RequireNil(t, err)
						atomic.AddInt64(&read, 1)
						c++
					}
					log("Ended read "+fmt.Sprint(m), c)
					wg.Done()
				}(m)
			}

			for atomic.LoadInt64(&read) < int64(test.Insert) {
				time.Sleep(time.Millisecond * 100)
			}
			log("Ended read total", int64(test.Insert))
			cancel()
			wg.Wait()
		})
	}
}

func exec(t *testing.T, session *gocql.Session, ts time.Time, q string, args ...interface{}) {
	t.Helper()
	if ts == z {
		ts = time.Now()
	}
	err := session.Query(q, args...).WithTimestamp(ts.UnixNano() / 1000).Consistency(gocql.One).Exec()
	jtest.RequireNil(t, err)
}

func get(t *testing.T, session *gocql.Session, q string, args ...interface{}) map[string]interface{} {
	t.Helper()
	res := make(map[string]interface{})
	err := session.Query(q, args...).Consistency(gocql.One).MapScan(res)
	jtest.RequireNil(t, err)
	return res
}

func list(t *testing.T, session *gocql.Session, q string, args ...interface{}) []map[string]interface{} {
	t.Helper()
	var res []map[string]interface{}
	iter := session.Query(q, args...).Consistency(gocql.One).Iter()
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		res = append(res, row)
	}

	jtest.RequireNil(t, iter.Close())
	return res
}
