package rscylla

import (
	"context"
	"errors"
	"fmt"
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

func setup(t *testing.T) *gocql.Session {
	cluster := gocql.NewCluster("localhost")
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy("local-dc"))
	s, err := cluster.CreateSession()
	jtest.RequireNil(t, err)
	t.Cleanup(s.Close)

	exec(t, s, "drop keyspace if exists test")
	exec(t, s, "create keyspace test with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
	exec(t, s, "CREATE TABLE test.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};")

	return s
}

func TestStream(t *testing.T) {
	s := setup(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		for j := 0; j < 2; j++ {
			exec(t, s, "INSERT INTO test.t (pk,ck,v) values (?,?,?);", i+10, j+100, i*j)
		}
	}

	time.Sleep(time.Second)

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
			exec(t, s, "INSERT INTO test.t (pk,ck,v) values (?,?,?);", i, j, i*j)
		}
	}

	// TODO(corver): Use WITH TIMESTAMP above to avoid sleeping.
	time.Sleep(time.Second * 2)

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

func TestSmallLoad(t *testing.T) {
	s := setup(t)
	ctx := context.Background()

	insert := func(pk, cv, v int) {
		exec(t, s, "INSERT INTO test.t (pk,ck,v) values (?,?,?);", pk, cv, v)
	}

	// Insert one row to get a cursor
	insert(0, 0, 0)

	cur, err := GetCursor(ctx, s, keyspace, table, time.Time{}, WithConsistency(gocql.One))
	jtest.RequireNil(t, err)

	n := 10000
	w := 100
	for i := 0; i < w; i++ {
		go func() {
			for j := 0; j < n/w; j++ {
				insert(j/10, j, j*j)
			}
		}()
	}

	stream := NewStream(s, keyspace, table, WithConsistency(gocql.One))
	sc, err := stream.Stream(ctx, cur, reflex.WithStreamLag(time.Second))
	jtest.RequireNil(t, err)

	t0 := time.Now()
	for i := 0; i < n; i++ {
		_, err := sc.Recv()
		jtest.RequireNil(t, err)
	}
	fmt.Printf("delta=%v\n", time.Since(t0))
}

func exec(t *testing.T, session *gocql.Session, q string, args ...interface{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := session.Query(q, args...).WithContext(ctx).Exec()
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
