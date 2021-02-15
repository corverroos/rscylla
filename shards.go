package rscylla

import (
	"context"
	"encoding/binary"
	"sort"
	"time"

	"github.com/gocql/gocql"
)

// These functions were copied from github.com/scylladb/scylla-cdc-go.

// shardStreams returns the stream IDs grouped by virtual node index.
func shardStreams(streamIDs []streamID) [][]streamID {
	vnodes := make(map[int64][]streamID)
	for _, stream := range streamIDs {
		idx := getVnodeIndex(stream)
		vnodes[idx] = append(vnodes[idx], stream)
	}

	var shards [][]streamID

	// Idx -1 means that we don't know the vnode for given stream,
	// therefore we will put those streams into a separate shard each.
	for _, stream := range vnodes[-1] {
		shards = append(shards, []streamID{stream})
	}
	delete(vnodes, -1)

	// Result order should be consistent.
	var idxl []int64
	for idx := range vnodes {
		idxl = append(idxl, idx)
	}
	sort.Slice(idxl, func(i, j int) bool {
		return idxl[i] < idxl[j]
	})

	for _, idx := range idxl {
		shards = append(shards, vnodes[idx])
	}

	return shards
}

// getVnodeIndex returns the vnode index fom given stream ID.
// It returns -1 if the stream ID format is unrecognized.
func getVnodeIndex(streamID streamID) int64 {
	if len(streamID) != 16 {
		// Don't know how to handle other sizes
		return -1
	}

	lowerQword := binary.BigEndian.Uint64(streamID[8:16])
	version := lowerQword & (1<<4 - 1)
	if version != 1 {
		// Unrecognized version
		return -1
	}

	vnodeIdx := (lowerQword >> 4) & (1<<22 - 1)
	return int64(vnodeIdx)
}

// getShards returns a subset of shards of the generation. Shards are streamIDs grouped by virtual node index.
// If n == 0, all shards returned.
func getShards(ctx context.Context, session *gocql.Session, consistency gocql.Consistency, gen time.Time, m, n int) ([][]streamID, error) {
	var streams []streamID
	err := session.Query("SELECT streams FROM "+generationsTableName+" WHERE time = ?", gen).
		WithContext(ctx).Consistency(consistency).Scan(&streams)
	if err != nil {
		return nil, err
	}

	return sliceShards(shardStreams(streams), m, n), err
}
