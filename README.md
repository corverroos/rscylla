# rscylla

A [reflex](https://github.com/luno/reflex) stream adapter for a [scylladb](https://docs.scylladb.com/using-scylla/cdc/) CDC log table. 
It provides at-least-once semantics*.

### ScyllaDB CDC Overview
ScyllaDB CDC log tables are implemented as normal scylladb tables. 
A CDC log table contains the append-only log of all changes applied to the base table.

Like all NoSQL DBs of this type, the CDC log data is sharded by the partition 
key `StreamId` and sorted by the clustering key `Time` (and `BatchSeqNo`). 

The CDC log is therefore multiple streams each identified by a `StreamID`. 
Ordering is only guaranteed within such a stream. 

Note that when the scyllaDB cluster topology changes by a node joining or leaving, 
a new set of streams are created. This is called "stream generations". 
Each generation has its own streams and `StreamID`s. Generations are sequential in time.

In Kafka terms a CDC log table is a topic and the streams are partitions.

### Mapping to Reflex
Reflex was designed for single ordered streams, like mysql event tables or Firehose S3 buckets. 
A reflex `EventID` is normally a point in that stream and can therefore be used as a cursor.

Mapping multiple streams to a single reflex stream is therefore a little different.

rsylla basically sliced the CDC streams into fixed time windows that are streamed one window at a time.
The start of the time window is used as the cursor, not the events themselves. 
Once all events in the window have been streamed, the cursor is updated to the next 
window and the cycle begins again.

A CDC stream `EventID` consists of two parts:
 - A pointer to the row. The rows primary key.
 - A cursor to the start of the time window. The generation and timestamp.

```
// eventID is json encoded as the reflex.EventID.
// It points both to the CDC log row itself and the
// time window it was streamed in.
type eventID struct {
	// Cursor of streamed window
	CursorGen  time.Time `json:"cursor_gen"`
	CursorTime time.Time `json:"cursor_time"`

	// CDC log row primary key
	StreamID   string `json:"stream_id"`
	StreamTime string `json:"stream_time"`
	StreamSeq  int `json:"stream_seq"`
}
```

### Characteristics
 - **When starting a streaming from a previous `EventID`, duplicates 
   are to be expected** since the cursor points to the start of the time window, not to the 
   specific event in the time window. #at-least-once #idempotent
 - **The ordering of events in the window is not gauranteed**, but the ordering of individual 
   streams is preserved.
 - **A lag must be specified via the `reflex.StreamLag` option**. Since a time window is 
   queried as a whole, it should only be streamed once it is complete; elapsed with 
   all events present. 
 - **Lagging by less than 30s is considered unsafe and doesn't provide at-least-once semantics**.
   Since scyllaDB is a distributed system, time doesn't have a single source of truth. 
   The timestamp of a CDC log row can be defined by different nodes in the system each 
   with a different clock. The events in a time window is therefore eventually consistent.
 - **CQL defined timestamps, `USING TIMESTAMP 123`, doesn't provide at-least-once semantics**. 
   Client defined timestamps can add events to arbitrary time windows. If the time window has
   already been streamed, that event is missed.
 - **Combine an unsafe 1s lag fast stream with a safe +1min sweep stream** for logic that requires 
   quick responses to events. The sweeper will pick up any missed events. #at-least-once #idempotent.    
 - **Use `GetCursor` instead of a zero cursor when streaming from the start of a stream**. 
   A zero cursor `""` is supported but starts at the first generation timestamp of the cluster, the cluster create timestamp.
   Streaming from this point might result in an initial delay if first events
   are long that.
   
### TODO
 - Add support for `m-of-n` sharded reflex streams for improved performance. `rscylla.WithShard(m,n int)`
 - Add support for base row data in the `Metadata` field depending on `type`.
 - Add support for easier lookup of base table entry from `ForeignID`.
 - Add support for business logic defined `Type` fields.
    








