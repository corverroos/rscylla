package rscylla

import "time"

// OperationType corresponds to the cdc$operation column in CDC log, and
// describes the type of the operation given row represents.
//
// For a comprehensive explanation of what each operation type means,
// see Scylla documentation about CDC.
type OperationType int

func (o OperationType) ReflexType() int {
	return int(o)
}

const (
	PreImage                  OperationType = 0
	Update                    OperationType = 1
	Insert                    OperationType = 2
	RowDelete                 OperationType = 3
	PartitionDelete           OperationType = 4
	RangeDeleteStartInclusive OperationType = 5
	RangeDeleteStartExclusive OperationType = 6
	RangeDeleteEndInclusive   OperationType = 7
	RangeDeleteEndExclusive   OperationType = 8
	PostImage                 OperationType = 9
)

type streamID []byte

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
	StreamSeq  int    `json:"stream_seq"`
}
