package logbuffer

import "unsafe"

const (
	magic          = 0x4c4f4742 // "LOGB"
	layoutVersion  = 1
	MetadataLength = 4096 // metadata section size in the file
	cacheLine      = 64

	MinTermLength = 64 * 1024
	MaxTermLength = 1 << 30
)

// Term slot states. clean -> active -> dirty -> clean.
const (
	stateClean int64 = iota
	stateActive
	stateDirty
)

// paddedInt64 occupies a whole cache line so that counters updated by
// different threads/processes never share one (no false sharing).
type paddedInt64 struct {
	V int64
	_ [56]byte
}

// Metadata is the control block at the end of the log file.
type Metadata struct {
	Tail             [3]paddedInt64 // termID<<32 | termOffset of each slot
	ActiveTermCount  paddedInt64    // number of rotations so far
	PublisherLimit   paddedInt64    // max position a publisher may claim up to
	ConsumerPosition paddedInt64    // position of the (single) subscriber
	State            [3]paddedInt64 // slot state machine

	// Immutable after creation, one cache line.
	Magic         int32
	Version       int32
	InitialTermID int32
	TermLength    int32
	MTU           int32
	SessionID     int32
	StreamID      int32
	_             [36]byte
}

const metadataSize = int(unsafe.Sizeof(Metadata{}))

func init() {
	if metadataSize%cacheLine != 0 || metadataSize > MetadataLength {
		panic("logbuffer: bad metadata layout")
	}
}
