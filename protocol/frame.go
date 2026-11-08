// Package protocol defines the on-wire / in-log frame layout. It follows the
// Aeron data header (32 bytes, little endian, host byte order).
package protocol

const (
	HeaderLength   = 32
	FrameAlignment = 32

	TypePad  uint16 = 0
	TypeData uint16 = 1
	TypeNak  uint16 = 2
	TypeSM   uint16 = 3

	FlagBegin        uint8 = 0x80
	FlagEnd          uint8 = 0x40
	FlagUnfragmented       = FlagBegin | FlagEnd
)

// DataHeader is the header of DATA and PAD frames. FrameLength is written
// last, atomically, and is > 0 only once the frame is fully visible.
type DataHeader struct {
	FrameLength int32
	Version     uint8
	Flags       uint8
	Type        uint16
	TermOffset  int32
	SessionID   int32
	StreamID    int32
	TermID      int32
	Reserved    int64
}

// AlignedLength rounds n up to the frame alignment.
func AlignedLength(n int) int { return (n + FrameAlignment - 1) &^ (FrameAlignment - 1) }
