package logbuffer

import (
	"sync/atomic"
	"unsafe"

	"github.com/xiaobaowan1988/go-media-driver/protocol"
)

// FragmentHandler receives one committed DATA frame payload. The slice
// aliases the shared log and is only valid during the call.
type FragmentHandler func(payload []byte, h *protocol.DataHeader)

func lenPtr(term []byte, off int64) *int32 { return (*int32)(unsafe.Pointer(&term[off])) }

// Poll reads up to limit committed frames starting at position (which must
// be a frame boundary) and returns the new position. A frame is visible only
// if its length is > 0 and its header term id matches the expected term, so
// stale bytes of a not-yet-cleaned slot are never delivered.
func (lb *LogBuffer) Poll(position int64, handler FragmentHandler, limit int) (int64, int) {
	n := 0
	for n < limit {
		count := position / lb.termLength
		off := position % lb.termLength
		term := lb.terms[count%3]
		flen := atomic.LoadInt32(lenPtr(term, off))
		if flen <= 0 || int64(flen) > lb.termLength-off {
			break
		}
		h := hdr(term, off)
		if h.TermID != lb.initialID+int32(count) {
			break
		}
		if h.Type == protocol.TypeData {
			handler(term[off+protocol.HeaderLength:off+int64(flen)], h)
			n++
		}
		position += int64(protocol.AlignedLength(int(flen)))
	}
	return position, n
}
