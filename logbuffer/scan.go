package logbuffer

import (
	"sync/atomic"
	"unsafe"

	"github.com/xiaobaowan1988/go-media-driver/protocol"
)

// ScanBlock returns, without copying, the longest run of committed frames
// starting at position that fits in maxLen bytes and does not cross the term
// end. `advance` is how far the position moves. A PAD frame is returned alone
// as just its 32-byte header (its FrameLength names the whole pad) with
// advance = the full pad length. (nil, 0) means nothing is available.
func (lb *LogBuffer) ScanBlock(position int64, maxLen int) (block []byte, advance int) {
	count := position / lb.termLength
	off := position % lb.termLength
	term := lb.terms[count%3]
	expect := lb.initialID + int32(count)
	total := int64(0)
	for off+total < lb.termLength {
		at := off + total
		flen := atomic.LoadInt32(lenPtr(term, at))
		if flen <= 0 || int64(flen) > lb.termLength-at {
			break
		}
		h := hdr(term, at)
		if h.TermID != expect {
			break
		}
		aligned := int64(protocol.AlignedLength(int(flen)))
		if h.Type == protocol.TypePad {
			if total == 0 {
				return term[at : at+protocol.HeaderLength : at+protocol.HeaderLength], int(aligned)
			}
			break
		}
		if total+aligned > int64(maxLen) {
			break
		}
		total += aligned
	}
	if total == 0 {
		return nil, 0
	}
	return term[off : off+total : off+total], int(total)
}

// Insert copies one received frame into the log at the position named by its
// header (receive-side rebuild). frame must start with a DataHeader; for DATA
// frames it holds FrameLength bytes, for PAD frames just the header. Only the
// receiver thread may call Insert. The frame length is stored last so a
// concurrent Poll never sees a partial frame.
func (lb *LogBuffer) Insert(frame []byte) error {
	if len(frame) < protocol.HeaderLength {
		return ErrBadFrame
	}
	h := (*protocol.DataHeader)(unsafe.Pointer(&frame[0]))
	flen, off := int64(h.FrameLength), int64(h.TermOffset)
	isPad := h.Type == protocol.TypePad
	count := int64(h.TermID - lb.initialID)
	if count < 0 || flen < protocol.HeaderLength || off < 0 || off%protocol.FrameAlignment != 0 ||
		off+flen > lb.termLength || (h.Type != protocol.TypeData && !isPad) ||
		(isPad && off+flen != lb.termLength) || (!isPad && int64(len(frame)) < flen) {
		return ErrBadFrame
	}
	active := atomic.LoadInt64(&lb.meta.ActiveTermCount.V)
	switch {
	case count == active:
	case count == active+1:
		lb.rotate(active, h.TermID-1)
		if atomic.LoadInt64(&lb.meta.ActiveTermCount.V) != count {
			return ErrFuture // next slot not cleaned yet
		}
	case count == active-1:
	case count < active:
		return ErrStale
	default:
		return ErrFuture
	}
	term := lb.terms[count%3]
	if atomic.LoadInt32(lenPtr(term, off)) != 0 {
		return nil // duplicate (retransmit of a frame we already have)
	}
	// Must come after the length check: a zeroed slot that was cleaned implies
	// consumption, hence ConsumerPosition >= end of that term > position.
	if lb.position(count, off) < atomic.LoadInt64(&lb.meta.ConsumerPosition.V) {
		return ErrStale
	}
	n := flen
	if isPad {
		n = protocol.HeaderLength
	}
	copy(term[off+4:off+n], frame[4:n])
	atomic.StoreInt32(lenPtr(term, off), int32(flen))
	return nil
}
