// Package logbuffer implements the Aeron-style term log buffer used for IPC
// between a Raft process and the media driver: three term buffers plus a
// metadata block, living in one shared memory file.
package logbuffer

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/xiaobaowan1988/go-media-driver/protocol"
	"github.com/xiaobaowan1988/go-media-driver/shm"
)

// Sentinel errors are pre-allocated so the hot path never allocates.
var (
	ErrBackPressured    = errors.New("logbuffer: back pressured")
	ErrAdminAction      = errors.New("logbuffer: admin action (term rotation), retry")
	ErrMaxMessageLength = errors.New("logbuffer: message exceeds MTU-header")
)

// LogBuffer is a view over a mapped log file. All methods are safe for
// concurrent use from several threads and processes unless noted.
type LogBuffer struct {
	m          *shm.Mapping
	terms      [3][]byte
	meta       *Metadata
	termLength int64
	initialID  int32
	maxPayload int
}

// Config describes a new log buffer.
type Config struct {
	TermLength    int
	InitialTermID int32
	MTU           int
	SessionID     int32
	StreamID      int32
	Prefault      bool
}

// FileLength is the size of the backing file for a term length.
func FileLength(termLength int) int { return 3*termLength + MetadataLength }

func validTermLength(n int) bool {
	return n >= MinTermLength && n <= MaxTermLength && n&(n-1) == 0
}

// Create creates the backing file and initialises the metadata.
func Create(path string, c Config) (*LogBuffer, error) {
	if !validTermLength(c.TermLength) {
		return nil, fmt.Errorf("logbuffer: term length %d must be a power of two in [%d,%d]", c.TermLength, MinTermLength, MaxTermLength)
	}
	if c.MTU < 2*protocol.HeaderLength || c.MTU > c.TermLength/8 {
		return nil, fmt.Errorf("logbuffer: invalid mtu %d", c.MTU)
	}
	m, err := shm.Create(path, FileLength(c.TermLength), c.Prefault)
	if err != nil {
		return nil, err
	}
	lb := bind(m, c.TermLength)
	md := lb.meta
	md.Version = layoutVersion
	md.InitialTermID = c.InitialTermID
	md.TermLength = int32(c.TermLength)
	md.MTU = int32(c.MTU)
	md.SessionID = c.SessionID
	md.StreamID = c.StreamID
	md.Tail[0].V = pack(c.InitialTermID, 0)
	md.State[0].V = stateActive
	md.State[1].V = stateClean
	md.State[2].V = stateClean
	md.PublisherLimit.V = math.MaxInt64
	atomic.StoreInt32(&md.Magic, magic) // publish: readers check Magic first
	lb.initialID = c.InitialTermID
	lb.maxPayload = c.MTU - protocol.HeaderLength
	return lb, nil
}

// Open maps an existing log file created by another process.
func Open(path string, prefault bool) (*LogBuffer, error) {
	m, err := shm.Open(path, prefault)
	if err != nil {
		return nil, err
	}
	b := m.Bytes()
	if len(b) < MetadataLength+3*MinTermLength {
		m.Close()
		return nil, errors.New("logbuffer: file too small")
	}
	md := (*Metadata)(unsafe.Pointer(&b[len(b)-MetadataLength]))
	if atomic.LoadInt32(&md.Magic) != magic || md.Version != layoutVersion {
		m.Close()
		return nil, errors.New("logbuffer: bad magic/version")
	}
	termLength := int(md.TermLength)
	if !validTermLength(termLength) || len(b) != FileLength(termLength) {
		m.Close()
		return nil, errors.New("logbuffer: corrupt metadata")
	}
	lb := bind(m, termLength)
	lb.initialID = md.InitialTermID
	lb.maxPayload = int(md.MTU) - protocol.HeaderLength
	return lb, nil
}

func bind(m *shm.Mapping, termLength int) *LogBuffer {
	b := m.Bytes()
	lb := &LogBuffer{m: m, termLength: int64(termLength)}
	for i := range lb.terms {
		lb.terms[i] = b[i*termLength : (i+1)*termLength : (i+1)*termLength]
	}
	lb.meta = (*Metadata)(unsafe.Pointer(&b[3*termLength]))
	return lb
}

func (lb *LogBuffer) Close() error { return lb.m.Close() }
func (lb *LogBuffer) Path() string { return lb.m.Path() }

// Meta exposes the shared control block.
func (lb *LogBuffer) Meta() *Metadata { return lb.meta }

func (lb *LogBuffer) TermLength() int      { return int(lb.termLength) }
func (lb *LogBuffer) InitialTermID() int32 { return lb.initialID }
func (lb *LogBuffer) MTU() int             { return int(lb.meta.MTU) }
func (lb *LogBuffer) MaxPayload() int      { return lb.maxPayload }
func (lb *LogBuffer) SessionID() int32     { return lb.meta.SessionID }
func (lb *LogBuffer) StreamID() int32      { return lb.meta.StreamID }

func pack(termID int32, off int64) int64 {
	return int64(uint64(uint32(termID))<<32 | uint64(off))
}

func unpack(raw int64) (termID int32, off int64) {
	return int32(raw >> 32), raw & 0xFFFFFFFF
}

// Position of a (termCount, offset) pair; absolute, grows monotonically.
func (lb *LogBuffer) position(count, off int64) int64 { return count*lb.termLength + off }

func hdr(term []byte, off int64) *protocol.DataHeader {
	return (*protocol.DataHeader)(unsafe.Pointer(&term[off]))
}

// Tail returns the current publish position (next byte to be claimed).
func (lb *LogBuffer) Tail() int64 {
	for {
		count := atomic.LoadInt64(&lb.meta.ActiveTermCount.V)
		raw := atomic.LoadInt64(&lb.meta.Tail[count%3].V)
		termID, off := unpack(raw)
		if termID != lb.initialID+int32(count) {
			continue // rotation in progress
		}
		return lb.position(count, off)
	}
}

// BufferClaim is a claimed, not yet visible, region of the log.
type BufferClaim struct {
	Payload  []byte
	lenPtr   *int32
	frameLen int32
	term     []byte
	off      int64
}

// Commit makes the frame visible to readers (release store).
func (c *BufferClaim) Commit() { atomic.StoreInt32(c.lenPtr, c.frameLen) }

// Abort turns the claim into a padding frame and publishes it.
func (c *BufferClaim) Abort() {
	h := hdr(c.term, c.off)
	h.Type = protocol.TypePad
	atomic.StoreInt32(c.lenPtr, c.frameLen)
}

// TryClaim atomically reserves space for a message of `length` payload bytes.
// On success it returns the position after the frame and fills claim; the
// caller writes claim.Payload and calls Commit. Lock-free, zero allocation.
func (lb *LogBuffer) TryClaim(length int, c *BufferClaim) (int64, error) {
	if length < 0 || length > lb.maxPayload {
		return 0, ErrMaxMessageLength
	}
	aligned := int64(protocol.AlignedLength(length + protocol.HeaderLength))
	m := lb.meta
	for {
		count := atomic.LoadInt64(&m.ActiveTermCount.V)
		idx := count % 3
		tailp := &m.Tail[idx].V
		raw := atomic.LoadInt64(tailp)
		termID, off := unpack(raw)
		if termID != lb.initialID+int32(count) {
			return 0, ErrAdminAction // another thread is rotating
		}
		if lb.position(count, off)+aligned > atomic.LoadInt64(&m.PublisherLimit.V) {
			return 0, ErrBackPressured
		}
		term := lb.terms[idx]
		if off+aligned > lb.termLength {
			// Does not fit: pad the rest of the term, then rotate.
			if off < lb.termLength && atomic.CompareAndSwapInt64(tailp, raw, pack(termID, lb.termLength)) {
				lb.writePad(term, off, lb.termLength-off, termID)
			}
			lb.rotate(count, termID)
			return 0, ErrAdminAction
		}
		if !atomic.CompareAndSwapInt64(tailp, raw, raw+aligned) {
			continue
		}
		h := hdr(term, off)
		h.Version = 0
		h.Flags = protocol.FlagUnfragmented
		h.Type = protocol.TypeData
		h.TermOffset = int32(off)
		h.SessionID = m.SessionID
		h.StreamID = m.StreamID
		h.TermID = termID
		c.frameLen = int32(length + protocol.HeaderLength)
		c.lenPtr = &h.FrameLength
		c.term, c.off = term, off
		c.Payload = term[off+protocol.HeaderLength : off+int64(c.frameLen) : off+int64(c.frameLen)]
		return lb.position(count, off+aligned), nil
	}
}

// Offer copies payload into the log. Convenience over TryClaim+Commit.
func (lb *LogBuffer) Offer(payload []byte) (int64, error) {
	var c BufferClaim
	pos, err := lb.TryClaim(len(payload), &c)
	if err != nil {
		return 0, err
	}
	copy(c.Payload, payload)
	c.Commit()
	return pos, nil
}

func (lb *LogBuffer) writePad(term []byte, off, length int64, termID int32) {
	h := hdr(term, off)
	h.Version = 0
	h.Flags = protocol.FlagUnfragmented
	h.Type = protocol.TypePad
	h.TermOffset = int32(off)
	h.SessionID = lb.meta.SessionID
	h.StreamID = lb.meta.StreamID
	h.TermID = termID
	atomic.StoreInt32(&h.FrameLength, int32(length))
}

// rotate activates the next slot. It only succeeds when that slot has been
// cleaned; otherwise publishers keep seeing ErrAdminAction until the cleaner
// (conductor / consumer) catches up. Exactly one caller wins the state CAS.
func (lb *LogBuffer) rotate(count int64, termID int32) {
	m := lb.meta
	n := (count + 1) % 3
	if !atomic.CompareAndSwapInt64(&m.State[n].V, stateClean, stateActive) {
		return
	}
	atomic.StoreInt64(&m.Tail[n].V, pack(termID+1, 0))
	atomic.StoreInt64(&m.ActiveTermCount.V, count+1)
	atomic.StoreInt64(&m.State[count%3].V, stateDirty)
}

// CleanConsumed zeroes dirty terms that lie entirely below minPosition (the
// slowest consumer). Single caller (conductor). Returns terms cleaned.
func (lb *LogBuffer) CleanConsumed(minPosition int64) int {
	cleaned := 0
	for i := range lb.terms {
		st := &lb.meta.State[i].V
		if atomic.LoadInt64(st) != stateDirty {
			continue
		}
		termID, _ := unpack(atomic.LoadInt64(&lb.meta.Tail[i].V))
		end := (int64(termID-lb.initialID) + 1) * lb.termLength
		if end > minPosition {
			continue
		}
		clear(lb.terms[i])
		atomic.StoreInt64(st, stateClean)
		cleaned++
	}
	return cleaned
}

// SetPublisherLimit / PublisherLimit: back-pressure bound shared with publishers.
func (lb *LogBuffer) SetPublisherLimit(p int64) { atomic.StoreInt64(&lb.meta.PublisherLimit.V, p) }
func (lb *LogBuffer) PublisherLimit() int64     { return atomic.LoadInt64(&lb.meta.PublisherLimit.V) }

// SetConsumerPosition / ConsumerPosition: the subscriber's read position.
func (lb *LogBuffer) SetConsumerPosition(p int64) { atomic.StoreInt64(&lb.meta.ConsumerPosition.V, p) }
func (lb *LogBuffer) ConsumerPosition() int64     { return atomic.LoadInt64(&lb.meta.ConsumerPosition.V) }
