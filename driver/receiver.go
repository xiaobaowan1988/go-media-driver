package driver

import (
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/protocol"
	"github.com/xiaobaowan1988/go-media-driver/udp"
)

// NetworkSubscription is the receiver-side state of one stream: the inbound
// log buffer datagrams are rebuilt into, plus loss detection and status
// message state. Plain fields are touched by the receiver thread only.
type NetworkSubscription struct {
	lb     *logbuffer.LogBuffer
	t      Timing
	window int64

	peer     unix.RawSockaddrInet4
	havePeer bool

	hwm        int64 // highest position seen (data end or heartbeat)
	complete   int64 // end of the contiguous received prefix
	lastSMNs   int64
	lastSMPos  int64
	nakStart   int64
	lastNakNs  int64
	smBuf      [protocol.HeaderLength]byte
	nakBuf     [protocol.HeaderLength]byte
	registered bool

	Frames  atomic.Int64
	Dropped atomic.Int64
	Naks    atomic.Int64
	SMs     atomic.Int64
}

// NewNetworkSubscription wraps lb. window is the receiver window advertised
// to the sender; it must be <= termLength/2.
func NewNetworkSubscription(lb *logbuffer.LogBuffer, t Timing, window int) *NetworkSubscription {
	if window > lb.TermLength()/2 {
		window = lb.TermLength() / 2
	}
	return &NetworkSubscription{lb: lb, t: t, window: int64(window), nakStart: -1}
}

func (s *NetworkSubscription) LogBuffer() *logbuffer.LogBuffer { return s.lb }

// CompletePosition is the end of the contiguous prefix received so far.
func (s *NetworkSubscription) CompletePosition() int64 { return atomic.LoadInt64(&s.complete) }

func (s *NetworkSubscription) pos(termID, off int32) int64 {
	return int64(termID-s.lb.InitialTermID())*int64(s.lb.TermLength()) + int64(off)
}

// onDatagram parses the frames in one datagram and copies each into the log.
func (s *NetworkSubscription) onDatagram(b []byte) {
	for off := 0; off+protocol.HeaderLength <= len(b); {
		h := (*protocol.DataHeader)(unsafe.Pointer(&b[off]))
		if h.Type != protocol.TypeData && h.Type != protocol.TypePad {
			return
		}
		flen := int(h.FrameLength)
		if flen == 0 { // heartbeat: only advances the high-water mark
			if p := s.pos(h.TermID, h.TermOffset); p > s.hwm {
				s.hwm = p
			}
			return
		}
		if flen < protocol.HeaderLength || (h.Type == protocol.TypeData && off+flen > len(b)) {
			s.Dropped.Add(1)
			return
		}
		if err := s.lb.Insert(b[off:]); err != nil {
			s.Dropped.Add(1)
		} else {
			s.Frames.Add(1)
			if end := s.pos(h.TermID, h.TermOffset) + int64(protocol.AlignedLength(flen)); end > s.hwm {
				s.hwm = end
			}
			if h.Type == protocol.TypePad {
				return
			}
		}
		if h.Type == protocol.TypePad {
			return // padding is always alone in its datagram
		}
		off += protocol.AlignedLength(flen)
	}
}

// housekeeping advances the contiguous prefix, detects holes (NAK, with
// suppression) and emits status messages.
func (s *NetworkSubscription) housekeeping(sock *udp.Socket, now int64) int {
	if !s.havePeer {
		return 0
	}
	work := 0
	c := s.lb.ContiguousEnd(s.complete)
	if c != s.complete {
		atomic.StoreInt64(&s.complete, c)
		s.nakStart = -1
		work++
	}
	if s.hwm > c {
		// A hole starts at c. Immediate NAK the first time; repeat only after NakInterval.
		if s.nakStart != c || now-s.lastNakNs >= int64(s.t.NakInterval) {
			end := s.lb.GapEnd(c, s.hwm)
			tl := int64(s.lb.TermLength())
			nak := (*protocol.NakHeader)(unsafe.Pointer(&s.nakBuf[0]))
			*nak = protocol.NakHeader{
				FrameLength: protocol.HeaderLength, Type: protocol.TypeNak,
				SessionID: s.lb.SessionID(), StreamID: s.lb.StreamID(),
				TermID:     s.lb.InitialTermID() + int32(c/tl),
				TermOffset: int32(c % tl), Length: int32(end - c),
			}
			if ok, _ := sock.SendTo(s.nakBuf[:], &s.peer); ok {
				s.nakStart, s.lastNakNs = c, now
				s.Naks.Add(1)
				work++
			}
		}
	}
	cp := s.lb.ConsumerPosition()
	if now-s.lastSMNs >= int64(s.t.SMInterval) || cp-s.lastSMPos >= s.window/4 {
		tl := int64(s.lb.TermLength())
		sm := (*protocol.StatusMessage)(unsafe.Pointer(&s.smBuf[0]))
		*sm = protocol.StatusMessage{
			FrameLength: protocol.HeaderLength, Type: protocol.TypeSM,
			SessionID: s.lb.SessionID(), StreamID: s.lb.StreamID(),
			ConsumerTermID:     s.lb.InitialTermID() + int32(cp/tl),
			ConsumerTermOffset: int32(cp % tl), ReceiverWindow: int32(s.window),
		}
		if ok, _ := sock.SendTo(s.smBuf[:], &s.peer); ok {
			s.lastSMNs, s.lastSMPos = now, cp
			s.SMs.Add(1)
			work++
		}
	}
	return work
}

// Receiver is the data-plane thread that pulls datagrams off one socket,
// dispatches them to subscriptions by (session, stream) and sends back
// NAKs and status messages.
type Receiver struct {
	sock  *udp.Socket
	subs  atomic.Pointer[[]*NetworkSubscription]
	buf   []byte
	Idle  Idle
	epoch atomic.Int64

	Datagrams atomic.Int64
	Unknown   atomic.Int64
}

// recvBatch bounds datagrams drained per duty cycle.
const recvBatch = 16

func NewReceiver(sock *udp.Socket, idle Idle) *Receiver {
	r := &Receiver{sock: sock, buf: make([]byte, 64*1024), Idle: idle}
	empty := []*NetworkSubscription{}
	r.subs.Store(&empty)
	return r
}

func (r *Receiver) AddSubscription(s *NetworkSubscription) {
	for {
		old := r.subs.Load()
		next := make([]*NetworkSubscription, len(*old)+1)
		copy(next, *old)
		next[len(*old)] = s
		if r.subs.CompareAndSwap(old, &next) {
			return
		}
	}
}

func (r *Receiver) RemoveSubscription(s *NetworkSubscription) {
	for {
		old := r.subs.Load()
		next := make([]*NetworkSubscription, 0, len(*old))
		for _, q := range *old {
			if q != s {
				next = append(next, q)
			}
		}
		if r.subs.CompareAndSwap(old, &next) {
			return
		}
	}
}

func (r *Receiver) Epoch() int64 { return r.epoch.Load() }

// DoWork drains up to recvBatch datagrams, then runs per-stream housekeeping.
func (r *Receiver) DoWork() int {
	work := 0
	for i := 0; i < recvBatch; i++ {
		n, _ := r.sock.Recv(r.buf)
		if n == 0 {
			break
		}
		work++
		r.Datagrams.Add(1)
		if n < protocol.HeaderLength {
			continue
		}
		b := r.buf[:n]
		h := (*protocol.DataHeader)(unsafe.Pointer(&b[0]))
		matched := false
		for _, s := range *r.subs.Load() {
			if s.lb.SessionID() == h.SessionID && s.lb.StreamID() == h.StreamID {
				s.peer, s.havePeer = *r.sock.Src(), true
				s.onDatagram(b)
				matched = true
				break
			}
		}
		if !matched {
			r.Unknown.Add(1)
		}
	}
	now := time.Now().UnixNano()
	for _, s := range *r.subs.Load() {
		work += s.housekeeping(r.sock, now)
	}
	r.epoch.Add(1)
	return work
}

// Run spins until stop is set.
func (r *Receiver) Run(stop *atomic.Bool) {
	for !stop.Load() {
		r.Idle.Do(r.DoWork())
	}
}
