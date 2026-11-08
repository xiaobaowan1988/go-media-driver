package driver

import (
	"sync/atomic"
	"unsafe"

	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/protocol"
	"github.com/xiaobaowan1988/go-media-driver/udp"
)

// NetworkSubscription is the receiver-side state of one stream: the inbound
// log buffer that datagrams are rebuilt into.
type NetworkSubscription struct {
	lb *logbuffer.LogBuffer

	Frames  atomic.Int64
	Dropped atomic.Int64
}

func NewNetworkSubscription(lb *logbuffer.LogBuffer) *NetworkSubscription {
	return &NetworkSubscription{lb: lb}
}

func (s *NetworkSubscription) LogBuffer() *logbuffer.LogBuffer { return s.lb }

// onDatagram parses the frames in one datagram and copies each into the log.
func (s *NetworkSubscription) onDatagram(b []byte) {
	for off := 0; off+protocol.HeaderLength <= len(b); {
		h := (*protocol.DataHeader)(unsafe.Pointer(&b[off]))
		if h.Type != protocol.TypeData && h.Type != protocol.TypePad {
			return
		}
		flen := int(h.FrameLength)
		if flen < protocol.HeaderLength || (h.Type == protocol.TypeData && off+flen > len(b)) {
			s.Dropped.Add(1)
			return
		}
		if err := s.lb.Insert(b[off:]); err != nil {
			s.Dropped.Add(1)
		} else {
			s.Frames.Add(1)
		}
		if h.Type == protocol.TypePad {
			return // padding is always alone in its datagram
		}
		off += protocol.AlignedLength(flen)
	}
}

// Receiver is the data-plane thread that pulls datagrams off one socket and
// dispatches them to subscriptions by (session, stream).
type Receiver struct {
	sock *udp.Socket
	subs atomic.Pointer[[]*NetworkSubscription]
	buf  []byte
	Idle Idle

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

// DoWork drains up to recvBatch datagrams.
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
				s.onDatagram(b)
				matched = true
				break
			}
		}
		if !matched {
			r.Unknown.Add(1)
		}
	}
	return work
}

// Run spins until stop is set.
func (r *Receiver) Run(stop *atomic.Bool) {
	for !stop.Load() {
		r.Idle.Do(r.DoWork())
	}
}
