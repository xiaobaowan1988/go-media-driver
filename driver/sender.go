package driver

import (
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/protocol"
	"github.com/xiaobaowan1988/go-media-driver/udp"
)

// MaxBatch is the maximum number of datagrams handed to one sendmmsg call.
const MaxBatch = 16

const (
	maxRetransmits = 8
	controlBatch   = 8
)

type retransmit struct {
	start, pos int64
	end        int64
	lingerTill int64 // unix ns; slot is reusable afterwards
	active     bool
}

// NetworkPublication is the sender-side state of one publication: the
// outbound log buffer, the UDP socket connected to the peer, flow control
// and retransmit state. Only the sender thread touches the plain fields.
type NetworkPublication struct {
	lb   *logbuffer.LogBuffer
	sock *udp.Socket
	t    Timing

	senderPos   atomic.Int64
	consumerPos atomic.Int64
	adv         [MaxBatch]int
	ctl         [64]byte
	hb          [protocol.HeaderLength]byte
	lastSendNs  int64
	retx        [maxRetransmits]retransmit

	Datagrams     atomic.Int64
	Retransmitted atomic.Int64
	Naks          atomic.Int64
	SMs           atomic.Int64
	Heartbeats    atomic.Int64
	SendErrs      atomic.Int64
}

// NewNetworkPublication wraps lb and a socket already connected to the
// receiver. initialWindow bounds the publisher until the first SM arrives.
func NewNetworkPublication(lb *logbuffer.LogBuffer, sock *udp.Socket, t Timing, initialWindow int) *NetworkPublication {
	lb.SetPublisherLimit(int64(initialWindow))
	return &NetworkPublication{lb: lb, sock: sock, t: t, lastSendNs: time.Now().UnixNano()}
}

func (p *NetworkPublication) LogBuffer() *logbuffer.LogBuffer { return p.lb }
func (p *NetworkPublication) SenderPosition() int64           { return p.senderPos.Load() }

// ConsumerPosition is the receiver's position as last reported by an SM.
func (p *NetworkPublication) ConsumerPosition() int64 { return p.consumerPos.Load() }

func (p *NetworkPublication) pos(termID, off int32) int64 {
	return int64(termID-p.lb.InitialTermID())*int64(p.lb.TermLength()) + int64(off)
}

// pollControl drains SM and NAK frames from the (connected) socket.
func (p *NetworkPublication) pollControl(now int64) int {
	work := 0
	for i := 0; i < controlBatch; i++ {
		n, _ := p.sock.Recv(p.ctl[:])
		if n == 0 {
			break
		}
		work++
		if n < protocol.HeaderLength {
			continue
		}
		switch *(*uint16)(unsafe.Pointer(&p.ctl[6])) {
		case protocol.TypeSM:
			p.onSM((*protocol.StatusMessage)(unsafe.Pointer(&p.ctl[0])))
		case protocol.TypeNak:
			p.onNak((*protocol.NakHeader)(unsafe.Pointer(&p.ctl[0])), now)
		}
	}
	return work
}

func (p *NetworkPublication) onSM(m *protocol.StatusMessage) {
	if m.SessionID != p.lb.SessionID() || m.StreamID != p.lb.StreamID() || m.ReceiverWindow <= 0 {
		return
	}
	cp := p.pos(m.ConsumerTermID, m.ConsumerTermOffset)
	if cp < p.consumerPos.Load() || cp > p.senderPos.Load() {
		return // stale or bogus
	}
	p.SMs.Add(1)
	p.consumerPos.Store(cp)
	p.lb.SetPublisherLimit(cp + int64(m.ReceiverWindow)) // back pressure bound
}

func (p *NetworkPublication) onNak(m *protocol.NakHeader, now int64) {
	if m.SessionID != p.lb.SessionID() || m.StreamID != p.lb.StreamID() || m.Length <= 0 || m.TermOffset%protocol.FrameAlignment != 0 {
		return
	}
	start := p.pos(m.TermID, m.TermOffset)
	end := start + int64(m.Length)
	if sp := p.senderPos.Load(); end > sp {
		end = sp
	}
	if start < p.consumerPos.Load() || start >= end {
		return // already consumed (possibly cleaned) or never sent
	}
	p.Naks.Add(1)
	free := -1
	for i := range p.retx {
		r := &p.retx[i]
		if r.active || now < r.lingerTill {
			if r.start == start || (r.active && r.pos <= start && start < r.end) {
				return // suppress: same hole already queued / just resent
			}
			continue
		}
		if free < 0 {
			free = i
		}
	}
	if free >= 0 {
		p.retx[free] = retransmit{start: start, pos: start, end: end, active: true}
	}
}

// retransmitOne resends one batch of the first pending retransmission,
// reading straight from the log buffer.
func (p *NetworkPublication) retransmitOne(now int64) int {
	for i := range p.retx {
		r := &p.retx[i]
		if !r.active {
			continue
		}
		mtu := p.lb.MTU()
		p.sock.Reset()
		n, scan := 0, r.pos
		for n < MaxBatch && scan < r.end {
			block, adv := p.lb.ScanBlock(scan, mtu)
			if adv == 0 {
				break
			}
			p.sock.Add(block)
			p.adv[n] = adv
			scan += int64(adv)
			n++
		}
		if n == 0 {
			r.active, r.lingerTill = false, now // data unavailable; receiver will NAK again
			return 0
		}
		sent, err := p.sock.Flush()
		if err != nil {
			p.SendErrs.Add(1)
		}
		for j := 0; j < sent; j++ {
			r.pos += int64(p.adv[j])
		}
		p.Retransmitted.Add(int64(sent))
		if r.pos >= r.end {
			r.active, r.lingerTill = false, now+int64(p.t.RetransmitLinger)
		}
		return sent
	}
	return 0
}

// send scans the log from the sender position, mounts up to MaxBatch
// datagram-sized blocks directly on the iovec array and transmits them with
// one syscall. Returns datagrams sent.
func (p *NetworkPublication) send() int {
	pos := p.senderPos.Load()
	mtu := p.lb.MTU()
	p.sock.Reset()
	n := 0
	scan := pos
	for n < MaxBatch {
		block, adv := p.lb.ScanBlock(scan, mtu)
		if adv == 0 {
			break
		}
		p.sock.Add(block)
		p.adv[n] = adv
		scan += int64(adv)
		n++
	}
	if n == 0 {
		return 0
	}
	sent, err := p.sock.Flush()
	if err != nil {
		p.SendErrs.Add(1)
	}
	for i := 0; i < sent; i++ {
		pos += int64(p.adv[i])
	}
	p.senderPos.Store(pos)
	p.Datagrams.Add(int64(sent))
	return sent
}

// heartbeat sends a header-only DATA frame (FrameLength 0) carrying the
// sender position so that a receiver can detect tail loss.
func (p *NetworkPublication) heartbeat(now int64) {
	sp := p.senderPos.Load()
	tl := int64(p.lb.TermLength())
	h := (*protocol.DataHeader)(unsafe.Pointer(&p.hb[0]))
	*h = protocol.DataHeader{
		Flags:      protocol.FlagUnfragmented,
		Type:       protocol.TypeData,
		TermOffset: int32(sp % tl),
		SessionID:  p.lb.SessionID(),
		StreamID:   p.lb.StreamID(),
		TermID:     p.lb.InitialTermID() + int32(sp/tl),
	}
	if ok, _ := p.sock.Send(p.hb[:]); ok {
		p.Heartbeats.Add(1)
		p.lastSendNs = now
	}
}

func (p *NetworkPublication) doWork(now int64) int {
	work := p.pollControl(now)
	work += p.retransmitOne(now)
	if n := p.send(); n > 0 {
		work += n
		p.lastSendNs = now
	} else if now-p.lastSendNs >= int64(p.t.HeartbeatInterval) {
		p.heartbeat(now)
	}
	return work
}

// Sender is the data-plane thread that transmits publications.
type Sender struct {
	pubs  atomic.Pointer[[]*NetworkPublication]
	Idle  Idle
	epoch atomic.Int64 // bumped every duty cycle; lets the conductor wait out removals
}

func NewSender(idle Idle) *Sender {
	s := &Sender{Idle: idle}
	empty := []*NetworkPublication{}
	s.pubs.Store(&empty)
	return s
}

// AddPublication publishes a copy-on-write snapshot so DoWork needs no lock.
func (s *Sender) AddPublication(p *NetworkPublication) {
	for {
		old := s.pubs.Load()
		next := make([]*NetworkPublication, len(*old)+1)
		copy(next, *old)
		next[len(*old)] = p
		if s.pubs.CompareAndSwap(old, &next) {
			return
		}
	}
}

// RemovePublication drops p from the snapshot; the caller must wait for
// Epoch to advance twice before closing p's resources.
func (s *Sender) RemovePublication(p *NetworkPublication) {
	for {
		old := s.pubs.Load()
		next := make([]*NetworkPublication, 0, len(*old))
		for _, q := range *old {
			if q != p {
				next = append(next, q)
			}
		}
		if s.pubs.CompareAndSwap(old, &next) {
			return
		}
	}
}

func (s *Sender) Epoch() int64 { return s.epoch.Load() }

// DoWork is one duty cycle; returns the amount of work done.
func (s *Sender) DoWork() int {
	now := time.Now().UnixNano()
	work := 0
	for _, p := range *s.pubs.Load() {
		work += p.doWork(now)
	}
	s.epoch.Add(1)
	return work
}

// Run spins until stop is set.
func (s *Sender) Run(stop *atomic.Bool) {
	for !stop.Load() {
		s.Idle.Do(s.DoWork())
	}
}
