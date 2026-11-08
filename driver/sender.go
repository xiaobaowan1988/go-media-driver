package driver

import (
	"sync/atomic"

	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/udp"
)

// MaxBatch is the maximum number of datagrams handed to one sendmmsg call.
const MaxBatch = 16

// NetworkPublication is the sender-side state of one publication: the
// outbound log buffer, the UDP socket connected to the peer, and how far the
// log has been transmitted.
type NetworkPublication struct {
	lb        *logbuffer.LogBuffer
	sock      *udp.Socket
	senderPos atomic.Int64
	adv       [MaxBatch]int

	Datagrams atomic.Int64
	SendErrs  atomic.Int64
}

// NewNetworkPublication wraps lb and a socket already connected to the receiver.
func NewNetworkPublication(lb *logbuffer.LogBuffer, sock *udp.Socket) *NetworkPublication {
	return &NetworkPublication{lb: lb, sock: sock}
}

func (p *NetworkPublication) LogBuffer() *logbuffer.LogBuffer { return p.lb }
func (p *NetworkPublication) SenderPosition() int64           { return p.senderPos.Load() }

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

// Sender is the data-plane thread that transmits publications.
type Sender struct {
	pubs atomic.Pointer[[]*NetworkPublication]
	Idle Idle
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

// DoWork is one duty cycle; returns the amount of work done.
func (s *Sender) DoWork() int {
	work := 0
	for _, p := range *s.pubs.Load() {
		work += p.send()
	}
	return work
}

// Run spins until stop is set.
func (s *Sender) Run(stop *atomic.Bool) {
	for !stop.Load() {
		s.Idle.Do(s.DoWork())
	}
}
