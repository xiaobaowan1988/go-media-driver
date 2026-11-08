package client_test

import (
	"encoding/binary"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xiaobaowan1988/go-media-driver/client"
	"github.com/xiaobaowan1988/go-media-driver/driver"
	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/protocol"
)

var lo = [4]byte{127, 0, 0, 1}

// lossyProxy sits between sender and receiver drivers and, in both
// directions, drops / duplicates / reorders datagrams.
type lossyProxy struct {
	front, back        *net.UDPConn // front faces the sender, back faces the receiver
	drop, dup, reorder float64
	mu                 sync.Mutex
	senderAddr         *net.UDPAddr
	rng                *rand.Rand
	Dropped, Passed    atomic.Int64
	wg                 sync.WaitGroup
}

func newProxy(t testing.TB, recvPort int, drop, dup, reorder float64) *lossyProxy {
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	back, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: recvPort})
	if err != nil {
		t.Fatal(err)
	}
	front.SetReadBuffer(4 << 20)
	back.SetReadBuffer(4 << 20)
	p := &lossyProxy{front: front, back: back, drop: drop, dup: dup, reorder: reorder, rng: rand.New(rand.NewSource(1))}
	p.wg.Add(2)
	go p.pump(front, func(b []byte) { back.Write(b) }, true)
	go p.pump(back, func(b []byte) {
		p.mu.Lock()
		a := p.senderAddr
		p.mu.Unlock()
		if a != nil {
			front.WriteToUDP(b, a)
		}
	}, false)
	t.Cleanup(func() { front.Close(); back.Close(); p.wg.Wait() })
	return p
}

func (p *lossyProxy) port() int { return p.front.LocalAddr().(*net.UDPAddr).Port }

func (p *lossyProxy) roll() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rng.Float64()
}

func (p *lossyProxy) pump(c *net.UDPConn, out func([]byte), learn bool) {
	defer p.wg.Done()
	buf := make([]byte, 65536)
	var held []byte
	for {
		n, from, err := c.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if learn {
			p.mu.Lock()
			p.senderAddr = from
			p.mu.Unlock()
		}
		pkt := append([]byte(nil), buf[:n]...)
		if p.roll() < p.drop {
			p.Dropped.Add(1)
			continue
		}
		p.Passed.Add(1)
		if held == nil && p.roll() < p.reorder {
			held = pkt // deliver after the next datagram
			continue
		}
		out(pkt)
		if held != nil {
			out(held)
			held = nil
		}
		if p.roll() < p.dup {
			out(pkt)
		}
	}
}

type pair struct {
	tx, rx     *driver.Driver
	txc, rxc   *client.Client
	pub        *client.Publication
	sub        *client.Subscription
	proxy      *lossyProxy
	txPubStats func() *driver.NetworkPublication
	rxSubStats func() *driver.NetworkSubscription
}

const (
	termLen = 256 * 1024
	window  = termLen / 2
)

func setup(t *testing.T, drop, dup, reorder float64) *pair {
	t.Helper()
	timing := driver.DefaultTiming()
	timing.NakInterval = time.Millisecond
	timing.SMInterval = time.Millisecond
	timing.HeartbeatInterval = 2 * time.Millisecond
	idle := driver.Idle{Kind: driver.Yield}
	rx, err := driver.Start(driver.Config{Dir: t.TempDir(), ReceiveIP: lo, Timing: timing, Idle: idle})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := driver.Start(driver.Config{Dir: t.TempDir(), ReceiveIP: lo, Timing: timing, Idle: idle})
	if err != nil {
		t.Fatal(err)
	}
	p := &pair{tx: tx, rx: rx}
	p.proxy = newProxy(t, rx.ReceivePort(), drop, dup, reorder)
	p.rxc, err = client.Connect(rx.Dir(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p.txc, err = client.Connect(tx.Dir(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p.sub, err = p.rxc.AddSubscription(client.SubscriptionConfig{SessionID: 1, StreamID: 10, InitialTermID: 5, TermLength: termLen, Window: window})
	if err != nil {
		t.Fatal(err)
	}
	p.pub, err = p.txc.AddPublication(client.PublicationConfig{SessionID: 1, StreamID: 10, InitialTermID: 5,
		RemoteIP: lo, RemotePort: p.proxy.port(), TermLength: termLen, InitialWindow: window})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.pub.Close()
		p.sub.Close()
		p.txc.Close()
		p.rxc.Close()
		tx.Close()
		rx.Close()
	})
	return p
}

func (p *pair) stats() (*driver.NetworkPublication, *driver.NetworkSubscription) {
	ps, _ := p.tx.Stats()
	_, ss := p.rx.Stats()
	var np *driver.NetworkPublication
	var ns *driver.NetworkSubscription
	for _, v := range ps {
		np = v
	}
	for _, v := range ss {
		ns = v
	}
	return np, ns
}

// run publishes `total` numbered messages and checks strict in-order delivery.
func (p *pair) run(t *testing.T, total int) {
	t.Helper()
	var prod sync.WaitGroup
	var failed atomic.Bool
	prod.Add(1)
	go func() {
		defer prod.Done()
		var c logbuffer.BufferClaim
		for i := 0; i < total && !failed.Load(); {
			size := 16 + (i*53)%700
			if _, err := p.pub.TryClaim(size, &c); err != nil {
				runtime.Gosched()
				continue
			}
			binary.LittleEndian.PutUint64(c.Payload, uint64(i))
			binary.LittleEndian.PutUint32(c.Payload[8:], uint32(size))
			c.Commit()
			i++
		}
	}()
	defer prod.Wait()
	got := 0
	deadline := time.Now().Add(60 * time.Second)
	for got < total {
		n := p.sub.Poll(func(b []byte, _ *protocol.DataHeader) {
			seq := binary.LittleEndian.Uint64(b)
			if seq != uint64(got) || len(b) != int(binary.LittleEndian.Uint32(b[8:])) {
				failed.Store(true)
				t.Errorf("out of order / corrupt: got seq %d len %d, want seq %d", seq, len(b), got)
				got = total
				return
			}
			got++
		}, 256)
		if failed.Load() {
			return
		}
		if n == 0 {
			if time.Now().After(deadline) {
				failed.Store(true)
				np, ns := p.stats()
				t.Fatalf("timeout at %d/%d: naks=%d retrans=%d dropped=%d complete=%d", got, total, np.Naks.Load(), np.Retransmitted.Load(), ns.Dropped.Load(), ns.CompletePosition())
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
}

func TestDriverClientNoLoss(t *testing.T) {
	p := setup(t, 0, 0, 0)
	p.run(t, 50000)
}

// M3: with datagram loss, duplication and reordering injected in both
// directions the receiver must still assemble the log strictly in order,
// and memory must stay bounded (fixed-size mmap'd logs, no heap growth).
func TestLossyStrictOrderAndBoundedMemory(t *testing.T) {
	p := setup(t, 0.05, 0.02, 0.05)
	p.run(t, 5000) // warm up
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	p.run(t, 60000)
	runtime.GC()
	runtime.ReadMemStats(&after)
	np, ns := p.stats()
	t.Logf("proxy dropped=%d passed=%d | naks=%d retransmitted=%d sms=%d | frames=%d dropped=%d",
		p.proxy.Dropped.Load(), p.proxy.Passed.Load(), np.Naks.Load(), np.Retransmitted.Load(), np.SMs.Load(), ns.Frames.Load(), ns.Dropped.Load())
	if p.proxy.Dropped.Load() == 0 || np.Retransmitted.Load() == 0 {
		t.Fatal("loss was not exercised")
	}
	if growth := int64(after.HeapAlloc) - int64(before.HeapAlloc); growth > 8<<20 {
		t.Fatalf("heap grew by %d bytes under loss", growth)
	}
	if lb := np.LogBuffer(); lb.Meta().ActiveTermCount.V < 8 {
		t.Fatalf("expected many term rotations, got %d", lb.Meta().ActiveTermCount.V)
	}
}

// Back pressure: when the receiver stops consuming, the SM window closes and
// TryClaim must start refusing instead of overrunning the receiver.
func TestBackPressureWhenReceiverStalls(t *testing.T) {
	p := setup(t, 0, 0, 0)
	var c logbuffer.BufferClaim
	var accepted int
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if _, err = p.pub.TryClaim(1000, &c); err == nil {
			c.Commit()
			accepted++
		} else if err == logbuffer.ErrBackPressured {
			break
		} else {
			runtime.Gosched()
		}
	}
	if err != logbuffer.ErrBackPressured {
		t.Fatalf("never back pressured (accepted %d), last err %v", accepted, err)
	}
	if max := window/1024 + 2; accepted > max {
		t.Fatalf("accepted %d messages, window allows at most ~%d", accepted, max)
	}
	// Consumer wakes up: everything accepted so far must arrive, then publishing resumes.
	got := 0
	for end := time.Now().Add(10 * time.Second); got < accepted && time.Now().Before(end); {
		if p.sub.Poll(func([]byte, *protocol.DataHeader) { got++ }, 256) == 0 {
			time.Sleep(50 * time.Microsecond)
		}
	}
	if got != accepted {
		t.Fatalf("delivered %d of %d", got, accepted)
	}
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); {
		p.sub.Poll(func([]byte, *protocol.DataHeader) {}, 256)
		if _, err := p.pub.TryClaim(1000, &c); err == nil {
			c.Commit()
			return
		}
		time.Sleep(50 * time.Microsecond)
	}
	t.Fatal("publisher did not resume after the consumer caught up")
}

func TestUnknownRegistrationAndDuplicateSubscription(t *testing.T) {
	p := setup(t, 0, 0, 0)
	if _, err := p.rxc.AddSubscription(client.SubscriptionConfig{SessionID: 1, StreamID: 10, TermLength: termLen}); err == nil {
		t.Fatal("duplicate subscription should be rejected")
	}
	if _, err := p.rxc.AddSubscription(client.SubscriptionConfig{SessionID: 2, StreamID: 10, TermLength: 1000}); err == nil {
		t.Fatal("bad term length should be rejected")
	}
}

// Sender and receiver threads pinned to their own cores (BusySpin where the
// machine has cores to spare), publication looped back to the same driver.
func TestPinnedBusySpinDriver(t *testing.T) {
	cpus, err := driver.ThreadAffinity()
	if err != nil || len(cpus) < 2 {
		t.Skip("need at least 2 allowed CPUs")
	}
	idle := driver.Idle{Kind: driver.Yield}
	if len(cpus) >= 4 {
		idle = driver.Idle{Kind: driver.BusySpin}
	}
	d, err := driver.Start(driver.Config{
		Dir: t.TempDir(), ReceiveIP: lo, Idle: idle,
		PinThreads: true, SenderCPU: cpus[len(cpus)-1], ReceiverCPU: cpus[len(cpus)-2],
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err := client.Connect(d.Dir(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sub, err := c.AddSubscription(client.SubscriptionConfig{SessionID: 3, StreamID: 4, TermLength: termLen, Window: window})
	if err != nil {
		t.Fatal(err)
	}
	pub, err := c.AddPublication(client.PublicationConfig{SessionID: 3, StreamID: 4, RemoteIP: lo,
		RemotePort: d.ReceivePort(), TermLength: termLen, InitialWindow: window})
	if err != nil {
		t.Fatal(err)
	}
	const total = 100000
	go func() {
		var cl logbuffer.BufferClaim
		for i := 0; i < total; {
			if _, err := pub.TryClaim(24, &cl); err != nil {
				runtime.Gosched()
				continue
			}
			binary.LittleEndian.PutUint64(cl.Payload, uint64(i))
			cl.Commit()
			i++
		}
	}()
	got := 0
	for end := time.Now().Add(30 * time.Second); got < total; {
		if sub.Poll(func(b []byte, _ *protocol.DataHeader) {
			if binary.LittleEndian.Uint64(b) != uint64(got) {
				t.Errorf("out of order at %d", got)
			}
			got++
		}, 256) == 0 && time.Now().After(end) {
			t.Fatalf("timeout at %d/%d", got, total)
		}
	}
	pub.Close()
	sub.Close()
	if err := func() error {
		bad, err := driver.Start(driver.Config{Dir: t.TempDir(), ReceiveIP: lo, PinThreads: true, SenderCPU: 100000})
		if bad != nil {
			bad.Close()
		}
		return err
	}(); err == nil {
		t.Fatal("Start must fail for an impossible CPU")
	}
}
