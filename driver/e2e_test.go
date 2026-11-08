package driver

import (
	"encoding/binary"
	"sync"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/protocol"
	"github.com/xiaobaowan1988/go-media-driver/udp"
)

const (
	termLen = 256 * 1024
	window  = termLen / 2
)

type rig struct {
	pub    *NetworkPublication
	sub    *NetworkSubscription
	sender *Sender
	recv   *Receiver
	stop   atomic.Bool
	done   chan struct{}
	prod   sync.WaitGroup // producer goroutines; cleanup waits before unmapping
}

func newRig(t testing.TB, idle Idle) *rig {
	t.Helper()
	dir := t.TempDir()
	cfg := logbuffer.Config{TermLength: termLen, InitialTermID: 100, MTU: 1408, SessionID: 9, StreamID: 3}
	outLB, err := logbuffer.Create(filepath.Join(dir, "out.log"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	inLB, err := logbuffer.Create(filepath.Join(dir, "in.log"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	rxSock, err := udp.NewSocket(1)
	if err != nil {
		t.Fatal(err)
	}
	rxSock.SetBuffers(0, 4<<20)
	if err := rxSock.Bind([4]byte{127, 0, 0, 1}, 0); err != nil {
		t.Fatal(err)
	}
	port, _ := rxSock.LocalPort()
	txSock, err := udp.NewSocket(MaxBatch)
	if err != nil {
		t.Fatal(err)
	}
	txSock.SetBuffers(4<<20, 0)
	if err := txSock.Connect([4]byte{127, 0, 0, 1}, port); err != nil {
		t.Fatal(err)
	}
	r := &rig{done: make(chan struct{})}
	r.pub = NewNetworkPublication(outLB, txSock)
	r.sub = NewNetworkSubscription(inLB)
	r.sender = NewSender(idle)
	r.sender.AddPublication(r.pub)
	r.recv = NewReceiver(rxSock, idle)
	r.recv.AddSubscription(r.sub)
	outLB.SetPublisherLimit(window) // until a status message exists (phase 3)
	go r.sender.Run(&r.stop)
	go func() { r.recv.Run(&r.stop); close(r.done) }()
	t.Cleanup(func() {
		r.stop.Store(true)
		<-r.done
		r.prod.Wait()
		time.Sleep(10 * time.Millisecond)
		txSock.Close()
		rxSock.Close()
		outLB.Close()
		inLB.Close()
	})
	return r
}

// Phase-2 stand-in for flow control: the consumer advances the publisher
// limit and cleans both logs. Phase 3 replaces this with SM frames.
func (r *rig) consume(t testing.TB, total int, check func(i int, b []byte)) {
	var pos int64
	got := 0
	deadline := time.Now().Add(30 * time.Second)
	for got < total {
		var n int
		pos, n = r.sub.lb.Poll(pos, func(b []byte, _ *protocol.DataHeader) {
			check(got, b)
			got++
		}, 1024)
		r.sub.lb.SetConsumerPosition(pos)
		r.sub.lb.CleanConsumed(pos)
		r.pub.lb.CleanConsumed(r.pub.SenderPosition())
		r.pub.lb.SetPublisherLimit(pos + window)
		if n == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("timeout: got %d/%d (recv datagrams=%d dropped=%d)", got, total, r.recv.Datagrams.Load(), r.sub.Dropped.Load())
			}
			time.Sleep(time.Microsecond)
		}
	}
}

func TestSendReceiveInOrder(t *testing.T) {
	const total = 100000
	r := newRig(t, Idle{Yield})
	r.prod.Add(1)
	go func() {
		defer r.prod.Done()
		var c logbuffer.BufferClaim
		for i := 0; i < total && !r.stop.Load(); {
			size := 9 + (i*37)%900
			if _, err := r.pub.lb.TryClaim(size, &c); err != nil {
				time.Sleep(time.Microsecond)
				continue
			}
			binary.LittleEndian.PutUint64(c.Payload, uint64(i))
			c.Payload[size-1] = byte(i)
			c.Commit()
			i++
		}
	}()
	r.consume(t, total, func(i int, b []byte) {
		if v := binary.LittleEndian.Uint64(b); v != uint64(i) {
			t.Fatalf("msg %d: got seq %#x len=%d want len=%d active=%d sub=%d", i, v, len(b), 8+(i*37)%900, r.sub.lb.Meta().ActiveTermCount.V, r.sub.Frames.Load())
		}
		if want := 9 + (i*37)%900; len(b) != want || b[len(b)-1] != byte(i) {
			t.Fatalf("msg %d: size %d want %d", i, len(b), want)
		}
	})
	if r.pub.lb.Meta().ActiveTermCount.V < 3 {
		t.Fatal("test should cross several terms (padding frames included)")
	}
}

// M2 benchmark over loopback (single host). 64B messages.
func BenchmarkLoopbackSendReceive(b *testing.B) {
	r := newRig(b, Idle{Yield})
	var c logbuffer.BufferClaim
	done := make(chan struct{})
	go func() {
		r.consume(b, b.N, func(int, []byte) {})
		close(done)
	}()
	b.ResetTimer()
	for i := 0; i < b.N; {
		if _, err := r.pub.lb.TryClaim(32, &c); err != nil {
			continue
		}
		binary.LittleEndian.PutUint64(c.Payload, uint64(i))
		c.Commit()
		i++
	}
	<-done
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}
