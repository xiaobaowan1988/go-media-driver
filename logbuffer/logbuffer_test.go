package logbuffer

import (
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/xiaobaowan1988/go-media-driver/protocol"
)

func newLB(t testing.TB, termLength, mtu int) *LogBuffer {
	t.Helper()
	lb, err := Create(filepath.Join(t.TempDir(), "log.dat"), Config{
		TermLength: termLength, InitialTermID: 7, MTU: mtu, SessionID: 1, StreamID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lb.Close() })
	return lb
}

func TestLayout(t *testing.T) {
	var m Metadata
	for name, off := range map[string]uintptr{
		"Tail[1]": unsafe.Offsetof(m.Tail)+64, "ActiveTermCount": unsafe.Offsetof(m.ActiveTermCount),
		"PublisherLimit": unsafe.Offsetof(m.PublisherLimit), "ConsumerPosition": unsafe.Offsetof(m.ConsumerPosition),
		"State[1]": unsafe.Offsetof(m.State)+64,
	} {
		if off%64 != 0 {
			t.Errorf("%s at %d not cache-line aligned", name, off)
		}
	}
	if unsafe.Sizeof(paddedInt64{}) != 64 || unsafe.Sizeof(protocol.DataHeader{}) != 32 {
		t.Fatal("bad struct sizes")
	}
}

func TestClaimCommitPoll(t *testing.T) {
	lb := newLB(t, MinTermLength, 1408)
	var c BufferClaim
	pos, err := lb.TryClaim(5, &c)
	if err != nil || pos != 64 {
		t.Fatalf("pos=%d err=%v", pos, err)
	}
	copy(c.Payload, "hello")
	got := 0
	h := func([]byte, *protocol.DataHeader) { got++ }
	if _, n := lb.Poll(0, h, 10); n != 0 {
		t.Fatal("uncommitted frame visible")
	}
	c.Commit()
	var payload string
	p, n := lb.Poll(0, func(b []byte, hd *protocol.DataHeader) {
		payload = string(b)
		if hd.SessionID != 1 || hd.StreamID != 2 || hd.TermID != 7 {
			t.Errorf("bad header %+v", *hd)
		}
	}, 10)
	if n != 1 || p != 64 || payload != "hello" {
		t.Fatalf("n=%d p=%d payload=%q", n, p, payload)
	}
	if _, err := lb.TryClaim(2000, &c); err != ErrMaxMessageLength {
		t.Fatalf("err=%v", err)
	}
}

func TestAbortBecomesPadding(t *testing.T) {
	lb := newLB(t, MinTermLength, 1408)
	var c BufferClaim
	lb.TryClaim(10, &c)
	c.Abort()
	lb.Offer([]byte("x"))
	var got []string
	lb.Poll(0, func(b []byte, _ *protocol.DataHeader) { got = append(got, string(b)) }, 10)
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("got %v", got)
	}
}

func TestBackPressure(t *testing.T) {
	lb := newLB(t, MinTermLength, 1408)
	lb.SetPublisherLimit(128)
	for i := 0; i < 2; i++ {
		if _, err := lb.Offer(make([]byte, 20)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := lb.Offer(make([]byte, 20)); err != ErrBackPressured {
		t.Fatalf("err=%v", err)
	}
	lb.SetPublisherLimit(192)
	if _, err := lb.Offer(make([]byte, 20)); err != nil {
		t.Fatal(err)
	}
}

// Fill several terms single-threaded: exercises padding, rotation and cleaning.
func TestRotationAndCleaning(t *testing.T) {
	lb := newLB(t, MinTermLength, 1408)
	payload := make([]byte, 1000)
	var pos, sent, recvd int64
	next := uint64(0)
	for sent < 20000 {
		binary.LittleEndian.PutUint64(payload, uint64(sent))
		_, err := lb.Offer(payload)
		switch err {
		case nil:
			sent++
		case ErrAdminAction:
			// Rotation blocked until the consumer cleans: drain + clean.
			var n int
			pos, n = lb.Poll(pos, func(b []byte, _ *protocol.DataHeader) {
				if v := binary.LittleEndian.Uint64(b); v != next {
					t.Fatalf("got %d want %d", v, next)
				}
				next++
			}, 1<<30)
			recvd += int64(n)
			lb.CleanConsumed(pos)
		default:
			t.Fatal(err)
		}
	}
	if lb.Meta().ActiveTermCount.V < 5 {
		t.Fatalf("expected several rotations, got %d", lb.Meta().ActiveTermCount.V)
	}
}

func TestRotationBlocksUntilClean(t *testing.T) {
	lb := newLB(t, MinTermLength, 1408)
	for i := 0; i < 1000; i++ { // ~180 frames fill all three terms
		if _, err := lb.Offer(make([]byte, 1000)); err != nil && err != ErrAdminAction {
			t.Fatal(err)
		}
	}
	if c := lb.Meta().ActiveTermCount.V; c != 2 {
		t.Fatalf("active count %d, want rotation blocked at 2", c)
	}
	if _, err := lb.Offer(make([]byte, 10)); err != ErrAdminAction {
		t.Fatalf("err=%v, want ErrAdminAction while next term is dirty", err)
	}
	pos, _ := lb.Poll(0, func([]byte, *protocol.DataHeader) {}, 1<<30)
	if n := lb.CleanConsumed(pos); n != 2 {
		t.Fatalf("cleaned %d terms, want 2 (terms 0 and 1 fully consumed)", n)
	}
	for i := 0; i < 3; i++ { // first retry rotates, then it succeeds
		if _, err := lb.Offer(make([]byte, 10)); err == nil {
			return
		}
	}
	t.Fatal("still blocked after cleaning")
}

func TestMultiProducer(t *testing.T) {
	const producers, perProducer = 4, 50000
	lb := newLB(t, 1<<20, 1408)
	var done atomic.Int32
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			var c BufferClaim
			for i := 0; i < perProducer; {
				_, err := lb.TryClaim(16, &c)
				if err != nil {
					continue // ErrAdminAction / back pressure: spin
				}
				binary.LittleEndian.PutUint64(c.Payload, uint64(p))
				binary.LittleEndian.PutUint64(c.Payload[8:], uint64(i))
				c.Commit()
				i++
			}
			done.Add(1)
		}(p)
	}
	next := make([]uint64, producers)
	var pos, total int64
	for total < producers*perProducer {
		var n int
		pos, n = lb.Poll(pos, func(b []byte, _ *protocol.DataHeader) {
			p, i := binary.LittleEndian.Uint64(b), binary.LittleEndian.Uint64(b[8:])
			if next[p] != i {
				t.Fatalf("producer %d: got %d want %d", p, i, next[p])
			}
			next[p]++
		}, 1<<20)
		total += int64(n)
		lb.SetConsumerPosition(pos)
		lb.CleanConsumed(pos)
	}
	wg.Wait()
}

// Cross-process: this test process is the "Raft" publisher; a child process
// plays the media driver and consumes from the same shared file.
func TestCrossProcess(t *testing.T) {
	const total = 200000
	lb := newLB(t, 1<<20, 1408)
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperDriver$")
	cmd.Env = append(os.Environ(), "GMD_HELPER=1", "GMD_LOG="+lb.Path(), "GMD_TOTAL="+strconv.Itoa(total))
	out := &syncBuf{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var c BufferClaim
	for i := 0; i < total; {
		if _, err := lb.TryClaim(8, &c); err != nil {
			continue
		}
		binary.LittleEndian.PutUint64(c.Payload, uint64(i))
		c.Commit()
		i++
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("driver process failed: %v\n%s", err, out.String())
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  []byte
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); s.b = append(s.b, p...); s.mu.Unlock(); return len(p), nil }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return string(s.b) }

func TestHelperDriver(t *testing.T) {
	if os.Getenv("GMD_HELPER") != "1" {
		t.Skip("helper process only")
	}
	total, _ := strconv.Atoi(os.Getenv("GMD_TOTAL"))
	var lb *LogBuffer
	var err error
	for lb, err = Open(os.Getenv("GMD_LOG"), false); err != nil; lb, err = Open(os.Getenv("GMD_LOG"), false) {
	}
	defer lb.Close()
	var pos int64
	next := uint64(0)
	for next < uint64(total) {
		var n int
		pos, n = lb.Poll(pos, func(b []byte, _ *protocol.DataHeader) {
			if v := binary.LittleEndian.Uint64(b); v != next {
				t.Fatalf("got %d want %d", v, next)
			}
			next++
		}, 1<<20)
		if n > 0 {
			lb.SetConsumerPosition(pos)
			lb.CleanConsumed(pos)
		}
	}
}

// M1 benchmark: producer goroutine -> consumer goroutine over the log buffer.
func BenchmarkTryClaimCommit(b *testing.B) {
	lb := newLB(b, 16<<20, 1408)
	var stop atomic.Bool
	var consumed atomic.Int64
	go func() {
		var pos int64
		for !stop.Load() {
			var n int
			pos, n = lb.Poll(pos, func([]byte, *protocol.DataHeader) {}, 1<<16)
			if n > 0 {
				consumed.Add(int64(n))
				lb.SetConsumerPosition(pos)
				lb.CleanConsumed(pos)
			}
		}
	}()
	var c BufferClaim
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; {
		if _, err := lb.TryClaim(32, &c); err != nil {
			continue
		}
		binary.LittleEndian.PutUint64(c.Payload, uint64(i))
		c.Commit()
		i++
	}
	for consumed.Load() < int64(b.N) {
	}
	b.StopTimer()
	stop.Store(true)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}
