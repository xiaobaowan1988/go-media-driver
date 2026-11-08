package driver

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/protocol"
)

// Phase 4: the whole data path must run with 0 heap allocations per duty
// cycle, including the loss-recovery paths (NAK, retransmit, SM, heartbeat).
func TestHotPathZeroAlloc(t *testing.T) {
	r := newRigOpts(t, Idle{BusySpin}, false)
	// Aggressive timers so every control path fires inside the measurement.
	r.pub.t = Timing{HeartbeatInterval: time.Microsecond, SMInterval: time.Microsecond, NakInterval: time.Microsecond, RetransmitLinger: time.Nanosecond}
	r.sub.t = r.pub.t

	var claim logbuffer.BufferClaim
	scratch := make([]byte, 4096)
	handler := func(b []byte, _ *protocol.DataHeader) {}
	var pos int64
	i := 0
	cycle := func() {
		i++
		for k := 0; k < 3 && i%4 != 0; k++ { // every 4th cycle is idle -> heartbeat
			if _, err := r.pub.lb.TryClaim(100+k, &claim); err == nil {
				claim.Payload[0] = byte(i)
				claim.Commit()
			}
		}
		r.sender.DoWork()
		if i%5 == 0 {
			r.rx.Recv(scratch) // steal a datagram: forces gap -> NAK -> retransmit
		}
		r.recv.DoWork()
		var n int
		pos, n = r.sub.lb.Poll(pos, handler, 64)
		_ = n
		r.sub.lb.SetConsumerPosition(pos)
		r.sub.lb.CleanConsumed(pos)
		r.pub.lb.CleanConsumed(r.pub.ConsumerPosition())
	}
	for w := 0; w < 5000; w++ { // warm up: fill sockets, rotate terms, learn peer
		cycle()
	}
	if allocs := testing.AllocsPerRun(20000, cycle); allocs != 0 {
		t.Fatalf("hot path allocates: %v allocs/cycle", allocs)
	}
	if r.pub.Naks.Load() == 0 || r.pub.Retransmitted.Load() == 0 || r.pub.Heartbeats.Load() == 0 || r.sub.SMs.Load() == 0 {
		t.Fatalf("control paths not exercised: naks=%d retrans=%d hb=%d sm=%d",
			r.pub.Naks.Load(), r.pub.Retransmitted.Load(), r.pub.Heartbeats.Load(), r.sub.SMs.Load())
	}
	if r.pub.lb.Meta().ActiveTermCount.V < 3 {
		t.Fatal("term rotation not exercised")
	}
}

func TestPinThread(t *testing.T) {
	allowed := make(chan []int, 1)
	go func() {
		cpus, _ := ThreadAffinity()
		allowed <- cpus
	}()
	cpus := <-allowed
	if len(cpus) == 0 {
		t.Skip("cannot read affinity")
	}
	target := cpus[len(cpus)-1]
	res := make(chan string, 1)
	go func() { // never unlocks: the runtime retires this thread on exit
		if err := PinThread(target); err != nil {
			res <- err.Error()
			return
		}
		got, _ := ThreadAffinity()
		if len(got) != 1 || got[0] != target {
			res <- "affinity not applied: " + strconv.Itoa(len(got)) + " cpus"
			return
		}
		for i := 0; i < 1e6; i++ { // actually running on that CPU?
			if cur := currentCPU(); cur != target {
				res <- "running on cpu " + strconv.Itoa(cur)
				return
			}
			if i > 1000 {
				break
			}
		}
		res <- ""
	}()
	if msg := <-res; msg != "" {
		t.Fatal(msg)
	}
	if err := PinThread(100000); err == nil {
		// PinThread locked this test goroutine's thread too; that is harmless
		// but an out-of-range CPU must be rejected.
		t.Fatal("out-of-range CPU accepted")
	}
}

// currentCPU parses the "processor" field of /proc/thread-self/stat.
func currentCPU() int {
	b, err := os.ReadFile("/proc/thread-self/stat")
	if err != nil {
		return -1
	}
	s := string(b)
	f := strings.Fields(s[strings.LastIndexByte(s, ')')+2:])
	// after the comm field: state is f[0] (field 3); processor is field 39 -> f[36]
	if len(f) < 37 {
		return -1
	}
	n, _ := strconv.Atoi(f[36])
	return n
}
