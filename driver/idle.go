package driver

import (
	"runtime"
	"time"
)

// IdleKind selects what an agent does when a duty cycle found no work.
type IdleKind uint8

const (
	// BusySpin never yields: 100% of one core, lowest latency (default).
	BusySpin IdleKind = iota
	// Yield calls runtime.Gosched; friendlier on machines with few cores.
	Yield
	// Sleep parks for ~50µs when idle.
	Sleep
)

// Idle is a value-type idle strategy (no interface, no allocation).
type Idle struct{ Kind IdleKind }

// Do is called after every duty cycle with the amount of work done.
func (i Idle) Do(work int) {
	if work > 0 {
		return
	}
	switch i.Kind {
	case Yield:
		runtime.Gosched()
	case Sleep:
		time.Sleep(50 * time.Microsecond)
	}
}
