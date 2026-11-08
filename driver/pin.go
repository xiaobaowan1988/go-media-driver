package driver

import (
	"fmt"
	"runtime"

	"golang.org/x/sys/unix"
)

// PinThread locks the calling goroutine to its OS thread and, if cpu >= 0,
// restricts that thread to the given CPU core. It must be called at the
// entry of the goroutine that will run the spin loop. The goroutine must
// NOT call runtime.UnlockOSThread afterwards: when it exits still locked,
// the Go runtime terminates the thread, so the altered affinity never leaks
// back into the thread pool.
func PinThread(cpu int) error {
	runtime.LockOSThread()
	if cpu < 0 {
		return nil
	}
	var set unix.CPUSet
	set.Zero()
	set.Set(cpu)
	if err := unix.SchedSetaffinity(0, &set); err != nil {
		return fmt.Errorf("driver: sched_setaffinity(cpu %d): %w", cpu, err)
	}
	return nil
}

// ThreadAffinity returns the CPUs the calling thread may run on.
func ThreadAffinity() ([]int, error) {
	var set unix.CPUSet
	if err := unix.SchedGetaffinity(0, &set); err != nil {
		return nil, err
	}
	var cpus []int
	for i := 0; i < 1024; i++ {
		if set.IsSet(i) {
			cpus = append(cpus, i)
		}
	}
	return cpus, nil
}
