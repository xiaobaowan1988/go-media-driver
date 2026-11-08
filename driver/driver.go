package driver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/udp"
)

const (
	// CommandFile is the client -> conductor command log inside Config.Dir.
	CommandFile = "to-driver.ipc"

	commandTermLength = 64 * 1024
	commandMTU        = 1024
)

// Config configures a media driver instance. Dir is owned by the driver:
// stale pub-*/sub-*/ipc files in it are removed at start-up.
type Config struct {
	Dir         string
	ReceiveIP   [4]byte
	ReceivePort int // 0 = ephemeral, see Driver.ReceivePort
	Timing      Timing
	Idle        Idle // for sender and receiver
	SocketBuf   int  // SO_SNDBUF/SO_RCVBUF, default 4 MiB

	// PinThreads pins the sender and receiver threads to SenderCPU and
	// ReceiverCPU (use isolated cores). Without it the threads are still
	// locked to their OS threads but may migrate between cores.
	PinThreads             bool
	SenderCPU, ReceiverCPU int
}

// Driver owns the three agents: sender, receiver and conductor.
type Driver struct {
	cfg      Config
	sender   *Sender
	receiver *Receiver
	rxSock   *udp.Socket
	cond     *Conductor

	stop atomic.Bool
	wg   sync.WaitGroup
}

// Start creates the command log and launches sender, receiver and conductor.
func Start(cfg Config) (*Driver, error) {
	if cfg.Dir == "" {
		return nil, errors.New("driver: Dir required")
	}
	if cfg.Timing == (Timing{}) {
		cfg.Timing = DefaultTiming()
	}
	if cfg.SocketBuf == 0 {
		cfg.SocketBuf = 4 << 20
	}
	if err := os.MkdirAll(cfg.Dir, 0o770); err != nil {
		return nil, err
	}
	for _, pat := range []string{"pub-*.log", "sub-*.log", "*.ipc"} {
		old, _ := filepath.Glob(filepath.Join(cfg.Dir, pat))
		for _, f := range old {
			os.Remove(f)
		}
	}
	cmd, err := logbuffer.Create(filepath.Join(cfg.Dir, CommandFile), logbuffer.Config{
		TermLength: commandTermLength, MTU: commandMTU, Prefault: true,
	})
	if err != nil {
		return nil, err
	}
	rx, err := udp.NewSocket(1)
	if err != nil {
		cmd.Close()
		return nil, err
	}
	if err := rx.SetBuffers(cfg.SocketBuf, cfg.SocketBuf); err != nil {
		rx.Close()
		cmd.Close()
		return nil, err
	}
	if err := rx.Bind(cfg.ReceiveIP, cfg.ReceivePort); err != nil {
		rx.Close()
		cmd.Close()
		return nil, fmt.Errorf("driver: bind: %w", err)
	}
	d := &Driver{cfg: cfg, rxSock: rx, sender: NewSender(cfg.Idle), receiver: NewReceiver(rx, cfg.Idle)}
	d.cond = newConductor(d, cmd)
	sCPU, rCPU := -1, -1
	if cfg.PinThreads {
		sCPU, rCPU = cfg.SenderCPU, cfg.ReceiverCPU
	}
	pinErr := make(chan error, 2)
	d.wg.Add(3)
	go func() {
		defer d.wg.Done()
		err := PinThread(sCPU)
		pinErr <- err
		if err == nil {
			d.sender.Run(&d.stop)
		}
	}()
	go func() {
		defer d.wg.Done()
		err := PinThread(rCPU)
		pinErr <- err
		if err == nil {
			d.receiver.Run(&d.stop)
		}
	}()
	go func() { defer d.wg.Done(); d.cond.Run(&d.stop) }()
	for i := 0; i < 2; i++ {
		if err := <-pinErr; err != nil {
			d.Close()
			return nil, err
		}
	}
	return d, nil
}

// ReceivePort is the UDP port subscriptions are served on.
func (d *Driver) ReceivePort() int {
	p, _ := d.rxSock.LocalPort()
	return p
}

// Dir returns the driver directory clients connect to.
func (d *Driver) Dir() string { return d.cfg.Dir }

// Close stops all agents and releases every resource.
func (d *Driver) Close() error {
	d.stop.Store(true)
	d.wg.Wait()
	d.cond.closeAll()
	d.rxSock.Close()
	return nil
}

// Stats returns a snapshot of publication/subscription counters by registration id.
func (d *Driver) Stats() (pubs map[int64]*NetworkPublication, subs map[int64]*NetworkSubscription) {
	return d.cond.snapshot()
}
