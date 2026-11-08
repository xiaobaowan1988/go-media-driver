package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/protocol"
	"github.com/xiaobaowan1988/go-media-driver/udp"
)

type pubEntry struct {
	np   *NetworkPublication
	sock *udp.Socket
	path string
}

type subEntry struct {
	ns   *NetworkSubscription
	path string
}

type closing struct {
	senderEpoch, recvEpoch int64
	closeFn                func()
}

// Conductor is the control-plane agent: it executes client commands from the
// IPC command log and performs periodic housekeeping (cleaning consumed
// terms, releasing removed resources).
type Conductor struct {
	d       *Driver
	cmd     *logbuffer.LogBuffer
	cmdPos  int64
	handler logbuffer.FragmentHandler

	mu      sync.Mutex // guards the maps for Stats()/closeAll (never taken by data-plane threads)
	pubs    map[int64]*pubEntry
	subs    map[int64]*subEntry
	nextReg int64
	pending []closing
	lastHK  int64
}

func newConductor(d *Driver, cmd *logbuffer.LogBuffer) *Conductor {
	c := &Conductor{d: d, cmd: cmd, pubs: map[int64]*pubEntry{}, subs: map[int64]*subEntry{}, nextReg: 1}
	c.handler = c.onCommand
	return c
}

func (c *Conductor) snapshot() (map[int64]*NetworkPublication, map[int64]*NetworkSubscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, s := map[int64]*NetworkPublication{}, map[int64]*NetworkSubscription{}
	for k, v := range c.pubs {
		p[k] = v.np
	}
	for k, v := range c.subs {
		s[k] = v.ns
	}
	return p, s
}

// DoWork polls commands and runs housekeeping on its timer.
func (c *Conductor) DoWork() int {
	var work int
	var n int
	c.cmdPos, n = c.cmd.Poll(c.cmdPos, c.handler, 16)
	work += n
	now := time.Now().UnixNano()
	if now-c.lastHK >= int64(c.d.cfg.Timing.HousekeepingPeriod) {
		c.lastHK = now
		work += c.housekeeping()
	}
	return work
}

func (c *Conductor) Run(stop *atomic.Bool) {
	for !stop.Load() {
		if c.DoWork() == 0 {
			time.Sleep(50 * time.Microsecond)
		}
	}
}

func (c *Conductor) housekeeping() int {
	work := 0
	c.cmd.SetConsumerPosition(c.cmdPos)
	work += c.cmd.CleanConsumed(c.cmdPos)
	c.mu.Lock()
	for _, p := range c.pubs {
		work += p.np.lb.CleanConsumed(p.np.ConsumerPosition())
	}
	for _, s := range c.subs {
		work += s.ns.lb.CleanConsumed(s.ns.lb.ConsumerPosition())
	}
	c.mu.Unlock()
	if len(c.pending) > 0 {
		se, re := c.d.sender.Epoch(), c.d.receiver.Epoch()
		keep := c.pending[:0]
		for _, p := range c.pending {
			if se >= p.senderEpoch && re >= p.recvEpoch {
				p.closeFn()
				work++
			} else {
				keep = append(keep, p)
			}
		}
		c.pending = keep
	}
	return work
}

func (c *Conductor) respond(path string, r protocol.Response) {
	lb, err := logbuffer.Open(path, false)
	if err != nil {
		return // client gone
	}
	defer lb.Close()
	b := r.Marshal()
	for i := 0; i < 1000; i++ {
		if _, err := lb.Offer(b); err == nil {
			return
		}
		time.Sleep(10 * time.Microsecond)
	}
}

func (c *Conductor) fail(path string, corr int64, format string, a ...any) {
	c.respond(path, protocol.Response{Type: protocol.RspError, CorrelationID: corr, Text: fmt.Sprintf(format, a...)})
}

func (c *Conductor) onCommand(payload []byte, _ *protocol.DataHeader) {
	typ, _, err := protocol.Header(payload)
	if err != nil {
		return
	}
	switch typ {
	case protocol.CmdAddPublication:
		var m protocol.AddPublication
		if m.Unmarshal(payload) == nil {
			c.addPublication(&m)
		}
	case protocol.CmdAddSubscription:
		var m protocol.AddSubscription
		if m.Unmarshal(payload) == nil {
			c.addSubscription(&m)
		}
	case protocol.CmdRemove:
		var m protocol.Remove
		if m.Unmarshal(payload) == nil {
			c.remove(&m)
		}
	}
}

func (c *Conductor) addPublication(m *protocol.AddPublication) {
	reg := c.nextReg
	c.nextReg++
	path := filepath.Join(c.d.cfg.Dir, fmt.Sprintf("pub-%d.log", reg))
	lb, err := logbuffer.Create(path, logbuffer.Config{
		TermLength: int(m.TermLength), InitialTermID: m.InitialTermID, MTU: int(m.MTU),
		SessionID: m.SessionID, StreamID: m.StreamID, Prefault: true,
	})
	if err != nil {
		c.fail(m.RespPath, m.CorrelationID, "%v", err)
		return
	}
	window := int(m.InitialWindow)
	if window <= 0 || window > lb.TermLength()/2 {
		window = lb.TermLength() / 2
	}
	sock, err := udp.NewSocket(MaxBatch)
	if err == nil {
		if err = sock.SetBuffers(c.d.cfg.SocketBuf, c.d.cfg.SocketBuf); err == nil {
			err = sock.Connect(m.RemoteIP, int(m.RemotePort))
		}
		if err != nil {
			sock.Close()
		}
	}
	if err != nil {
		lb.Close()
		os.Remove(path)
		c.fail(m.RespPath, m.CorrelationID, "socket: %v", err)
		return
	}
	np := NewNetworkPublication(lb, sock, c.d.cfg.Timing, window)
	c.mu.Lock()
	c.pubs[reg] = &pubEntry{np: np, sock: sock, path: path}
	c.mu.Unlock()
	c.d.sender.AddPublication(np)
	c.respond(m.RespPath, protocol.Response{Type: protocol.RspReady, CorrelationID: m.CorrelationID, Text: fmt.Sprintf("%d|%s", reg, path)})
}

func (c *Conductor) addSubscription(m *protocol.AddSubscription) {
	c.mu.Lock()
	for _, s := range c.subs {
		if s.ns.lb.SessionID() == m.SessionID && s.ns.lb.StreamID() == m.StreamID {
			c.mu.Unlock()
			c.fail(m.RespPath, m.CorrelationID, "subscription for session %d stream %d already exists", m.SessionID, m.StreamID)
			return
		}
	}
	c.mu.Unlock()
	reg := c.nextReg
	c.nextReg++
	path := filepath.Join(c.d.cfg.Dir, fmt.Sprintf("sub-%d.log", reg))
	lb, err := logbuffer.Create(path, logbuffer.Config{
		TermLength: int(m.TermLength), InitialTermID: m.InitialTermID, MTU: int(m.MTU),
		SessionID: m.SessionID, StreamID: m.StreamID, Prefault: true,
	})
	if err != nil {
		c.fail(m.RespPath, m.CorrelationID, "%v", err)
		return
	}
	ns := NewNetworkSubscription(lb, c.d.cfg.Timing, int(m.Window))
	c.mu.Lock()
	c.subs[reg] = &subEntry{ns: ns, path: path}
	c.mu.Unlock()
	c.d.receiver.AddSubscription(ns)
	c.respond(m.RespPath, protocol.Response{Type: protocol.RspReady, CorrelationID: m.CorrelationID, Text: fmt.Sprintf("%d|%s", reg, path)})
}

func (c *Conductor) remove(m *protocol.Remove) {
	c.mu.Lock()
	pe, isPub := c.pubs[m.RegistrationID]
	se, isSub := c.subs[m.RegistrationID]
	delete(c.pubs, m.RegistrationID)
	delete(c.subs, m.RegistrationID)
	c.mu.Unlock()
	switch {
	case isPub:
		c.d.sender.RemovePublication(pe.np)
		c.pending = append(c.pending, closing{c.d.sender.Epoch() + 2, 0, func() {
			pe.sock.Close()
			pe.np.lb.Close()
			os.Remove(pe.path)
		}})
	case isSub:
		c.d.receiver.RemoveSubscription(se.ns)
		c.pending = append(c.pending, closing{0, c.d.receiver.Epoch() + 2, func() {
			se.ns.lb.Close()
			os.Remove(se.path)
		}})
	default:
		c.fail(m.RespPath, m.CorrelationID, "unknown registration %d", m.RegistrationID)
		return
	}
	c.respond(m.RespPath, protocol.Response{Type: protocol.RspOK, CorrelationID: m.CorrelationID})
}

// closeAll releases everything; agents must already be stopped.
func (c *Conductor) closeAll() {
	for _, p := range c.pending {
		p.closeFn()
	}
	c.pending = nil
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pubs {
		p.sock.Close()
		p.np.lb.Close()
		os.Remove(p.path)
	}
	for _, s := range c.subs {
		s.ns.lb.Close()
		os.Remove(s.path)
	}
	c.pubs, c.subs = nil, nil
	path := c.cmd.Path()
	c.cmd.Close()
	os.Remove(path)
}
