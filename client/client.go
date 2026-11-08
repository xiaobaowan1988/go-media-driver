// Package client is the application-side API (used by the Raft engine): it
// talks to a media driver through shared-memory logs only.
package client

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xiaobaowan1988/go-media-driver/driver"
	"github.com/xiaobaowan1988/go-media-driver/logbuffer"
	"github.com/xiaobaowan1988/go-media-driver/protocol"
)

var clientSeq atomic.Int64

// Client is a connection to a driver. Control-plane calls are serialised;
// Publication.TryClaim/Offer and Subscription.Poll are the lock-free data path.
type Client struct {
	mu       sync.Mutex
	timeout  time.Duration
	cmd      *logbuffer.LogBuffer
	resp     *logbuffer.LogBuffer
	respPath string
	respPos  int64
	nextCorr int64
}

// Connect opens the driver's command log in dir and creates a private
// response log. It waits up to timeout for the driver to appear.
func Connect(dir string, timeout time.Duration) (*Client, error) {
	deadline := time.Now().Add(timeout)
	var cmd *logbuffer.LogBuffer
	var err error
	for {
		cmd, err = logbuffer.Open(filepath.Join(dir, driver.CommandFile), false)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("client: driver not available: %w", err)
		}
		time.Sleep(time.Millisecond)
	}
	path := filepath.Join(dir, fmt.Sprintf("client-%d-%d.resp", os.Getpid(), clientSeq.Add(1)))
	resp, err := logbuffer.Create(path, logbuffer.Config{TermLength: 64 * 1024, MTU: 1024})
	if err != nil {
		cmd.Close()
		return nil, err
	}
	return &Client{timeout: timeout, cmd: cmd, resp: resp, respPath: path}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cmd.Close()
	c.resp.Close()
	return os.Remove(c.respPath)
}

func (c *Client) send(b []byte) error {
	deadline := time.Now().Add(c.timeout)
	for {
		_, err := c.cmd.Offer(b)
		if err == nil {
			return nil
		}
		if err != logbuffer.ErrAdminAction && err != logbuffer.ErrBackPressured {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("client: timed out writing command")
		}
	}
}

func (c *Client) await(corr int64) (protocol.Response, error) {
	deadline := time.Now().Add(c.timeout)
	var out protocol.Response
	found := false
	for !found {
		var n int
		c.respPos, n = c.resp.Poll(c.respPos, func(b []byte, _ *protocol.DataHeader) {
			var r protocol.Response
			if r.Unmarshal(b) == nil && r.CorrelationID == corr {
				out, found = r, true
			}
		}, 16)
		c.resp.SetConsumerPosition(c.respPos)
		c.resp.CleanConsumed(c.respPos)
		if n == 0 && !found {
			if time.Now().After(deadline) {
				return out, errors.New("client: timed out waiting for driver response")
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
	if out.Type == protocol.RspError {
		return out, errors.New("driver: " + out.Text)
	}
	return out, nil
}

func (c *Client) corr() int64 { c.nextCorr++; return c.nextCorr }

func parseReady(text string) (reg int64, path string, err error) {
	i := strings.IndexByte(text, '|')
	if i < 0 {
		return 0, "", errors.New("client: bad response")
	}
	reg, err = strconv.ParseInt(text[:i], 10, 64)
	return reg, text[i+1:], err
}

// PublicationConfig describes an outbound stream.
type PublicationConfig struct {
	SessionID, StreamID int32
	RemoteIP            [4]byte
	RemotePort          int
	InitialTermID       int32
	TermLength          int // default 1 MiB
	MTU                 int // default 1408
	InitialWindow       int // default TermLength/2
}

// Publication appends messages to the outbound log. Safe for concurrent
// publishers (CAS append).
type Publication struct {
	c   *Client
	lb  *logbuffer.LogBuffer
	reg int64
}

func (c *Client) AddPublication(cfg PublicationConfig) (*Publication, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cfg.TermLength == 0 {
		cfg.TermLength = 1 << 20
	}
	if cfg.MTU == 0 {
		cfg.MTU = 1408
	}
	corr := c.corr()
	m := protocol.AddPublication{
		CorrelationID: corr, SessionID: cfg.SessionID, StreamID: cfg.StreamID, RemoteIP: cfg.RemoteIP,
		RemotePort: int32(cfg.RemotePort), InitialTermID: cfg.InitialTermID, TermLength: int32(cfg.TermLength),
		MTU: int32(cfg.MTU), InitialWindow: int32(cfg.InitialWindow), RespPath: c.respPath,
	}
	if err := c.send(m.Marshal()); err != nil {
		return nil, err
	}
	r, err := c.await(corr)
	if err != nil {
		return nil, err
	}
	reg, path, err := parseReady(r.Text)
	if err != nil {
		return nil, err
	}
	lb, err := logbuffer.Open(path, false)
	if err != nil {
		return nil, err
	}
	return &Publication{c: c, lb: lb, reg: reg}, nil
}

// TryClaim / Offer: see logbuffer. ErrBackPressured means the receiver
// window is exhausted; ErrAdminAction means retry immediately.
func (p *Publication) TryClaim(length int, claim *logbuffer.BufferClaim) (int64, error) {
	return p.lb.TryClaim(length, claim)
}
func (p *Publication) Offer(b []byte) (int64, error) { return p.lb.Offer(b) }
func (p *Publication) Position() int64               { return p.lb.Tail() }
func (p *Publication) MaxPayload() int               { return p.lb.MaxPayload() }

func (p *Publication) Close() error {
	p.c.mu.Lock()
	defer p.c.mu.Unlock()
	corr := p.c.corr()
	m := protocol.Remove{CorrelationID: corr, RegistrationID: p.reg, RespPath: p.c.respPath}
	err := p.c.send(m.Marshal())
	if err == nil {
		_, err = p.c.await(corr)
	}
	p.lb.Close()
	return err
}

// SubscriptionConfig describes an inbound stream.
type SubscriptionConfig struct {
	SessionID, StreamID int32
	InitialTermID       int32
	TermLength          int // must equal the publisher's; default 1 MiB
	MTU                 int // must equal the publisher's; default 1408
	Window              int // default TermLength/2
}

// Subscription reads messages in order. Single consumer.
type Subscription struct {
	c   *Client
	lb  *logbuffer.LogBuffer
	reg int64
	pos int64
}

func (c *Client) AddSubscription(cfg SubscriptionConfig) (*Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cfg.TermLength == 0 {
		cfg.TermLength = 1 << 20
	}
	if cfg.MTU == 0 {
		cfg.MTU = 1408
	}
	if cfg.Window == 0 {
		cfg.Window = cfg.TermLength / 2
	}
	corr := c.corr()
	m := protocol.AddSubscription{
		CorrelationID: corr, SessionID: cfg.SessionID, StreamID: cfg.StreamID, InitialTermID: cfg.InitialTermID,
		TermLength: int32(cfg.TermLength), MTU: int32(cfg.MTU), Window: int32(cfg.Window), RespPath: c.respPath,
	}
	if err := c.send(m.Marshal()); err != nil {
		return nil, err
	}
	r, err := c.await(corr)
	if err != nil {
		return nil, err
	}
	reg, path, err := parseReady(r.Text)
	if err != nil {
		return nil, err
	}
	lb, err := logbuffer.Open(path, false)
	if err != nil {
		return nil, err
	}
	return &Subscription{c: c, lb: lb, reg: reg}, nil
}

// Poll delivers up to limit messages and publishes the new consumer position
// (which drives the receiver window and term cleaning).
func (s *Subscription) Poll(h logbuffer.FragmentHandler, limit int) int {
	pos, n := s.lb.Poll(s.pos, h, limit)
	if pos != s.pos {
		s.pos = pos
		s.lb.SetConsumerPosition(pos)
	}
	return n
}
func (s *Subscription) Position() int64 { return s.pos }

func (s *Subscription) Close() error {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	corr := s.c.corr()
	m := protocol.Remove{CorrelationID: corr, RegistrationID: s.reg, RespPath: s.c.respPath}
	err := s.c.send(m.Marshal())
	if err == nil {
		_, err = s.c.await(corr)
	}
	s.lb.Close()
	return err
}
