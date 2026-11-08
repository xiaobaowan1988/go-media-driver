package protocol

import (
	"encoding/binary"
	"errors"
)

// IPC command / response types (client <-> conductor). Control plane only:
// these encoders allocate and are never used on the data path.
const (
	CmdAddPublication  int32 = 1
	CmdAddSubscription int32 = 2
	CmdRemove          int32 = 3

	RspReady int32 = 100
	RspError int32 = 101
	RspOK    int32 = 102
)

var ErrBadCommand = errors.New("protocol: malformed command")

type wbuf struct{ b []byte }

func (w *wbuf) i32(v int32)  { w.b = binary.LittleEndian.AppendUint32(w.b, uint32(v)) }
func (w *wbuf) i64(v int64)  { w.b = binary.LittleEndian.AppendUint64(w.b, uint64(v)) }
func (w *wbuf) str(s string) { w.i32(int32(len(s))); w.b = append(w.b, s...) }

type rbuf struct {
	b   []byte
	bad bool
}

func (r *rbuf) take(n int) []byte {
	if n < 0 || len(r.b) < n {
		r.bad = true
		return make([]byte, n&0xff)
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}
func (r *rbuf) i32() int32 { return int32(binary.LittleEndian.Uint32(r.take(4))) }
func (r *rbuf) i64() int64 { return int64(binary.LittleEndian.Uint64(r.take(8))) }
func (r *rbuf) str() string {
	n := int(r.i32())
	if n > 512 {
		r.bad = true
		return ""
	}
	return string(r.take(n))
}

// Header returns the command/response type and correlation id.
func Header(b []byte) (typ int32, corr int64, err error) {
	if len(b) < 12 {
		return 0, 0, ErrBadCommand
	}
	return int32(binary.LittleEndian.Uint32(b)), int64(binary.LittleEndian.Uint64(b[4:])), nil
}

// AddPublication creates an outbound stream to RemoteIP:RemotePort.
type AddPublication struct {
	CorrelationID int64
	SessionID     int32
	StreamID      int32
	RemoteIP      [4]byte
	RemotePort    int32
	InitialTermID int32
	TermLength    int32
	MTU           int32
	InitialWindow int32
	RespPath      string
}

func (c *AddPublication) Marshal() []byte {
	w := &wbuf{}
	w.i32(CmdAddPublication)
	w.i64(c.CorrelationID)
	w.i32(c.SessionID)
	w.i32(c.StreamID)
	w.i32(int32(binary.LittleEndian.Uint32(c.RemoteIP[:])))
	w.i32(c.RemotePort)
	w.i32(c.InitialTermID)
	w.i32(c.TermLength)
	w.i32(c.MTU)
	w.i32(c.InitialWindow)
	w.str(c.RespPath)
	return w.b
}

func (c *AddPublication) Unmarshal(b []byte) error {
	r := &rbuf{b: b[12:]}
	c.CorrelationID = int64(binary.LittleEndian.Uint64(b[4:]))
	c.SessionID, c.StreamID = r.i32(), r.i32()
	binary.LittleEndian.PutUint32(c.RemoteIP[:], uint32(r.i32()))
	c.RemotePort, c.InitialTermID, c.TermLength, c.MTU, c.InitialWindow = r.i32(), r.i32(), r.i32(), r.i32(), r.i32()
	c.RespPath = r.str()
	if r.bad {
		return ErrBadCommand
	}
	return nil
}

// AddSubscription creates an inbound stream demuxed by (SessionID, StreamID).
type AddSubscription struct {
	CorrelationID int64
	SessionID     int32
	StreamID      int32
	InitialTermID int32
	TermLength    int32
	MTU           int32
	Window        int32
	RespPath      string
}

func (c *AddSubscription) Marshal() []byte {
	w := &wbuf{}
	w.i32(CmdAddSubscription)
	w.i64(c.CorrelationID)
	w.i32(c.SessionID)
	w.i32(c.StreamID)
	w.i32(c.InitialTermID)
	w.i32(c.TermLength)
	w.i32(c.MTU)
	w.i32(c.Window)
	w.str(c.RespPath)
	return w.b
}

func (c *AddSubscription) Unmarshal(b []byte) error {
	r := &rbuf{b: b[12:]}
	c.CorrelationID = int64(binary.LittleEndian.Uint64(b[4:]))
	c.SessionID, c.StreamID, c.InitialTermID, c.TermLength, c.MTU, c.Window = r.i32(), r.i32(), r.i32(), r.i32(), r.i32(), r.i32()
	c.RespPath = r.str()
	if r.bad {
		return ErrBadCommand
	}
	return nil
}

// Remove releases a publication or subscription by registration id (the
// correlation id of the Add command).
type Remove struct {
	CorrelationID  int64
	RegistrationID int64
	RespPath       string
}

func (c *Remove) Marshal() []byte {
	w := &wbuf{}
	w.i32(CmdRemove)
	w.i64(c.CorrelationID)
	w.i64(c.RegistrationID)
	w.str(c.RespPath)
	return w.b
}

func (c *Remove) Unmarshal(b []byte) error {
	r := &rbuf{b: b[12:]}
	c.CorrelationID = int64(binary.LittleEndian.Uint64(b[4:]))
	c.RegistrationID = r.i64()
	c.RespPath = r.str()
	if r.bad {
		return ErrBadCommand
	}
	return nil
}

// Response is READY (Text = log file path), OK, or ERROR (Text = message).
type Response struct {
	Type          int32
	CorrelationID int64
	Text          string
}

func (c *Response) Marshal() []byte {
	w := &wbuf{}
	w.i32(c.Type)
	w.i64(c.CorrelationID)
	w.str(c.Text)
	return w.b
}

func (c *Response) Unmarshal(b []byte) error {
	if len(b) < 12 {
		return ErrBadCommand
	}
	r := &rbuf{b: b[12:]}
	c.Type = int32(binary.LittleEndian.Uint32(b))
	c.CorrelationID = int64(binary.LittleEndian.Uint64(b[4:]))
	c.Text = r.str()
	if r.bad {
		return ErrBadCommand
	}
	return nil
}
