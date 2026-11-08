// Package udp is a thin, allocation-free layer over raw non-blocking UDP
// sockets using sendmmsg(2)/recvfrom(2) directly, bypassing package net.
package udp

import (
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Socket owns a non-blocking IPv4 UDP socket plus pre-allocated send
// batch state. It is NOT safe for concurrent use: one thread owns it.
type Socket struct {
	fd   int
	n    int
	hdrs []unix.Mmsghdr
	iovs []unix.Iovec

	src    unix.RawSockaddrAny
	srcLen uint32
	one    unix.Iovec
	oneHdr unix.Msghdr
}

// NewSocket creates a non-blocking UDP socket able to batch maxBatch datagrams.
func NewSocket(maxBatch int) (*Socket, error) {
	if maxBatch < 1 {
		return nil, errors.New("udp: maxBatch must be >= 1")
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("udp: socket: %w", err)
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("udp: nonblock: %w", err)
	}
	s := &Socket{fd: fd, hdrs: make([]unix.Mmsghdr, maxBatch), iovs: make([]unix.Iovec, maxBatch)}
	for i := range s.hdrs {
		s.hdrs[i].Hdr.Iov = &s.iovs[i]
		s.hdrs[i].Hdr.SetIovlen(1)
	}
	return s, nil
}

func (s *Socket) Close() error { return unix.Close(s.fd) }
func (s *Socket) FD() int      { return s.fd }

// SetBuffers sets SO_SNDBUF / SO_RCVBUF (0 = leave as is).
func (s *Socket) SetBuffers(snd, rcv int) error {
	if snd > 0 {
		if err := unix.SetsockoptInt(s.fd, unix.SOL_SOCKET, unix.SO_SNDBUF, snd); err != nil {
			return err
		}
	}
	if rcv > 0 {
		if err := unix.SetsockoptInt(s.fd, unix.SOL_SOCKET, unix.SO_RCVBUF, rcv); err != nil {
			return err
		}
	}
	return nil
}

func (s *Socket) Bind(ip [4]byte, port int) error {
	return unix.Bind(s.fd, &unix.SockaddrInet4{Port: port, Addr: ip})
}

// Connect fixes the peer so that Flush can use sendmmsg without addresses.
func (s *Socket) Connect(ip [4]byte, port int) error {
	return unix.Connect(s.fd, &unix.SockaddrInet4{Port: port, Addr: ip})
}

func (s *Socket) LocalPort() (int, error) {
	sa, err := unix.Getsockname(s.fd)
	if err != nil {
		return 0, err
	}
	in4, ok := sa.(*unix.SockaddrInet4)
	if !ok {
		return 0, errors.New("udp: not an inet4 socket")
	}
	return in4.Port, nil
}

// MaxBatch is the capacity of the send batch.
func (s *Socket) MaxBatch() int { return len(s.hdrs) }

// Reset empties the send batch.
func (s *Socket) Reset() { s.n = 0 }

// Pending is the number of datagrams queued.
func (s *Socket) Pending() int { return s.n }

// Add queues one datagram; its Iovec points straight at p (no copy), so p
// must stay valid and unchanged until Flush returns. False if the batch is full.
func (s *Socket) Add(p []byte) bool {
	if s.n == len(s.iovs) || len(p) == 0 {
		return false
	}
	s.iovs[s.n].Base = &p[0]
	s.iovs[s.n].SetLen(len(p))
	s.n++
	return true
}

// Flush sends the batch with a single sendmmsg and returns how many leading
// datagrams were sent. EAGAIN/ENOBUFS are reported as (0, nil): retry later.
func (s *Socket) Flush() (int, error) {
	if s.n == 0 {
		return 0, nil
	}
	r, _, e := unix.RawSyscall6(unix.SYS_SENDMMSG, uintptr(s.fd),
		uintptr(unsafe.Pointer(&s.hdrs[0])), uintptr(s.n), uintptr(unix.MSG_DONTWAIT), 0, 0)
	if e != 0 {
		if e == unix.EAGAIN || e == unix.ENOBUFS || e == unix.EINTR {
			return 0, nil
		}
		return 0, e
	}
	return int(r), nil
}

// SendTo sends a single datagram to an explicit address (used for control
// frames replying to the peer learnt via Recv).
func (s *Socket) SendTo(p []byte, to *unix.RawSockaddrInet4) (bool, error) {
	if len(p) == 0 {
		return false, nil
	}
	s.one.Base = &p[0]
	s.one.SetLen(len(p))
	s.oneHdr.Iov = &s.one
	s.oneHdr.SetIovlen(1)
	s.oneHdr.Name = (*byte)(unsafe.Pointer(to))
	s.oneHdr.Namelen = unix.SizeofSockaddrInet4
	_, _, e := unix.RawSyscall(unix.SYS_SENDMSG, uintptr(s.fd), uintptr(unsafe.Pointer(&s.oneHdr)), uintptr(unix.MSG_DONTWAIT))
	if e != 0 {
		if e == unix.EAGAIN || e == unix.ENOBUFS || e == unix.EINTR {
			return false, nil
		}
		return false, e
	}
	return true, nil
}

// Send sends one datagram on a connected socket.
func (s *Socket) Send(p []byte) (bool, error) {
	s.Reset()
	if !s.Add(p) {
		return false, nil
	}
	n, err := s.Flush()
	s.Reset()
	return n == 1, err
}

// Recv reads one datagram into buf. n==0 with nil error means nothing
// available. The sender's address is retained for Src.
func (s *Socket) Recv(buf []byte) (int, error) {
	s.srcLen = unix.SizeofSockaddrAny
	r, _, e := unix.RawSyscall6(unix.SYS_RECVFROM, uintptr(s.fd), uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)), uintptr(unix.MSG_DONTWAIT), uintptr(unsafe.Pointer(&s.src)), uintptr(unsafe.Pointer(&s.srcLen)))
	if e != 0 {
		if e == unix.EAGAIN || e == unix.EINTR {
			return 0, nil
		}
		return 0, e
	}
	return int(r), nil
}

// Src returns the source address of the last datagram received.
func (s *Socket) Src() *unix.RawSockaddrInet4 {
	return (*unix.RawSockaddrInet4)(unsafe.Pointer(&s.src))
}
