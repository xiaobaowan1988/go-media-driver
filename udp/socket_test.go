package udp

import (
	"bytes"
	"testing"
	"time"
)

func pair(t testing.TB) (tx, rx *Socket) {
	rx, err := NewSocket(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := rx.Bind([4]byte{127, 0, 0, 1}, 0); err != nil {
		t.Fatal(err)
	}
	port, _ := rx.LocalPort()
	tx, err = NewSocket(8)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Connect([4]byte{127, 0, 0, 1}, port); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Close(); rx.Close() })
	return
}

func recvWait(t testing.TB, s *Socket, buf []byte) int {
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); {
		if n, err := s.Recv(buf); err != nil {
			t.Fatal(err)
		} else if n > 0 {
			return n
		}
	}
	t.Fatal("timeout")
	return 0
}

func TestBatchRoundTrip(t *testing.T) {
	tx, rx := pair(t)
	msgs := [][]byte{[]byte("alpha"), []byte("bravo-bravo"), []byte("c")}
	for _, m := range msgs {
		if !tx.Add(m) {
			t.Fatal("add")
		}
	}
	n, err := tx.Flush()
	if err != nil || n != 3 {
		t.Fatalf("flush n=%d err=%v", n, err)
	}
	buf := make([]byte, 2048)
	for _, m := range msgs {
		if got := recvWait(t, rx, buf); !bytes.Equal(buf[:got], m) {
			t.Fatalf("got %q want %q", buf[:got], m)
		}
	}
	if n, err := rx.Recv(buf); n != 0 || err != nil {
		t.Fatalf("expected EAGAIN as (0,nil), got %d,%v", n, err)
	}
}

func TestReplyToSource(t *testing.T) {
	tx, rx := pair(t)
	tx.Send([]byte("ping"))
	buf := make([]byte, 64)
	recvWait(t, rx, buf)
	if ok, err := rx.SendTo([]byte("pong"), rx.Src()); err != nil || !ok {
		t.Fatalf("sendto ok=%v err=%v", ok, err)
	}
	if n := recvWait(t, tx, buf); string(buf[:n]) != "pong" {
		t.Fatalf("got %q", buf[:n])
	}
}

func TestZeroAlloc(t *testing.T) {
	tx, rx := pair(t)
	p := make([]byte, 100)
	buf := make([]byte, 2048)
	if a := testing.AllocsPerRun(200, func() {
		tx.Reset()
		tx.Add(p)
		tx.Add(p)
		tx.Flush()
		rx.Recv(buf)
		rx.Recv(buf)
	}); a != 0 {
		t.Fatalf("allocs/op = %v", a)
	}
}
