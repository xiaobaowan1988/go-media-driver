package logbuffer

import (
	"testing"
	"unsafe"

	"github.com/xiaobaowan1988/go-media-driver/protocol"
)

func TestScanBlockGroupsFramesUpToMTU(t *testing.T) {
	lb := newLB(t, MinTermLength, 256)
	for i := 0; i < 5; i++ {
		lb.Offer(make([]byte, 40)) // 72 -> aligned 96
	}
	var c BufferClaim
	lb.TryClaim(40, &c) // claimed, uncommitted: must stop the scan
	b, adv := lb.ScanBlock(0, 256)
	if adv != 192 || len(b) != 192 {
		t.Fatalf("adv=%d len=%d, want 2 frames (192)", adv, len(b))
	}
	_, adv = lb.ScanBlock(192, 256)
	if adv != 192 {
		t.Fatalf("adv=%d", adv)
	}
	b, adv = lb.ScanBlock(384, 256)
	if adv != 96 {
		t.Fatalf("adv=%d want 96", adv)
	}
	if _, adv = lb.ScanBlock(480, 256); adv != 0 {
		t.Fatal("uncommitted frame must not be scanned")
	}
	// The block aliases the log memory (zero copy).
	b, _ = lb.ScanBlock(0, 256)
	if uintptr(unsafe.Pointer(&b[0])) != uintptr(unsafe.Pointer(&lb.terms[0][0])) {
		t.Fatal("ScanBlock copied")
	}
}

func TestInsertOutOfOrderDuplicateAndRotation(t *testing.T) {
	src := newLB(t, MinTermLength, 1408)
	dst := newLB(t, MinTermLength, 1408)
	var frames [][]byte
	for i := 0; i < 5; i++ {
		src.Offer([]byte{byte(i), 1, 2, 3})
	}
	for pos := int64(0); ; {
		b, adv := src.ScanBlock(pos, 64)
		if adv == 0 {
			break
		}
		frames = append(frames, append([]byte(nil), b...))
		pos += int64(adv)
	}
	if len(frames) != 5 {
		t.Fatalf("%d frames", len(frames))
	}
	for _, i := range []int{3, 1, 4, 1, 0, 2} { // out of order + duplicate
		if err := dst.Insert(frames[i]); err != nil {
			t.Fatal(err)
		}
	}
	var got []byte
	dst.Poll(0, func(b []byte, _ *protocol.DataHeader) { got = append(got, b[0]) }, 100)
	if string(got) != "\x00\x01\x02\x03\x04" {
		t.Fatalf("got %v", got)
	}
	// Bad / stale / future frames.
	if err := dst.Insert(frames[0][:10]); err != ErrBadFrame {
		t.Fatalf("short frame: %v", err)
	}
	bad := append([]byte(nil), frames[0]...)
	(*protocol.DataHeader)(unsafe.Pointer(&bad[0])).TermID += 5
	if err := dst.Insert(bad); err != ErrFuture {
		t.Fatalf("future: %v", err)
	}
}

func TestInsertRejectsConsumedFrame(t *testing.T) {
	src := newLB(t, MinTermLength, 1408)
	dst := newLB(t, MinTermLength, 1408)
	src.Offer([]byte("abcd"))
	b, adv := src.ScanBlock(0, 1408)
	frame := append([]byte(nil), b...)
	dst.Insert(frame)
	pos, _ := dst.Poll(0, func([]byte, *protocol.DataHeader) {}, 10)
	dst.SetConsumerPosition(pos)
	clear(dst.terms[0][:adv]) // simulate a cleaned region
	if err := dst.Insert(frame); err != ErrStale {
		t.Fatalf("want ErrStale, got %v", err)
	}
}
