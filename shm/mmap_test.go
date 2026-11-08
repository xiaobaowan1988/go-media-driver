package shm

import (
	"path/filepath"
	"testing"
)

func TestSharedPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.dat")
	a, err := Create(path, 8192, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := Create(path, 8192, false); err == nil {
		t.Fatal("expected O_EXCL failure")
	}
	b, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if len(b.Bytes()) != 8192 {
		t.Fatalf("size %d", len(b.Bytes()))
	}
	a.Bytes()[100] = 42
	if b.Bytes()[100] != 42 {
		t.Fatal("mappings do not share pages")
	}
	if err := a.Close(); err != nil || a.Close() != nil {
		t.Fatal("close not idempotent")
	}
}
