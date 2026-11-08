package main

import (
	"os"
	"testing"
)

func TestHotPathHasNoHeapEscapes(t *testing.T) {
	if err := os.Chdir("../.."); err != nil {
		t.Fatal(err)
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
}
