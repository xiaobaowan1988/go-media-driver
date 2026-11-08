// Package shm wraps mmap(2) over files (normally on /dev/shm) so that several
// processes can map the same physical pages.
package shm

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// DefaultDir is the tmpfs directory used for shared memory files.
const DefaultDir = "/dev/shm"

// Mapping is a MAP_SHARED mapping of a whole file.
type Mapping struct {
	data []byte
	path string
}

func mapFd(fd, size int, prefault bool) ([]byte, error) {
	flags := unix.MAP_SHARED
	if prefault {
		flags |= unix.MAP_POPULATE
	}
	return unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, flags)
}

// Create creates a new zero-filled file of the given size and maps it.
// It fails if the file already exists. With prefault, all pages are touched
// at map time (MAP_POPULATE) so the hot path never takes a page fault.
func Create(path string, size int, prefault bool) (*Mapping, error) {
	if size <= 0 {
		return nil, errors.New("shm: size must be positive")
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0o660)
	if err != nil {
		return nil, fmt.Errorf("shm: create %s: %w", path, err)
	}
	defer unix.Close(fd)
	if err := unix.Ftruncate(fd, int64(size)); err != nil {
		unix.Unlink(path)
		return nil, fmt.Errorf("shm: truncate %s: %w", path, err)
	}
	data, err := mapFd(fd, size, prefault)
	if err != nil {
		unix.Unlink(path)
		return nil, fmt.Errorf("shm: mmap %s: %w", path, err)
	}
	return &Mapping{data: data, path: path}, nil
}

// Open maps an existing file created by another (or the same) process.
func Open(path string, prefault bool) (*Mapping, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("shm: open %s: %w", path, err)
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, fmt.Errorf("shm: stat %s: %w", path, err)
	}
	if st.Size <= 0 {
		return nil, fmt.Errorf("shm: %s is empty", path)
	}
	data, err := mapFd(fd, int(st.Size), prefault)
	if err != nil {
		return nil, fmt.Errorf("shm: mmap %s: %w", path, err)
	}
	return &Mapping{data: data, path: path}, nil
}

// Bytes returns the mapped memory. It is invalid after Close.
func (m *Mapping) Bytes() []byte { return m.data }

// Path returns the backing file path.
func (m *Mapping) Path() string { return m.path }

// Close unmaps the memory. It is idempotent.
func (m *Mapping) Close() error {
	if m.data == nil {
		return nil
	}
	d := m.data
	m.data = nil
	return unix.Munmap(d)
}

// Remove unlinks a backing file.
func Remove(path string) error { return unix.Unlink(path) }
