//go:build unix

package pager

import (
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// lockFile takes an flock on f. It is exclusive for a writer and shared
// for a read-only open. It waits up to timeout for the lock.
func lockFile(f *os.File, exclusive bool, timeout time.Duration) error {
	how := unix.LOCK_SH
	if exclusive {
		how = unix.LOCK_EX
	}
	deadline := time.Now().Add(timeout)
	for {
		err := unix.Flock(int(f.Fd()), how|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if err != unix.EWOULDBLOCK {
			return err
		}
		if !time.Now().Before(deadline) {
			return ErrLocked
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func unlockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}

// syncDir makes a new file name in dir durable.
func syncDir(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
