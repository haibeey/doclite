//go:build windows

package pager

import (
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// The lock is on one byte far past the end of the file. A lock on
// Windows also blocks reads of the locked bytes, so it must not cover
// real data.
func lockRegion() *windows.Overlapped { return &windows.Overlapped{OffsetHigh: 0x40000000} }

func lockFile(f *os.File, exclusive bool, timeout time.Duration) error {
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	deadline := time.Now().Add(timeout)
	for {
		err := windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, 1, 0, lockRegion())
		if err == nil {
			return nil
		}
		if err != windows.ERROR_LOCK_VIOLATION {
			return err
		}
		if !time.Now().Before(deadline) {
			return ErrLocked
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func unlockFile(f *os.File) error {
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, lockRegion())
}

// syncDir is not needed on Windows.
func syncDir(string) error { return nil }
