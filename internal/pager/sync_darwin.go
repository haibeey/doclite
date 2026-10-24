package pager

import (
	"os"

	"golang.org/x/sys/unix"
)

// fsync on macOS does not flush the drive cache. F_FULLFSYNC does.
func fsync(f *os.File) error {
	if _, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0); err == nil {
		return nil
	}
	return f.Sync()
}
