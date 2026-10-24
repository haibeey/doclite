//go:build !darwin

package pager

import "os"

func fsync(f *os.File) error { return f.Sync() }
