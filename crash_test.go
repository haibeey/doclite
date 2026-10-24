//go:build unix

package doclite

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// txnSize is the number of documents that transaction i inserts.
func txnSize(i int) int { return 1 + i%7 }

// TestCrashChild is the process that the crash test kills. It runs only
// when DOCLITE_CRASH_DIR is set.
//
// Transaction i inserts txnSize(i) documents with "txn": i, and deletes
// the documents of transaction i-3. After each commit returns, the child
// writes i to a log and syncs it.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv("DOCLITE_CRASH_DIR")
	if dir == "" {
		t.Skip("only runs as the child of TestCrash")
	}
	db, err := Open(filepath.Join(dir, "crash.db"), &Options{CheckpointSize: 64, LockTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.OpenFile(filepath.Join(dir, "log"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	start := lastLogged(t, dir) + 1
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	c := db.Collection("c")
	c.CreateIndex("txn")
	for i := start; ; i++ {
		err := db.Update(func(tx *Tx) error {
			tc := tx.Collection("c")
			for j := 0; j < txnSize(i); j++ {
				pad := strings.Repeat("x", r.Intn(6000)) // some docs use overflow pages
				if _, err := tc.Insert(M{"txn": i, "j": j, "pad": pad}); err != nil {
					return err
				}
			}
			_, err := tc.DeleteMany(M{"txn": i - 3})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(log, "%d\n", i)
		log.Sync()
	}
}

func lastLogged(t *testing.T, dir string) int {
	f, err := os.Open(filepath.Join(dir, "log"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	last := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		if n, err := strconv.Atoi(s.Text()); err == nil {
			last = n
		}
	}
	return last
}

// TestCrash kills a writing process at random times with SIGKILL and
// checks that every commit that returned is still there, that no
// transaction is half applied, and that the file is not corrupt.
func TestCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	dir := t.TempDir()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for round := 0; round < 8; round++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.v")
		cmd.Env = append(os.Environ(), "DOCLITE_CRASH_DIR="+dir)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(100+r.Intn(400)) * time.Millisecond)
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()

		last := lastLogged(t, dir)
		db, err := Open(filepath.Join(dir, "crash.db"), nil)
		if err != nil {
			t.Fatalf("round %d: open after crash: %v", round, err)
		}
		if err := db.Check(); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		c := db.Collection("c")
		count := func(i int) int64 {
			n, err := c.Count(M{"txn": i})
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
		// The child can commit one transaction (last+1) and stop
		// before it writes the log.
		for i := 1; i <= last+1; i++ {
			n, want := count(i), int64(txnSize(i))
			switch {
			case i <= last-3:
				want = 0 // deleted by i+3, which is logged
			case i == last-2 || i == last+1:
				if n == 0 {
					want = 0 // depends on whether last+1 committed
				}
			}
			if n != want {
				t.Fatalf("round %d (last logged %d): txn %d has %d docs, want %d", round, last, i, n, want)
			}
		}
		t.Logf("round %d: %d transactions logged", round, last)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if lastLogged(t, dir) == 0 {
		t.Fatal("the child never committed")
	}
}
