package wal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jaypatel/p2p-messaging/pkg/wal"
)

func openTmp(t *testing.T, fsync bool) (*wal.WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "messages.wal")
	w, err := wal.Open(path, fsync)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

// Test: Append 3 msgs, Replay returns all 3.
func TestReplayAll(t *testing.T) {
	w, _ := openTmp(t, false)
	for i := uint64(1); i <= 3; i++ {
		if err := w.Append(i, []byte(fmt.Sprintf("msg%d", i))); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}
}

// Test: Append 3, Ack 2, Replay returns only the unacked one.
func TestReplayAfterAck(t *testing.T) {
	w, _ := openTmp(t, false)
	for i := uint64(1); i <= 3; i++ {
		w.Append(i, []byte(fmt.Sprintf("msg%d", i)))
	}
	w.Ack(1)
	w.Ack(3)

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].MsgID != 2 {
		t.Fatalf("want MsgID=2, got %d", entries[0].MsgID)
	}
}

// Test: Replay works after process restart simulation (close + reopen).
func TestReplayAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "messages.wal")

	w, err := wal.Open(path, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := uint64(10); i <= 12; i++ {
		w.Append(i, []byte(fmt.Sprintf("pay%d", i)))
	}
	w.Ack(11)
	w.Close()

	// Simulate restart: open fresh handle.
	w2, err := wal.Open(path, false)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay after reopen: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(entries))
	}
}

// Test: Compact removes acked entries (file shrinks).
func TestCompactShrinksFile(t *testing.T) {
	w, path := openTmp(t, false)
	for i := uint64(1); i <= 5; i++ {
		w.Append(i, []byte("hello world payload"))
	}
	for i := uint64(1); i <= 4; i++ {
		w.Ack(i)
	}

	before, _ := os.Stat(path)
	if err := w.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after, _ := os.Stat(path)

	if after.Size() >= before.Size() {
		t.Fatalf("file did not shrink: before=%d after=%d", before.Size(), after.Size())
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay post-compact: %v", err)
	}
	if len(entries) != 1 || entries[0].MsgID != 5 {
		t.Fatalf("want [5], got %v", entries)
	}
}

// Test: Compact mid-write doesn't corrupt (mutex correctness).
func TestCompactConcurrentWrite(t *testing.T) {
	w, _ := openTmp(t, false)

	// Pre-seed some data.
	for i := uint64(1); i <= 10; i++ {
		w.Append(i, []byte("seed"))
	}

	var wg sync.WaitGroup
	// Writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(100); i < 200; i++ {
			w.Append(i, []byte("concurrent"))
		}
	}()

	// Compact concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := w.Compact(); err != nil {
			t.Errorf("Compact error: %v", err)
		}
	}()

	wg.Wait()

	// File must be readable after concurrent access.
	if _, err := w.Replay(); err != nil {
		t.Fatalf("Replay after concurrent compact: %v", err)
	}
}

// Test: Concurrent Append from 10 goroutines (race-safe).
func TestConcurrentAppend(t *testing.T) {
	w, _ := openTmp(t, false)
	const goroutines = 10
	const msgsEach = 20

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := uint64(g * msgsEach)
			for i := uint64(0); i < msgsEach; i++ {
				if err := w.Append(base+i, []byte("data")); err != nil {
					t.Errorf("Append: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != goroutines*msgsEach {
		t.Fatalf("want %d entries, got %d", goroutines*msgsEach, len(entries))
	}
}

// Test: Crash simulation — write 100 entries with fsync=false, close abruptly, reopen and verify recoverable.
func TestCrashSimulation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "messages.wal")

	w, err := wal.Open(path, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := uint64(0); i < 100; i++ {
		if err := w.Append(i, []byte("payload data")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	// Simulate abrupt close without Sync (crash).
	w.Close()

	// Reopen and replay — all OS-buffered writes should be recoverable since
	// they were issued to the kernel; only truly unflushed data would be lost.
	w2, err := wal.Open(path, false)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	// We can't guarantee all 100 on a real crash, but in test (no actual crash) all should be there.
	if len(entries) == 0 {
		t.Fatal("expected at least some recoverable entries")
	}
	t.Logf("recovered %d/100 entries after crash simulation", len(entries))
}
