package broker

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
	"github.com/jdp5949/p2p-messaging/pkg/wal"
)

// openTestWAL opens a WAL at a temp path (no fsync for test speed).
// Returns the WAL and its path for re-open tests.
func openTestWAL(t *testing.T) (*wal.WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := wal.Open(path, false)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	return w, path
}

// walBrokerPair creates a Broker with WAL support plus a raw server conn.
// The server conn is kept by the caller for sending ACKs.
// A drain goroutine is started automatically so sends don't block.
func walBrokerPair(t *testing.T, w *wal.WAL) (*Broker, net.Conn) {
	t.Helper()
	clientRaw, srvRaw := net.Pipe()

	c, err := conn.New(conn.Config{
		DialFunc: func() (net.Conn, error) { return clientRaw, nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	b, err := New(Config{
		Conn:       c,
		ACKTimeout: 5 * time.Second,
		WAL:        w,
	})
	if err != nil {
		c.Close()
		t.Fatal(err)
	}

	t.Cleanup(func() { b.Close(); srvRaw.Close() })
	return b, srvRaw
}

// sendN sends n messages via broker, draining the server side concurrently,
// and returns the slice of message IDs in send order.
func sendN(t *testing.T, b *Broker, srv net.Conn, n int) []uint64 {
	t.Helper()

	// Drain goroutine reads everything the broker writes.
	stopDrain := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		buf := make([]byte, 4096)
		for {
			select {
			case <-stopDrain:
				return
			default:
				srv.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
				srv.Read(buf) //nolint:errcheck
			}
		}
	}()

	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		id, err := b.Send(protocol.ContentText, protocol.PriorityNormal, []byte("payload"))
		if err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
		ids[i] = id
	}

	close(stopDrain)
	<-drainDone
	srv.SetReadDeadline(time.Time{}) // clear deadline

	return ids
}

// TestWALAppendsOnSend verifies that 5 sends produce 5 OpSend entries in the WAL.
func TestWALAppendsOnSend(t *testing.T) {
	w, _ := openTestWAL(t)
	b, srv := walBrokerPair(t, w)

	sendN(t, b, srv, 5)

	// Allow time for WAL writes to flush (they are synchronous, but give margin).
	time.Sleep(20 * time.Millisecond)

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("expected 5 WAL entries after 5 sends, got %d", len(entries))
	}
}

// TestWALAckRemovesEntries verifies that ACKing 3 of 5 messages leaves 2 in WAL.
func TestWALAckRemovesEntries(t *testing.T) {
	w, _ := openTestWAL(t)
	b, srv := walBrokerPair(t, w)

	ids := sendN(t, b, srv, 5)

	// ACK first 3 via the server connection.
	for i := 0; i < 3; i++ {
		writeMsg(t, srv, protocol.Message{
			Header: protocol.Header{MsgID: ids[i], MsgType: protocol.MsgACK},
		})
	}

	// Wait for freeSlot to process all 3 ACKs.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := w.Replay()
		if len(entries) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 WAL entries after 3 acks, got %d", len(entries))
	}
}

// TestWALCompactKeepsUnacked verifies that Compact() reduces WAL to only unacked entries.
func TestWALCompactKeepsUnacked(t *testing.T) {
	w, _ := openTestWAL(t)
	b, srv := walBrokerPair(t, w)

	ids := sendN(t, b, srv, 5)

	for i := 0; i < 3; i++ {
		writeMsg(t, srv, protocol.Message{
			Header: protocol.Header{MsgID: ids[i], MsgType: protocol.MsgACK},
		})
	}

	// Wait for acks to be processed.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := w.Replay()
		if len(entries) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := w.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay after compact: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 entries after compact, got %d", len(entries))
	}
}

// TestWALReplayRequeuesOnRestart verifies that a new Broker with the same WAL
// re-enqueues the 2 unacked messages from the previous session.
func TestWALReplayRequeuesOnRestart(t *testing.T) {
	w, walPath := openTestWAL(t)

	// --- Session 1: send 5, ACK 3 ---
	b1, srv1 := walBrokerPair(t, w)
	ids := sendN(t, b1, srv1, 5)

	for i := 0; i < 3; i++ {
		writeMsg(t, srv1, protocol.Message{
			Header: protocol.Header{MsgID: ids[i], MsgType: protocol.MsgACK},
		})
	}

	// Wait for acks to land.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := w.Replay()
		if len(entries) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// "Crash": close without WAL cleanup.
	b1.Close()
	srv1.Close()
	w.Close()

	// --- Session 2: re-open WAL, create fresh Broker, verify 2 slots active ---
	w2, err := wal.Open(walPath, false)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer w2.Close()

	clientRaw2, srvRaw2 := net.Pipe()
	c2, err := conn.New(conn.Config{
		DialFunc: func() (net.Conn, error) { return clientRaw2, nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	// Drain replayed sends so broker's WriteMsg doesn't block.
	go func() {
		buf := make([]byte, 4096)
		for {
			srvRaw2.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			if _, err := srvRaw2.Read(buf); err != nil {
				return
			}
		}
	}()

	b2, err := New(Config{
		Conn:       c2,
		ACKTimeout: 5 * time.Second,
		WAL:        w2,
	})
	if err != nil {
		t.Fatalf("New broker session 2: %v", err)
	}
	defer b2.Close()
	defer srvRaw2.Close()

	// Wait for replay to populate ring.
	time.Sleep(100 * time.Millisecond)

	b2.mu.Lock()
	active := 0
	for i := range b2.ring {
		if b2.ring[i].active {
			active++
		}
	}
	b2.mu.Unlock()

	if active != 2 {
		t.Errorf("expected 2 active slots after WAL replay, got %d", active)
	}
}
