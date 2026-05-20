package conn

import (
	"bytes"
	"net"
	"sync"
	"testing"

	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

func pipePair(t *testing.T) (*Conn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	dialDone := false
	c, err := New(Config{
		DialFunc: func() (net.Conn, error) {
			if !dialDone {
				dialDone = true
				return client, nil
			}
			a, _ := net.Pipe()
			return a, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = server.Close()
	})
	return c, server
}

func serverConn(t *testing.T, raw net.Conn) *Conn {
	t.Helper()
	sc, err := New(Config{DialFunc: func() (net.Conn, error) { return raw, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

// TestRoundTrip: single WriteMsg / ReadMsg.
func TestRoundTrip(t *testing.T) {
	c, server := pipePair(t)
	sc := serverConn(t, server)
	defer sc.Close()

	want := protocol.Message{
		Header:  protocol.Header{MsgID: 42, MsgType: protocol.MsgData},
		Payload: []byte("hello p2p"),
	}

	errCh := make(chan error, 1)
	go func() { errCh <- c.WriteMsg(want) }()

	got, err := sc.ReadMsg()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if got.Header.MsgID != want.Header.MsgID {
		t.Errorf("MsgID: got %d want %d", got.Header.MsgID, want.Header.MsgID)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("Payload mismatch")
	}
}

// TestConcurrentWrite: multiple goroutines write simultaneously (race-safe).
func TestConcurrentWrite(t *testing.T) {
	c, server := pipePair(t)

	// drain server in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := serverConn(t, server)
		defer sc.Close()
		for i := 0; i < 10; i++ {
			if _, err := sc.ReadMsg(); err != nil {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := protocol.Message{
				Header:  protocol.Header{MsgID: uint64(id), MsgType: protocol.MsgData},
				Payload: []byte("concurrent"),
			}
			if err := c.WriteMsg(msg); err != nil {
				t.Errorf("WriteMsg %d: %v", id, err)
			}
		}(i)
	}
	wg.Wait()
	<-done
}

// TestReadClosedConn: ReadMsg on closed conn returns error.
func TestReadClosedConn(t *testing.T) {
	c, server := pipePair(t)
	_ = server.Close()
	_ = c.Close()

	_, err := c.ReadMsg()
	if err == nil {
		t.Fatal("expected error reading from closed conn")
	}
}

// TestLargePayload: 1MB payload round-trip.
func TestLargePayload(t *testing.T) {
	c, server := pipePair(t)
	sc := serverConn(t, server)
	defer sc.Close()

	payload := bytes.Repeat([]byte("x"), 1<<20) // 1 MB
	want := protocol.Message{
		Header:  protocol.Header{MsgID: 1, MsgType: protocol.MsgData},
		Payload: payload,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- c.WriteMsg(want) }()

	got, err := sc.ReadMsg()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("large payload mismatch: got len %d want %d", len(got.Payload), len(want.Payload))
	}
}
