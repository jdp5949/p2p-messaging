package conn

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

// countingConn wraps a net.Conn and counts Write calls.
type countingConn struct {
	net.Conn
	writes atomic.Int64
}

func (c *countingConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(b)
}

// batchPair creates a Conn with BatchSize>0 backed by a net.Pipe.
// Returns the Conn, a counting wrapper so we can assert Write call count,
// and the raw server-side net.Conn for reading.
func batchPair(t *testing.T, batchSize int, batchTimeout time.Duration) (*Conn, *countingConn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	counting := &countingConn{Conn: client}
	dialDone := false
	c, err := New(Config{
		DialFunc: func() (net.Conn, error) {
			if !dialDone {
				dialDone = true
				return counting, nil
			}
			a, _ := net.Pipe()
			return a, nil
		},
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = server.Close()
	})
	return c, counting, server
}

// readN reads n framed messages from a raw net.Conn using protocol helpers.
func readN(t *testing.T, srv net.Conn, n int) []protocol.Message {
	t.Helper()
	msgs := make([]protocol.Message, 0, n)
	for i := 0; i < n; i++ {
		var hdrBuf [protocol.HeaderSize]byte
		if _, err := io.ReadFull(srv, hdrBuf[:]); err != nil {
			t.Fatalf("readN header %d: %v", i, err)
		}
		hdr := protocol.DecodeHeader(hdrBuf)
		msg := protocol.Message{Header: hdr}
		if hdr.PayloadLen > 0 {
			msg.Payload = make([]byte, hdr.PayloadLen)
			if _, err := io.ReadFull(srv, msg.Payload); err != nil {
				t.Fatalf("readN payload %d: %v", i, err)
			}
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// TestBatchedWriteFlush: write 100 small messages; underlying Write() calls
// should be far fewer than 100 (batching coalesces them).
func TestBatchedWriteFlush(t *testing.T) {
	const n = 100
	c, counting, srv := batchPair(t, 8*1024, 50*time.Millisecond)

	done := make(chan []protocol.Message, 1)
	go func() {
		done <- readN(t, srv, n)
	}()

	payload := bytes.Repeat([]byte("x"), 64) // 64 B each → ~7 KB total for 100 msgs
	for i := 0; i < n; i++ {
		msg := protocol.Message{
			Header:  protocol.Header{MsgID: uint64(i), MsgType: protocol.MsgData},
			Payload: payload,
		}
		if err := c.WriteMsg(msg); err != nil {
			t.Fatalf("WriteMsg %d: %v", i, err)
		}
	}
	// Explicit flush so reader gets all data.
	if err := c.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout reading batched messages")
	}

	writes := counting.writes.Load()
	// With 64 B payload + 8 B header = 72 B per msg, 100 msgs = 7200 B < 8192 batch.
	// bufio should coalesce into very few Write calls (ideally 1 or 2).
	if writes >= int64(n) {
		t.Errorf("expected fewer Write() calls than messages (%d), got %d — batching not working", n, writes)
	}
	t.Logf("Write() calls: %d for %d messages (%.1f%% reduction)", writes, n, 100*(1-float64(writes)/float64(n)))
}

// TestBatchTimeout: write 1 msg without explicit flush; reader should see it
// within ~BatchTimeout*3 (the background tick delivers it).
func TestBatchTimeout(t *testing.T) {
	const timeout = 50 * time.Millisecond
	c, _, srv := batchPair(t, 1*1024*1024, timeout) // huge batch size so threshold never triggers

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = readN(t, srv, 1)
	}()

	msg := protocol.Message{
		Header:  protocol.Header{MsgID: 1, MsgType: protocol.MsgData},
		Payload: []byte("timeout test"),
	}
	if err := c.WriteMsg(msg); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
		// Good: flushed by background ticker
	case <-time.After(timeout * 6):
		t.Fatalf("message not delivered within %v (batch timeout not firing)", timeout*6)
	}
}

// TestFlushExplicit: huge BatchTimeout (1 hour), but Flush() delivers immediately.
func TestFlushExplicit(t *testing.T) {
	c, _, srv := batchPair(t, 1*1024*1024, time.Hour)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = readN(t, srv, 1)
	}()

	msg := protocol.Message{
		Header:  protocol.Header{MsgID: 99, MsgType: protocol.MsgData},
		Payload: []byte("explicit flush"),
	}
	if err := c.WriteMsg(msg); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	select {
	case <-done:
		// Good: explicit flush worked
	case <-time.After(500 * time.Millisecond):
		t.Fatal("message not delivered after explicit Flush()")
	}
}

// TestBatchedReadMatches: write 100 msgs batched, verify all received with correct payload.
// net.Pipe is synchronous so we must read and write concurrently.
func TestBatchedReadMatches(t *testing.T) {
	const n = 100
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
		BatchSize:    32 * 1024,
		BatchTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := New(Config{DialFunc: func() (net.Conn, error) { return server, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = sc.Close()
	})

	payload := bytes.Repeat([]byte("batch"), 200) // 1 KB each

	type result struct {
		idx int
		msg protocol.Message
		err error
	}
	results := make(chan result, n)

	// Read concurrently (net.Pipe blocks writers until readers drain).
	go func() {
		for i := 0; i < n; i++ {
			msg, err := sc.ReadMsg()
			results <- result{i, msg, err}
		}
	}()

	// Write all msgs then flush.
	for i := 0; i < n; i++ {
		msg := protocol.Message{
			Header:  protocol.Header{MsgID: uint64(i), MsgType: protocol.MsgData},
			Payload: payload,
		}
		if err := c.WriteMsg(msg); err != nil {
			t.Fatalf("WriteMsg %d: %v", i, err)
		}
	}
	if err := c.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for i := 0; i < n; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("ReadMsg %d: %v", r.idx, r.err)
			}
			if !bytes.Equal(r.msg.Payload, payload) {
				t.Errorf("msg %d: payload mismatch", r.idx)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for msg %d", i)
		}
	}
}

// TestNoBatchBackwardCompat: BatchSize=0 behaves exactly like before — every
// WriteMsg writes to transport immediately (no bw, existing net.Pipe path).
func TestNoBatchBackwardCompat(t *testing.T) {
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
		// BatchSize == 0 (default)
	})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := New(Config{DialFunc: func() (net.Conn, error) { return server, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = sc.Close()
	})

	want := protocol.Message{
		Header:  protocol.Header{MsgID: 7, MsgType: protocol.MsgData},
		Payload: []byte("no-batch"),
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
	if got.Header.MsgID != want.Header.MsgID || !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("backward compat broken: got %+v", got.Header)
	}
}
