package broker

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/chunker"
	"github.com/jdp5949/p2p-messaging/pkg/compress"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// pipeConn wraps net.Pipe ends as conn.Conn.
func pipeConn(t *testing.T) (client *conn.Conn, srvConn net.Conn) {
	t.Helper()
	clientRaw, srvRaw := net.Pipe()
	c, err := conn.New(conn.Config{
		DialFunc: func() (net.Conn, error) { return clientRaw, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, srvRaw
}

// readOneMsg reads a single framed message from a raw net.Conn.
func readOneMsg(t *testing.T, raw net.Conn) protocol.Message {
	t.Helper()
	var hdrBuf [protocol.HeaderSize]byte
	if _, err := io.ReadFull(raw, hdrBuf[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}
	hdr := protocol.DecodeHeader(hdrBuf)
	msg := protocol.Message{Header: hdr}
	if hdr.PayloadLen > 0 {
		msg.Payload = make([]byte, hdr.PayloadLen)
		if _, err := io.ReadFull(raw, msg.Payload); err != nil {
			t.Fatalf("read payload: %v", err)
		}
	}
	return msg
}

// writeMsg writes a framed message to a raw net.Conn.
func writeMsg(t *testing.T, raw net.Conn, msg protocol.Message) {
	t.Helper()
	msg.Header.PayloadLen = uint32(len(msg.Payload))
	hdr := protocol.EncodeHeader(msg.Header)
	if _, err := raw.Write(hdr[:]); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if len(msg.Payload) > 0 {
		if _, err := raw.Write(msg.Payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}
}

// TestSendACKFreesSlot: send a msg, receive ACK → slot freed.
func TestSendACKFreesSlot(t *testing.T) {
	c, srv := pipeConn(t)
	defer srv.Close()

	b, err := New(Config{
		Conn:       c,
		ACKTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Send message synchronously (net.Pipe has no buffer, so server must read first).
	// Run send in background; server reads concurrently.
	type sendResult struct {
		id  uint64
		err error
	}
	resCh := make(chan sendResult, 1)
	go func() {
		id, err := b.Send(protocol.ContentText, protocol.PriorityNormal, []byte("hello"))
		resCh <- sendResult{id, err}
	}()

	// Server reads the fragment.
	sent := readOneMsg(t, srv)

	res := <-resCh
	if res.err != nil {
		t.Fatalf("send error: %v", res.err)
	}

	// Server sends ACK.
	writeMsg(t, srv, protocol.Message{
		Header: protocol.Header{
			MsgID:   sent.Header.MsgID,
			MsgType: protocol.MsgACK,
		},
	})

	// Wait for slot to be freed.
	time.Sleep(100 * time.Millisecond)

	b.mu.Lock()
	active := 0
	for i := range b.ring {
		if b.ring[i].active {
			active++
		}
	}
	b.mu.Unlock()

	_ = res.id
	if active != 0 {
		t.Errorf("expected 0 active slots after ACK, got %d", active)
	}
}

// TestTimeoutIncrementsRetry: send, no ACK → retry count increments.
func TestTimeoutIncrementsRetry(t *testing.T) {
	c, srv := pipeConn(t)
	defer srv.Close()

	b, err := New(Config{
		Conn:       c,
		ACKTimeout: 100 * time.Millisecond,
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Drain messages from server so writes don't block.
	go func() {
		for {
			buf := make([]byte, 4096)
			if _, err := srv.Read(buf); err != nil {
				return
			}
		}
	}()

	_, err = b.Send(protocol.ContentText, protocol.PriorityNormal, []byte("ping"))
	if err != nil {
		t.Fatal(err)
	}

	// Wait for at least one retry cycle.
	time.Sleep(500 * time.Millisecond)

	b.mu.Lock()
	maxRetries := 0
	for i := range b.ring {
		if b.ring[i].active && b.ring[i].retries > maxRetries {
			maxRetries = b.ring[i].retries
		}
	}
	b.mu.Unlock()

	if maxRetries == 0 {
		t.Error("expected retries > 0 after timeout")
	}
}

// TestExhaustRetriesCallsOnDead: send, exhaust retries → OnDead called.
func TestExhaustRetriesCallsOnDead(t *testing.T) {
	c, srv := pipeConn(t)
	defer srv.Close()

	var deadCalled atomic.Int32
	var deadMu sync.Mutex
	var deadIDs []uint64

	// Drain messages so writes don't block.
	go func() {
		for {
			buf := make([]byte, 4096)
			if _, err := srv.Read(buf); err != nil {
				return
			}
		}
	}()

	b, err := New(Config{
		Conn:       c,
		ACKTimeout: 50 * time.Millisecond,
		MaxRetries: 2,
		OnDead: func(msgID uint64, err error) {
			deadCalled.Add(1)
			deadMu.Lock()
			deadIDs = append(deadIDs, msgID)
			deadMu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	msgID, err := b.Send(protocol.ContentText, protocol.PriorityNormal, []byte("dead letter"))
	if err != nil {
		t.Fatal(err)
	}

	// Wait enough for MaxRetries (2) to exhaust.
	// backoff: retry0=50ms(ack timeout), retry1=2s, retry2=4s... but our custom timeout is 50ms
	// Actually with ACKTimeout=50ms and MaxRetries=2:
	// - First send at t=0, sendTime=t0
	// - retryLoop ticks every 1s — but ACKTimeout=50ms means it should fire fast
	// Wait 3 seconds to be safe.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if deadCalled.Load() > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if deadCalled.Load() == 0 {
		t.Error("OnDead never called after exhausting retries")
	}

	deadMu.Lock()
	found := false
	for _, id := range deadIDs {
		if id == msgID {
			found = true
		}
	}
	deadMu.Unlock()

	if !found {
		t.Errorf("OnDead called but msgID %d not in dead list %v", msgID, deadIDs)
	}
}

// TestInboundReassembly: receive multi-fragment message → OnInbound called with full payload.
func TestInboundReassembly(t *testing.T) {
	c, srv := pipeConn(t)
	defer srv.Close()

	var inboundMu sync.Mutex
	var received []InboundMsg

	b, err := New(Config{
		Conn:       c,
		ACKTimeout: 5 * time.Second,
		OnInbound: func(msg InboundMsg) {
			inboundMu.Lock()
			received = append(received, msg)
			inboundMu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Build a multi-fragment payload using chunker.
	enc, err := compress.NewEncoder()
	if err != nil {
		t.Fatal(err)
	}

	// Use a payload larger than ChunkSize to force fragmentation.
	largePayload := make([]byte, chunker.ChunkSize+100)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}

	frags, err := chunker.Split(42, protocol.ContentBinary, largePayload, enc)
	if err != nil {
		t.Fatal(err)
	}

	// Drain ACK that broker sends back so broker goroutine doesn't block.
	go func() {
		for {
			buf := make([]byte, 4096)
			if _, err := srv.Read(buf); err != nil {
				return
			}
		}
	}()

	// Server sends fragments to client.
	for _, f := range frags {
		writeMsg(t, srv, f)
	}

	// Wait for reassembly and ACK.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		inboundMu.Lock()
		n := len(received)
		inboundMu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	inboundMu.Lock()
	defer inboundMu.Unlock()

	if len(received) == 0 {
		t.Fatal("OnInbound never called")
	}

	got := received[0]
	if got.MsgID != 42 {
		t.Errorf("MsgID: want 42, got %d", got.MsgID)
	}
	if got.ContentType != protocol.ContentBinary {
		t.Errorf("ContentType: want ContentBinary, got %v", got.ContentType)
	}
	if len(got.Payload) != len(largePayload) {
		t.Errorf("Payload len: want %d, got %d", len(largePayload), len(got.Payload))
	}
}

// TestPingPong: server sends Ping → broker replies Pong.
func TestPingPong(t *testing.T) {
	c, srv := pipeConn(t)
	defer srv.Close()

	b, err := New(Config{
		Conn:       c,
		ACKTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Server sends ping.
	writeMsg(t, srv, protocol.Message{
		Header: protocol.Header{
			MsgID:   99,
			MsgType: protocol.MsgPing,
		},
	})

	// Expect pong back.
	pong := readOneMsg(t, srv)
	if pong.Header.MsgType != protocol.MsgPong {
		t.Errorf("expected MsgPong, got %v", pong.Header.MsgType)
	}
}
