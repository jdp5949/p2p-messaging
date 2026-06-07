package rendezvous

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// mockRelay implements just enough of the relay protocol for one client:
// it reads the session id + peer info, replies with a forged partner Info
// whose only advertised endpoint is unreachable (127.0.0.1:1), sends START,
// consumes the client's PUNCH_OK/FAIL line, then echoes any further bytes.
//
// Because the advertised partner endpoint is unreachable and no second peer
// dials this client's local port, AttemptPunch deterministically fails and the
// client takes the bridge fallback — exercising the protocol parsing and the
// fallback path without the loopback simultaneous-open race of a real punch.
// (The direct-punch path is covered by the live end-to-end test.)
func mockRelay(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		if _, err := r.ReadString('\n'); err != nil { // session id
			return
		}
		if _, err := r.ReadString('\n'); err != nil { // peer info
			return
		}
		if _, err := io.WriteString(c, `{"public_addr":"127.0.0.1:1"}`+"\n"); err != nil {
			return
		}
		if _, err := io.WriteString(c, "START\n"); err != nil {
			return
		}
		if _, err := r.ReadString('\n'); err != nil { // PUNCH_OK / PUNCH_FAIL
			return
		}
		if _, err := io.WriteString(c, "BRIDGE\n"); err != nil { // relay decision
			return
		}
		io.Copy(c, r) // echo the bridged data path back to the client
	}()
	return ln.Addr().String()
}

func TestDialFallbackBridge(t *testing.T) {
	relay := mockRelay(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := Dial(ctx, Options{
		RelayAddr:    relay,
		SessionID:    "testsession00000",
		PunchTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer res.Conn.Close()

	if !res.UsedFallback {
		t.Fatalf("expected fallback bridge path, got direct (UsedFallback=false)")
	}
	if res.Partner.PublicAddr != "127.0.0.1:1" {
		t.Fatalf("partner addr = %q, want 127.0.0.1:1", res.Partner.PublicAddr)
	}

	// Data path round-trips through the bridged relay conn (mock echoes).
	if _, err := res.Conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	res.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(res.Conn, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("got %q want ping", buf)
	}
}
