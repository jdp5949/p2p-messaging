package holepunch

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// getFreePort returns a free TCP port on the local machine.
func getFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("getFreePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// TestLoopbackPunch verifies that AttemptPunch establishes a working
// direct connection when both endpoints are on the same host.
//
// We simulate two peers by starting a listener and then calling
// AttemptPunch which will succeed via the listener path.
func TestLoopbackPunch(t *testing.T) {
	// Pick two ports: one for the "remote" listener, one for local port.
	remotePort := getFreePort(t)
	localPort := getFreePort(t)

	// Start the "remote" listener that accepts the punch.
	serverReady := make(chan struct{})
	serverConn := make(chan net.Conn, 1)
	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", remotePort))
		if err != nil {
			t.Errorf("server listen: %v", err)
			close(serverReady)
			return
		}
		defer ln.Close()
		close(serverReady)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		serverConn <- conn
	}()

	<-serverReady

	remote := Info{
		PublicAddr: fmt.Sprintf("127.0.0.1:%d", remotePort),
	}

	conn, err := AttemptPunch(remote, localPort, 3*time.Second)
	if err != nil {
		t.Fatalf("AttemptPunch failed: %v", err)
	}
	defer conn.Close()

	// Round-trip: send "hello" and read it back via server echo.
	srv := <-serverConn
	defer srv.Close()

	go func() {
		io.Copy(srv, srv) // echo
	}()

	msg := []byte("hello-punch")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("got %q want %q", buf, msg)
	}
}

// TestPunchTimeout verifies that AttemptPunch returns an error within the
// timeout when no peer is reachable.
func TestPunchTimeout(t *testing.T) {
	// Use a blackhole address (TEST-NET-1 per RFC 5737 — not routable).
	remote := Info{
		PublicAddr: "192.0.2.1:19999",
	}
	localPort := getFreePort(t)

	timeout := 300 * time.Millisecond
	start := time.Now()
	conn, err := AttemptPunch(remote, localPort, timeout)
	elapsed := time.Since(start)

	if conn != nil {
		conn.Close()
		t.Fatal("expected nil conn on timeout")
	}
	if err == nil {
		t.Fatal("expected error on timeout")
	}
	// Allow generous margin — CI can be slow.
	if elapsed > timeout+2*time.Second {
		t.Fatalf("took %v, expected <= %v", elapsed, timeout+2*time.Second)
	}
	t.Logf("returned after %v with: %v", elapsed, err)
}

// TestReusePort verifies that SO_REUSEPORT allows simultaneous listen and
// dial on the same local port.
func TestReusePort(t *testing.T) {
	port := getFreePort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Listen on the port with SO_REUSEPORT.
	lc := listenConfig()
	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("listen with REUSEPORT: %v", err)
	}
	defer ln.Close()

	// Also dial from the same port (via dialDirect) to a separate listener.
	remotePort := getFreePort(t)
	remoteLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", remotePort))
	if err != nil {
		t.Fatalf("remote listen: %v", err)
	}
	defer remoteLn.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := dialDirect(ctx, port, fmt.Sprintf("127.0.0.1:%d", remotePort))
		if err != nil {
			done <- err
			return
		}
		conn.Close()
		done <- nil
	}()

	if err := <-done; err != nil {
		t.Fatalf("dialDirect with reused port: %v", err)
	}
}
