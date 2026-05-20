package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/holepunch"
)

// startTestRelay boots the relay on a random port and returns the address.
func startTestRelay(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("relay listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handle(conn)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return addr
}

// connectPeer dials the relay, sends sessionID + peerInfo JSON, and returns
// the conn + buffered reader positioned right after the two sent lines.
func connectPeer(t *testing.T, relayAddr, sessionID string, localAddrs []string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", relayAddr)
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}

	info := peerInfo{LocalAddrs: localAddrs}
	infoJSON, _ := json.Marshal(info)

	fmt.Fprintf(conn, "%s\n", sessionID)
	fmt.Fprintf(conn, "%s\n", infoJSON)

	return conn, bufio.NewReader(conn)
}

// readLine reads one newline-terminated line with a 3-second deadline.
func readLine(t *testing.T, conn net.Conn, r *bufio.Reader) string {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second)) //nolint:errcheck
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("readLine: %v", err)
	}
	conn.SetReadDeadline(time.Time{}) //nolint:errcheck
	return strings.TrimRight(line, "\r\n")
}

// TestTwoPeersExchangeInfoAndReceiveStart verifies the relay sends each peer
// the other's Info JSON and then "START".
func TestTwoPeersExchangeInfoAndReceiveStart(t *testing.T) {
	relayAddr := startTestRelay(t)
	sessionID := "test-session-exchange"

	connA, rA := connectPeer(t, relayAddr, sessionID, []string{"192.168.1.10:8001"})
	defer connA.Close()
	connB, rB := connectPeer(t, relayAddr, sessionID, []string{"192.168.1.20:8002"})
	defer connB.Close()

	// A receives B's Info.
	lineA := readLine(t, connA, rA)
	var infoForA holepunch.Info
	if err := json.Unmarshal([]byte(lineA), &infoForA); err != nil {
		t.Fatalf("A parse info JSON: %v", err)
	}
	if infoForA.PublicAddr == "" {
		t.Error("A: expected non-empty PublicAddr")
	}
	if len(infoForA.LocalAddrs) != 1 || infoForA.LocalAddrs[0] != "192.168.1.20:8002" {
		t.Errorf("A: unexpected LocalAddrs: %v", infoForA.LocalAddrs)
	}

	// B receives A's Info.
	lineB := readLine(t, connB, rB)
	var infoForB holepunch.Info
	if err := json.Unmarshal([]byte(lineB), &infoForB); err != nil {
		t.Fatalf("B parse info JSON: %v", err)
	}
	if infoForB.PublicAddr == "" {
		t.Error("B: expected non-empty PublicAddr")
	}

	// Both receive START.
	startA := readLine(t, connA, rA)
	startB := readLine(t, connB, rB)
	if startA != "START" {
		t.Errorf("A: expected START, got %q", startA)
	}
	if startB != "START" {
		t.Errorf("B: expected START, got %q", startB)
	}
}

// TestRelayFallbackBridge verifies that after both peers send PUNCH_FAIL,
// the relay bridges the connections (data flows through).
func TestRelayFallbackBridge(t *testing.T) {
	relayAddr := startTestRelay(t)
	sessionID := "test-session-fallback"

	connA, rA := connectPeer(t, relayAddr, sessionID, nil)
	defer connA.Close()
	connB, rB := connectPeer(t, relayAddr, sessionID, nil)
	defer connB.Close()

	// Consume info + START on both sides.
	readLine(t, connA, rA) // other's Info
	readLine(t, connB, rB)
	readLine(t, connA, rA) // START
	readLine(t, connB, rB)

	// Both report punch failure.
	fmt.Fprintf(connA, "PUNCH_FAIL\n")
	fmt.Fprintf(connB, "PUNCH_FAIL\n")

	// Now the relay bridge should be active — send data A→B and B→A.
	msg := "hello-via-relay\n"
	done := make(chan error, 2)

	go func() {
		_, err := fmt.Fprint(connA, msg)
		done <- err
	}()

	go func() {
		connB.SetReadDeadline(time.Now().Add(3 * time.Second)) //nolint:errcheck
		got, err := rB.ReadString('\n')
		if err != nil {
			done <- fmt.Errorf("B read: %w", err)
			return
		}
		if got != msg {
			done <- fmt.Errorf("B got %q want %q", got, msg)
			return
		}
		done <- nil
	}()

	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

// TestRelayPunchOKClosesRelay verifies that the relay closes its end when
// a peer sends PUNCH_OK.
func TestRelayPunchOKClosesRelay(t *testing.T) {
	relayAddr := startTestRelay(t)
	sessionID := "test-session-punchok"

	connA, rA := connectPeer(t, relayAddr, sessionID, nil)
	defer connA.Close()
	connB, rB := connectPeer(t, relayAddr, sessionID, nil)
	defer connB.Close()

	readLine(t, connA, rA)
	readLine(t, connB, rB)
	readLine(t, connA, rA)
	readLine(t, connB, rB)

	fmt.Fprintf(connA, "PUNCH_OK\n")
	fmt.Fprintf(connB, "PUNCH_OK\n")

	// After PUNCH_OK the relay should close both connections.
	connA.SetReadDeadline(time.Now().Add(3 * time.Second)) //nolint:errcheck
	buf := make([]byte, 4)
	_, err := connA.Read(buf)
	if err == nil {
		t.Error("expected relay to close connA after PUNCH_OK")
	}
}
