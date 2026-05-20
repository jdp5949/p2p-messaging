// Relay server with NAT hole-punch coordination.
//
// Wire protocol (newline-terminated lines):
//
//	Client → Relay   line 1: <session-id>\n
//	Client → Relay   line 2: <peer-info JSON>\n   e.g. {"local_addrs":["192.168.1.5:9000"]}
//	Relay  → Client  line 1: <other-peer JSON>\n   e.g. {"public_addr":"1.2.3.4:5678","local_addrs":[...]}
//	Relay  → Client  line 2: START\n
//	Client → Relay   line 1: PUNCH_OK\n  or  PUNCH_FAIL\n
//
// If either peer sends PUNCH_OK within punchReportTimeout the relay closes
// (the peers are now directly connected).  Otherwise the relay bridges.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/holepunch"
)

const (
	sessionIDMaxBytes  = 256
	sessionWaitTimeout = 60 * time.Second
	punchReportTimeout = 6 * time.Second
)

// peerInfo is what a peer sends in line 2.
type peerInfo struct {
	LocalAddrs []string `json:"local_addrs,omitempty"`
}

// readyPeer is stored while we wait for the matching peer.
type readyPeer struct {
	conn   net.Conn
	reader *bufio.Reader
	info   holepunch.Info
	// matched receives the partner's data.
	matched chan readyPeer
}

var (
	mu      sync.Mutex
	pending = make(map[string]*readyPeer)
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("[RELAY] listen error: %v", err)
	}
	log.Printf("[RELAY] listening on %s", *addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[RELAY] accept error: %v", err)
			continue
		}
		go handle(conn)
	}
}

// handle runs the full lifecycle for one peer connection.
func handle(c net.Conn) {
	limited := io.LimitedReader{R: c, N: sessionIDMaxBytes + 1}
	reader := bufio.NewReader(&limited)

	// Line 1: session ID.
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[RELAY] read session id: %v", err)
		c.Close()
		return
	}
	sessionID := strings.TrimRight(line, "\r\n")

	// Line 2: peer JSON info.
	infoLine, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[RELAY] read peer info session=%s: %v", sessionID, err)
		c.Close()
		return
	}
	var pi peerInfo
	if err := json.Unmarshal([]byte(strings.TrimRight(infoLine, "\r\n")), &pi); err != nil {
		log.Printf("[RELAY] parse peer info session=%s: %v", sessionID, err)
		c.Close()
		return
	}

	myInfo := holepunch.Info{
		PublicAddr: c.RemoteAddr().String(),
		LocalAddrs: pi.LocalAddrs,
	}
	log.Printf("[RELAY] peer %s connected session=%s", c.RemoteAddr(), sessionID)

	me := &readyPeer{
		conn:    c,
		reader:  reader,
		info:    myInfo,
		matched: make(chan readyPeer, 1),
	}

	mu.Lock()
	other, exists := pending[sessionID]
	if !exists {
		pending[sessionID] = me
		mu.Unlock()

		// Wait for second peer.
		select {
		case partner := <-me.matched:
			coordinate(me, &partner, sessionID)
		case <-time.After(sessionWaitTimeout):
			mu.Lock()
			delete(pending, sessionID)
			mu.Unlock()
			log.Printf("[RELAY] session=%s wait timeout", sessionID)
			c.Close()
		}
		return
	}

	// Second peer: unblock first.
	delete(pending, sessionID)
	mu.Unlock()
	log.Printf("[RELAY] matched session=%s", sessionID)
	other.matched <- *me
	// Second peer's goroutine exits; first peer's goroutine drives coordination.
}

// coordinate handles the punch-signalling and optional fallback bridge.
// Called from the FIRST peer's goroutine; it has full ownership of both conns.
func coordinate(first, second *readyPeer, sessionID string) {
	a, b := first.conn, second.conn

	// Send each peer the other's Info.
	bJSON, _ := json.Marshal(second.info)
	aJSON, _ := json.Marshal(first.info)

	if _, err := a.Write(append(bJSON, '\n')); err != nil {
		log.Printf("[RELAY] write to a session=%s: %v", sessionID, err)
		a.Close()
		b.Close()
		return
	}
	if _, err := b.Write(append(aJSON, '\n')); err != nil {
		log.Printf("[RELAY] write to b session=%s: %v", sessionID, err)
		a.Close()
		b.Close()
		return
	}

	// Send START simultaneously.
	a.Write([]byte("START\n")) //nolint:errcheck
	b.Write([]byte("START\n")) //nolint:errcheck
	log.Printf("[RELAY] session=%s START sent", sessionID)

	// Collect punch results.
	type res struct{ ok bool }
	ch := make(chan res, 2)

	readResult := func(conn net.Conn, r *bufio.Reader) {
		conn.SetReadDeadline(time.Now().Add(punchReportTimeout)) //nolint:errcheck
		line, err := r.ReadString('\n')
		ok := err == nil && strings.TrimRight(line, "\r\n") == "PUNCH_OK"
		ch <- res{ok}
	}

	go readResult(a, first.reader)
	go readResult(b, bufio.NewReader(b))

	r1 := <-ch
	r2 := <-ch

	if r1.ok || r2.ok {
		log.Printf("[RELAY] session=%s punch succeeded, closing relay", sessionID)
		a.Close()
		b.Close()
		return
	}

	log.Printf("[RELAY] session=%s punch failed, starting bridge", sessionID)
	// Reset deadlines before bridging.
	a.SetDeadline(time.Time{}) //nolint:errcheck
	b.SetDeadline(time.Time{}) //nolint:errcheck
	bridge(a, b, sessionID)
}

func bridge(a, b net.Conn, sessionID string) {
	defer a.Close()
	defer b.Close()

	type halfCloser interface{ CloseWrite() error }

	var wg sync.WaitGroup
	wg.Add(2)
	pipe := func(dst, src net.Conn) {
		defer wg.Done()
		io.Copy(dst, src)
		if hc, ok := dst.(halfCloser); ok {
			hc.CloseWrite()
		}
	}
	go pipe(a, b)
	go pipe(b, a)
	wg.Wait()
	log.Printf("[RELAY] session=%s bridge closed", sessionID)
}
