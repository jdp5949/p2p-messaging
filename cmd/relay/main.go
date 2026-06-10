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
	"crypto/tls"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/holepunch"
)

const (
	sessionIDMaxBytes    = 256
	controlPhaseMaxBytes = 4096 // session-id line + peer JSON; Windows machines with many adapters can have large JSON
	sessionWaitTimeout   = 60 * time.Second
	punchReportTimeout   = 6 * time.Second
)

// peerInfo is what a peer sends in line 2.
type peerInfo struct {
	LocalAddrs []string `json:"local_addrs,omitempty"`
}

// readyPeer is stored while we wait for the matching peer.
type readyPeer struct {
	conn   net.Conn
	reader *bufio.Reader
	lr     *io.LimitedReader // the byte cap used for the control phase; lifted before bridging
	info   holepunch.Info
	// matched receives the partner's data.
	matched chan readyPeer
}

var (
	mu      sync.Mutex
	pending = make(map[string]*readyPeer)
)

// multiFlag collects a repeatable string flag.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// wrapTLS returns a TLS listener over base when cfg is non-nil, else base.
func wrapTLS(base net.Listener, cfg *tls.Config) net.Listener {
	if cfg == nil {
		return base
	}
	return tls.NewListener(base, cfg)
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	useTLS := flag.Bool("tls", false, "enable TLS on the listener")
	// -tls-cert / -tls-key may be repeated (paired by order) to serve multiple
	// hostnames; TLS picks the matching cert per connection via SNI.
	var certPaths, keyPaths multiFlag
	flag.Var(&certPaths, "tls-cert", "path to TLS fullchain cert (PEM); repeatable")
	flag.Var(&keyPaths, "tls-key", "path to TLS private key (PEM); repeatable")
	flag.Parse()

	base, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("[RELAY] listen error: %v", err)
	}

	var ln net.Listener = base
	if *useTLS {
		if len(certPaths) == 0 || len(certPaths) != len(keyPaths) {
			log.Fatalf("[RELAY] -tls requires matching -tls-cert and -tls-key (repeatable)")
		}
		var certs []tls.Certificate
		for i := range certPaths {
			cert, err := tls.LoadX509KeyPair(certPaths[i], keyPaths[i])
			if err != nil {
				log.Fatalf("[RELAY] load cert %s: %v", certPaths[i], err)
			}
			certs = append(certs, cert)
		}
		ln = wrapTLS(base, &tls.Config{Certificates: certs})
		log.Printf("[RELAY] TLS enabled (%d cert(s), SNI-selected)", len(certs))
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
	// Cap reads during the control phase to guard against unbounded input.
	// controlPhaseMaxBytes covers both the session-id line and the peer JSON
	// (which can be large on Windows with many virtual network adapters).
	// The cap is lifted before bridging (see coordinate).
	limited := &io.LimitedReader{R: c, N: controlPhaseMaxBytes}
	reader := bufio.NewReader(limited)

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
		lr:      limited,
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
	type res struct {
		ok bool
		r  *bufio.Reader // reader retained to drain buffered bytes into bridge
	}
	ch := make(chan res, 2)

	readResult := func(conn net.Conn, r *bufio.Reader) {
		conn.SetReadDeadline(time.Now().Add(punchReportTimeout)) //nolint:errcheck
		line, err := r.ReadString('\n')
		ok := err == nil && strings.TrimRight(line, "\r\n") == "PUNCH_OK"
		ch <- res{ok, r}
	}

	bReader := bufio.NewReader(b)
	go readResult(a, first.reader)
	go readResult(b, bReader)

	r1 := <-ch
	r2 := <-ch

	// Clear the read deadlines set during result collection.
	a.SetDeadline(time.Time{}) //nolint:errcheck
	b.SetDeadline(time.Time{}) //nolint:errcheck

	// Unanimous decision: DIRECT only when BOTH peers validated their direct
	// link. Otherwise BRIDGE — guaranteeing both peers use the same, working
	// transport (no asymmetric half-open direct conn).
	if r1.ok && r2.ok {
		log.Printf("[RELAY] session=%s punch validated both sides, going DIRECT", sessionID)
		a.Write([]byte("DIRECT\n")) //nolint:errcheck
		b.Write([]byte("DIRECT\n")) //nolint:errcheck
		a.Close()
		b.Close()
		return
	}

	log.Printf("[RELAY] session=%s punch not validated, BRIDGE", sessionID)
	a.Write([]byte("BRIDGE\n")) //nolint:errcheck
	b.Write([]byte("BRIDGE\n")) //nolint:errcheck
	// Lift the control-phase byte cap on the first peer's reader; otherwise the
	// bridge would truncate that peer's stream at sessionIDMaxBytes.
	if first.lr != nil {
		first.lr.N = 1 << 62
	}
	// Pass bufio readers so any buffered bytes flow through the bridge.
	bridge2(a, b, first.reader, bReader, sessionID)
}

// bridge2 forwards bytes between a and b, using bufio.Readers that may already
// contain buffered data (read during the PUNCH result phase).
func bridge2(a, b net.Conn, rA, rB *bufio.Reader, sessionID string) {
	defer a.Close()
	defer b.Close()

	type halfCloser interface{ CloseWrite() error }

	var wg sync.WaitGroup
	wg.Add(2)
	pipe := func(dst net.Conn, src io.Reader) {
		defer wg.Done()
		io.Copy(dst, src)
		if hc, ok := dst.(halfCloser); ok {
			hc.CloseWrite()
		}
	}
	go pipe(b, rA)
	go pipe(a, rB)
	wg.Wait()
	log.Printf("[RELAY] session=%s bridge closed", sessionID)
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
