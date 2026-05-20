package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	sessionIDMaxBytes  = 256
	sessionWaitTimeout = 60 * time.Second
)

type waitingPeer struct {
	conn net.Conn
	ch   chan net.Conn
}

var (
	mu      sync.Mutex
	pending = make(map[string]*waitingPeer)
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

func handle(c net.Conn) {
	// Limit session ID read to prevent memory exhaustion.
	limited := io.LimitedReader{R: c, N: sessionIDMaxBytes}
	reader := bufio.NewReader(&limited)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[RELAY] read session id error: %v", err)
		c.Close()
		return
	}
	sessionID := strings.TrimRight(line, "\r\n")
	log.Printf("[RELAY] peer %s connected session=%s", c.RemoteAddr(), sessionID)

	mu.Lock()
	wp, exists := pending[sessionID]
	if !exists {
		ch := make(chan net.Conn, 1)
		pending[sessionID] = &waitingPeer{conn: c, ch: ch}
		mu.Unlock()
		// Wait for a matching peer with timeout to prevent goroutine leak.
		select {
		case peer := <-ch:
			bridge(c, peer, sessionID)
		case <-time.After(sessionWaitTimeout):
			mu.Lock()
			delete(pending, sessionID)
			mu.Unlock()
			log.Printf("[RELAY] session=%s timed out waiting for peer", sessionID)
			c.Close()
		}
		return
	}
	delete(pending, sessionID)
	mu.Unlock()

	log.Printf("[RELAY] matched session=%s", sessionID)
	wp.ch <- c
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
	log.Printf("[RELAY] session=%s closed", sessionID)
}
