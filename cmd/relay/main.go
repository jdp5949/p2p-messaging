package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"strings"
	"sync"
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

func handle(conn net.Conn) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[RELAY] read session id error: %v", err)
		conn.Close()
		return
	}
	sessionID := strings.TrimRight(line, "\r\n")
	log.Printf("[RELAY] peer %s connected session=%s", conn.RemoteAddr(), sessionID)

	mu.Lock()
	wp, exists := pending[sessionID]
	if !exists {
		ch := make(chan net.Conn, 1)
		pending[sessionID] = &waitingPeer{conn: conn, ch: ch}
		mu.Unlock()
		peer := <-ch
		bridge(conn, peer, sessionID)
		return
	}
	delete(pending, sessionID)
	mu.Unlock()

	log.Printf("[RELAY] matched session=%s", sessionID)
	wp.ch <- conn
}

func bridge(a, b net.Conn, sessionID string) {
	defer a.Close()
	defer b.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	copy := func(dst, src net.Conn) {
		defer wg.Done()
		io.Copy(dst, src)
		dst.(*net.TCPConn).CloseWrite()
	}

	go copy(a, b)
	go copy(b, a)
	wg.Wait()
	log.Printf("[RELAY] session=%s closed", sessionID)
}
