package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/broker"
	"github.com/jaypatel/p2p-messaging/pkg/conn"
	"github.com/jaypatel/p2p-messaging/pkg/crypto"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
	"github.com/jaypatel/p2p-messaging/pkg/wal"
)

func main() {
	listen := flag.String("listen", "", "listen address (server mode)")
	addr := flag.String("addr", "", "dial address (client mode)")
	count := flag.Int("count", 10000, "messages to send")
	size := flag.Int("size", 1024, "payload size bytes")

	// crypto flags (default OFF for clean baseline)
	useCrypto := flag.Bool("crypto", false, "enable encryption for benchmark")
	idPath := flag.String("id", "~/.p2p/id_ed25519", "path to identity key")
	knownPath := flag.String("known", "~/.p2p/known_peers", "path to known_peers file")
	peerName := flag.String("peer-name", "", "name of remote peer (for key pinning)")
	pakeCode := flag.String("pake", "", "PAKE one-time code for first connect")

	// WAL flag
	walPath := flag.String("wal", "", "path to WAL file (empty = no persistence)")

	flag.Parse()

	if *listen == "" && *addr == "" {
		fmt.Fprintln(os.Stderr, "usage: bench -listen :9100 OR bench -addr host:9100")
		os.Exit(1)
	}

	// Build optional handshake config.
	var hsCfg *crypto.HandshakeConfig
	if *useCrypto {
		id, err := crypto.LoadOrGenerateIdentity(*idPath)
		if err != nil {
			log.Fatalf("identity: %v", err)
		}
		kp, err := crypto.LoadKnownPeers(*knownPath)
		if err != nil {
			log.Fatalf("known_peers: %v", err)
		}
		hsCfg = &crypto.HandshakeConfig{
			Identity:   id,
			KnownPeers: kp,
			PeerName:   *peerName,
			PAKECode:   *pakeCode,
			Initiator:  *addr != "",
		}
	}

	// Build optional WAL.
	var w *wal.WAL
	if *walPath != "" {
		expanded, err := expandPath(*walPath)
		if err != nil {
			log.Fatalf("wal path: %v", err)
		}
		w, err = wal.Open(expanded, false) // fsync off for bench perf
		if err != nil {
			log.Fatalf("wal: %v", err)
		}
		defer w.Close()
	}

	raw, err := dial(*listen, *addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	var recvCount atomic.Int64
	var recvBytes atomic.Int64
	var firstRecv atomic.Int64
	var lastRecv atomic.Int64

	c, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return raw, nil },
		ReadTimeout:  60 * time.Second,
		HandshakeCfg: hsCfg,
	})
	if err != nil {
		log.Fatalf("conn: %v", err)
	}

	b, err := broker.New(broker.Config{
		Conn:       c,
		ACKTimeout: 30 * time.Second,
		MaxRetries: 3,
		OnInbound: func(msg broker.InboundMsg) {
			n := recvCount.Add(1)
			recvBytes.Add(int64(len(msg.Payload)))
			now := time.Now().UnixNano()
			if n == 1 {
				firstRecv.Store(now)
			}
			lastRecv.Store(now)
		},
		OnDead: func(msgID uint64, err error) {
			log.Printf("[DEAD] msgID=%d err=%v", msgID, err)
		},
		WAL: w,
	})
	if err != nil {
		log.Fatalf("broker: %v", err)
	}
	defer b.Close()

	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// If client mode (-addr): send messages and print stats.
	// If server mode (-listen): just receive and report stats every second.
	if *addr != "" {
		fmt.Printf("Sending %d messages × %d bytes = %.2f MB total...\n",
			*count, *size, float64(*count**size)/1e6)

		start := time.Now()
		for i := 0; i < *count; i++ {
			if _, err := b.Send(protocol.ContentBinary, protocol.PriorityNormal, payload); err != nil {
				log.Printf("send err: %v", err)
			}
		}
		sendElapsed := time.Since(start)

		fmt.Printf("\n--- SEND STATS ---\n")
		fmt.Printf("Messages sent : %d\n", *count)
		fmt.Printf("Payload/msg   : %d bytes\n", *size)
		fmt.Printf("Total data    : %.2f MB\n", float64(*count**size)/1e6)
		fmt.Printf("Send time     : %v\n", sendElapsed.Round(time.Millisecond))
		fmt.Printf("Throughput    : %.0f msg/s\n", float64(*count)/sendElapsed.Seconds())
		fmt.Printf("Bandwidth     : %.2f MB/s\n", float64(*count**size)/1e6/sendElapsed.Seconds())

		// Wait up to 30s for ACKs.
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if recvCount.Load() >= int64(*count) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		rc := recvCount.Load()
		rb := recvBytes.Load()
		fr := firstRecv.Load()
		lr := lastRecv.Load()

		fmt.Printf("\n--- RECV STATS (from remote) ---\n")
		fmt.Printf("Messages recv : %d / %d\n", rc, *count)
		fmt.Printf("Bytes recv    : %.2f MB\n", float64(rb)/1e6)
		if fr > 0 && lr > fr {
			elapsed := time.Duration(lr - fr)
			fmt.Printf("Recv duration : %v\n", elapsed.Round(time.Millisecond))
			fmt.Printf("Recv rate     : %.0f msg/s\n", float64(rc)/elapsed.Seconds())
			fmt.Printf("Recv BW       : %.2f MB/s\n", float64(rb)/1e6/elapsed.Seconds())
		}
	} else {
		// Server mode: just receive.
		fmt.Printf("Listening. Receiving messages...\n")
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var last int64
		for range ticker.C {
			cur := recvCount.Load()
			byt := recvBytes.Load()
			delta := cur - last
			last = cur
			fmt.Printf("recv total=%d (+%d/s)  %.2f MB total\n", cur, delta, float64(byt)/1e6)
		}
	}
}

func dial(listenAddr, dialAddr string) (net.Conn, error) {
	if listenAddr != "" {
		ln, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return nil, err
		}
		fmt.Printf("Listening on %s...\n", listenAddr)
		c, err := ln.Accept()
		ln.Close()
		return c, err
	}
	return net.Dial("tcp", dialAddr)
}

// expandPath resolves ~ to the user's home directory.
func expandPath(path string) (string, error) {
	if len(path) == 0 || path[0] != '~' {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return home + path[1:], nil
}
