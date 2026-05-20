// Bench measures true end-to-end messaging throughput between two peers.
//
// Protocol (so the client measures wire-delivery time, not local TCP buffer):
//   1. Client sends N data messages.
//   2. Client sends a sentinel message: ContentText "BENCH_END:N".
//   3. Server counts data messages. When it sees the sentinel and count == N,
//      it sends back a single ContentText "BENCH_DONE" message.
//   4. Client measures wall time from first send to receiving BENCH_DONE.
//      That elapsed time reflects true end-to-end wire delivery + ACK round-trip.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/broker"
	"github.com/jaypatel/p2p-messaging/pkg/conn"
	"github.com/jaypatel/p2p-messaging/pkg/crypto"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
	"github.com/jaypatel/p2p-messaging/pkg/wal"
)

const sentinelPrefix = "BENCH_END:"
const donePayload = "BENCH_DONE"

func main() {
	listen := flag.String("listen", "", "listen address (server mode)")
	addr := flag.String("addr", "", "dial address (client mode)")
	count := flag.Int("count", 10000, "messages to send")
	size := flag.Int("size", 1024, "payload size bytes")

	useCrypto := flag.Bool("crypto", false, "enable encryption for benchmark")
	idPath := flag.String("id", "~/.p2p/id_ed25519", "path to identity key")
	knownPath := flag.String("known", "~/.p2p/known_peers", "path to known_peers file")
	peerName := flag.String("peer-name", "", "name of remote peer (for key pinning)")
	pakeCode := flag.String("pake", "", "PAKE one-time code for first connect")

	walPath := flag.String("wal", "", "path to WAL file (empty = no persistence)")

	flag.Parse()

	if *listen == "" && *addr == "" {
		fmt.Fprintln(os.Stderr, "usage: bench -listen :9100 OR bench -addr host:9100")
		os.Exit(1)
	}

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

	var w *wal.WAL
	if *walPath != "" {
		expanded, err := expandPath(*walPath)
		if err != nil {
			log.Fatalf("wal path: %v", err)
		}
		w, err = wal.Open(expanded, false)
		if err != nil {
			log.Fatalf("wal: %v", err)
		}
		defer w.Close()
	}

	raw, err := dial(*listen, *addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	clientMode := *addr != ""

	// Server state.
	var srvDataCount atomic.Int64
	var srvDataBytes atomic.Int64
	srvDone := make(chan struct{}, 1)

	var b *broker.Broker

	c, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return raw, nil },
		ReadTimeout:  120 * time.Second,
		HandshakeCfg: hsCfg,
	})
	if err != nil {
		log.Fatalf("conn: %v", err)
	}

	b, err = broker.New(broker.Config{
		Conn:       c,
		ACKTimeout: 30 * time.Second,
		MaxRetries: 3,
		OnInbound: func(msg broker.InboundMsg) {
			if clientMode {
				if msg.ContentType == protocol.ContentText && string(msg.Payload) == donePayload {
					benchDoneFlag.Store(true)
				}
				return
			}
			// Server mode.
			if msg.ContentType == protocol.ContentText && strings.HasPrefix(string(msg.Payload), sentinelPrefix) {
				// Sentinel arrived. Wait until count catches up, bailing only if no progress for 30s.
				expected, _ := strconv.ParseInt(strings.TrimPrefix(string(msg.Payload), sentinelPrefix), 10, 64)
				lastSeen := srvDataCount.Load()
				stallDeadline := time.Now().Add(30 * time.Second)
				for srvDataCount.Load() < expected {
					time.Sleep(20 * time.Millisecond)
					cur := srvDataCount.Load()
					if cur > lastSeen {
						lastSeen = cur
						stallDeadline = time.Now().Add(30 * time.Second)
					}
					if time.Now().After(stallDeadline) {
						break
					}
				}
				select {
				case srvDone <- struct{}{}:
				default:
				}
				return
			}
			srvDataCount.Add(1)
			srvDataBytes.Add(int64(len(msg.Payload)))
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

	if clientMode {
		runClient(b, *count, *size)
	} else {
		runServer(b, &srvDataCount, &srvDataBytes, srvDone)
	}
}

func runClient(b *broker.Broker, count, size int) {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	totalBytes := int64(count) * int64(size)
	fmt.Printf("Bench: %d messages × %d B = %.2f MB total\n", count, size, float64(totalBytes)/1e6)

	start := time.Now()
	for i := 0; i < count; i++ {
		if _, err := b.Send(protocol.ContentBinary, protocol.PriorityNormal, payload); err != nil {
			log.Printf("send err: %v", err)
		}
	}
	sendDone := time.Now()

	// Sentinel.
	if _, err := b.Send(protocol.ContentText, protocol.PriorityNormal, []byte(fmt.Sprintf("%s%d", sentinelPrefix, count))); err != nil {
		log.Fatalf("sentinel send: %v", err)
	}

	// Wait for BENCH_DONE from server. We hooked doneCh inside OnInbound via global.
	// Simple: poll for done via a shared variable. Implement using a global channel set in main.
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if benchDoneFlag.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !benchDoneFlag.Load() {
		fmt.Println("ERROR: did not receive BENCH_DONE within 120s")
		os.Exit(1)
	}
	totalElapsed := time.Since(start)
	sendElapsed := sendDone.Sub(start)
	roundTrip := totalElapsed - sendElapsed

	mbps := float64(totalBytes*8) / 1e6 / totalElapsed.Seconds()

	fmt.Printf("\n--- E2E RESULTS ---\n")
	fmt.Printf("Messages       : %d\n", count)
	fmt.Printf("Payload/msg    : %d B\n", size)
	fmt.Printf("Total data     : %.2f MB\n", float64(totalBytes)/1e6)
	fmt.Printf("Send phase     : %v  (local app → TCP send buffer)\n", sendElapsed.Round(time.Millisecond))
	fmt.Printf("E2E delivery   : %v  (first send → final server ACK)\n", totalElapsed.Round(time.Millisecond))
	fmt.Printf("Drain + ACK    : %v  (TCP queue drain + sentinel round-trip)\n", roundTrip.Round(time.Millisecond))
	fmt.Printf("Throughput     : %.0f msg/s\n", float64(count)/totalElapsed.Seconds())
	fmt.Printf("Wire bandwidth : %.2f Mbps  (%.2f MB/s)\n", mbps, mbps/8)
}

func runServer(b *broker.Broker, dataCount, dataBytes *atomic.Int64, srvDone <-chan struct{}) {
	fmt.Printf("Server: receiving...\n")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var last int64
	for {
		select {
		case <-srvDone:
			// Reply with BENCH_DONE.
			if _, err := b.Send(protocol.ContentText, protocol.PriorityNormal, []byte(donePayload)); err != nil {
				log.Printf("send DONE: %v", err)
			}
			cur := dataCount.Load()
			byt := dataBytes.Load()
			fmt.Printf("\nFinal: recv=%d data msgs, %.2f MB\n", cur, float64(byt)/1e6)
		case <-ticker.C:
			cur := dataCount.Load()
			byt := dataBytes.Load()
			delta := cur - last
			last = cur
			fmt.Printf("recv total=%d (+%d/s)  %.2f MB\n", cur, delta, float64(byt)/1e6)
		}
	}
}

// benchDoneFlag is set by OnInbound when BENCH_DONE arrives.
var benchDoneFlag atomic.Bool

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
