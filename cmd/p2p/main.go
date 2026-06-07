// Command p2p is a croc-style front end: `p2p send` prints a code phrase and
// waits; `p2p <code>` joins. After connecting, an interactive chat opens.
package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/broker"
	"github.com/jdp5949/p2p-messaging/pkg/codephrase"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/crypto"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
	"github.com/jdp5949/p2p-messaging/pkg/transfer"
)

// defaultRelay is the hosted relay. Override with -relay.
const defaultRelay = "129.153.24.33.nip.io:9009"

func main() {
	relayAddr := flag.String("relay", defaultRelay, "relay host:port")
	noTLS := flag.Bool("no-tls", false, "disable TLS to the relay (dev only)")
	idPath := flag.String("id", "~/.p2p/id_ed25519", "identity key path")
	knownPath := flag.String("known", "~/.p2p/known_peers", "known_peers path")
	noCrypto := flag.Bool("no-crypto", false, "disable Noise encryption (dev only)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	var code string
	var initiator bool
	var sendPaths []string
	switch args[0] {
	case "send":
		code = codephrase.Generate()
		initiator = true
		if len(args) > 1 {
			sendPaths = args[1:]
		}
		fmt.Printf("Code is: %s\n", code)
		fmt.Printf("On the other computer run:\n\n    p2p %s\n\n", code)
	case "relay":
		fmt.Fprintln(os.Stderr, "run the relay with: relay -addr :9009  (see cmd/relay)")
		os.Exit(2)
	default:
		code = args[0]
		initiator = false
	}

	sessionID := codephrase.SessionID(code)

	var hsCfg *crypto.HandshakeConfig
	if !*noCrypto {
		id, err := crypto.LoadOrGenerateIdentity(*idPath)
		fatalOn(err, "identity")
		kp, err := crypto.LoadKnownPeers(*knownPath)
		fatalOn(err, "known_peers")
		hsCfg = &crypto.HandshakeConfig{
			Identity:   id,
			KnownPeers: kp,
			PeerName:   sessionID,
			PAKECode:   code,
			Initiator:  initiator,
		}
	}

	dialer := &sessionDialer{
		relayAddr:    *relayAddr,
		sessionID:    sessionID,
		useTLS:       !*noTLS,
		punchTimeout: 6 * time.Second,
	}
	if dialer.useTLS {
		host, _, _ := net.SplitHostPort(*relayAddr)
		dialer.tlsConfig = &tls.Config{ServerName: host}
	}

	fmt.Println("Connecting…")
	c, err := conn.New(conn.Config{
		DialFunc:     dialer.DialFunc,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PingInterval: 10 * time.Second,
		HandshakeCfg: hsCfg,
	})
	fatalOn(err, "connect")

	// After the first successful connect, switch XX -> KK for reconnects.
	if hsCfg != nil {
		hsCfg.PAKECode = ""
	}

	inbound := make(chan transfer.Msg, 128)
	dropped := make(chan struct{})
	quit := make(chan struct{})
	b, err := broker.New(broker.Config{
		Conn:       c,
		ACKTimeout: 30 * time.Second,
		OnInbound: func(m broker.InboundMsg) {
			inbound <- transfer.Msg{ContentType: m.ContentType, Payload: m.Payload}
		},
		OnDisconnected: func() {
			fmt.Fprintln(os.Stderr, "⚠ peer offline — reconnecting (up to 60s)…")
		},
		OnReconnected: func() {
			fmt.Fprintf(os.Stderr, "✓ peer back online (%s)\n", connMode(dialer))
		},
		OnReconnectFailed: func() {
			fmt.Fprintln(os.Stderr, "✗ peer lost — gave up after 60s. Exiting.")
			close(dropped)
		},
		PingInterval: 10 * time.Second,
	})
	fatalOn(err, "broker")

	sendMsg := func(ct protocol.ContentType, p []byte) error {
		_, e := b.Send(ct, protocol.PriorityNormal, p)
		return e
	}

	// File-send mode.
	if len(sendPaths) > 0 {
		fmt.Fprintf(os.Stderr, "Sending %d item(s) (%s)…\n", len(sendPaths), connMode(dialer))
		serr := transfer.Send(sendMsg, inbound, sendPaths, progressBar)
		fmt.Fprintln(os.Stderr)
		_ = b.Close()
		fatalOn(serr, "send")
		fmt.Fprintln(os.Stderr, "✓ sent and verified by peer")
		return
	}

	// Receiver / chat: peek the first inbound message to decide.
	go func() {
		select {
		case first := <-inbound:
			if transferIsHeader(first) {
				merged := make(chan transfer.Msg, 128)
				merged <- first
				go func() {
					for m := range inbound {
						merged <- m
					}
				}()
				dest, _ := os.Getwd()
				saved, rerr := transfer.Receive(sendMsg, merged, dest, promptOverwrite, progressBar)
				fmt.Fprintln(os.Stderr)
				if rerr != nil {
					fmt.Fprintf(os.Stderr, "receive failed: %v\n", rerr)
				} else {
					fmt.Fprintf(os.Stderr, "✓ saved %s (sha256 verified)\n", saved)
				}
				close(quit)
				return
			}
			fmt.Printf("peer> %s\n", first.Payload)
			go chatLoop(b, quit)
			for m := range inbound {
				fmt.Printf("peer> %s\n", m.Payload)
			}
		case <-quit:
		}
	}()

	fmt.Printf("\r✓ Connected — peer online (%s). Type messages or send a file. /quit or Ctrl-C to exit.\n", connMode(dialer))
	go chatLoop(b, quit)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-quit:
		fmt.Fprintln(os.Stderr, "bye")
	case <-dropped:
	}
	_ = b.Close()
}

// connMode reports the human label for the current connection path.
func connMode(d *sessionDialer) string {
	if d.LastDirect() {
		return "direct P2P"
	}
	return "via relay"
}

func chatLoop(b *broker.Broker, quit chan struct{}) {
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := sc.Text()
		if line == "/quit" || line == "/exit" {
			close(quit)
			return
		}
		if _, err := b.Send(protocol.ContentText, protocol.PriorityNormal, []byte(line)); err != nil {
			fmt.Fprintln(os.Stderr, "⚠ not delivered (peer offline?)")
		}
	}
}

// transferIsHeader reports whether m is a transfer HEADER message.
func transferIsHeader(m transfer.Msg) bool {
	if m.ContentType != protocol.ContentJSON {
		return false
	}
	var probe struct {
		T string `json:"t"`
	}
	return json.Unmarshal(m.Payload, &probe) == nil && probe.T == "header"
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:\n  p2p send            # print a code and wait\n  p2p <code>          # join with a code")
	os.Exit(2)
}

func fatalOn(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "p2p: %s: %v\n", what, err)
		os.Exit(1)
	}
}
