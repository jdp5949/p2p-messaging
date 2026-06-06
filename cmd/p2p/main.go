// Command p2p is a croc-style front end: `p2p send` prints a code phrase and
// waits; `p2p <code>` joins. After connecting, an interactive chat opens.
package main

import (
	"bufio"
	"crypto/tls"
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
	switch args[0] {
	case "send":
		code = codephrase.Generate()
		initiator = true
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

	fmt.Println("Connected. Type messages, Ctrl-C to quit.")

	dropped := make(chan struct{})
	b, err := broker.New(broker.Config{
		Conn:       c,
		ACKTimeout: 30 * time.Second,
		OnInbound: func(m broker.InboundMsg) {
			fmt.Printf("peer> %s\n", m.Payload)
		},
		OnReconnectFailed: func() {
			fmt.Fprintln(os.Stderr, "\npeer lost, dropping after 60s")
			close(dropped)
		},
		PingInterval: 10 * time.Second,
	})
	fatalOn(err, "broker")

	go chatLoop(b)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-dropped:
	}
	_ = b.Close()
}

func chatLoop(b *broker.Broker) {
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		if _, err := b.Send(protocol.ContentText, protocol.PriorityNormal, sc.Bytes()); err != nil {
			fmt.Fprintf(os.Stderr, "send: %v\n", err)
		}
	}
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
