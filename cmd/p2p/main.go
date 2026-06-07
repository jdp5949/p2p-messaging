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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/broker"
	"github.com/jdp5949/p2p-messaging/pkg/codephrase"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/crypto"
	"github.com/jdp5949/p2p-messaging/pkg/humanize"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
	"github.com/jdp5949/p2p-messaging/pkg/transfer"
)

// defaultRelay is the hosted relay. Override with -relay.
const defaultRelay = "p2pmsg.duckdns.org:9009"

func main() {
	relayAddr := flag.String("relay", defaultRelay, "relay host:port")
	noTLS := flag.Bool("no-tls", false, "disable TLS to the relay (dev only)")
	idPath := flag.String("id", "~/.p2p/id_ed25519", "identity key path")
	knownPath := flag.String("known", "~/.p2p/known_peers", "known_peers path")
	noCrypto := flag.Bool("no-crypto", false, "disable Noise encryption (dev only)")
	debug := flag.Bool("debug", false, "verbose metrics (latency, connect time, path)")
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
	case "bench":
		runBench(*relayAddr, !*noTLS, *idPath, *knownPath, args[1:])
		return
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
	connStart := time.Now()
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

	if *debug {
		fmt.Fprintf(os.Stderr, "connected in %s (%s)\n", humanize.Dur(time.Since(connStart)), connMode(dialer))
	}

	inbound := make(chan transfer.Msg, 128)
	dropped := make(chan struct{})
	quit := make(chan struct{})
	// inTransfer silences the chat-oriented reconnect notices during a file
	// transfer (its teardown/drops are reported by the transfer layer instead).
	var inTransfer atomic.Bool
	b, err := broker.New(broker.Config{
		Conn:       c,
		ACKTimeout: 30 * time.Second,
		OnInbound: func(m broker.InboundMsg) {
			inbound <- transfer.Msg{ContentType: m.ContentType, Payload: m.Payload}
		},
		OnDisconnected: func() {
			if !inTransfer.Load() {
				fmt.Fprintln(os.Stderr, "⚠ peer offline — reconnecting (up to 60s)…")
			}
		},
		OnReconnected: func() {
			if !inTransfer.Load() {
				fmt.Fprintf(os.Stderr, "✓ peer back online (%s)\n", connMode(dialer))
			}
		},
		OnReconnectFailed: func() {
			fmt.Fprintln(os.Stderr, "✗ peer lost — gave up after 60s.")
			close(dropped)
		},
		OnAck: func(_ uint64, rtt time.Duration) {
			// Only report per-message delivery for chat; during a file transfer
			// this would fire per chunk and bury the progress bar.
			if *debug && !inTransfer.Load() {
				fmt.Fprintf(os.Stderr, "  ✓ delivered (%s)\n", humanize.Dur(rtt))
			}
		},
		PingInterval: 10 * time.Second,
	})
	fatalOn(err, "broker")

	sendMsg := func(ct protocol.ContentType, p []byte) error {
		_, e := b.Send(ct, protocol.PriorityNormal, p)
		return e
	}

	// Mode A: sending file(s)/dir(s) — we never read stdin as a sender.
	if len(sendPaths) > 0 {
		inTransfer.Store(true)
		fmt.Fprintf(os.Stderr, "Sending %d item(s) (%s)…\n", len(sendPaths), connMode(dialer))
		type sr struct {
			st  transfer.Stats
			err error
		}
		errc := make(chan sr, 1)
		go func() { st, e := transfer.Send(sendMsg, inbound, sendPaths, progressBar); errc <- sr{st, e} }()
		var r sr
		select {
		case r = <-errc:
		case <-dropped:
			r.err = fmt.Errorf("peer lost during transfer")
		}
		fmt.Fprintln(os.Stderr)
		_ = b.Close()
		fatalOn(r.err, "send")
		fmt.Fprintf(os.Stderr, "✓ sent %s in %s (%s)\n", humanize.Bytes(r.st.Bytes), humanize.Dur(r.st.Duration), humanize.Rate(r.st.Bytes, r.st.Duration))
		return
	}

	if initiator {
		// Mode B: chat initiator (`p2p send` with no path). As the initiator we
		// never receive a file, so reading stdin immediately is safe. Announce
		// chat so the joiner can show "connected" and type without waiting for
		// us to speak first.
		_ = sendMsg(protocol.ContentJSON, transfer.ChatHello())
		fmt.Printf("✓ Connected — peer online (%s). Type messages. /quit or Ctrl-C to exit.\n", connMode(dialer))
		go chatLoop(b, quit)
		go func() {
			for m := range inbound {
				if transfer.Kind(m) == "chat" {
					continue // skip the peer's own hello, if any
				}
				fmt.Printf("peer> %s\n", m.Payload)
			}
		}()
	} else {
		// Mode C: joiner. Peek the first inbound message to decide file-receive
		// vs chat. Do NOT touch stdin until we know it is chat — otherwise the
		// file overwrite prompt and chat input would both read stdin. The
		// initiator always announces its mode first (HEADER for a file, a chat
		// HELLO otherwise), so this resolves within milliseconds of connecting.
		go func() {
			var first transfer.Msg
			select {
			case first = <-inbound:
			case <-quit:
				return
			}
			if transfer.Kind(first) == "header" {
				inTransfer.Store(true)
				merged := make(chan transfer.Msg, 128)
				merged <- first
				go func() {
					for m := range inbound {
						merged <- m
					}
				}()
				dest, derr := os.Getwd()
				if derr != nil {
					fmt.Fprintf(os.Stderr, "receive failed: %v\n", derr)
					close(quit)
					return
				}
				saved, st, rerr := transfer.Receive(sendMsg, merged, dest, promptOverwrite, progressBar)
				fmt.Fprintln(os.Stderr)
				if rerr != nil {
					fmt.Fprintf(os.Stderr, "receive failed: %v\n", rerr)
				} else {
					fmt.Fprintf(os.Stderr, "✓ saved %s — %s in %s (%s), sha256 ok\n", saved,
						humanize.Bytes(st.Bytes), humanize.Dur(st.Duration), humanize.Rate(st.Bytes, st.Duration))
				}
				close(quit)
				return
			}
			// Chat session.
			fmt.Printf("✓ Connected — peer online (%s). Type messages. /quit or Ctrl-C to exit.\n", connMode(dialer))
			if transfer.Kind(first) != "chat" {
				// A plain text message (e.g. from an older peer that sent no
				// hello) — show it; a hello is just a signal and is not printed.
				fmt.Printf("peer> %s\n", first.Payload)
			}
			go chatLoop(b, quit)
			for m := range inbound {
				fmt.Printf("peer> %s\n", m.Payload)
			}
		}()
	}

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
