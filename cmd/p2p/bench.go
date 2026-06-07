package main

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/broker"
	"github.com/jdp5949/p2p-messaging/pkg/codephrase"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/crypto"
	"github.com/jdp5949/p2p-messaging/pkg/humanize"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
	"github.com/jdp5949/p2p-messaging/pkg/transfer"
)

// defaultBenchSizes is the payload-size matrix used when -sizes is not given.
const defaultBenchSizes = "1KB,64KB,1MB,10MB,50MB"

// benchPlan is the control message the initiator sends first, announcing the
// matrix of payload sizes it intends to push so the responder knows the session
// is a benchmark (not a chat or plain file transfer).
type benchPlan struct {
	T     string  `json:"t"`
	Sizes []int64 `json:"sizes"`
}

// parseSizes turns a comma-separated list of human sizes (e.g. "1KB,10MB") into
// a slice of byte counts. It errors on an empty list or any unparsable entry.
func parseSizes(csv string) ([]int64, error) {
	parts := strings.Split(csv, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, fmt.Errorf("parseSizes: empty size entry")
		}
		n, err := humanize.ParseSize(p)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("parseSizes: no sizes")
	}
	return out, nil
}

// writeRandomFile fills path with n random bytes using a reusable 64KB buffer.
func writeRandomFile(path string, n int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 64*1024)
	var written int64
	for written < n {
		want := int64(len(buf))
		if rem := n - written; rem < want {
			want = rem
		}
		if _, err := rand.Read(buf[:want]); err != nil {
			return err
		}
		w, err := f.Write(buf[:want])
		written += int64(w)
		if err != nil {
			return err
		}
	}
	return nil
}

// runBench is the `p2p bench` entrypoint. With a join code argument it acts as
// the responder (receiver of the benchmark payloads); otherwise it generates a
// code, prints it, and drives the benchmark as the initiator.
func runBench(relayAddr string, useTLS bool, idPath, knownPath string, args []string) {
	// Decide role. A bare first arg (not a flag) is the join code => responder.
	var code string
	var initiator bool
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		code = args[0]
		initiator = false
	} else {
		code = codephrase.Generate()
		initiator = true
		fmt.Printf("Code is: %s\n", code)
		fmt.Printf("On the other computer run:\n\n    p2p bench %s\n\n", code)
	}

	// Initiator size matrix: scan args for `-sizes <csv>`.
	sizesCSV := defaultBenchSizes
	for i := 0; i < len(args); i++ {
		if args[i] == "-sizes" && i+1 < len(args) {
			sizesCSV = args[i+1]
		}
	}

	sessionID := codephrase.SessionID(code)

	id, err := crypto.LoadOrGenerateIdentity(idPath)
	fatalOn(err, "identity")
	kp, err := crypto.LoadKnownPeers(knownPath)
	fatalOn(err, "known_peers")
	hsCfg := &crypto.HandshakeConfig{
		Identity:   id,
		KnownPeers: kp,
		PeerName:   sessionID,
		PAKECode:   code,
		Initiator:  initiator,
	}

	dialer := &sessionDialer{
		relayAddr:    relayAddr,
		sessionID:    sessionID,
		useTLS:       useTLS,
		punchTimeout: 6 * time.Second,
	}
	if dialer.useTLS {
		host, _, _ := net.SplitHostPort(relayAddr)
		dialer.tlsConfig = &tls.Config{ServerName: host}
	}

	fmt.Fprintln(os.Stderr, "Connecting…")
	start := time.Now()
	c, err := conn.New(conn.Config{
		DialFunc:     dialer.DialFunc,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PingInterval: 10 * time.Second,
		HandshakeCfg: hsCfg,
	})
	fatalOn(err, "connect")
	connectTime := time.Since(start)

	// First connect done; future reconnects use KK (no PAKE).
	hsCfg.PAKECode = ""

	inbound := make(chan transfer.Msg, 256)
	rtt := make(chan time.Duration, 8)
	b, err := broker.New(broker.Config{
		Conn:       c,
		ACKTimeout: 30 * time.Second,
		OnInbound: func(m broker.InboundMsg) {
			inbound <- transfer.Msg{ContentType: m.ContentType, Payload: m.Payload}
		},
		OnAck: func(_ uint64, d time.Duration) {
			select {
			case rtt <- d:
			default:
			}
		},
		PingInterval: 10 * time.Second,
	})
	fatalOn(err, "broker")
	defer b.Close()

	sendMsg := func(ct protocol.ContentType, p []byte) error {
		_, e := b.Send(ct, protocol.PriorityNormal, p)
		return e
	}

	if initiator {
		runBenchInitiator(sendMsg, inbound, rtt, sizesCSV, connectTime, connMode(dialer))
	} else {
		runBenchResponder(sendMsg, inbound)
	}
}

// runBenchInitiator pushes each payload size over the live connection and prints
// a timing/throughput table.
func runBenchInitiator(sendMsg transfer.SendFunc, inbound <-chan transfer.Msg, rtt <-chan time.Duration, sizesCSV string, connectTime time.Duration, mode string) {
	sizes, err := parseSizes(sizesCSV)
	fatalOn(err, "sizes")

	// Announce the plan so the responder enters bench mode.
	plan := benchPlan{T: "bench", Sizes: sizes}
	pj, _ := json.Marshal(plan)
	fatalOn(sendMsg(protocol.ContentJSON, pj), "plan")

	// Drain any ACKs already queued (e.g. for the plan) so the latency probe
	// measures the round-trip of the ping itself.
	for draining := true; draining; {
		select {
		case <-rtt:
		default:
			draining = false
		}
	}

	// Latency probe: one tiny text message, read its ACK round-trip.
	var latency time.Duration
	if err := sendMsg(protocol.ContentText, []byte("ping")); err == nil {
		select {
		case latency = <-rtt:
		case <-time.After(5 * time.Second):
		}
	}

	fmt.Printf("Connected : %s (%s)\n", humanize.Dur(connectTime), mode)
	fmt.Printf("Latency   : %s (round-trip)\n", humanize.Dur(latency))
	fmt.Println("  SIZE  TIME  THROUGHPUT")

	tmp, err := os.MkdirTemp("", "p2p-bench-*")
	fatalOn(err, "tempdir")
	defer os.RemoveAll(tmp)

	for _, sz := range sizes {
		path := filepath.Join(tmp, fmt.Sprintf("payload-%d.bin", sz))
		if err := writeRandomFile(path, sz); err != nil {
			fmt.Printf("  %s  FAILED (%v)\n", humanize.Bytes(sz), err)
			break
		}
		st, err := transfer.Send(sendMsg, inbound, []string{path}, nil)
		if err != nil {
			fmt.Printf("  %s  FAILED (%v)\n", humanize.Bytes(sz), err)
			break
		}
		fmt.Printf("  %s  %s  %s\n", humanize.Bytes(sz), humanize.Dur(st.Duration), humanize.Rate(st.Bytes, st.Duration))
	}

	done, _ := json.Marshal(map[string]string{"t": "benchdone"})
	_ = sendMsg(protocol.ContentJSON, done)
	fmt.Fprintln(os.Stderr, "bench complete")
}

// runBenchResponder receives the benchmark payloads. Exactly one goroutine (this
// one) consumes the inbound channel; per-transfer Receive calls are fed via a
// dedicated channel that always starts with the transfer's header.
func runBenchResponder(sendMsg transfer.SendFunc, inbound <-chan transfer.Msg) {
	// First message must be the bench plan.
	first := <-inbound
	if transfer.Kind(first) != "bench" {
		fmt.Fprintln(os.Stderr, "bench: peer did not start a benchmark (run `p2p bench` on the other side)")
		return
	}

	tmp, err := os.MkdirTemp("", "p2p-bench-recv-*")
	fatalOn(err, "tempdir")
	defer os.RemoveAll(tmp)

	var cur chan transfer.Msg
	recvErr := make(chan error, 1)
	receiving := false

	for {
		m := <-inbound
		switch transfer.Kind(m) {
		case "benchdone":
			fmt.Fprintln(os.Stderr, "bench complete")
			return
		case "header":
			cur = make(chan transfer.Msg, 256)
			cur <- m
			receiving = true
			go func(in chan transfer.Msg) {
				_, _, e := transfer.Receive(sendMsg, in, tmp, func(string) bool { return true }, nil)
				recvErr <- e
			}(cur)
		default:
			// Forward data/trailer/etc. into the active receive; ignore stray
			// messages (e.g. the latency ping) when no transfer is in flight.
			if receiving {
				cur <- m
			}
		}

		// Reap a finished transfer without blocking the inbound loop.
		select {
		case e := <-recvErr:
			receiving = false
			if e != nil {
				fmt.Fprintf(os.Stderr, "bench: receive failed: %v\n", e)
				return
			}
		default:
		}
	}
}
