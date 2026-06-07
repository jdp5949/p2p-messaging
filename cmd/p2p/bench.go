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
		fmt.Printf("Bench code: %s\n", code)
		fmt.Printf("On the other computer run:\n\n    p2p bench %s\n\n", code)
	}

	// Scan args for `-sizes <csv>` and `-streams <n>`.
	sizesCSV := defaultBenchSizes
	streamsN := 4
	for i := 0; i < len(args); i++ {
		if args[i] == "-sizes" && i+1 < len(args) {
			sizesCSV = args[i+1]
		}
		if args[i] == "-streams" && i+1 < len(args) {
			fmt.Sscanf(args[i+1], "%d", &streamsN)
		}
	}

	sessionID := codephrase.SessionID(code)

	id, err := crypto.LoadOrGenerateIdentity(idPath)
	fatalOn(err, "identity")
	kp, err := crypto.LoadKnownPeers(knownPath)
	fatalOn(err, "known_peers")

	// Multi-stream bench path (default): measure throughput over N parallel
	// data sessions, exactly like `p2p send -streams N`.
	if streamsN > 1 {
		scfg := streamConfig{relayAddr: relayAddr, useTLS: useTLS, code: code, id: id, known: kp, initiator: initiator}
		if useTLS {
			host, _, _ := net.SplitHostPort(relayAddr)
			scfg.tlsConfig = &tls.Config{ServerName: host}
		}
		runBenchParallel(scfg, streamsN, sizesCSV, initiator)
		return
	}
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
	// Give the last control message (DONE/benchdone) a moment to flush before
	// the deferred b.Close() tears down the connection.
	time.Sleep(300 * time.Millisecond)
}

// runBenchInitiator pushes each payload size over the live connection and prints
// a timing/throughput table.
func runBenchInitiator(sendMsg transfer.SendFunc, inbound <-chan transfer.Msg, rtt <-chan time.Duration, sizesCSV string, connectTime time.Duration, mode string) {
	sizes, err := parseSizes(sizesCSV)
	fatalOn(err, "sizes")

	// Drain any stale ACKs, then announce the plan and measure latency as the
	// round-trip of the plan message's own ACK (no separate ping needed — that
	// keeps the data stream clean for the responder's sequential Receive loop).
	for draining := true; draining; {
		select {
		case <-rtt:
		default:
			draining = false
		}
	}
	plan := benchPlan{T: "bench", Sizes: sizes}
	pj, _ := json.Marshal(plan)
	fatalOn(sendMsg(protocol.ContentJSON, pj), "plan")
	var latency time.Duration
	select {
	case latency = <-rtt:
	case <-time.After(5 * time.Second):
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

// runBenchResponder receives the benchmark payloads. The bench plan tells us
// exactly how many transfers to expect, so we call transfer.Receive that many
// times directly on the inbound channel. Receive stops at each transfer's
// trailer and never over-reads, so the next call cleanly picks up the next
// transfer's header — no forwarding goroutine, no message-routing race.
func runBenchResponder(sendMsg transfer.SendFunc, inbound <-chan transfer.Msg) {
	first := <-inbound
	if transfer.Kind(first) != "bench" {
		fmt.Fprintln(os.Stderr, "bench: peer did not start a benchmark (run `p2p bench` on the other side)")
		return
	}
	var plan benchPlan
	if err := json.Unmarshal(first.Payload, &plan); err != nil {
		fmt.Fprintf(os.Stderr, "bench: bad plan: %v\n", err)
		return
	}

	tmp, err := os.MkdirTemp("", "p2p-bench-recv-*")
	fatalOn(err, "tempdir")
	defer os.RemoveAll(tmp)

	fmt.Fprintf(os.Stderr, "Receiving %d benchmark transfer(s)…\n", len(plan.Sizes))
	for i := range plan.Sizes {
		if _, _, e := transfer.Receive(sendMsg, inbound, tmp, func(string) bool { return true }, nil); e != nil {
			fmt.Fprintf(os.Stderr, "bench: transfer %d failed: %v\n", i+1, e)
			return
		}
	}
	fmt.Fprintln(os.Stderr, "bench complete")
}

// runBenchParallel measures throughput over n parallel data streams (the same
// path as `p2p send -streams n`).
func runBenchParallel(scfg streamConfig, n int, sizesCSV string, initiator bool) {
	fmt.Fprintln(os.Stderr, "Connecting…")
	t0 := time.Now()
	ps, err := openStreams(scfg, n)
	fatalOn(err, "open streams")
	connectTime := time.Since(t0)
	defer func() {
		for _, s := range ps {
			s.Close()
		}
	}()

	if initiator {
		sizes, e := parseSizes(sizesCSV)
		fatalOn(e, "sizes")
		// Latency: round-trip a 1-byte ping on the control stream.
		var latency time.Duration
		lt := time.Now()
		if ps[0].WriteMsg([]byte{'P'}) == nil {
			if _, re := ps[0].ReadMsg(); re == nil {
				latency = time.Since(lt)
			}
		}
		pj, _ := json.Marshal(benchPlan{T: "bench", Sizes: sizes})
		fatalOn(ps[0].WriteMsg(pj), "plan")
		fmt.Printf("Connected : %s (%d streams)\n", humanize.Dur(connectTime), len(ps))
		fmt.Printf("Latency   : %s (round-trip)\n", humanize.Dur(latency))
		fmt.Println("  SIZE  TIME  THROUGHPUT")
		tmp, _ := os.MkdirTemp("", "p2pbench")
		defer os.RemoveAll(tmp)
		for _, sz := range sizes {
			path := filepath.Join(tmp, "blob")
			if e := writeRandomFile(path, sz); e != nil {
				fmt.Printf("  %s  FAILED (%v)\n", humanize.Bytes(sz), e)
				break
			}
			st, e := transfer.SendParallel(ps, []string{path}, nil)
			if e != nil {
				fmt.Printf("  %s  FAILED (%v)\n", humanize.Bytes(sz), e)
				break
			}
			fmt.Printf("  %s  %s  %s\n", humanize.Bytes(sz), humanize.Dur(st.Duration), humanize.Rate(st.Bytes, st.Duration))
		}
		fmt.Fprintln(os.Stderr, "bench complete")
		return
	}

	// Responder.
	if mb, e := ps[0].ReadMsg(); e == nil && len(mb) > 0 && mb[0] == 'P' {
		_ = ps[0].WriteMsg([]byte{'P'})
	}
	pb, e := ps[0].ReadMsg()
	fatalOn(e, "plan")
	var plan benchPlan
	_ = json.Unmarshal(pb, &plan)
	fmt.Fprintf(os.Stderr, "Receiving %d benchmark transfer(s) over %d stream(s)…\n", len(plan.Sizes), len(ps))
	tmp, _ := os.MkdirTemp("", "p2pbenchrecv")
	defer os.RemoveAll(tmp)
	for i := range plan.Sizes {
		if _, _, e := transfer.ReceiveParallel(ps, tmp, func(string) bool { return true }, nil); e != nil {
			fmt.Fprintf(os.Stderr, "transfer %d failed: %v\n", i+1, e)
			return
		}
	}
	fmt.Fprintln(os.Stderr, "bench complete")
}
