# Metrics + Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Human-readable sizes everywhere, transfer speed summaries, a `-debug` mode (chat latency, connect time, path), and a `p2p bench` subcommand measuring the real connection across sizes.

**Architecture:** New `pkg/humanize` for formatting/parsing; `pkg/transfer` returns `Stats`; `pkg/broker` gains an `OnAck(msgID, rtt)` callback; `cmd/p2p` prints metrics and adds a `bench` subcommand that reuses `transfer` over the real relay/punch connection.

**Tech Stack:** Go 1.21+, existing pkgs.

---

## File Structure
- Create `pkg/humanize/humanize.go` + `humanize_test.go`.
- Modify `pkg/transfer/transfer.go` (add `Stats`), `send.go`, `receive.go`, `send_test.go`, `receive_test.go`.
- Modify `pkg/broker/broker.go` (+ `broker_test.go`) — `OnAck`.
- Modify `cmd/p2p/progress.go` (+ `progress_test.go`) — humanized.
- Create `cmd/p2p/bench.go` (+ `bench_test.go`).
- Modify `cmd/p2p/main.go` — `-debug`, speed summaries, connect timing, latency, route `bench`, update transfer call sites.

---

## Task 1: pkg/humanize

**Files:** Create `pkg/humanize/humanize.go`, `pkg/humanize/humanize_test.go`.

- [ ] **Step 1: failing test** — `pkg/humanize/humanize_test.go`:

```go
package humanize

import (
	"testing"
	"time"
)

func TestBytes(t *testing.T) {
	cases := map[int64]string{
		0: "0 B", 512: "512 B", 1023: "1023 B",
		1024: "1.0 KB", 1536: "1.5 KB",
		5 * 1024 * 1024: "5.0 MB",
		3 * 1024 * 1024 * 1024: "3.0 GB",
	}
	for n, want := range cases {
		if got := Bytes(n); got != want {
			t.Errorf("Bytes(%d)=%q want %q", n, got, want)
		}
	}
}

func TestRate(t *testing.T) {
	if got := Rate(1024*1024, time.Second); got != "1.0 MB/s" {
		t.Errorf("Rate=%q want 1.0 MB/s", got)
	}
	if got := Rate(100, 0); got != "—" {
		t.Errorf("Rate(_,0)=%q want —", got)
	}
}

func TestDur(t *testing.T) {
	cases := map[time.Duration]string{
		850 * time.Millisecond: "850ms",
		4200 * time.Millisecond: "4.2s",
		63 * time.Second: "1m3s",
	}
	for d, want := range cases {
		if got := Dur(d); got != want {
			t.Errorf("Dur(%v)=%q want %q", d, got, want)
		}
	}
}

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"512": 512, "1KB": 1024, "10MB": 10 * 1024 * 1024,
		"1.5MB": 1572864, "2GB": 2 * 1024 * 1024 * 1024, "256B": 256,
	}
	for s, want := range cases {
		got, err := ParseSize(s)
		if err != nil || got != want {
			t.Errorf("ParseSize(%q)=%d,%v want %d", s, got, err, want)
		}
	}
	if _, err := ParseSize("abc"); err == nil {
		t.Error("expected error for bad size")
	}
}
```

- [ ] **Step 2: run, expect FAIL** — `go test ./pkg/humanize/` → undefined.

- [ ] **Step 3: implement** — `pkg/humanize/humanize.go`:

```go
// Package humanize formats and parses byte sizes, rates, and durations for
// human-friendly CLI output. Units are 1024-based (KB, MB, GB, TB).
package humanize

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Bytes formats a byte count like "512 B", "1.5 KB", "5.0 MB".
func Bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	if exp >= len(units) {
		exp = len(units) - 1
	}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), units[exp])
}

// Rate formats throughput; returns "—" for non-positive durations.
func Rate(n int64, d time.Duration) string {
	if d <= 0 {
		return "—"
	}
	return Bytes(int64(float64(n)/d.Seconds())) + "/s"
}

// Dur formats a duration as "850ms", "4.2s", or "1m3s".
func Dur(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	default:
		return fmt.Sprintf("%dm%ds", int(d/time.Minute), int((d%time.Minute)/time.Second))
	}
}

// ParseSize parses "512", "10MB", "1.5GB" (case-insensitive) into bytes.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "KB"):
		mult, s = 1024, strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "MB"):
		mult, s = 1024*1024, strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "GB"):
		mult, s = 1024*1024*1024, strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimSuffix(s, "B")
	}
	s = strings.TrimSpace(s)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("humanize: bad size %q: %w", s, err)
	}
	return int64(f * float64(mult)), nil
}
```

- [ ] **Step 4: run, expect PASS** — `go test ./pkg/humanize/ -v`.
- [ ] **Step 5: commit** — `git add pkg/humanize && git commit -m "feat(humanize): byte/rate/duration formatting + size parsing"`.

---

## Task 2: broker OnAck

**Files:** Modify `pkg/broker/broker.go`, `pkg/broker/broker_test.go`.

- [ ] **Step 1: failing test** — add to `pkg/broker/broker_test.go`:

```go
func TestOnAckReportsRTT(t *testing.T) {
	c, srv := pipeConn(t)
	defer srv.Close()

	acked := make(chan time.Duration, 1)
	b, err := New(Config{
		Conn:       c,
		ACKTimeout: 5 * time.Second,
		OnAck:      func(_ uint64, rtt time.Duration) { acked <- rtt },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	resCh := make(chan uint64, 1)
	go func() {
		id, _ := b.Send(protocol.ContentText, protocol.PriorityNormal, []byte("hi"))
		resCh <- id
	}()
	sent := readOneMsg(t, srv)
	<-resCh
	writeMsg(t, srv, protocol.Message{Header: protocol.Header{MsgID: sent.Header.MsgID, MsgType: protocol.MsgACK}})

	select {
	case rtt := <-acked:
		if rtt < 0 {
			t.Fatalf("rtt negative: %v", rtt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnAck not called")
	}
}
```

- [ ] **Step 2: run, expect FAIL** — `go test ./pkg/broker/ -run TestOnAckReportsRTT` → unknown field OnAck.

- [ ] **Step 3: implement**:

(a) Add to `Config` (after `OnReconnected`):
```go
	// OnAck is called when an ACK is received for a sent message, with the
	// round-trip time from send to ACK. Optional.
	OnAck func(msgID uint64, rtt time.Duration)
```

(b) Change `freeSlot` to report the slot's send time:
```go
// freeSlot marks the slot for msgID free; returns the slot's sendTime and
// whether it was found. Signals waiting enqueue callers.
func (b *Broker) freeSlot(msgID uint64) (time.Time, bool) {
	b.mu.Lock()
	for i := range b.ring {
		if b.ring[i].active && b.ring[i].msgID == msgID {
			st := b.ring[i].sendTime
			b.ring[i].active = false
			b.freeIdx = append(b.freeIdx, i)
			if b.cfg.WAL != nil {
				_ = b.cfg.WAL.Ack(msgID)
				atomic.AddUint64(&b.ackCount, 1)
			}
			b.mu.Unlock()
			b.cond.Signal()
			return st, true
		}
	}
	b.mu.Unlock()
	return time.Time{}, false
}
```

(c) Update the three other `freeSlot` callers to ignore the return — at the
two sites in `Send`'s write-error path (search `b.freeSlot(msgID)` near the
`WriteMsg` error handling) and the NACK case in `dispatch`, change
`b.freeSlot(msgID)` to `b.freeSlot(msgID) //nolint` is NOT enough; Go allows
ignoring multiple returns only with assignment. Use `_, _ = b.freeSlot(msgID)`.

(d) In `dispatch`, the `MsgACK` case:
```go
	case protocol.MsgACK:
		st, ok := b.freeSlot(msg.Header.MsgID)
		if ok && b.cfg.OnAck != nil {
			b.cfg.OnAck(msg.Header.MsgID, time.Since(st))
		}
```

- [ ] **Step 4: run, expect PASS** — `go test ./pkg/broker/ -v` (new + all existing). `go vet ./pkg/broker/`.
- [ ] **Step 5: commit** — `git add pkg/broker && git commit -m "feat(broker): OnAck callback with send→ack RTT"`.

---

## Task 3: transfer Stats

**Files:** Modify `pkg/transfer/transfer.go`, `send.go`, `receive.go`, `send_test.go`, `receive_test.go`.

- [ ] **Step 1: failing test** — add to `pkg/transfer/receive_test.go`:

```go
func TestRoundTripStats(t *testing.T) {
	src := t.TempDir()
	path := filepath.Join(src, "s.bin")
	if err := os.WriteFile(path, make([]byte, 100000), 0o644); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()

	a2b := make(chan Msg, 512)
	b2a := make(chan Msg, 8)
	sendA := func(ct protocol.ContentType, p []byte) error { a2b <- Msg{ct, append([]byte(nil), p...)}; return nil }
	sendB := func(ct protocol.ContentType, p []byte) error { b2a <- Msg{ct, append([]byte(nil), p...)}; return nil }

	type rr struct {
		st  Stats
		err error
	}
	res := make(chan rr, 1)
	go func() {
		_, st, e := Receive(sendB, a2b, dst, func(string) bool { return true }, nil)
		res <- rr{st, e}
	}()
	sst, serr := Send(sendA, b2a, []string{path}, nil)
	r := <-res
	if serr != nil || r.err != nil {
		t.Fatalf("send=%v recv=%v", serr, r.err)
	}
	if sst.Bytes != 100000 || r.st.Bytes != 100000 {
		t.Fatalf("bytes send=%d recv=%d want 100000", sst.Bytes, r.st.Bytes)
	}
	if sst.Duration <= 0 || r.st.Duration <= 0 {
		t.Fatalf("durations must be >0: send=%v recv=%v", sst.Duration, r.st.Duration)
	}
}
```

Also update the existing `pair` helper in `receive_test.go` and all existing
call sites to the new arities (see below).

- [ ] **Step 2: run, expect FAIL** — `go test ./pkg/transfer/ -run TestRoundTripStats` → Send/Receive arity mismatch / undefined Stats.

- [ ] **Step 3: implement**

(a) Add to `pkg/transfer/transfer.go`:
```go
import "time" // add to the existing import block

// Stats summarizes a completed transfer.
type Stats struct {
	Bytes    int64
	Duration time.Duration
}
```

(b) `pkg/transfer/send.go` — change signature + timing. Replace
`func Send(send SendFunc, in <-chan Msg, paths []string, progress ProgressFn) error {`
with `(Stats, error)` and wrap:
- add `start := time.Now()` at the very top (after the `len(paths)==0` check, return `Stats{}, err` there).
- every `return err`/`return nil` in the function becomes `return Stats{...}, err`. Specifically:
  - early errors: `return Stats{}, <err>`.
  - success after DONE: `return Stats{Bytes: offset, Duration: time.Since(start)}, nil`.
  - the timeout/closed errors: `return Stats{}, <err>`.
- `offset` is already tracked; use it for Bytes.

(c) `pkg/transfer/receive.go` — change signature to
`func Receive(...) (string, Stats, error)`:
- add `start := time.Now()` at top; early returns become `return "", Stats{}, <err>`.
- track received bytes: after verifying, `total` from `trailer.Total`.
- final success: `return saved, Stats{Bytes: trailer.Total, Duration: time.Since(start)}, nil`.
- the DONE send happens before return as today.

(d) Update existing tests in `pkg/transfer` to new arities:
- `send_test.go` `TestSendSingleFileSequence`: `if _, err := Send(send, done, []string{path}, nil); err != nil` → `if _, err := Send(...); err != nil` (Send now returns 2 values; the first is Stats — discard with `_`).
- `send_test.go` `TestSendTimesOutWithoutAck`: `err := Send(...)` → `_, err := Send(...)`.
- `receive_test.go` `pair` helper: `p, e := Receive(sendB, a2b, dst, ow, nil)` → `p, _, e := Receive(...)`. And in tests calling `Receive` directly (`TestReceiveHashMismatch`, `TestReceiveOutOfOrderChunks`): `_, err := Receive(...)` → `_, _, err := Receive(...)`; for ones using saved path `saved, err := Receive(...)` → `saved, _, err := Receive(...)`.

- [ ] **Step 4: run, expect PASS** — `go test ./pkg/transfer/ -race -v`.
- [ ] **Step 5: commit** — `git add pkg/transfer && git commit -m "feat(transfer): return Stats (bytes + duration) from Send/Receive"`.

---

## Task 4: humanized progress (cmd/p2p)

**Files:** Modify `cmd/p2p/progress.go`, `cmd/p2p/progress_test.go`. Depends on Task 1.

- [ ] **Step 1: failing test** — replace `TestFormatProgress` in `cmd/p2p/progress_test.go`:

```go
func TestFormatProgress(t *testing.T) {
	s := formatProgress(5*1024*1024, 10*1024*1024)
	if !strings.Contains(s, "50.0%") {
		t.Fatalf("want percent, got %q", s)
	}
	if !strings.Contains(s, "5.0 MB") || !strings.Contains(s, "10.0 MB") {
		t.Fatalf("want humanized sizes, got %q", s)
	}
	u := formatProgress(2048, 0)
	if !strings.Contains(u, "2.0 KB") {
		t.Fatalf("want humanized byte count, got %q", u)
	}
}
```

- [ ] **Step 2: run, expect FAIL** — `go test ./cmd/p2p/ -run TestFormatProgress`.

- [ ] **Step 3: implement** — in `cmd/p2p/progress.go`, add import
`"github.com/jdp5949/p2p-messaging/pkg/humanize"` and rewrite `formatProgress`:

```go
func formatProgress(done, total int64) string {
	if total > 0 {
		pct := float64(done) / float64(total) * 100
		bars := int(pct / 5)
		if bars > 20 {
			bars = 20
		}
		return fmt.Sprintf("[%-20s] %5.1f%% (%s / %s)", strings.Repeat("=", bars), pct,
			humanize.Bytes(done), humanize.Bytes(total))
	}
	return humanize.Bytes(done)
}
```

- [ ] **Step 4: run, expect PASS** — `go test ./cmd/p2p/ -run TestFormatProgress`.
- [ ] **Step 5: commit** — `git add cmd/p2p/progress.go cmd/p2p/progress_test.go && git commit -m "feat(p2p): humanized progress bar"`.

---

## Task 5: `p2p bench` subcommand

**Files:** Create `cmd/p2p/bench.go`, `cmd/p2p/bench_test.go`. Depends on Tasks 1, 2, 3. Uses the existing `sessionDialer`, `connMode`, `fatalOn` in package main.

- [ ] **Step 1: failing test** — `cmd/p2p/bench_test.go` (parsing only; the live flow is integration/manual):

```go
package main

import "testing"

func TestParseSizes(t *testing.T) {
	got, err := parseSizes("1KB,1MB,10MB")
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1024, 1024 * 1024, 10 * 1024 * 1024}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%d want %d", i, got[i], want[i])
		}
	}
	if _, err := parseSizes("1KB,nope"); err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 2: run, expect FAIL** — `go test ./cmd/p2p/ -run TestParseSizes` → undefined parseSizes.

- [ ] **Step 3: implement** — `cmd/p2p/bench.go`:

```go
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

// parseSizes parses a comma list like "1KB,1MB,10MB" into byte counts.
func parseSizes(csv string) ([]int64, error) {
	var out []int64
	for _, p := range strings.Split(csv, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := humanize.ParseSize(p)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no sizes")
	}
	return out, nil
}

type benchPlan struct {
	T     string  `json:"t"`
	Sizes []int64 `json:"sizes"`
}

// runBench runs the benchmark. args are the post-"bench" CLI args.
func runBench(relayAddr string, useTLS bool, idPath, knownPath string, args []string) {
	sizesCSV := "1KB,64KB,1MB,10MB,50MB"
	var code string
	initiator := true
	// args: [<code>] with optional -sizes handled by caller via flag parse;
	// here args[0], if present and not a flag, is the join code.
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		code = args[0]
		initiator = false
	}
	// allow -sizes after the verb on the initiator
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-sizes" {
			sizesCSV = args[i+1]
		}
	}

	if initiator {
		code = codephrase.Generate()
		fmt.Printf("Bench code: %s\nOn the other machine run:\n\n    p2p bench %s\n\n", code, code)
	}
	sessionID := codephrase.SessionID(code)

	id, err := crypto.LoadOrGenerateIdentity(idPath)
	fatalOn(err, "identity")
	kp, err := crypto.LoadKnownPeers(knownPath)
	fatalOn(err, "known_peers")
	hsCfg := &crypto.HandshakeConfig{Identity: id, KnownPeers: kp, PeerName: sessionID, PAKECode: code, Initiator: initiator}

	dialer := &sessionDialer{relayAddr: relayAddr, sessionID: sessionID, useTLS: useTLS, punchTimeout: 6 * time.Second}
	if useTLS {
		host, _, _ := net.SplitHostPort(relayAddr)
		dialer.tlsConfig = &tls.Config{ServerName: host}
	}

	fmt.Fprintln(os.Stderr, "Connecting…")
	t0 := time.Now()
	c, err := conn.New(conn.Config{DialFunc: dialer.DialFunc, ReadTimeout: 120 * time.Second, WriteTimeout: 120 * time.Second, PingInterval: 10 * time.Second, HandshakeCfg: hsCfg})
	fatalOn(err, "connect")
	connectTime := time.Since(t0)
	if hsCfg != nil {
		hsCfg.PAKECode = ""
	}

	inbound := make(chan transfer.Msg, 256)
	rtt := make(chan time.Duration, 8)
	b, err := broker.New(broker.Config{
		Conn:       c,
		ACKTimeout: 30 * time.Second,
		OnInbound:  func(m broker.InboundMsg) { inbound <- transfer.Msg{ContentType: m.ContentType, Payload: m.Payload} },
		OnAck:      func(_ uint64, d time.Duration) { select { case rtt <- d: default: } },
		PingInterval: 10 * time.Second,
	})
	fatalOn(err, "broker")
	send := func(ct protocol.ContentType, p []byte) error { _, e := b.Send(ct, protocol.PriorityNormal, p); return e }

	if initiator {
		runBenchInitiator(b, send, inbound, rtt, sizesCSV, connectTime, dialer)
	} else {
		runBenchResponder(send, inbound)
	}
	_ = b.Close()
}

func runBenchInitiator(b *broker.Broker, send transfer.SendFunc, inbound <-chan transfer.Msg, rtt <-chan time.Duration, sizesCSV string, connectTime time.Duration, dialer *sessionDialer) {
	sizes, err := parseSizes(sizesCSV)
	fatalOn(err, "sizes")

	// Announce the plan.
	plan, _ := json.Marshal(benchPlan{T: "bench", Sizes: sizes})
	fatalOn(send(protocol.ContentJSON, plan), "send plan")

	// Latency probe: one tiny chat message; measure to its ACK.
	_ = send(protocol.ContentText, []byte("ping"))
	var latency time.Duration
	select {
	case latency = <-rtt:
	case <-time.After(5 * time.Second):
	}

	mode := "via relay"
	if dialer.LastDirect() {
		mode = "direct P2P"
	}
	fmt.Printf("\nConnected : %s (%s)\n", humanize.Dur(connectTime), mode)
	fmt.Printf("Latency   : %s (round-trip)\n\n", humanize.Dur(latency))
	fmt.Printf("  %-12s %-10s %s\n", "SIZE", "TIME", "THROUGHPUT")

	tmp, _ := os.MkdirTemp("", "p2pbench")
	defer os.RemoveAll(tmp)
	for _, sz := range sizes {
		path := filepath.Join(tmp, "blob")
		f, _ := os.Create(path)
		_, _ = io_copyN(f, sz)
		f.Close()
		st, err := transfer.Send(send, inbound, []string{path}, nil)
		if err != nil {
			fmt.Printf("  %-12s FAILED: %v\n", humanize.Bytes(sz), err)
			break
		}
		fmt.Printf("  %-12s %-10s %s\n", humanize.Bytes(sz), humanize.Dur(st.Duration), humanize.Rate(st.Bytes, st.Duration))
	}

	done, _ := json.Marshal(map[string]string{"t": "benchdone"})
	_ = send(protocol.ContentJSON, done)
	fmt.Fprintln(os.Stderr, "\nbench complete")
}

func runBenchResponder(send transfer.SendFunc, inbound <-chan transfer.Msg) {
	// First message is the plan.
	first := <-inbound
	if transfer.Kind(first) != "bench" {
		fmt.Fprintln(os.Stderr, "expected a bench session; is the other side running `p2p bench`?")
		return
	}
	fmt.Fprintln(os.Stderr, "Bench started by peer…")
	tmp, _ := os.MkdirTemp("", "p2pbenchrecv")
	defer os.RemoveAll(tmp)
	// Drain the latency-probe ping (a chat text) if it arrives before headers.
	for {
		m := <-inbound
		switch transfer.Kind(m) {
		case "benchdone":
			fmt.Fprintln(os.Stderr, "bench complete")
			return
		case "header":
			merged := make(chan transfer.Msg, 256)
			merged <- m
			go func() {
				for x := range inbound {
					merged <- x
				}
			}()
			_, _, err := transfer.Receive(send, merged, tmp, func(string) bool { return true }, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "recv: %v\n", err)
				return
			}
			// NOTE: after Receive consumes one transfer, the forwarding goroutine
			// keeps moving messages into `merged`, which is abandoned. Re-read
			// below is incorrect; instead handle one transfer per loop without a
			// forwarding goroutine — see Step 3b.
		default:
			// ignore latency ping / others
		}
	}
}

// io_copyN writes n pseudo-random bytes to f.
func io_copyN(f *os.File, n int64) (int64, error) {
	return io_copy(f, rand.Reader, n)
}
```

**Step 3b — fix the responder loop (the note above is a real bug):** a forwarding
goroutine per transfer leaks and steals messages. Implement the responder so a
SINGLE drain goroutine feeds one shared channel, and `Receive` reads from a
per-transfer channel fed from it. Simplest correct version — replace
`runBenchResponder` with:

```go
func runBenchResponder(send transfer.SendFunc, inbound <-chan transfer.Msg) {
	first := <-inbound
	if transfer.Kind(first) != "bench" {
		fmt.Fprintln(os.Stderr, "expected a bench session; is the other side running `p2p bench`?")
		return
	}
	fmt.Fprintln(os.Stderr, "Bench started by peer…")
	tmp, _ := os.MkdirTemp("", "p2pbenchrecv")
	defer os.RemoveAll(tmp)

	cur := make(chan transfer.Msg, 256) // feeds the active Receive
	recvErr := make(chan error, 1)
	receiving := false
	done := false
	for !done {
		m := <-inbound
		switch transfer.Kind(m) {
		case "benchdone":
			done = true
		case "header":
			cur = make(chan transfer.Msg, 256)
			cur <- m
			receiving = true
			go func(ch chan transfer.Msg) {
				_, _, err := transfer.Receive(send, ch, tmp, func(string) bool { return true }, nil)
				recvErr <- err
			}(cur)
		default:
			if receiving {
				cur <- m
			}
		}
		// When a Receive finishes, it sent on recvErr; reset receiving.
		select {
		case err := <-recvErr:
			receiving = false
			if err != nil {
				fmt.Fprintf(os.Stderr, "recv: %v\n", err)
				return
			}
		default:
		}
	}
	fmt.Fprintln(os.Stderr, "bench complete")
}
```

Also add the byte-writer helpers at the bottom of `bench.go`:
```go
func io_copy(f *os.File, r interface{ Read([]byte) (int, error) }, n int64) (int64, error) {
	buf := make([]byte, 64*1024)
	var written int64
	for written < n {
		want := int64(len(buf))
		if rem := n - written; rem < want {
			want = rem
		}
		_, _ = r.Read(buf[:want])
		w, err := f.Write(buf[:want])
		written += int64(w)
		if err != nil {
			return written, err
		}
	}
	return written, nil
}
```
(`io_copyN` above calls this with `rand.Reader`.)

> Implementer note: keep it simple and correct over clever. If the
> header-then-data ordering on `inbound` makes the single-channel approach
> awkward, it is acceptable to process strictly sequentially: block reading the
> whole transfer inline. The key invariants: (1) exactly one consumer of
> `inbound`; (2) each `transfer.Receive` gets its HEADER first; (3) `benchdone`
> ends the loop. Adjust the code to satisfy these and make `go test ./cmd/p2p/`
> + `go build` pass.

- [ ] **Step 4: build + unit test** — `go build ./... && go vet ./... && go test ./cmd/p2p/ -run TestParseSizes -v`.
- [ ] **Step 5: commit** — `git add cmd/p2p/bench.go cmd/p2p/bench_test.go && git commit -m "feat(p2p): bench subcommand (size matrix over real connection)"`.

---

## Task 6: wire `-debug`, speed summaries, route bench (cmd/p2p/main.go)

**Files:** Modify `cmd/p2p/main.go`. Depends on Tasks 1, 3, 4, 5.

- [ ] **Step 1: implement**

(a) Add imports: `"time"` (already present), `"github.com/jdp5949/p2p-messaging/pkg/humanize"`.

(b) Add flag near the others:
```go
	debug := flag.Bool("debug", false, "verbose metrics (latency, connect time, path)")
```

(c) Route the bench verb — in the `switch args[0]` block add a case BEFORE `default`:
```go
	case "bench":
		runBench(*relayAddr, !*noTLS, *idPath, *knownPath, args[1:])
		return
```
(Place this switch handling after flags are parsed and idPath/knownPath/relayAddr/noTLS are known. `runBench` does its own connect; the rest of `main` is skipped via `return`.)

(d) Time the connect and, under debug, print it. After the existing
`c, err := conn.New(...)` + `fatalOn(err, "connect")`, where `dialer` is known,
add a `connStart` measurement: declare `connStart := time.Now()` immediately
before `conn.New`, and after success:
```go
	if *debug {
		fmt.Fprintf(os.Stderr, "connected in %s (%s)\n", humanize.Dur(time.Since(connStart)), connMode(dialer))
	}
```

(e) File-send speed summary — update the send-mode block. `transfer.Send` now
returns `(Stats, error)`:
```go
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
```

(f) Receive speed summary — in the joiner header branch, `transfer.Receive` now
returns `(string, Stats, error)`:
```go
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
```

(g) Chat latency under debug — add `OnAck` to the `broker.Config` used in
`main` (the chat/receive broker):
```go
		OnAck: func(_ uint64, rtt time.Duration) {
			if *debug {
				fmt.Fprintf(os.Stderr, "  ✓ delivered (%s)\n", humanize.Dur(rtt))
			}
		},
```

- [ ] **Step 2: build + vet** — `go build ./... && go vet ./...` (fix any unused import).
- [ ] **Step 3: full tests** — `go test ./...` all pass.
- [ ] **Step 4: smoke** — `go run ./cmd/p2p ; echo exit=$?` → usage, exit 2.
- [ ] **Step 5: commit** — `git add cmd/p2p/main.go && git commit -m "feat(p2p): -debug metrics, speed summaries, bench routing"`.

---

## Self-Review
**Spec coverage:** humanize (T1) ✓; transfer Stats (T3) ✓; broker OnAck (T2) ✓; humanized progress (T4) ✓; bench (T5) ✓; -debug + speed + connect + latency + bench routing (T6) ✓; sizes always-on via progress + summaries ✓.
**Placeholders:** none; the bench responder has TWO versions — Step 3b is authoritative (replaces the buggy first draft) with an explicit invariants note.
**Type consistency:** `transfer.Stats{Bytes,Duration}`; `Send(...)(Stats,error)`; `Receive(...)(string,Stats,error)`; `broker.Config.OnAck(msgID uint64, rtt time.Duration)`; `freeSlot() (time.Time,bool)`; `humanize.{Bytes,Rate,Dur,ParseSize}`; `parseSizes`, `runBench`, `runBenchInitiator`, `runBenchResponder`. Call sites updated in T3 (tests), T5/T6 (CLI).
**Risk:** bench responder message routing — Step 3b note gives invariants; implementer must make build+test pass. Real cross-machine bench verified manually on ec2 after merge.
