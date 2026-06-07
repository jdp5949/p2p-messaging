# Parallel multi-stream transfer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. TDD, checkbox steps.

**Goal:** Send files/dirs over N parallel connections for higher throughput, with clean fallback to single-stream.

**Architecture:** `pkg/transfer` gains a `Stream` interface + `SendParallel`/`ReceiveParallel` using explicit 1-byte message tags and offset-tagged data (WriteAt merge). `cmd/p2p` opens N rendezvous+Noise sessions from one code, negotiates `min(opened)`, adapts `crypto.Session`→`Stream`, and dispatches parallel vs single-stream.

**Tech Stack:** Go, existing pkg/{rendezvous,crypto,codephrase,transfer}.

---

## Task 1: Header fields + transfer.Stream + parallel protocol

**Files:** Modify `pkg/transfer/transfer.go`; Create `pkg/transfer/parallel.go`, `pkg/transfer/parallel_test.go`.

- [ ] **Step 1: extend Header** — in `pkg/transfer/transfer.go` replace the Header struct with:

```go
// Header is the first message of a transfer.
type Header struct {
	T       string `json:"t"`
	Kind    string `json:"kind"` // "file" | "archive"
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	Mode    uint32 `json:"mode"`
	SHA256  string `json:"sha256,omitempty"`  // parallel: whole-payload hash
	Streams int    `json:"streams,omitempty"` // parallel: number of data streams
}
```

- [ ] **Step 2: write the failing test** — `pkg/transfer/parallel_test.go`:

```go
package transfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// memStream is an in-memory Stream backed by two channels (one per direction).
type memStream struct {
	in  <-chan []byte
	out chan<- []byte
}

func (m *memStream) WriteMsg(p []byte) error {
	cp := append([]byte(nil), p...)
	m.out <- cp
	return nil
}
func (m *memStream) ReadMsg() ([]byte, error) {
	b, ok := <-m.in
	if !ok {
		return nil, io.EOF
	}
	return b, nil
}
func (m *memStream) Close() error { return nil }

// streamPair returns two connected Streams.
func streamPair() (a, b Stream) {
	ab := make(chan []byte, 1024)
	ba := make(chan []byte, 1024)
	return &memStream{in: ba, out: ab}, &memStream{in: ab, out: ba}
}

func TestParallelRoundTrip(t *testing.T) {
	const n = 3
	src := t.TempDir()
	path := filepath.Join(src, "blob.bin")
	data := make([]byte, 3*ChunkSize+777) // spans multiple chunks per stream
	for i := range data {
		data[i] = byte(i * 7)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()

	sendStreams := make([]Stream, n)
	recvStreams := make([]Stream, n)
	for i := 0; i < n; i++ {
		sendStreams[i], recvStreams[i] = streamPair()
	}

	var wg sync.WaitGroup
	var rPath string
	var rStats Stats
	var rErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		rPath, rStats, rErr = ReceiveParallel(recvStreams, dst, func(string) bool { return true }, nil)
	}()
	sStats, sErr := SendParallel(sendStreams, []string{path}, nil)
	wg.Wait()

	if sErr != nil || rErr != nil {
		t.Fatalf("send=%v recv=%v", sErr, rErr)
	}
	got, _ := os.ReadFile(rPath)
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %d bytes want %d", len(got), len(data))
	}
	if sStats.Bytes != int64(len(data)) || rStats.Bytes != int64(len(data)) {
		t.Fatalf("bytes send=%d recv=%d want %d", sStats.Bytes, rStats.Bytes, len(data))
	}
	sum := sha256.Sum256(data)
	_ = hex.EncodeToString(sum[:])
}

func TestSplitRanges(t *testing.T) {
	got := splitRanges(100, 3)
	want := [][2]int64{{0, 33}, {33, 66}, {66, 100}}
	if len(got) != 3 {
		t.Fatalf("len %d", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("range %d = %v want %v", i, got[i], want[i])
		}
	}
}
```

Add `"io"` to the test imports (memStream uses io.EOF).

- [ ] **Step 3: run, expect FAIL** — `go test ./pkg/transfer/ -run 'Parallel|SplitRanges'` → undefined.

- [ ] **Step 4: implement** — `pkg/transfer/parallel.go`:

```go
package transfer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Stream is one framed, ordered, reliable message channel (a crypto.Session in
// production). SendParallel/ReceiveParallel use several of them at once.
type Stream interface {
	WriteMsg(p []byte) error
	ReadMsg() ([]byte, error)
	Close() error
}

// 1-byte message tags for the parallel protocol (Streams carry both binary data
// and JSON control, so every message is tagged).
const (
	tagHeader  = 'H'
	tagData    = 'D' // followed by encodeChunk(offset, bytes)
	tagEOS     = 'E' // end of a stream's range
	tagTrailer = 'T'
	tagDone    = 'O'
)

// splitRanges divides [0,total) into n contiguous [lo,hi) ranges.
func splitRanges(total int64, n int) [][2]int64 {
	out := make([][2]int64, n)
	for i := 0; i < n; i++ {
		lo := int64(i) * total / int64(n)
		hi := int64(i+1) * total / int64(n)
		if i == n-1 {
			hi = total
		}
		out[i] = [2]int64{lo, hi}
	}
	return out
}

// SendParallel sends paths across len(streams) streams. streams[0] is control.
func SendParallel(streams []Stream, paths []string, progress ProgressFn) (Stats, error) {
	start := time.Now()
	m := len(streams)
	if m == 0 {
		return Stats{}, fmt.Errorf("transfer: no streams")
	}

	// Resolve the payload to a single file on disk (tar dirs/multi to temp).
	srcPath, hdr, cleanup, err := prepareSource(paths)
	if err != nil {
		return Stats{}, err
	}
	defer cleanup()

	sum, size, err := hashFile(srcPath)
	if err != nil {
		return Stats{}, err
	}
	hdr.SHA256, hdr.Size, hdr.Streams = sum, size, m

	if err := streams[0].WriteMsg(tagged(tagHeader, marshalHeader(hdr))); err != nil {
		return Stats{}, err
	}

	f, err := os.Open(srcPath)
	if err != nil {
		return Stats{}, err
	}
	defer f.Close()

	ranges := splitRanges(size, m)
	var sent int64
	errc := make(chan error, m)
	for i := 0; i < m; i++ {
		go func(i int, lo, hi int64) {
			buf := make([]byte, ChunkSize)
			off := lo
			for off < hi {
				n := int64(len(buf))
				if rem := hi - off; rem < n {
					n = rem
				}
				if _, e := f.ReadAt(buf[:n], off); e != nil {
					errc <- e
					return
				}
				if e := streams[i].WriteMsg(tagged(tagData, encodeChunk(off, buf[:n]))); e != nil {
					errc <- e
					return
				}
				off += n
				if progress != nil {
					progress(atomic.AddInt64(&sent, n), size)
				}
			}
			errc <- streams[i].WriteMsg([]byte{tagEOS})
		}(i, ranges[i][0], ranges[i][1])
	}
	for i := 0; i < m; i++ {
		if e := <-errc; e != nil {
			return Stats{}, e
		}
	}

	if err := streams[0].WriteMsg(tagged(tagTrailer, marshalTrailer(Trailer{SHA256: sum, Total: size}))); err != nil {
		return Stats{}, err
	}

	// Wait for DONE on stream0 (with timeout).
	donec := make(chan error, 1)
	go func() {
		for {
			mb, e := streams[0].ReadMsg()
			if e != nil {
				donec <- e
				return
			}
			if len(mb) > 0 && mb[0] == tagDone {
				donec <- nil
				return
			}
		}
	}()
	select {
	case e := <-donec:
		if e != nil {
			return Stats{}, fmt.Errorf("transfer: waiting for ack: %w", e)
		}
	case <-time.After(ackTimeout):
		return Stats{}, fmt.Errorf("transfer: peer did not acknowledge within %s", ackTimeout)
	}
	return Stats{Bytes: size, Duration: time.Since(start)}, nil
}

// ReceiveParallel receives a parallel transfer. streams[0] is control.
func ReceiveParallel(streams []Stream, destDir string, overwrite OverwriteFn, progress ProgressFn) (string, Stats, error) {
	start := time.Now()
	m := len(streams)

	// HEADER on stream0.
	hb, err := streams[0].ReadMsg()
	if err != nil {
		return "", Stats{}, err
	}
	if len(hb) == 0 || hb[0] != tagHeader {
		return "", Stats{}, fmt.Errorf("transfer: expected header")
	}
	var hdr Header
	if err := json.Unmarshal(hb[1:], &hdr); err != nil {
		return "", Stats{}, fmt.Errorf("transfer: bad header: %w", err)
	}

	tmp, err := os.CreateTemp(destDir, ".p2p-recv-*")
	if err != nil {
		return "", Stats{}, err
	}
	tmpName := tmp.Name()
	if err := tmp.Truncate(hdr.Size); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", Stats{}, err
	}
	cleanup := func() { tmp.Close(); os.Remove(tmpName) }

	// Reader per stream: write DATA at offset until EOS. stream0 also yields
	// TRAILER (after its EOS) — capture it.
	var written int64
	var trailer atomic.Value // Trailer
	errc := make(chan error, m)
	var mu sync.Mutex // guards WriteAt (os.File.WriteAt is safe concurrently on most platforms, but lock to be safe)
	for i := 0; i < m; i++ {
		go func(i int) {
			eosSeen := false
			for {
				mb, e := streams[i].ReadMsg()
				if e != nil {
					errc <- e
					return
				}
				switch mb[0] {
				case tagData:
					off, data, de := decodeChunk(mb[1:])
					if de != nil {
						errc <- de
						return
					}
					mu.Lock()
					_, we := tmp.WriteAt(data, off)
					mu.Unlock()
					if we != nil {
						errc <- we
						return
					}
					if progress != nil {
						progress(atomic.AddInt64(&written, int64(len(data))), hdr.Size)
					}
				case tagEOS:
					eosSeen = true
					if i != 0 {
						errc <- nil
						return
					}
					// stream0: keep reading for TRAILER.
				case tagTrailer:
					var tr Trailer
					if e := json.Unmarshal(mb[1:], &tr); e != nil {
						errc <- e
						return
					}
					trailer.Store(tr)
					errc <- nil
					return
				}
				_ = eosSeen
			}
		}(i)
	}
	for i := 0; i < m; i++ {
		if e := <-errc; e != nil {
			cleanup()
			return "", Stats{}, e
		}
	}

	tr, _ := trailer.Load().(Trailer)
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", Stats{}, err
	}
	tmp.Close()

	sum, total, err := hashFile(tmpName)
	if err != nil {
		os.Remove(tmpName)
		return "", Stats{}, err
	}
	if sum != tr.SHA256 || total != tr.Total {
		os.Remove(tmpName)
		return "", Stats{}, fmt.Errorf("transfer: integrity check failed")
	}

	saved, err := finalize(tmpName, destDir, hdr, overwrite)
	if err != nil {
		return "", Stats{}, err
	}
	_ = streams[0].WriteMsg([]byte{tagDone})
	return saved, Stats{Bytes: total, Duration: time.Since(start)}, nil
}

// tagged prefixes a 1-byte tag.
func tagged(tag byte, body []byte) []byte {
	out := make([]byte, 1+len(body))
	out[0] = tag
	copy(out[1:], body)
	return out
}
```

The helpers `prepareSource` and `finalize` factor logic shared with the
single-stream path. Add them to `parallel.go`:

```go
// prepareSource resolves paths to a single on-disk file to send. A lone regular
// file is used directly; a dir or multiple paths are tarred to a temp file.
func prepareSource(paths []string) (srcPath string, hdr Header, cleanup func(), err error) {
	cleanup = func() {}
	if len(paths) == 0 {
		return "", Header{}, cleanup, fmt.Errorf("transfer: no paths")
	}
	if len(paths) == 1 {
		fi, e := os.Stat(paths[0])
		if e != nil {
			return "", Header{}, cleanup, e
		}
		if fi.Mode().IsRegular() {
			return paths[0], Header{Kind: "file", Name: filepath.Base(paths[0]), Mode: uint32(fi.Mode())}, cleanup, nil
		}
	}
	// archive
	tf, e := os.CreateTemp("", "p2p-tar-*")
	if e != nil {
		return "", Header{}, cleanup, e
	}
	name := "bundle.tar"
	if len(paths) == 1 {
		name = filepath.Base(filepath.Clean(paths[0])) + ".tar"
	}
	if e := writeTar(tf, paths); e != nil {
		tf.Close()
		os.Remove(tf.Name())
		return "", Header{}, cleanup, e
	}
	tf.Close()
	tmpName := tf.Name()
	return tmpName, Header{Kind: "archive", Name: name}, func() { os.Remove(tmpName) }, nil
}

// finalize places the verified temp file as the final file, or unpacks an
// archive into destDir. Mirrors the single-stream Receive end-game.
func finalize(tmpName, destDir string, hdr Header, overwrite OverwriteFn) (string, error) {
	if hdr.Kind == "archive" {
		f, e := os.Open(tmpName)
		if e != nil {
			os.Remove(tmpName)
			return "", e
		}
		e = extractTar(f, destDir)
		f.Close()
		os.Remove(tmpName)
		if e != nil {
			return "", e
		}
		return destDir, nil
	}
	final := filepath.Join(destDir, filepath.Base(hdr.Name))
	if _, statErr := os.Stat(final); statErr == nil && (overwrite == nil || !overwrite(filepath.Base(hdr.Name))) {
		final += ".part"
	}
	if hdr.Mode != 0 {
		_ = os.Chmod(tmpName, os.FileMode(hdr.Mode))
	}
	if e := os.Rename(tmpName, final); e != nil {
		os.Remove(tmpName)
		return "", e
	}
	return final, nil
}
```

- [ ] **Step 5: run, expect PASS** — `go test ./pkg/transfer/ -race -run 'Parallel|SplitRanges' -v`, then full `go test ./pkg/transfer/ -race`. `go vet ./pkg/transfer/`.

- [ ] **Step 6: commit** — `git add pkg/transfer && git commit -m "feat(transfer): parallel multi-stream Send/Receive over Stream interface"`.

---

## Task 2: cmd/p2p stream setup + negotiation + adapter

**Files:** Create `cmd/p2p/streams.go`, `cmd/p2p/streams_test.go`. Depends on Task 1 + existing `sessionDialer`, `rendezvous`, `crypto`, `codephrase`.

- [ ] **Step 1: failing test** — `cmd/p2p/streams_test.go`:

```go
package main

import "testing"

func TestNegotiateMin(t *testing.T) {
	if got := minInt(4, 2); got != 2 {
		t.Fatalf("minInt=%d", got)
	}
	if got := minInt(1, 5); got != 1 {
		t.Fatalf("minInt=%d", got)
	}
}
```

- [ ] **Step 2: run, expect FAIL** — `go test ./cmd/p2p/ -run TestNegotiateMin`.

- [ ] **Step 3: implement** — `cmd/p2p/streams.go`:

```go
package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/codephrase"
	"github.com/jdp5949/p2p-messaging/pkg/crypto"
	"github.com/jdp5949/p2p-messaging/pkg/rendezvous"
	"github.com/jdp5949/p2p-messaging/pkg/transfer"
)

const streamReadBuf = 1 << 20 // 1 MB: max framed message the adapter reads

// sessionStream adapts a *crypto.Session to transfer.Stream.
type sessionStream struct{ s *crypto.Session }

func (a *sessionStream) WriteMsg(p []byte) error { _, e := a.s.Write(p); return e }
func (a *sessionStream) ReadMsg() ([]byte, error) {
	buf := make([]byte, streamReadBuf)
	n, e := a.s.Read(buf)
	if e != nil {
		return nil, e
	}
	return buf[:n], nil
}
func (a *sessionStream) Close() error { return a.s.Close() }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// streamConfig carries what openSession needs.
type streamConfig struct {
	relayAddr string
	useTLS    bool
	tlsConfig *tls.Config
	code      string
	id        *crypto.Identity
	known     *crypto.KnownPeers
	initiator bool
}

// openSession opens one stream i: rendezvous + Noise handshake.
func openSession(cfg streamConfig, i int) (*crypto.Session, error) {
	sid := codephrase.SessionID(fmt.Sprintf("%s#%d", cfg.code, i))
	ctx, cancel := contextWithTimeout(15 * time.Second)
	defer cancel()
	res, err := rendezvous.Dial(ctx, rendezvous.Options{
		RelayAddr: cfg.relayAddr, SessionID: sid, TLS: cfg.useTLS, TLSConfig: cfg.tlsConfig, PunchTimeout: 6 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	sess, err := crypto.Handshake(res.Conn, crypto.HandshakeConfig{
		Identity: cfg.id, KnownPeers: cfg.known, PeerName: sid, PAKECode: cfg.code, Initiator: cfg.initiator,
	})
	if err != nil {
		res.Conn.Close()
		return nil, err
	}
	return sess, nil
}

// openControlMsg is the small JSON used to negotiate the stream count on stream0.
type ctrlMsg struct {
	T string `json:"t"`
	N int    `json:"n,omitempty"`
	K int    `json:"k,omitempty"`
}

// openStreams establishes up to want streams and negotiates the agreed count m.
// Returns the transfer.Streams (len m, index 0 = control). Caller closes them.
func openStreams(cfg streamConfig, want int) ([]transfer.Stream, error) {
	// Stream 0 (control) must succeed.
	s0, err := openSession(cfg, 0)
	if err != nil {
		return nil, fmt.Errorf("control stream: %w", err)
	}
	ctrl := &sessionStream{s: s0}

	// Negotiate N (initiator announces; joiner learns).
	if cfg.initiator {
		b, _ := json.Marshal(ctrlMsg{T: "want", N: want})
		if err := ctrl.WriteMsg(b); err != nil {
			ctrl.Close()
			return nil, err
		}
	} else {
		mb, err := ctrl.ReadMsg()
		if err != nil {
			ctrl.Close()
			return nil, err
		}
		var cm ctrlMsg
		if json.Unmarshal(mb, &cm) != nil || cm.T != "want" {
			ctrl.Close()
			return nil, fmt.Errorf("expected want, got %q", mb)
		}
		want = cm.N
	}

	// Open streams 1..want-1 concurrently.
	sessions := make([]*crypto.Session, want)
	sessions[0] = s0
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 1; i < want; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := openSession(cfg, i)
			if err == nil {
				mu.Lock()
				sessions[i] = s
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	kSelf := 0
	for _, s := range sessions {
		if s != nil {
			kSelf++
		}
	}

	// Exchange opened counts; both compute min.
	myCount, _ := json.Marshal(ctrlMsg{T: "opened", K: kSelf})
	if err := ctrl.WriteMsg(myCount); err != nil {
		closeAll(sessions)
		return nil, err
	}
	mb, err := ctrl.ReadMsg()
	if err != nil {
		closeAll(sessions)
		return nil, err
	}
	var peer ctrlMsg
	if json.Unmarshal(mb, &peer) != nil || peer.T != "opened" {
		closeAll(sessions)
		return nil, fmt.Errorf("expected opened, got %q", mb)
	}
	m := minInt(kSelf, peer.K)

	// Compact the first m non-nil contiguous sessions; close the rest.
	// (Both sides opened the same indices set on success; index 0 always present.
	//  Use the first m sessions by index that are non-nil — they match because
	//  rendezvous#i pairs deterministically; if asymmetric, m already accounts.)
	out := make([]transfer.Stream, 0, m)
	for i := 0; i < want && len(out) < m; i++ {
		if sessions[i] != nil {
			out = append(out, &sessionStream{s: sessions[i]})
			sessions[i] = nil
		}
	}
	closeAll(sessions) // close leftovers
	return out, nil
}

func closeAll(ss []*crypto.Session) {
	for _, s := range ss {
		if s != nil {
			s.Close()
		}
	}
}
```

Add a tiny context helper (avoid importing context everywhere) at the bottom:

```go
import "context"

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
```
(Place the `context` import in the import block, not inline — adjust as the
compiler requires.)

> Implementer note: the index-matching across peers relies on `rendezvous#i`
> pairing the same indices on both ends. Because each index is its own relay
> session, only matching indices connect; a failed index is nil on BOTH sides
> (the peer's Dial for that index times out too). So the non-nil index sets
> match, and taking the first `m` by index is consistent on both sides. Keep
> this property; if a test shows divergence, fall back to using only
> contiguous indices `0..m-1` (require those specific indices open) — simpler
> and still correct.

- [ ] **Step 4: run, expect PASS** — `go test ./cmd/p2p/ -run TestNegotiateMin`. `go build ./... && go vet ./cmd/p2p/`.

- [ ] **Step 5: commit** — `git add cmd/p2p/streams.go cmd/p2p/streams_test.go && git commit -m "feat(p2p): multi-stream session setup, min-negotiation, crypto.Session adapter"`.

---

## Task 3: wire -streams into the CLI send/receive

**Files:** Modify `cmd/p2p/main.go`. Depends on Tasks 1, 2.

- [ ] **Step 1: implement** — READ main.go first. Add:

1. Flag: `streams := flag.Int("streams", 4, "parallel connections for file transfer (1 = single stream)")`.

2. For **file send** (`len(sendPaths) > 0`) and for the **receiver header path**, when `*streams > 1` (sender) or always (receiver learns N), use the multi-stream path instead of building one conn+broker. Concretely, restructure: BEFORE the existing single conn/broker setup, branch:

```go
	// Multi-stream file transfer (sender with -streams>1, or any receiver:
	// the negotiation downgrades to 1 transparently).
	if len(sendPaths) > 0 && *streams > 1 || (initiator == false /* receiver */ && *streams >= 1 && wantParallel) {
		...
	}
```

This is fiddly; instead implement a clean helper and call it for the two file
cases. Add to main.go:

```go
// runParallelSend connects N streams and sends paths; falls back to single
// stream via the existing transfer.Send when only 1 stream is available.
func runParallelSend(cfg streamConfig, want int, sendPaths []string, debug bool) {
	streams, err := openStreams(cfg, want)
	fatalOn(err, "open streams")
	defer func() {
		for _, s := range streams {
			s.Close()
		}
	}()
	if debug {
		fmt.Fprintf(os.Stderr, "negotiated %d stream(s)\n", len(streams))
	}
	fmt.Fprintf(os.Stderr, "Sending %d item(s) over %d stream(s)…\n", len(sendPaths), len(streams))
	st, err := transfer.SendParallel(streams, sendPaths, progressBar)
	fmt.Fprintln(os.Stderr)
	fatalOn(err, "send")
	fmt.Fprintf(os.Stderr, "✓ sent %s in %s (%s)\n", humanize.Bytes(st.Bytes), humanize.Dur(st.Duration), humanize.Rate(st.Bytes, st.Duration))
}

// runParallelReceive connects N streams (count learned via negotiation) and
// receives into the cwd.
func runParallelReceive(cfg streamConfig, want int, debug bool) {
	streams, err := openStreams(cfg, want)
	fatalOn(err, "open streams")
	defer func() {
		for _, s := range streams {
			s.Close()
		}
	}()
	if debug {
		fmt.Fprintf(os.Stderr, "negotiated %d stream(s)\n", len(streams))
	}
	dest, _ := os.Getwd()
	saved, st, err := transfer.ReceiveParallel(streams, dest, promptOverwrite, progressBar)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "receive failed: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "✓ saved %s — %s in %s (%s), sha256 ok\n", saved,
		humanize.Bytes(st.Bytes), humanize.Dur(st.Duration), humanize.Rate(st.Bytes, st.Duration))
}
```

3. In `main`, after building `hsCfg` (identity/known loaded) and `dialer`'s
relay/TLS fields are known, construct a `streamConfig`:

```go
	scfg := streamConfig{
		relayAddr: *relayAddr, useTLS: !*noTLS, code: code,
		id: id, known: kp, initiator: initiator,
	}
	if scfg.useTLS {
		host, _, _ := net.SplitHostPort(*relayAddr)
		scfg.tlsConfig = &tls.Config{ServerName: host}
	}
```
(You will need `id` and `kp` in scope — they are created when `!*noCrypto`. For
the multi-stream path require crypto on; if `*noCrypto`, skip parallel and use
the existing single path.)

4. Dispatch:
- If `len(sendPaths) > 0 && !*noCrypto`: `runParallelSend(scfg, *streams, sendPaths, *debug); return`.
- Receiver (`!initiator`) chat-vs-file is decided by the FIRST message today. For
  parallel, the receiver must decide BEFORE connecting data streams whether this
  is a file (parallel) or chat. KEEP IT SIMPLE: a parallel **file** session uses
  stream indices `#0..#N-1`; a **chat** session uses the plain code (index-less,
  today's path). To avoid ambiguity, the receiver tries parallel-receive only
  when the initiator used `send <path>`. Since the receiver can't know that
  a-priori, use this rule: **the receiver always attempts the single
  control-stream path first (today's `p2p <code>` flow), and file transfers are
  carried on it as stream `#0`'s HEADER.** 

  Therefore, simplest correct integration: the SENDER with `-streams>1` still
  opens `#0..#N-1`; the RECEIVER's existing `p2p <code>` opens the plain code
  session as today AND, upon reading a transfer HEADER with `Streams>1`, opens
  the additional streams `#1..#N-1` then calls `transfer.ReceiveParallel`.

  **Decision for v1 (keep main.go changes contained):** Make the SENDER open
  `#0..#N-1` where `#0`'s session is the SAME as today's default session id
  `SessionID(code)` (NOT `code#0`). I.e. stream 0 uses `SessionID(code)`, streams
  1..N-1 use `SessionID(code#i)`. The receiver's normal `p2p <code>` connects
  stream 0 (today's path), reads HEADER; if `Streams>1`, it opens `#1..#N-1`
  and runs `ReceiveParallel`; else single-stream `Receive` as today.

  Update `openSession` accordingly: index 0 → `SessionID(cfg.code)`; index i>0 →
  `SessionID(code#i)`. And the negotiation `want/opened` over stream0 happens
  AFTER the HEADER? No — restructure so stream0 is the existing conn and the
  parallel extras are added on demand. **This is the integration crux**; if it
  proves too tangled in one pass, fall back to: parallel send/receive BOTH use a
  dedicated `bench`-like path (not the chat path) keyed entirely on `code#i`
  including i=0, and the receiver enters it only via an explicit signal — but the
  cleanest is the on-demand upgrade above.

> Implementer: choose the on-demand-upgrade integration. Make `go build`,
> `go vet`, `go test ./...` pass and the help path work. Keep chat unchanged.
> If full integration risks correctness, implement the simplest version that
> passes tests and clearly note what was deferred (status DONE_WITH_CONCERNS).

- [ ] **Step 2: build/vet/test** — `go build ./... && go vet ./... && go test ./...`.
- [ ] **Step 3: smoke** — `go run ./cmd/p2p ; echo exit=$?` → usage, exit 2.
- [ ] **Step 4: commit** — `git add cmd/p2p/main.go && git commit -m "feat(p2p): -streams parallel file transfer with on-demand stream upgrade"`.

---

## Task 4: README + docs

**Files:** Modify `README.md`, `docs/site/index.html`.

- [ ] **Step 1** — document `-streams` (default 4) under the file-send usage; note it speeds large transfers on real internet paths and falls back to single-stream automatically. Add one line to the pages "Getting Started"/"Two Nodes" code blocks: `p2p send bigfile.iso        # uses 4 parallel streams by default`.
- [ ] **Step 2** — `go build ./...` sanity.
- [ ] **Step 3** — `git add README.md docs/site/index.html && git commit -m "docs: document -streams parallel transfer"`.

---

## Self-Review
**Spec coverage:** Stream interface + SendParallel/ReceiveParallel (T1); session setup + min-negotiation + adapter (T2); CLI -streams + dispatch + fallback (T3); docs (T4). Tagged framing replaces ContentType-based classify for streams. Offset WriteAt merge. SHA verify. ✓
**Placeholders:** none for T1/T2/T4 (full code). T3 is integration with an explicit decision (on-demand upgrade) + permission to ship simplest-correct with DONE_WITH_CONCERNS — flagged, not a silent gap.
**Type consistency:** `transfer.Stream`, `SendParallel(streams,paths,progress)(Stats,error)`, `ReceiveParallel(streams,dest,ow,progress)(string,Stats,error)`, `splitRanges`, `tagged`, tags H/D/E/T/O; `sessionStream`, `openStreams`, `openSession`, `minInt`, `streamConfig`, `ctrlMsg`. Reuses `marshalHeader/marshalTrailer/encodeChunk/decodeChunk/hashFile/writeTar/extractTar/ackTimeout/ChunkSize` from transfer pkg.
**Risk:** Task 3 integration is the hard part (chat-vs-file receiver routing + on-demand stream upgrade). Real ec2 bench (single vs 4-stream) will validate throughput gain; correctness gated by SHA + roundtrip tests.
