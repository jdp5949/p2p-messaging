# File transfer + one-line installer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add croc-style file/dir transfer (`p2p send <path...>`) on the existing reliable encrypted stack, plus one-line installer scripts (`install.sh`, `install.ps1`).

**Architecture:** A new `pkg/transfer` package speaks only abstract `SendFunc`/`<-chan Msg` interfaces (no broker/crypto coupling) so it is unit-testable with plain channels. Protocol = HEADER(json) + DATA(binary `[8-byte offset][bytes]`) + TRAILER(json sha256) + DONE(json) ack. Single files stream raw; dirs/multi-path stream as a tar. `cmd/p2p` wires `transfer` to the broker and auto-detects file-vs-chat from the first inbound message.

**Tech Stack:** Go 1.21+, existing `pkg/{broker,conn,crypto,protocol}`, stdlib `archive/tar`, `crypto/sha256`. Installer: POSIX `sh` + PowerShell.

---

## File Structure

- Create `pkg/transfer/transfer.go` — types, chunk codec, message classify, json marshal.
- Create `pkg/transfer/transfer_test.go`
- Create `pkg/transfer/archive.go` — tar pack/unpack with zip-slip guard.
- Create `pkg/transfer/archive_test.go`
- Create `pkg/transfer/send.go` — `Send`.
- Create `pkg/transfer/send_test.go`
- Create `pkg/transfer/receive.go` — `Receive` + `hashFile`.
- Create `pkg/transfer/receive_test.go` (includes full Send↔Receive roundtrips).
- Create `cmd/p2p/progress.go` — progress bar + overwrite prompt.
- Create `cmd/p2p/progress_test.go`
- Modify `cmd/p2p/main.go` — send/receive modes, auto-detect.
- Create `install.sh`, `install.ps1`.
- Modify `README.md` — one-line install.

---

## Task 1: transfer core (types, chunk codec, classify)

**Files:**
- Create: `pkg/transfer/transfer.go`
- Test: `pkg/transfer/transfer_test.go`

- [ ] **Step 1: Write the failing test**

`pkg/transfer/transfer_test.go`:

```go
package transfer

import (
	"bytes"
	"testing"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

func TestChunkCodecRoundTrip(t *testing.T) {
	data := []byte("hello world")
	enc := encodeChunk(42, data)
	off, got, err := decodeChunk(enc)
	if err != nil {
		t.Fatal(err)
	}
	if off != 42 {
		t.Fatalf("offset = %d, want 42", off)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data = %q, want %q", got, data)
	}
	if _, _, err := decodeChunk([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error on short chunk")
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		m    Msg
		want string
	}{
		{Msg{protocol.ContentJSON, marshalHeader(Header{Kind: "file", Name: "a"})}, "header"},
		{Msg{protocol.ContentJSON, marshalTrailer(Trailer{SHA256: "x"})}, "trailer"},
		{Msg{protocol.ContentJSON, marshalDone()}, "done"},
		{Msg{protocol.ContentBinary, []byte{0, 0, 0, 0, 0, 0, 0, 0, 9}}, "data"},
		{Msg{protocol.ContentText, []byte("hi")}, "other"},
	}
	for _, c := range cases {
		if got := classify(c.m); got != c.want {
			t.Fatalf("classify=%q want %q", got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/transfer/ -run 'TestChunkCodecRoundTrip|TestClassify' -v`
Expected: FAIL — undefined package/symbols.

- [ ] **Step 3: Implement**

`pkg/transfer/transfer.go`:

```go
// Package transfer implements croc-style file/dir transfer over an abstract
// reliable, ordered, encrypted message channel (provided by the broker). It is
// deliberately decoupled from the broker so it can be tested with plain
// channels.
//
// Protocol (each item is one channel message):
//
//	HEADER  ContentJSON  {"t":"header","kind":"file|archive","name":..,"size":..,"mode":..}
//	DATA    ContentBinary [8-byte big-endian offset][bytes]   (repeated)
//	TRAILER ContentJSON  {"t":"trailer","sha256":hex,"total":N}
//	DONE    ContentJSON  {"t":"done"}     (receiver -> sender, after save+verify)
package transfer

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// ChunkSize is the DATA payload size (excluding the 8-byte offset prefix).
const ChunkSize = 512 * 1024

// Msg is one message on the transfer channel.
type Msg struct {
	ContentType protocol.ContentType
	Payload     []byte
}

// SendFunc transmits one message. In production it wraps broker.Send.
type SendFunc func(ct protocol.ContentType, payload []byte) error

// ProgressFn reports bytes transferred; total is 0 when unknown (archives).
type ProgressFn func(done, total int64)

// OverwriteFn is consulted when a destination file already exists.
type OverwriteFn func(name string) bool

// Header is the first message of a transfer.
type Header struct {
	T    string `json:"t"`
	Kind string `json:"kind"` // "file" | "archive"
	Name string `json:"name"`
	Size int64  `json:"size"`
	Mode uint32 `json:"mode"`
}

// Trailer ends the data stream.
type Trailer struct {
	T      string `json:"t"`
	SHA256 string `json:"sha256"`
	Total  int64  `json:"total"`
}

func marshalHeader(h Header) []byte  { h.T = "header"; b, _ := json.Marshal(h); return b }
func marshalTrailer(t Trailer) []byte { t.T = "trailer"; b, _ := json.Marshal(t); return b }
func marshalDone() []byte             { b, _ := json.Marshal(map[string]string{"t": "done"}); return b }

// encodeChunk prefixes data with an 8-byte big-endian offset.
func encodeChunk(offset int64, data []byte) []byte {
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[:8], uint64(offset))
	copy(buf[8:], data)
	return buf
}

// decodeChunk splits the 8-byte offset prefix from the data.
func decodeChunk(b []byte) (int64, []byte, error) {
	if len(b) < 8 {
		return 0, nil, errors.New("transfer: chunk too short")
	}
	return int64(binary.BigEndian.Uint64(b[:8])), b[8:], nil
}

// classify returns "header","trailer","done","data", or "other".
func classify(m Msg) string {
	if m.ContentType == protocol.ContentBinary {
		return "data"
	}
	if m.ContentType == protocol.ContentJSON {
		var probe struct {
			T string `json:"t"`
		}
		if json.Unmarshal(m.Payload, &probe) == nil && probe.T != "" {
			return probe.T
		}
	}
	return "other"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/transfer/ -run 'TestChunkCodecRoundTrip|TestClassify' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/transfer/transfer.go pkg/transfer/transfer_test.go
git commit -m "feat(transfer): protocol types, chunk codec, message classify"
```

---

## Task 2: archive (tar pack/unpack + zip-slip guard)

**Files:**
- Create: `pkg/transfer/archive.go`
- Test: `pkg/transfer/archive_test.go`

- [ ] **Step 1: Write the failing test**

`pkg/transfer/archive_test.go`:

```go
package transfer

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestTarRoundTrip(t *testing.T) {
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("beta"), 0o600); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := writeTar(&buf, []string{src}); err != nil {
		t.Fatalf("writeTar: %v", err)
	}

	dst := t.TempDir()
	if err := extractTar(&buf, dst); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	base := filepath.Base(src)
	got, err := os.ReadFile(filepath.Join(dst, base, "a.txt"))
	if err != nil || string(got) != "alpha" {
		t.Fatalf("a.txt = %q err=%v", got, err)
	}
	got, err = os.ReadFile(filepath.Join(dst, base, "sub", "b.txt"))
	if err != nil || string(got) != "beta" {
		t.Fatalf("b.txt = %q err=%v", got, err)
	}
	fi, _ := os.Stat(filepath.Join(dst, base, "sub", "b.txt"))
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("perm = %o, want 600", fi.Mode().Perm())
	}
}

func TestExtractTarRejectsZipSlip(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "../evil.txt", Mode: 0o644, Size: 4, Typeflag: tar.TypeReg})
	tw.Write([]byte("evil"))
	tw.Close()

	dst := t.TempDir()
	if err := extractTar(&buf, dst); err == nil {
		t.Fatal("expected zip-slip rejection")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dst), "evil.txt")); err == nil {
		t.Fatal("zip-slip wrote outside dest")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/transfer/ -run 'TestTar' -v`
Expected: FAIL — undefined writeTar/extractTar.

- [ ] **Step 3: Implement**

`pkg/transfer/archive.go`:

```go
package transfer

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// writeTar streams a tar of paths into w, preserving relative structure
// (relative to each path's parent) and file modes. Directories recurse.
func writeTar(w io.Writer, paths []string) error {
	tw := tar.NewWriter(w)
	for _, p := range paths {
		clean := filepath.Clean(p)
		base := filepath.Dir(clean)
		err := filepath.Walk(clean, func(file string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(base, file)
			if err != nil {
				return err
			}
			hdr, err := tar.FileInfoHeader(fi, "")
			if err != nil {
				return err
			}
			hdr.Name = filepath.ToSlash(rel)
			if fi.IsDir() {
				hdr.Name += "/"
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			if fi.Mode().IsRegular() {
				f, err := os.Open(file)
				if err != nil {
					return err
				}
				_, err = io.Copy(tw, f)
				f.Close()
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return tw.Close()
}

// extractTar unpacks r into destDir, rejecting any entry that escapes destDir
// (zip-slip) and skipping symlinks/special files.
func extractTar(r io.Reader, destDir string) error {
	tr := tar.NewReader(r)
	cleanDest := filepath.Clean(destDir)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target := filepath.Join(cleanDest, hdr.Name)
		if target != cleanDest &&
			!strings.HasPrefix(filepath.Clean(target)+string(os.PathSeparator), cleanDest+string(os.PathSeparator)) {
			return fmt.Errorf("transfer: unsafe path in archive: %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil { //nolint:gosec
				f.Close()
				return err
			}
			f.Close()
		default:
			// skip symlinks and special files for safety
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/transfer/ -run 'TestTar' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/transfer/archive.go pkg/transfer/archive_test.go
git commit -m "feat(transfer): tar pack/unpack with zip-slip protection"
```

---

## Task 3: Send

Depends on Task 1 + Task 2 being present in the working tree.

**Files:**
- Create: `pkg/transfer/send.go`
- Test: `pkg/transfer/send_test.go`

- [ ] **Step 1: Write the failing test**

`pkg/transfer/send_test.go`:

```go
package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

func TestSendSingleFileSequence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	content := make([]byte, ChunkSize+1234) // forces 2 data chunks
	for i := range content {
		content[i] = byte(i)
	}
	if err := os.WriteFile(path, content, 0o640); err != nil {
		t.Fatal(err)
	}

	var msgs []Msg
	send := func(ct protocol.ContentType, p []byte) error {
		msgs = append(msgs, Msg{ct, append([]byte(nil), p...)})
		return nil
	}
	// Provide an immediate DONE so Send returns.
	done := make(chan Msg, 1)
	done <- Msg{protocol.ContentJSON, marshalDone()}
	close(done)

	if err := Send(send, done, []string{path}, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if classify(msgs[0]) != "header" {
		t.Fatalf("first msg = %s, want header", classify(msgs[0]))
	}
	var h Header
	json.Unmarshal(msgs[0].Payload, &h)
	if h.Kind != "file" || h.Name != "data.bin" || h.Size != int64(len(content)) {
		t.Fatalf("bad header: %+v", h)
	}
	last := msgs[len(msgs)-1]
	if classify(last) != "trailer" {
		t.Fatalf("last msg = %s, want trailer", classify(last))
	}
	var tr Trailer
	json.Unmarshal(last.Payload, &tr)
	sum := sha256.Sum256(content)
	if tr.SHA256 != hex.EncodeToString(sum[:]) || tr.Total != int64(len(content)) {
		t.Fatalf("bad trailer: %+v", tr)
	}
	// middle messages are data chunks
	dataCount := 0
	for _, m := range msgs[1 : len(msgs)-1] {
		if classify(m) == "data" {
			dataCount++
		}
	}
	if dataCount != 2 {
		t.Fatalf("data chunks = %d, want 2", dataCount)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/transfer/ -run TestSendSingleFileSequence -v`
Expected: FAIL — undefined Send.

- [ ] **Step 3: Implement**

`pkg/transfer/send.go`:

```go
package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// Send transmits paths via send. A single regular file is streamed raw; a
// directory or multiple paths are streamed as a tar archive. After the trailer,
// Send blocks until the receiver returns a DONE message on in, guaranteeing the
// data was saved+verified before the caller exits.
func Send(send SendFunc, in <-chan Msg, paths []string, progress ProgressFn) error {
	if len(paths) == 0 {
		return fmt.Errorf("transfer: no paths")
	}

	var (
		hdr     Header
		source  io.Reader
		size    int64
		closeFn = func() error { return nil }
	)

	single := false
	if len(paths) == 1 {
		fi, err := os.Stat(paths[0])
		if err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			single = true
			f, err := os.Open(paths[0])
			if err != nil {
				return err
			}
			source, closeFn, size = f, f.Close, fi.Size()
			hdr = Header{Kind: "file", Name: filepath.Base(paths[0]), Size: size, Mode: uint32(fi.Mode())}
		}
	}

	if !single {
		pr, pw := io.Pipe()
		go func() { pw.CloseWithError(writeTar(pw, paths)) }()
		source, closeFn = pr, pr.Close
		name := "bundle.tar"
		if len(paths) == 1 {
			name = filepath.Base(filepath.Clean(paths[0])) + ".tar"
		}
		hdr = Header{Kind: "archive", Name: name}
	}
	defer closeFn()

	if err := send(protocol.ContentJSON, marshalHeader(hdr)); err != nil {
		return err
	}

	h := sha256.New()
	tee := io.TeeReader(source, h)
	buf := make([]byte, ChunkSize)
	var offset int64
	for {
		n, rerr := tee.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if err := send(protocol.ContentBinary, encodeChunk(offset, chunk)); err != nil {
				return err
			}
			offset += int64(n)
			if progress != nil {
				progress(offset, size)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}

	tr := Trailer{SHA256: hex.EncodeToString(h.Sum(nil)), Total: offset}
	if err := send(protocol.ContentJSON, marshalTrailer(tr)); err != nil {
		return err
	}

	// Wait for the receiver's DONE ack.
	for m := range in {
		if classify(m) == "done" {
			return nil
		}
	}
	return fmt.Errorf("transfer: peer closed before acknowledging")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/transfer/ -run TestSendSingleFileSequence -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/transfer/send.go pkg/transfer/send_test.go
git commit -m "feat(transfer): streaming Send for files and tar archives with sha256"
```

---

## Task 4: Receive (+ full roundtrips)

Depends on Task 1, 2, 3.

**Files:**
- Create: `pkg/transfer/receive.go`
- Test: `pkg/transfer/receive_test.go`

- [ ] **Step 1: Write the failing test**

`pkg/transfer/receive_test.go`:

```go
package transfer

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// pair wires Send (A) and Receive (B) over two channels and runs the transfer.
func pair(t *testing.T, paths []string, dst string, ow OverwriteFn) (string, error) {
	t.Helper()
	a2b := make(chan Msg, 512)
	b2a := make(chan Msg, 8)
	sendA := func(ct protocol.ContentType, p []byte) error { a2b <- Msg{ct, append([]byte(nil), p...)}; return nil }
	sendB := func(ct protocol.ContentType, p []byte) error { b2a <- Msg{ct, append([]byte(nil), p...)}; return nil }

	type rr struct {
		path string
		err  error
	}
	res := make(chan rr, 1)
	go func() {
		p, e := Receive(sendB, a2b, dst, ow, nil)
		res <- rr{p, e}
	}()
	sendErr := Send(sendA, b2a, paths, nil)
	r := <-res
	if sendErr != nil && r.err == nil {
		return r.path, sendErr
	}
	return r.path, r.err
}

func TestRoundTripFile(t *testing.T) {
	src := t.TempDir()
	path := filepath.Join(src, "hello.txt")
	os.WriteFile(path, []byte("hello transfer"), 0o644)

	dst := t.TempDir()
	saved, err := pair(t, []string{path}, dst, func(string) bool { return true })
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	got, _ := os.ReadFile(saved)
	if string(got) != "hello transfer" {
		t.Fatalf("got %q", got)
	}
	if filepath.Base(saved) != "hello.txt" {
		t.Fatalf("saved name = %s", filepath.Base(saved))
	}
}

func TestRoundTripDir(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "d"), 0o755)
	os.WriteFile(filepath.Join(src, "d", "x.txt"), []byte("xx"), 0o644)

	dst := t.TempDir()
	_, err := pair(t, []string{filepath.Join(src, "d")}, dst, func(string) bool { return true })
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "d", "x.txt"))
	if err != nil || string(got) != "xx" {
		t.Fatalf("unpacked = %q err=%v", got, err)
	}
}

func TestReceiveHashMismatch(t *testing.T) {
	dst := t.TempDir()
	in := make(chan Msg, 8)
	in <- Msg{protocol.ContentJSON, marshalHeader(Header{Kind: "file", Name: "f.bin", Size: 3})}
	in <- Msg{protocol.ContentBinary, encodeChunk(0, []byte("abc"))}
	in <- Msg{protocol.ContentJSON, marshalTrailer(Trailer{SHA256: "deadbeef", Total: 3})}
	close(in)

	_, err := Receive(func(protocol.ContentType, []byte) error { return nil }, in, dst, nil, nil)
	if err == nil {
		t.Fatal("expected integrity error")
	}
	// no leftover files in dst
	entries, _ := os.ReadDir(dst)
	if len(entries) != 0 {
		t.Fatalf("temp not cleaned: %v", entries)
	}
}

func TestReceiveOverwriteDeclinedKeepsPart(t *testing.T) {
	src := t.TempDir()
	path := filepath.Join(src, "f.txt")
	os.WriteFile(path, []byte("new"), 0o644)

	dst := t.TempDir()
	os.WriteFile(filepath.Join(dst, "f.txt"), []byte("OLD"), 0o644)

	saved, err := pair(t, []string{path}, dst, func(string) bool { return false })
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	if filepath.Ext(saved) != ".part" {
		t.Fatalf("expected .part, got %s", saved)
	}
	orig, _ := os.ReadFile(filepath.Join(dst, "f.txt"))
	if string(orig) != "OLD" {
		t.Fatalf("original clobbered: %q", orig)
	}
}

func TestReceiveOutOfOrderChunks(t *testing.T) {
	dst := t.TempDir()
	full := []byte("ABCDEFGHIJ")
	sum := sha256OfBytes(full)

	in := make(chan Msg, 8)
	in <- Msg{protocol.ContentJSON, marshalHeader(Header{Kind: "file", Name: "ooo.txt", Size: int64(len(full))})}
	// out of order + duplicate
	in <- Msg{protocol.ContentBinary, encodeChunk(5, full[5:])}
	in <- Msg{protocol.ContentBinary, encodeChunk(0, full[0:5])}
	in <- Msg{protocol.ContentBinary, encodeChunk(5, full[5:])} // dup
	in <- Msg{protocol.ContentJSON, marshalTrailer(Trailer{SHA256: sum, Total: int64(len(full))})}
	close(in)

	saved, err := Receive(func(protocol.ContentType, []byte) error { return nil }, in, dst, func(string) bool { return true }, nil)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	got, _ := os.ReadFile(saved)
	if !bytes.Equal(got, full) {
		t.Fatalf("got %q want %q", got, full)
	}
}

func TestHeaderMarshalIsJSON(t *testing.T) {
	var h Header
	if err := json.Unmarshal(marshalHeader(Header{Kind: "file"}), &h); err != nil {
		t.Fatal(err)
	}
	if h.T != "header" {
		t.Fatalf("t=%q", h.T)
	}
}
```

This test references a helper `sha256OfBytes`; add it to `receive.go` (exported-lowercase) so both the package and tests can use it.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/transfer/ -run 'TestRoundTrip|TestReceive|TestHeaderMarshal' -v`
Expected: FAIL — undefined Receive / sha256OfBytes.

- [ ] **Step 3: Implement**

`pkg/transfer/receive.go`:

```go
package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// Receive consumes a transfer from in, writing into destDir. It verifies the
// SHA-256 from the trailer, then for "file" places the result at
// destDir/<name> (consulting overwrite when it exists), or for "archive"
// unpacks the tar into destDir. On success it sends a DONE ack via send and
// returns the saved path. overwrite-declined keeps the data as "<name>.part".
func Receive(send SendFunc, in <-chan Msg, destDir string, overwrite OverwriteFn, progress ProgressFn) (string, error) {
	first, ok := <-in
	if !ok {
		return "", fmt.Errorf("transfer: stream closed before header")
	}
	if classify(first) != "header" {
		return "", fmt.Errorf("transfer: expected header, got %s", classify(first))
	}
	var hdr Header
	if err := json.Unmarshal(first.Payload, &hdr); err != nil {
		return "", fmt.Errorf("transfer: bad header: %w", err)
	}

	tmp, err := os.CreateTemp(destDir, ".p2p-recv-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	removeTmp := func() { _ = tmp.Close(); _ = os.Remove(tmpName) }

	var trailer *Trailer
	for m := range in {
		switch classify(m) {
		case "data":
			off, data, derr := decodeChunk(m.Payload)
			if derr != nil {
				removeTmp()
				return "", derr
			}
			if _, werr := tmp.WriteAt(data, off); werr != nil {
				removeTmp()
				return "", werr
			}
			if progress != nil {
				progress(off+int64(len(data)), hdr.Size)
			}
		case "trailer":
			var tr Trailer
			if err := json.Unmarshal(m.Payload, &tr); err != nil {
				removeTmp()
				return "", err
			}
			trailer = &tr
		}
		if trailer != nil {
			break
		}
	}
	if trailer == nil {
		removeTmp()
		return "", fmt.Errorf("transfer: stream ended before trailer")
	}
	if err := tmp.Sync(); err != nil {
		removeTmp()
		return "", err
	}
	_ = tmp.Close()

	sum, total, err := hashFile(tmpName)
	if err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	if sum != trailer.SHA256 || total != trailer.Total {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("transfer: integrity check failed")
	}

	var saved string
	if hdr.Kind == "archive" {
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			_ = os.Remove(tmpName)
			return "", oerr
		}
		err = extractTar(f, destDir)
		_ = f.Close()
		_ = os.Remove(tmpName)
		if err != nil {
			return "", err
		}
		saved = destDir
	} else {
		final := filepath.Join(destDir, filepath.Base(hdr.Name))
		if _, statErr := os.Stat(final); statErr == nil &&
			(overwrite == nil || !overwrite(filepath.Base(hdr.Name))) {
			final += ".part"
		}
		if hdr.Mode != 0 {
			_ = os.Chmod(tmpName, os.FileMode(hdr.Mode))
		}
		if err := os.Rename(tmpName, final); err != nil {
			_ = os.Remove(tmpName)
			return "", err
		}
		saved = final
	}

	// Acknowledge completion so the sender can exit.
	if send != nil {
		_ = send(protocol.ContentJSON, marshalDone())
	}
	return saved, nil
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func sha256OfBytes(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/transfer/ -race -v`
Expected: PASS (all transfer tests, race-clean).

- [ ] **Step 5: Commit**

```bash
git add pkg/transfer/receive.go pkg/transfer/receive_test.go
git commit -m "feat(transfer): Receive with sha256 verify, overwrite policy, DONE ack"
```

---

## Task 5: progress bar + overwrite prompt (CLI helpers)

**Files:**
- Create: `cmd/p2p/progress.go`
- Test: `cmd/p2p/progress_test.go`

- [ ] **Step 1: Write the failing test**

`cmd/p2p/progress_test.go`:

```go
package main

import (
	"strings"
	"testing"
)

func TestFormatProgress(t *testing.T) {
	s := formatProgress(50, 100)
	if !strings.Contains(s, "50.0%") {
		t.Fatalf("want percent, got %q", s)
	}
	if !strings.Contains(s, "==========") { // 50% -> 10 bars
		t.Fatalf("want bar, got %q", s)
	}
	u := formatProgress(1234, 0)
	if !strings.Contains(u, "1234") {
		t.Fatalf("want byte count, got %q", u)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/p2p/ -run TestFormatProgress -v`
Expected: FAIL — undefined formatProgress.

- [ ] **Step 3: Implement**

`cmd/p2p/progress.go`:

```go
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// formatProgress renders a single-line progress string. total==0 => unknown
// size, shows a running byte count.
func formatProgress(done, total int64) string {
	if total > 0 {
		pct := float64(done) / float64(total) * 100
		bars := int(pct / 5) // 20-wide bar
		if bars > 20 {
			bars = 20
		}
		return fmt.Sprintf("[%-20s] %5.1f%% (%d/%d)", strings.Repeat("=", bars), pct, done, total)
	}
	return fmt.Sprintf("%d bytes", done)
}

// progressBar prints formatProgress to stderr in place.
func progressBar(done, total int64) {
	fmt.Fprintf(os.Stderr, "\r%s", formatProgress(done, total))
}

// promptOverwrite asks the user whether to overwrite name.
func promptOverwrite(name string) bool {
	fmt.Fprintf(os.Stderr, "overwrite %s? [y/N] ", name)
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	line = strings.TrimSpace(strings.ToLower(line))
	return line == "y" || line == "yes"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/p2p/ -run TestFormatProgress -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/p2p/progress.go cmd/p2p/progress_test.go
git commit -m "feat(p2p): progress bar + overwrite prompt helpers"
```

---

## Task 6: wire transfer into the CLI

Depends on Task 3, 4, 5. No unit test (integration/manual); verified by build/vet/test + manual.

**Files:**
- Modify: `cmd/p2p/main.go`

- [ ] **Step 1: Implement**

Replace the body of `main()` in `cmd/p2p/main.go` from the `args` switch onward
so it supports file send + receive auto-detect. Full replacement of the function
and the chat helper (keep the package, imports get `path/filepath`,
`github.com/jdp5949/p2p-messaging/pkg/transfer`, and drop nothing still used):

Add imports:
```go
	"path/filepath"

	"github.com/jdp5949/p2p-messaging/pkg/transfer"
```

Change the verb handling: capture optional paths for `send`:

```go
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
```

After the broker is created (replace the `go chatLoop(b)` block and the select
that follows it) wire an inbound channel and branch on send/receive:

```go
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
		err := transfer.Send(sendMsg, inbound, sendPaths, progressBar)
		fmt.Fprintln(os.Stderr)
		_ = b.Close()
		fatalOn(err, "send")
		fmt.Fprintln(os.Stderr, "✓ sent and verified by peer")
		return
	}

	// Receiver / chat: peek the first inbound message.
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
				saved, err := transfer.Receive(sendMsg, merged, dest, promptOverwrite, progressBar)
				fmt.Fprintln(os.Stderr)
				if err != nil {
					fmt.Fprintf(os.Stderr, "receive failed: %v\n", err)
				} else {
					fmt.Fprintf(os.Stderr, "✓ saved %s (sha256 verified)\n", saved)
				}
				close(quit)
				return
			}
			// Not a transfer: it's chat. Print and continue.
			fmt.Printf("peer> %s\n", first.Payload)
			go chatLoop(b, quit)
			for m := range inbound {
				fmt.Printf("peer> %s\n", m.Payload)
			}
		case <-quit:
		}
	}()

	if len(sendPaths) == 0 {
		fmt.Printf("\r✓ Connected — peer online (%s). Type messages. /quit or Ctrl-C to exit.\n", connMode(dialer))
		// If the user types first (sender side of a chat), start chat loop too.
		go chatLoop(b, quit)
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
```

Add the header-detection helper near the other helpers:

```go
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
```

Add `"encoding/json"` to imports. Remove the now-unused old `OnInbound` chat
print and the previous `dropped`/`b` declaration block that this replaces (there
must be exactly one `b, err := broker.New(...)`).

> Note: `chatLoop`, `connMode`, `fatalOn`, `usage` are unchanged from the
> existing file. The `filepath` import is used only if you reference it; if not,
> omit it to avoid an unused-import error. Run goimports/`go build` and fix
> imports as the compiler directs.

- [ ] **Step 2: Build + vet**

Run: `go build ./... && go vet ./...`
Expected: clean. Fix any unused import the compiler flags.

- [ ] **Step 3: Full test suite**

Run: `go test ./...`
Expected: all pass.

- [ ] **Step 4: Manual usage smoke (help path)**

Run: `go run ./cmd/p2p ; echo "exit=$?"`
Expected: usage on stderr, exit=2.

- [ ] **Step 5: Commit**

```bash
git add cmd/p2p/main.go
git commit -m "feat(p2p): p2p send <path...> file transfer + receive auto-detect"
```

---

## Task 7: install.sh

**Files:**
- Create: `install.sh`

- [ ] **Step 1: Write the script**

`install.sh`:

```sh
#!/bin/sh
# p2p installer: detects OS/arch, downloads the matching release binary, and
# installs it onto PATH. Usage:
#   curl -fsSL https://raw.githubusercontent.com/jdp5949/p2p-messaging/main/install.sh | sh
set -eu

REPO="jdp5949/p2p-messaging"

# map_os_arch echoes "<os> <arch>" or exits non-zero with a message.
map_os_arch() {
	os=$(uname -s 2>/dev/null || echo unknown)
	arch=$(uname -m 2>/dev/null || echo unknown)
	case "$os" in
		Darwin) os=darwin ;;
		Linux)  os=linux ;;
		*) echo "unsupported OS: $os (see https://github.com/$REPO/releases)" >&2; return 1 ;;
	esac
	case "$arch" in
		x86_64|amd64) arch=amd64 ;;
		aarch64|arm64) arch=arm64 ;;
		*) echo "unsupported arch: $arch (see https://github.com/$REPO/releases)" >&2; return 1 ;;
	esac
	echo "$os $arch"
}

# Allow a self-test: `OS_ARCH_SELFTEST=1 sh install.sh` just prints mapping.
if [ "${OS_ARCH_SELFTEST:-}" = "1" ]; then
	map_os_arch
	exit $?
fi

set -- $(map_os_arch)
OS=$1
ARCH=$2
ASSET="p2p-${OS}-${ARCH}"
URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

echo "Downloading ${ASSET}…" >&2
if command -v curl >/dev/null 2>&1; then
	curl -fsSL "$URL" -o "$TMP"
elif command -v wget >/dev/null 2>&1; then
	wget -qO "$TMP" "$URL"
else
	echo "need curl or wget" >&2
	exit 1
fi
chmod +x "$TMP"

# macOS: clear quarantine so Gatekeeper does not block.
if [ "$OS" = "darwin" ]; then
	xattr -d com.apple.quarantine "$TMP" 2>/dev/null || true
fi

DEST="/usr/local/bin/p2p"
install_to() { mv "$TMP" "$1" && chmod +x "$1"; }

if [ -w "$(dirname "$DEST")" ] || [ "$(id -u)" = "0" ]; then
	install_to "$DEST"
elif command -v sudo >/dev/null 2>&1 && [ -t 0 ]; then
	sudo mv "$TMP" "$DEST" && sudo chmod +x "$DEST"
else
	DEST="$HOME/.local/bin/p2p"
	mkdir -p "$(dirname "$DEST")"
	install_to "$DEST"
	case ":$PATH:" in
		*":$HOME/.local/bin:"*) ;;
		*) echo "note: add $HOME/.local/bin to your PATH" >&2 ;;
	esac
fi
trap - EXIT

echo "✓ installed p2p to $DEST" >&2
echo "run: p2p send" >&2
```

- [ ] **Step 2: Syntax check + mapping self-test**

Run:
```sh
sh -n install.sh && echo SYNTAX_OK
OS_ARCH_SELFTEST=1 sh install.sh
```
Expected: `SYNTAX_OK`, then a line like `darwin arm64` (or your platform).
If `shellcheck` is installed, also run `shellcheck install.sh` and address errors.

- [ ] **Step 3: Commit**

```bash
git add install.sh
git commit -m "feat(install): one-line POSIX installer (uname detect + PATH)"
```

---

## Task 8: install.ps1 + README

**Files:**
- Create: `install.ps1`
- Modify: `README.md`

- [ ] **Step 1: Write install.ps1**

`install.ps1`:

```powershell
# p2p installer for Windows. Usage:
#   irm https://raw.githubusercontent.com/jdp5949/p2p-messaging/main/install.ps1 | iex
$ErrorActionPreference = "Stop"
$repo = "jdp5949/p2p-messaging"
$asset = "p2p-windows-amd64.exe"
$url = "https://github.com/$repo/releases/latest/download/$asset"

$dir = Join-Path $env:LOCALAPPDATA "p2p"
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$dest = Join-Path $dir "p2p.exe"

Write-Host "Downloading $asset…"
Invoke-WebRequest -Uri $url -OutFile $dest

# Add install dir to the user PATH if missing.
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$dir*") {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$dir", "User")
    Write-Host "Added $dir to your user PATH."
}

Write-Host "installed p2p. Open a NEW terminal, then run: p2p send"
```

- [ ] **Step 2: Parse check (if pwsh available)**

Run:
```sh
command -v pwsh >/dev/null 2>&1 && pwsh -NoProfile -Command '$null = [ScriptBlock]::Create((Get-Content -Raw ./install.ps1)); "PS_PARSE_OK"' || echo "pwsh not present, skip"
```
Expected: `PS_PARSE_OK` or the skip note.

- [ ] **Step 3: Update README install section**

In `README.md`, replace the body of `### Install binary (pre-built, no Go needed)`
with one-liners first:

````markdown
### Install (one line, no Go needed)

macOS / Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/jdp5949/p2p-messaging/main/install.sh | sh
```

Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/jdp5949/p2p-messaging/main/install.ps1 | iex
```

Then just:

```sh
p2p send                 # send a file or start a chat
p2p send movie.mp4       # send a file
p2p send ./myfolder      # send a directory
p2p <code>               # other machine: receive / join
```

Prefer to grab a specific binary yourself? See the
[releases page](https://github.com/jdp5949/p2p-messaging/releases/latest)
(`p2p-darwin-arm64`, `p2p-linux-amd64`, `p2p-windows-amd64.exe`, …).
````

Also update the **Easiest: croc-style chat** section heading/intro to mention
file transfer (`p2p send <path>`), so the docs reflect the new capability.

- [ ] **Step 4: Build + tests still green**

Run: `go build ./... && go test ./...`
Expected: PASS (no Go changes here, sanity only).

- [ ] **Step 5: Commit**

```bash
git add install.ps1 README.md
git commit -m "feat(install): Windows installer + README one-line install & file-transfer docs"
```

---

## Self-Review

**Spec coverage:**
- `p2p send <path...>` files + multi + dirs → Task 3 (Send, tar) + Task 6 (CLI). ✓
- HEADER/DATA(offset)/TRAILER protocol → Task 1. ✓
- tar + zip-slip → Task 2. ✓
- streaming SHA-256 verify, mismatch rejects → Task 3 (send hash) + Task 4 (verify). ✓
- WriteAt offset reassembly, dup-safe → Task 4 (TestReceiveOutOfOrderChunks). ✓
- overwrite prompt + keep .part → Task 4 + Task 5 (prompt). ✓
- progress bar → Task 5 + wired Task 6. ✓
- DONE ack so sender waits for save before exit → Task 3/4. ✓
- receiver auto-detect file vs chat → Task 6 (transferIsHeader). ✓
- no resume → not built. ✓
- install.sh (uname, PATH, quarantine) → Task 7. ✓
- install.ps1 (LOCALAPPDATA + user PATH) → Task 8. ✓
- README one-liners → Task 8. ✓

**Placeholder scan:** No TBD/TODO; every code step has complete code. The one
soft spot (Task 6 import hygiene) is explicit: run `go build` and fix imports as
flagged; `filepath` may be unused and should be omitted if so.

**Type consistency:** `transfer.Msg{ContentType,Payload}`, `SendFunc(ct,payload)`,
`Send(send, in, paths, progress)`, `Receive(send, in, destDir, overwrite, progress)`,
`Header{T,Kind,Name,Size,Mode}`, `Trailer{T,SHA256,Total}`, `classify` values
(`header/trailer/done/data/other`), `encodeChunk/decodeChunk`, `marshalDone`,
`sha256OfBytes`, `formatProgress`, `progressBar`, `promptOverwrite`,
`transferIsHeader` — all defined once and used consistently across tasks. Broker
fields `OnInbound/OnDisconnected/OnReconnected/OnReconnectFailed` and
`Send(ct,prio,payload)` match the current broker API.

**Known risk:** Task 6 rewires a chunk of `main()`; the dispatch goroutine for
chat-vs-transfer must read exactly one inbound message before deciding. The plan
prepends that first message into a `merged` channel so `transfer.Receive` (which
expects HEADER first) sees it. Verified by the manual smoke + the existing
machine-to-machine test once built.
```
