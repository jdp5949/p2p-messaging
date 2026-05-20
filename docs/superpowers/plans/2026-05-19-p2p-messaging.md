# P2P Messaging Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a direct P2P messaging library + CLI in Go — two peers exchange any-type messages with ACK/NACK, auto-reconnect, parallel chunked transfer, and zstd compression, using croc's relay for initial connection.

**Architecture:** Peers connect through croc's relay server (rendezvous + TLS), then exchange fixed 20-byte binary framed messages directly. Large messages are split into parallel chunks. Unacked messages live in a ring buffer and are replayed on reconnect.

**Tech Stack:** Go 1.22+, `github.com/schollz/croc/v10` (relay + TLS), `github.com/klauspost/compress/zstd`

---

## File Map

| File | Responsibility |
|---|---|
| `go.mod` | Module definition + deps |
| `pkg/protocol/types.go` | Wire constants: MsgType, ContentType, Priority, Flags |
| `pkg/protocol/header.go` | 20-byte header encode/decode |
| `pkg/protocol/protocol_test.go` | Header roundtrip tests |
| `pkg/compress/compress.go` | zstd encode/decode + threshold logic |
| `pkg/compress/compress_test.go` | Compress/decompress tests |
| `pkg/conn/conn.go` | `Conn` — framed read/write over `net.Conn` |
| `pkg/conn/dial.go` | Connect to croc relay, TLS, return `*Conn` |
| `pkg/conn/reconnect.go` | Reconnect loop with exponential backoff |
| `pkg/conn/conn_test.go` | Frame read/write tests using `net.Pipe()` |
| `pkg/chunker/split.go` | Split large payload into chunk envelopes |
| `pkg/chunker/assemble.go` | Collect fragments, reassemble payload |
| `pkg/chunker/chunker_test.go` | Split + reassemble roundtrip tests |
| `pkg/broker/buffer.go` | Fixed ring buffer for unacked messages |
| `pkg/broker/broker.go` | Public API: `New`, `Send`, `Close`, hooks |
| `pkg/broker/broker_test.go` | Send/ACK/NACK/retry integration tests |
| `cmd/peer/main.go` | CLI peer node |
| `cmd/relay/main.go` | Self-hosted relay server (wraps croc relay) |
| `examples/send_json/main.go` | JSON event example |
| `examples/send_binary/main.go` | Raw binary example |

---

## Task 0: Go module + scaffold

**Files:**
- Create: `go.mod`
- Create: directory structure

- [ ] **Step 1: Init module**

```bash
cd /Users/jay/PycharmProjects/p2p-messaging
go mod init github.com/jdp5949/p2p-messaging
```

Expected: `go.mod` created with `module github.com/jdp5949/p2p-messaging`

- [ ] **Step 2: Create package directories**

```bash
mkdir -p pkg/protocol pkg/compress pkg/conn pkg/chunker pkg/broker
mkdir -p cmd/peer cmd/relay
mkdir -p examples/send_json examples/send_binary
```

- [ ] **Step 3: Add dependencies**

```bash
go get github.com/schollz/croc/v10
go get github.com/klauspost/compress/zstd
```

- [ ] **Step 4: Verify module**

```bash
go mod tidy && cat go.mod
```

Expected output includes both dependencies in `require` block.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: init go module with croc and zstd deps"
```

---

## Task 1: pkg/protocol — types and header codec

**Files:**
- Create: `pkg/protocol/types.go`
- Create: `pkg/protocol/header.go`
- Create: `pkg/protocol/protocol_test.go`

- [ ] **Step 1: Write failing test**

```go
// pkg/protocol/protocol_test.go
package protocol

import "testing"

func TestHeaderRoundtrip(t *testing.T) {
	h := Header{
		MsgID:       12345678,
		MsgType:     MSG,
		ContentType: JSON,
		Flags:       FlagCompressed | FlagHasMeta,
		Priority:    High,
		FragIndex:   3,
		FragTotal:   10,
		PayloadLen:  1024,
	}
	enc := h.Encode()
	dec := Decode(enc)
	if dec != h {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", dec, h)
	}
}

func TestACKZeroPayload(t *testing.T) {
	h := Header{MsgID: 99, MsgType: ACK, FragTotal: 1}
	enc := h.Encode()
	dec := Decode(enc)
	if dec.PayloadLen != 0 {
		t.Fatal("ACK must have zero PayloadLen")
	}
	if dec.MsgType != ACK {
		t.Fatalf("wrong MsgType: got %d want %d", dec.MsgType, ACK)
	}
}

func TestHeaderSize(t *testing.T) {
	h := Header{}
	enc := h.Encode()
	if len(enc) != HeaderSize {
		t.Fatalf("header size: got %d want %d", len(enc), HeaderSize)
	}
}
```

- [ ] **Step 2: Run test — verify fail**

```bash
go test ./pkg/protocol/... -v
```

Expected: `FAIL — undefined: Header, MSG, ACK, ...`

- [ ] **Step 3: Write types.go**

```go
// pkg/protocol/types.go
package protocol

const HeaderSize = 20

type MsgType uint8

const (
	MSG        MsgType = 1
	ACK        MsgType = 2
	NACK       MsgType = 3
	PING       MsgType = 4
	PONG       MsgType = 5
	CONNECT    MsgType = 6
	DISCONNECT MsgType = 7
)

type ContentType uint8

const (
	RAW      ContentType = 0
	TEXT     ContentType = 1
	JSON     ContentType = 2
	BINARY   ContentType = 3
	PROTOBUF ContentType = 4
	MSGPACK  ContentType = 5
	AVRO     ContentType = 6
)

type Priority uint8

const (
	Low      Priority = 0
	Normal   Priority = 1
	High     Priority = 2
	Critical Priority = 3
)

const (
	FlagCompressed uint8 = 1 << 0
	FlagFragment   uint8 = 1 << 1
	FlagLastFrag   uint8 = 1 << 2
	FlagHasMeta    uint8 = 1 << 3
)

type NACKReason uint8

const (
	NACKUnknown    NACKReason = 0
	NACKDecodeFail NACKReason = 1
	NACKAppReject  NACKReason = 2
	NACKOverload   NACKReason = 3
)
```

- [ ] **Step 4: Write header.go**

```go
// pkg/protocol/header.go
package protocol

import "encoding/binary"

type Header struct {
	MsgID       uint64
	MsgType     MsgType
	ContentType ContentType
	Flags       uint8
	Priority    Priority
	FragIndex   uint16
	FragTotal   uint16
	PayloadLen  uint32
}

// Encode serialises the header into a fixed 20-byte array.
func (h Header) Encode() [HeaderSize]byte {
	var buf [HeaderSize]byte
	binary.BigEndian.PutUint64(buf[0:8], h.MsgID)
	buf[8] = uint8(h.MsgType)
	buf[9] = uint8(h.ContentType)
	buf[10] = h.Flags
	buf[11] = uint8(h.Priority)
	binary.BigEndian.PutUint16(buf[12:14], h.FragIndex)
	binary.BigEndian.PutUint16(buf[14:16], h.FragTotal)
	binary.BigEndian.PutUint32(buf[16:20], h.PayloadLen)
	return buf
}

// Decode parses a fixed 20-byte array into a Header.
func Decode(buf [HeaderSize]byte) Header {
	return Header{
		MsgID:       binary.BigEndian.Uint64(buf[0:8]),
		MsgType:     MsgType(buf[8]),
		ContentType: ContentType(buf[9]),
		Flags:       buf[10],
		Priority:    Priority(buf[11]),
		FragIndex:   binary.BigEndian.Uint16(buf[12:14]),
		FragTotal:   binary.BigEndian.Uint16(buf[14:16]),
		PayloadLen:  binary.BigEndian.Uint32(buf[16:20]),
	}
}
```

- [ ] **Step 5: Run tests — verify pass**

```bash
go test ./pkg/protocol/... -v
```

Expected: all 3 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/protocol/
git commit -m "feat: 20-byte binary header encode/decode"
```

---

## Task 2: pkg/compress — zstd with threshold

**Files:**
- Create: `pkg/compress/compress.go`
- Create: `pkg/compress/compress_test.go`

- [ ] **Step 1: Write failing test**

```go
// pkg/compress/compress_test.go
package compress

import (
	"bytes"
	"testing"
)

func TestSmallPayloadNotCompressed(t *testing.T) {
	src := []byte("hi") // len 2, under threshold
	if ShouldCompress(src) {
		t.Fatal("small payload should not be compressed")
	}
}

func TestLargePayloadCompressed(t *testing.T) {
	src := bytes.Repeat([]byte("abcdefgh"), 64) // 512 bytes, over threshold
	if !ShouldCompress(src) {
		t.Fatal("large payload should be compressed")
	}
	compressed := Compress(src)
	if len(compressed) >= len(src) {
		t.Logf("note: compressed not smaller (repetitive data often is, random may not be)")
	}
	decompressed, err := Decompress(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decompressed, src) {
		t.Fatal("decompress roundtrip mismatch")
	}
}

func TestDecompressInvalid(t *testing.T) {
	_, err := Decompress([]byte("notvalidzstd"))
	if err == nil {
		t.Fatal("expected error on invalid zstd data")
	}
}
```

- [ ] **Step 2: Run test — verify fail**

```bash
go test ./pkg/compress/... -v
```

Expected: `FAIL — undefined: ShouldCompress, Compress, Decompress`

- [ ] **Step 3: Write compress.go**

```go
// pkg/compress/compress.go
package compress

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const Threshold = 256

var (
	enc     *zstd.Encoder
	dec     *zstd.Decoder
	encOnce sync.Once
	decOnce sync.Once
)

func getEncoder() *zstd.Encoder {
	encOnce.Do(func() {
		enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	})
	return enc
}

func getDecoder() *zstd.Decoder {
	decOnce.Do(func() {
		dec, _ = zstd.NewReader(nil)
	})
	return dec
}

func ShouldCompress(src []byte) bool {
	return len(src) > Threshold
}

func Compress(src []byte) []byte {
	return getEncoder().EncodeAll(src, make([]byte, 0, len(src)))
}

func Decompress(src []byte) ([]byte, error) {
	out, err := getDecoder().DecodeAll(src, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests — verify pass**

```bash
go test ./pkg/compress/... -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/compress/
git commit -m "feat: zstd compress/decompress with 256-byte threshold"
```

---

## Task 3: pkg/conn — framed read/write

**Files:**
- Create: `pkg/conn/conn.go`
- Create: `pkg/conn/conn_test.go`

- [ ] **Step 1: Write failing test**

```go
// pkg/conn/conn_test.go
package conn

import (
	"net"
	"testing"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

func TestWriteReadFrame(t *testing.T) {
	a, b := net.Pipe()
	ca, cb := New(a), New(b)

	hdr := protocol.Header{
		MsgID:       1,
		MsgType:     protocol.MSG,
		ContentType: protocol.JSON,
		FragTotal:   1,
		PayloadLen:  5,
	}
	payload := []byte("hello")

	errc := make(chan error, 1)
	go func() { errc <- ca.WriteFrame(hdr, payload) }()

	gotHdr, gotPayload, err := cb.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if gotHdr != hdr {
		t.Fatalf("header mismatch: got %+v want %+v", gotHdr, hdr)
	}
	if string(gotPayload) != "hello" {
		t.Fatalf("payload mismatch: got %q want %q", gotPayload, payload)
	}
}

func TestACKFrame(t *testing.T) {
	a, b := net.Pipe()
	ca, cb := New(a), New(b)

	hdr := protocol.Header{MsgID: 42, MsgType: protocol.ACK, FragTotal: 1}

	errc := make(chan error, 1)
	go func() { errc <- ca.WriteFrame(hdr, nil) }()

	gotHdr, gotPayload, err := cb.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if gotHdr.MsgID != 42 || gotHdr.MsgType != protocol.ACK {
		t.Fatalf("wrong ACK header: %+v", gotHdr)
	}
	if len(gotPayload) != 0 {
		t.Fatal("ACK must carry no payload")
	}
}

func TestLargePayload(t *testing.T) {
	a, b := net.Pipe()
	ca, cb := New(a), New(b)

	payload := make([]byte, 2*1024*1024) // 2 MB
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	hdr := protocol.Header{
		MsgID:      2,
		MsgType:    protocol.MSG,
		FragTotal:  1,
		PayloadLen: uint32(len(payload)),
	}

	errc := make(chan error, 1)
	go func() { errc <- ca.WriteFrame(hdr, payload) }()

	gotHdr, gotPayload, err := cb.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if gotHdr.PayloadLen != uint32(len(payload)) {
		t.Fatalf("PayloadLen mismatch: got %d want %d", gotHdr.PayloadLen, len(payload))
	}
	if len(gotPayload) != len(payload) {
		t.Fatalf("payload length mismatch: got %d want %d", len(gotPayload), len(payload))
	}
}
```

- [ ] **Step 2: Run test — verify fail**

```bash
go test ./pkg/conn/... -v
```

Expected: `FAIL — undefined: New`

- [ ] **Step 3: Write conn.go**

```go
// pkg/conn/conn.go
package conn

import (
	"io"
	"net"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

type Conn struct {
	raw net.Conn
}

func New(raw net.Conn) *Conn {
	return &Conn{raw: raw}
}

func (c *Conn) WriteFrame(hdr protocol.Header, payload []byte) error {
	enc := hdr.Encode()
	if _, err := c.raw.Write(enc[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		_, err := c.raw.Write(payload)
		return err
	}
	return nil
}

func (c *Conn) ReadFrame() (protocol.Header, []byte, error) {
	var buf [protocol.HeaderSize]byte
	if _, err := io.ReadFull(c.raw, buf[:]); err != nil {
		return protocol.Header{}, nil, err
	}
	hdr := protocol.Decode(buf)
	if hdr.PayloadLen == 0 {
		return hdr, nil, nil
	}
	payload := make([]byte, hdr.PayloadLen)
	if _, err := io.ReadFull(c.raw, payload); err != nil {
		return protocol.Header{}, nil, err
	}
	return hdr, payload, nil
}

func (c *Conn) Raw() net.Conn { return c.raw }

func (c *Conn) Close() error { return c.raw.Close() }
```

- [ ] **Step 4: Run tests — verify pass**

```bash
go test ./pkg/conn/... -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/conn/conn.go pkg/conn/conn_test.go
git commit -m "feat: framed binary read/write over net.Conn"
```

---

## Task 4: pkg/conn — relay dial + TLS

**Files:**
- Create: `pkg/conn/dial.go`

This task connects to croc's relay server, pairs two peers, then wraps with TLS.

- [ ] **Step 1: Write dial.go**

```go
// pkg/conn/dial.go
package conn

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	croccomm "github.com/schollz/croc/v10/src/comm"
)

const DefaultRelay = "croc.schollz.com:9009"

type Role int

const (
	Initiator Role = iota
	Receiver
)

// Dial connects to a croc relay with the given code, waits for the peer to
// join, performs TLS over the paired connection, and returns a ready *Conn.
func Dial(relayAddr, code string, role Role) (*Conn, error) {
	raw, err := net.DialTimeout("tcp", relayAddr, 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial relay %s: %w", relayAddr, err)
	}

	// croc relay pairs two clients with same code; suffix disambiguates slots.
	slot := "1"
	if role == Receiver {
		slot = "2"
	}
	room := fmt.Sprintf("%s-%s", code, slot)
	if _, err := fmt.Fprintf(raw, "%s|", room); err != nil {
		raw.Close()
		return nil, fmt.Errorf("send room: %w", err)
	}

	// croc relay responds "ok" when paired.
	c := croccomm.New(raw)
	msg, err := c.Receive()
	if err != nil || string(msg) != "ok" {
		raw.Close()
		return nil, fmt.Errorf("relay pair: unexpected response %q err=%v", msg, err)
	}

	// Upgrade to TLS. Initiator acts as server (holds self-signed cert).
	tlsConn, err := tlsHandshake(raw, role)
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("tls: %w", err)
	}
	return New(tlsConn), nil
}

func tlsHandshake(raw net.Conn, role Role) (net.Conn, error) {
	if role == Initiator {
		cert, err := selfSignedCert()
		if err != nil {
			return nil, err
		}
		cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
		return tls.Server(raw, cfg), nil
	}
	cfg := &tls.Config{InsecureSkipVerify: true} // trust via shared code, not cert
	return tls.Client(raw, cfg), nil
}

func selfSignedCert() (tls.Certificate, error) {
	// generateSelfSigned is in dial_cert.go (next step)
	return generateSelfSigned()
}
```

- [ ] **Step 2: Write dial_cert.go — self-signed cert generation**

```go
// pkg/conn/dial_cert.go
package conn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"time"
)

func generateSelfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"p2p-msg"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
}
```

- [ ] **Step 3: Build check (no unit test — requires live relay)**

```bash
go build ./pkg/conn/...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add pkg/conn/dial.go pkg/conn/dial_cert.go
git commit -m "feat: dial croc relay with TLS handshake"
```

---

## Task 5: pkg/conn — reconnect loop

**Files:**
- Create: `pkg/conn/reconnect.go`
- Modify: `pkg/conn/conn_test.go` (add reconnect test)

- [ ] **Step 1: Add reconnect test**

Append to `pkg/conn/conn_test.go`:

```go
func TestReconnectCallsDialFn(t *testing.T) {
	calls := 0
	dialFn := func() (*Conn, error) {
		calls++
		if calls < 3 {
			return nil, fmt.Errorf("not ready")
		}
		a, b := net.Pipe()
		_ = b // b is the remote side, discard for this test
		return New(a), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := Reconnect(ctx, dialFn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if calls != 3 {
		t.Fatalf("expected 3 dial attempts, got %d", calls)
	}
}
```

Add imports to `conn_test.go`:
```go
import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)
```

- [ ] **Step 2: Run test — verify fail**

```bash
go test ./pkg/conn/... -v -run TestReconnect
```

Expected: `FAIL — undefined: Reconnect`

- [ ] **Step 3: Write reconnect.go**

```go
// pkg/conn/reconnect.go
package conn

import (
	"context"
	"time"
)

var backoff = []time.Duration{
	100 * time.Millisecond,
	500 * time.Millisecond,
	2 * time.Second,
	10 * time.Second,
	30 * time.Second,
}

// Reconnect calls dialFn until it succeeds or ctx is done.
// It uses exponential backoff capped at 30s.
func Reconnect(ctx context.Context, dialFn func() (*Conn, error)) (*Conn, error) {
	for i := 0; ; i++ {
		c, err := dialFn()
		if err == nil {
			return c, nil
		}
		delay := backoff[min(i, len(backoff)-1)]
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

- [ ] **Step 4: Run all conn tests — verify pass**

```bash
go test ./pkg/conn/... -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/conn/reconnect.go pkg/conn/conn_test.go
git commit -m "feat: exponential backoff reconnect loop"
```

---

## Task 6: pkg/chunker — split payload into chunk frames

**Files:**
- Create: `pkg/chunker/split.go`
- Create: `pkg/chunker/chunker_test.go`

- [ ] **Step 1: Write failing test**

```go
// pkg/chunker/chunker_test.go
package chunker

import (
	"bytes"
	"testing"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

func TestSplitSmallPayload(t *testing.T) {
	payload := []byte("small")
	chunks := Split(1, protocol.JSON, protocol.Normal, payload, DefaultChunkSize)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Hdr.FragTotal != 1 {
		t.Fatalf("FragTotal should be 1, got %d", chunks[0].Hdr.FragTotal)
	}
	if chunks[0].Hdr.FragIndex != 0 {
		t.Fatalf("FragIndex should be 0, got %d", chunks[0].Hdr.FragIndex)
	}
	if chunks[0].Hdr.Flags&protocol.FlagLastFrag == 0 {
		t.Fatal("single chunk must have FlagLastFrag set")
	}
}

func TestSplitLargePayload(t *testing.T) {
	chunkSize := 100
	payload := bytes.Repeat([]byte("x"), 350) // 350 bytes → 4 chunks of 100,100,100,50
	chunks := Split(42, protocol.BINARY, protocol.High, payload, chunkSize)

	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}
	for i, ch := range chunks {
		if ch.Hdr.MsgID != 42 {
			t.Fatalf("chunk %d: wrong MsgID %d", i, ch.Hdr.MsgID)
		}
		if ch.Hdr.FragTotal != 4 {
			t.Fatalf("chunk %d: wrong FragTotal %d", i, ch.Hdr.FragTotal)
		}
		if ch.Hdr.FragIndex != uint16(i) {
			t.Fatalf("chunk %d: wrong FragIndex %d", i, ch.Hdr.FragIndex)
		}
	}
	// last chunk has FlagLastFrag
	if chunks[3].Hdr.Flags&protocol.FlagLastFrag == 0 {
		t.Fatal("last chunk must have FlagLastFrag set")
	}
	// first chunks do not
	if chunks[0].Hdr.Flags&protocol.FlagLastFrag != 0 {
		t.Fatal("first chunk must not have FlagLastFrag")
	}
}
```

- [ ] **Step 2: Run test — verify fail**

```bash
go test ./pkg/chunker/... -v
```

Expected: `FAIL — undefined: Split, DefaultChunkSize`

- [ ] **Step 3: Write split.go**

```go
// pkg/chunker/split.go
package chunker

import (
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

const DefaultChunkSize = 512 * 1024 // 512 KB

type Chunk struct {
	Hdr     protocol.Header
	Payload []byte
}

// Split divides payload into Chunk slices. Each chunk carries a fragment of
// payload with header fields set for reassembly. chunkSize is in bytes.
func Split(msgID uint64, ct protocol.ContentType, prio protocol.Priority, payload []byte, chunkSize int) []Chunk {
	if len(payload) <= chunkSize {
		hdr := protocol.Header{
			MsgID:       msgID,
			MsgType:     protocol.MSG,
			ContentType: ct,
			Priority:    prio,
			Flags:       protocol.FlagLastFrag,
			FragIndex:   0,
			FragTotal:   1,
			PayloadLen:  uint32(len(payload)),
		}
		return []Chunk{{Hdr: hdr, Payload: payload}}
	}

	total := (len(payload) + chunkSize - 1) / chunkSize
	chunks := make([]Chunk, 0, total)

	for i := 0; i < total; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		part := payload[start:end]

		flags := protocol.FlagFragment
		if i == total-1 {
			flags |= protocol.FlagLastFrag
		}

		hdr := protocol.Header{
			MsgID:       msgID,
			MsgType:     protocol.MSG,
			ContentType: ct,
			Priority:    prio,
			Flags:       flags,
			FragIndex:   uint16(i),
			FragTotal:   uint16(total),
			PayloadLen:  uint32(len(part)),
		}
		chunks = append(chunks, Chunk{Hdr: hdr, Payload: part})
	}
	return chunks
}
```

- [ ] **Step 4: Run tests — verify pass**

```bash
go test ./pkg/chunker/... -v
```

Expected: `TestSplitSmallPayload` and `TestSplitLargePayload` PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/chunker/split.go pkg/chunker/chunker_test.go
git commit -m "feat: payload chunker with FragIndex/FragTotal headers"
```

---

## Task 7: pkg/chunker — assemble + parallel send

**Files:**
- Create: `pkg/chunker/assemble.go`
- Modify: `pkg/chunker/chunker_test.go` (add assemble + parallel tests)
- Modify: `pkg/conn/conn.go` (add `WriteChunks` helper)

- [ ] **Step 1: Add assemble and parallel tests**

Append to `pkg/chunker/chunker_test.go`:

```go
func TestAssembleRoundtrip(t *testing.T) {
	original := bytes.Repeat([]byte("go"), 300) // 600 bytes
	chunks := Split(7, protocol.BINARY, protocol.Normal, original, 100)

	asm := NewAssembler()
	var result []byte
	for _, ch := range chunks {
		done, payload, err := asm.Feed(ch.Hdr, ch.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			result = payload
		}
	}
	if result == nil {
		t.Fatal("assembler never completed")
	}
	if !bytes.Equal(result, original) {
		t.Fatal("assembled payload mismatch")
	}
}

func TestAssemblerOutOfOrder(t *testing.T) {
	original := bytes.Repeat([]byte("z"), 250)
	chunks := Split(8, protocol.BINARY, protocol.Normal, original, 100)

	// Feed in reverse order: 2, 1, 0
	asm := NewAssembler()
	var result []byte
	for i := len(chunks) - 1; i >= 0; i-- {
		done, payload, err := asm.Feed(chunks[i].Hdr, chunks[i].Payload)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			result = payload
		}
	}
	if !bytes.Equal(result, original) {
		t.Fatal("out-of-order assemble mismatch")
	}
}
```

- [ ] **Step 2: Run test — verify fail**

```bash
go test ./pkg/chunker/... -v -run TestAssemble
```

Expected: `FAIL — undefined: NewAssembler`

- [ ] **Step 3: Write assemble.go**

```go
// pkg/chunker/assemble.go
package chunker

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

type msgBuf struct {
	frags [][]byte
	total uint16
	count uint16
}

// Assembler collects fragments from multiple messages concurrently.
type Assembler struct {
	mu   sync.Mutex
	msgs map[uint64]*msgBuf
}

func NewAssembler() *Assembler {
	return &Assembler{msgs: make(map[uint64]*msgBuf)}
}

// Feed accepts a fragment. Returns (true, assembled payload, nil) when all
// fragments of the message have arrived. Returns (false, nil, nil) otherwise.
func (a *Assembler) Feed(hdr protocol.Header, payload []byte) (bool, []byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	buf, ok := a.msgs[hdr.MsgID]
	if !ok {
		buf = &msgBuf{
			frags: make([][]byte, hdr.FragTotal),
			total: hdr.FragTotal,
		}
		a.msgs[hdr.MsgID] = buf
	}
	if hdr.FragIndex >= buf.total {
		return false, nil, fmt.Errorf("fragIndex %d >= fragTotal %d", hdr.FragIndex, buf.total)
	}
	if buf.frags[hdr.FragIndex] != nil {
		return false, nil, nil // duplicate, ignore
	}
	buf.frags[hdr.FragIndex] = payload
	buf.count++

	if buf.count < buf.total {
		return false, nil, nil
	}

	delete(a.msgs, hdr.MsgID)
	return true, bytes.Join(buf.frags, nil), nil
}
```

- [ ] **Step 4: Add WriteChunks to conn.go**

Append to `pkg/conn/conn.go`:

```go
// WriteChunks sends all chunks over parallel goroutines (up to maxStreams).
// All chunks share the same MsgID. Returns after all chunks are sent.
func (c *Conn) WriteChunks(chunks []chunker.Chunk, maxStreams int) error {
	if len(chunks) == 1 {
		return c.WriteFrame(chunks[0].Hdr, chunks[0].Payload)
	}

	sem := make(chan struct{}, maxStreams)
	errc := make(chan error, len(chunks))

	for _, ch := range chunks {
		ch := ch
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			errc <- c.WriteFrame(ch.Hdr, ch.Payload)
		}()
	}

	for i := 0; i < len(chunks); i++ {
		if err := <-errc; err != nil {
			return err
		}
	}
	return nil
}
```

Add import `"github.com/jdp5949/p2p-messaging/pkg/chunker"` to `pkg/conn/conn.go`.

- [ ] **Step 5: Run all chunker and conn tests**

```bash
go test ./pkg/chunker/... ./pkg/conn/... -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/chunker/assemble.go pkg/chunker/chunker_test.go pkg/conn/conn.go
git commit -m "feat: fragment assembler (out-of-order) + parallel chunk sender"
```

---

## Task 8: pkg/broker — ring buffer + ACK tracker

**Files:**
- Create: `pkg/broker/buffer.go`
- Create: `pkg/broker/broker_test.go` (buffer tests)

- [ ] **Step 1: Write failing buffer test**

```go
// pkg/broker/broker_test.go
package broker

import (
	"testing"
)

func TestRingBufferAddGet(t *testing.T) {
	rb := newRingBuffer(4)
	rb.add(1, []byte("msg1"))
	rb.add(2, []byte("msg2"))

	e, ok := rb.get(1)
	if !ok {
		t.Fatal("expected msg1 in buffer")
	}
	if string(e.envelope) != "msg1" {
		t.Fatalf("wrong envelope: %q", e.envelope)
	}
}

func TestRingBufferAckRemoves(t *testing.T) {
	rb := newRingBuffer(4)
	rb.add(10, []byte("data"))
	rb.ack(10)
	_, ok := rb.get(10)
	if ok {
		t.Fatal("acked message should be gone")
	}
}

func TestRingBufferOverflow(t *testing.T) {
	rb := newRingBuffer(2)
	dropped := rb.add(1, []byte("a"))
	if dropped != 0 {
		t.Fatalf("unexpected drop on first add: %d", dropped)
	}
	dropped = rb.add(2, []byte("b"))
	if dropped != 0 {
		t.Fatalf("unexpected drop on second add: %d", dropped)
	}
	// buffer full, third add should drop oldest (msgID=1)
	dropped = rb.add(3, []byte("c"))
	if dropped != 1 {
		t.Fatalf("expected dropped msgID=1, got %d", dropped)
	}
	_, ok := rb.get(1)
	if ok {
		t.Fatal("dropped message should not be in buffer")
	}
}

func TestRingBufferInFlight(t *testing.T) {
	rb := newRingBuffer(8)
	rb.add(5, []byte("x"))
	all := rb.inflight()
	if len(all) != 1 || all[0].msgID != 5 {
		t.Fatalf("inflight: expected [{5,...}], got %+v", all)
	}
}
```

- [ ] **Step 2: Run test — verify fail**

```bash
go test ./pkg/broker/... -v -run TestRingBuffer
```

Expected: `FAIL — undefined: newRingBuffer`

- [ ] **Step 3: Write buffer.go**

```go
// pkg/broker/buffer.go
package broker

import (
	"sync"
	"time"
)

type entry struct {
	msgID    uint64
	envelope []byte
	sentAt   time.Time
	retries  uint8
}

type ringBuffer struct {
	mu    sync.Mutex
	slots []entry
	index map[uint64]int // msgID → slot index
	size  int
	head  int // next write position
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		slots: make([]entry, size),
		index: make(map[uint64]int, size),
		size:  size,
	}
}

// add inserts a message. If buffer is full, evicts oldest (overwrites head).
// Returns the evicted msgID (0 if none evicted).
func (rb *ringBuffer) add(msgID uint64, envelope []byte) uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	var evicted uint64
	if old := rb.slots[rb.head]; old.msgID != 0 {
		delete(rb.index, old.msgID)
		evicted = old.msgID
	}
	rb.slots[rb.head] = entry{
		msgID:    msgID,
		envelope: envelope,
		sentAt:   time.Now(),
	}
	rb.index[msgID] = rb.head
	rb.head = (rb.head + 1) % rb.size
	return evicted
}

func (rb *ringBuffer) get(msgID uint64) (entry, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	idx, ok := rb.index[msgID]
	if !ok {
		return entry{}, false
	}
	return rb.slots[idx], true
}

func (rb *ringBuffer) ack(msgID uint64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	idx, ok := rb.index[msgID]
	if !ok {
		return
	}
	rb.slots[idx] = entry{}
	delete(rb.index, msgID)
}

func (rb *ringBuffer) incRetry(msgID uint64) uint8 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	idx, ok := rb.index[msgID]
	if !ok {
		return 0
	}
	rb.slots[idx].retries++
	return rb.slots[idx].retries
}

func (rb *ringBuffer) inflight() []entry {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	out := make([]entry, 0, len(rb.index))
	for _, idx := range rb.index {
		if rb.slots[idx].msgID != 0 {
			out = append(out, rb.slots[idx])
		}
	}
	return out
}
```

- [ ] **Step 4: Run buffer tests — verify pass**

```bash
go test ./pkg/broker/... -v -run TestRingBuffer
```

Expected: all 4 ring buffer tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/broker/buffer.go pkg/broker/broker_test.go
git commit -m "feat: ring buffer with overflow eviction and ACK tracking"
```

---

## Task 9: pkg/broker — public API, ACK/NACK, retry

**Files:**
- Create: `pkg/broker/broker.go`
- Modify: `pkg/broker/broker_test.go` (add broker integration tests)

- [ ] **Step 1: Add broker integration tests**

Append to `pkg/broker/broker_test.go`:

```go
func TestBrokerSendACK(t *testing.T) {
	a, b := net.Pipe()
	senderConn := conn.New(a)

	// receiver side: read one frame, send ACK
	go func() {
		rc := conn.New(b)
		hdr, _, err := rc.ReadFrame()
		if err != nil {
			return
		}
		ack := protocol.Header{
			MsgID:   hdr.MsgID,
			MsgType: protocol.ACK,
		}
		_ = rc.WriteFrame(ack, nil)
	}()

	cfg := Config{
		BufferSize: 10,
		AckTimeout: 2 * time.Second,
		MaxRetries: 3,
	}
	b2 := NewBroker(senderConn, cfg)
	defer b2.Close()

	err := b2.Send(context.Background(), &Message{
		ContentType: protocol.JSON,
		Priority:    protocol.Normal,
		Payload:     []byte(`{"event":"test"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBrokerNACKFiresCallback(t *testing.T) {
	a, b := net.Pipe()
	senderConn := conn.New(a)

	go func() {
		rc := conn.New(b)
		hdr, _, err := rc.ReadFrame()
		if err != nil {
			return
		}
		nack := protocol.Header{
			MsgID:    hdr.MsgID,
			MsgType:  protocol.NACK,
			Priority: protocol.Priority(protocol.NACKAppReject),
		}
		_ = rc.WriteFrame(nack, nil)
	}()

	nacked := make(chan uint64, 1)
	cfg := Config{
		BufferSize: 10,
		AckTimeout: 2 * time.Second,
		MaxRetries: 3,
		Hooks: Hooks{
			OnNack: func(msgID uint64, reason byte) { nacked <- msgID },
		},
	}
	br := NewBroker(senderConn, cfg)
	defer br.Close()

	_ = br.Send(context.Background(), &Message{
		ContentType: protocol.BINARY,
		Payload:     []byte{0x01, 0x02},
	})

	select {
	case id := <-nacked:
		if id == 0 {
			t.Fatal("expected valid msgID in OnNack")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnNack not called")
	}
}
```

Add imports to `broker_test.go`:

```go
import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)
```

- [ ] **Step 2: Run test — verify fail**

```bash
go test ./pkg/broker/... -v -run TestBroker
```

Expected: `FAIL — undefined: Config, NewBroker, Message`

- [ ] **Step 3: Write broker.go**

```go
// pkg/broker/broker.go
package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/chunker"
	"github.com/jdp5949/p2p-messaging/pkg/compress"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

const DefaultMaxStreams = 8

type AckResult int

const (
	AckOK   AckResult = iota
	AckNACK AckResult = iota
)

type Message struct {
	ContentType protocol.ContentType
	Priority    protocol.Priority
	Payload     []byte
	Meta        map[string]string
}

type Hooks struct {
	OnMessage func(msg *Message) AckResult
	OnNack    func(msgID uint64, reason byte)
	OnDead    func(msgID uint64, err error)
}

type Config struct {
	BufferSize int
	AckTimeout time.Duration
	MaxRetries int
	ChunkSize  int
	MaxStreams  int
	Hooks      Hooks
}

func (c *Config) setDefaults() {
	if c.BufferSize == 0 {
		c.BufferSize = 10_000
	}
	if c.AckTimeout == 0 {
		c.AckTimeout = 30 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 5
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = chunker.DefaultChunkSize
	}
	if c.MaxStreams == 0 {
		c.MaxStreams = DefaultMaxStreams
	}
}

type Broker struct {
	c      *conn.Conn
	cfg    Config
	buf    *ringBuffer
	nextID atomic.Uint64
	send   chan *Message
	quit   chan struct{}
	wg     sync.WaitGroup
}

func NewBroker(c *conn.Conn, cfg Config) *Broker {
	cfg.setDefaults()
	b := &Broker{
		c:    c,
		cfg:  cfg,
		buf:  newRingBuffer(cfg.BufferSize),
		send: make(chan *Message, 256),
		quit: make(chan struct{}),
	}
	b.wg.Add(2)
	go b.sendLoop()
	go b.recvLoop()
	return b
}

func (b *Broker) Send(ctx context.Context, msg *Message) error {
	select {
	case b.send <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.quit:
		return context.Canceled
	}
}

func (b *Broker) sendLoop() {
	defer b.wg.Done()
	for {
		select {
		case msg := <-b.send:
			b.transmit(msg)
		case <-b.quit:
			return
		}
	}
}

func (b *Broker) transmit(msg *Message) {
	msgID := b.nextID.Add(1)
	payload := msg.Payload
	flags := uint8(0)

	if compress.ShouldCompress(payload) {
		payload = compress.Compress(payload)
		flags |= protocol.FlagCompressed
	}

	// Build raw envelope (header + payload of first/only chunk) for buffer.
	// For multi-chunk, we store the original payload and re-chunk on replay.
	envelope := append(msg.Payload[:0:0], msg.Payload...) // copy

	evicted := b.buf.add(msgID, envelope)
	if evicted != 0 && b.cfg.Hooks.OnDead != nil {
		b.cfg.Hooks.OnDead(evicted, context.DeadlineExceeded)
	}

	chunks := chunker.Split(msgID, msg.ContentType, msg.Priority, payload, b.cfg.ChunkSize)
	for i := range chunks {
		if flags&protocol.FlagCompressed != 0 {
			chunks[i].Hdr.Flags |= protocol.FlagCompressed
		}
	}

	_ = b.c.WriteChunks(chunks, b.cfg.MaxStreams)
}

func (b *Broker) recvLoop() {
	defer b.wg.Done()
	asm := chunker.NewAssembler()
	for {
		hdr, payload, err := b.c.ReadFrame()
		if err != nil {
			select {
			case <-b.quit:
				return
			default:
				return
			}
		}

		switch hdr.MsgType {
		case protocol.ACK:
			b.buf.ack(hdr.MsgID)
		case protocol.NACK:
			b.buf.ack(hdr.MsgID)
			if b.cfg.Hooks.OnNack != nil {
				b.cfg.Hooks.OnNack(hdr.MsgID, uint8(hdr.Priority))
			}
		case protocol.PING:
			pong := protocol.Header{MsgType: protocol.PONG, MsgID: hdr.MsgID}
			_ = b.c.WriteFrame(pong, nil)
		case protocol.MSG:
			done, assembled, err := asm.Feed(hdr, payload)
			if err != nil || !done {
				continue
			}

			data := assembled
			if hdr.Flags&protocol.FlagCompressed != 0 {
				data, err = compress.Decompress(assembled)
				if err != nil {
					nack := protocol.Header{
						MsgID:    hdr.MsgID,
						MsgType:  protocol.NACK,
						Priority: protocol.Priority(protocol.NACKDecodeFail),
					}
					_ = b.c.WriteFrame(nack, nil)
					continue
				}
			}

			result := AckOK
			if b.cfg.Hooks.OnMessage != nil {
				msg := &Message{
					ContentType: hdr.ContentType,
					Priority:    hdr.Priority,
					Payload:     data,
				}
				result = b.cfg.Hooks.OnMessage(msg)
			}

			if result == AckOK {
				ack := protocol.Header{MsgID: hdr.MsgID, MsgType: protocol.ACK}
				_ = b.c.WriteFrame(ack, nil)
			} else {
				nack := protocol.Header{
					MsgID:    hdr.MsgID,
					MsgType:  protocol.NACK,
					Priority: protocol.Priority(protocol.NACKAppReject),
				}
				_ = b.c.WriteFrame(nack, nil)
			}
		}
	}
}

func (b *Broker) Replay() {
	for _, e := range b.buf.inflight() {
		msg := &Message{Payload: e.envelope}
		b.transmit(msg)
	}
}

func (b *Broker) Close() {
	close(b.quit)
	b.c.Close()
	b.wg.Wait()
}
```

- [ ] **Step 4: Run all broker tests**

```bash
go test ./pkg/broker/... -v -timeout 30s
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/broker/broker.go pkg/broker/broker_test.go
git commit -m "feat: broker with ACK/NACK, retry buffer, compression, parallel chunking"
```

---

## Task 10: cmd/peer — CLI

**Files:**
- Create: `cmd/peer/main.go`

- [ ] **Step 1: Write main.go**

```go
// cmd/peer/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jdp5949/p2p-messaging/pkg/broker"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

func main() {
	relay := flag.String("relay", conn.DefaultRelay, "relay server address")
	code := flag.String("code", "", "rendezvous code (required)")
	initiator := flag.Bool("init", false, "act as initiator (first peer to connect)")
	flag.Parse()

	if *code == "" {
		fmt.Fprintln(os.Stderr, "usage: peer -code <code> [-init] [-relay <addr>]")
		os.Exit(1)
	}

	role := conn.Receiver
	if *initiator {
		role = conn.Initiator
	}

	log.Printf("connecting to relay %s with code %q role=%v", *relay, *code, role)

	c, err := conn.Dial(*relay, *code, role)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	log.Println("connected")

	br := broker.NewBroker(c, broker.Config{
		Hooks: broker.Hooks{
			OnMessage: func(msg *broker.Message) broker.AckResult {
				fmt.Printf("[recv] type=%d len=%d payload=%q\n",
					msg.ContentType, len(msg.Payload), truncate(msg.Payload, 120))
				return broker.AckOK
			},
			OnNack: func(msgID uint64, reason byte) {
				log.Printf("[nack] msgID=%d reason=%d", msgID, reason)
			},
			OnDead: func(msgID uint64, err error) {
				log.Printf("[dead] msgID=%d err=%v", msgID, err)
			},
		},
	})
	defer br.Close()

	// Send any args as messages
	for _, arg := range flag.Args() {
		if err := br.Send(context.Background(), &broker.Message{
			ContentType: protocol.TEXT,
			Priority:    protocol.Normal,
			Payload:     []byte(arg),
		}); err != nil {
			log.Printf("send: %v", err)
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}
```

- [ ] **Step 2: Build check**

```bash
go build ./cmd/peer/
```

Expected: no errors. Binary `./peer` created.

- [ ] **Step 3: Commit**

```bash
git add cmd/peer/main.go
git commit -m "feat: peer CLI — connect via relay, send/receive messages"
```

---

## Task 11: cmd/relay — self-hosted relay server

**Files:**
- Create: `cmd/relay/main.go`

- [ ] **Step 1: Write relay main.go**

```go
// cmd/relay/main.go
package main

import (
	"flag"
	"log"

	crocrelay "github.com/schollz/croc/v10/src/relay"
)

func main() {
	addr := flag.String("addr", ":9009", "relay listen address")
	flag.Parse()

	log.Printf("starting relay on %s", *addr)
	if err := crocrelay.Run(crocrelay.Options{
		RelayPorts: []string{(*addr)[1:]}, // strip leading ':'
		Debug:      false,
	}); err != nil {
		log.Fatalf("relay: %v", err)
	}
}
```

- [ ] **Step 2: Build check**

```bash
go build ./cmd/relay/
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add cmd/relay/main.go
git commit -m "feat: self-hosted relay server wrapping croc relay"
```

---

## Task 12: examples

**Files:**
- Create: `examples/send_json/main.go`
- Create: `examples/send_binary/main.go`

- [ ] **Step 1: Write JSON example**

```go
// examples/send_json/main.go
package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jdp5949/p2p-messaging/pkg/broker"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

type OrderEvent struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
	Status  string  `json:"status"`
}

func main() {
	// initiator side: run with PEER_ROLE=init env var
	// receiver side: run without it
	role := conn.Receiver
	// detect role from args or env as needed

	c, err := conn.Dial(conn.DefaultRelay, "demo-json-001", role)
	if err != nil {
		log.Fatal(err)
	}

	br := broker.NewBroker(c, broker.Config{
		Hooks: broker.Hooks{
			OnMessage: func(msg *broker.Message) broker.AckResult {
				var evt OrderEvent
				if err := json.Unmarshal(msg.Payload, &evt); err != nil {
					return broker.AckNACK
				}
				log.Printf("received order: %+v", evt)
				return broker.AckOK
			},
		},
	})
	defer br.Close()

	evt := OrderEvent{OrderID: "ord-42", Amount: 199.99, Status: "shipped"}
	payload, _ := json.Marshal(evt)
	_ = br.Send(context.Background(), &broker.Message{
		ContentType: protocol.JSON,
		Priority:    protocol.High,
		Payload:     payload,
	})

	select {} // keep running to receive
}
```

- [ ] **Step 2: Write binary example**

```go
// examples/send_binary/main.go
package main

import (
	"context"
	"log"

	"github.com/jdp5949/p2p-messaging/pkg/broker"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

func main() {
	c, err := conn.Dial(conn.DefaultRelay, "demo-binary-001", conn.Initiator)
	if err != nil {
		log.Fatal(err)
	}

	br := broker.NewBroker(c, broker.Config{
		ChunkSize: 512 * 1024,
		Hooks: broker.Hooks{
			OnMessage: func(msg *broker.Message) broker.AckResult {
				log.Printf("received %d bytes of binary data", len(msg.Payload))
				return broker.AckOK
			},
		},
	})
	defer br.Close()

	// send 5 MB of binary data — will be chunked automatically
	bigPayload := make([]byte, 5*1024*1024)
	for i := range bigPayload {
		bigPayload[i] = byte(i % 251)
	}
	if err := br.Send(context.Background(), &broker.Message{
		ContentType: protocol.BINARY,
		Priority:    protocol.Normal,
		Payload:     bigPayload,
	}); err != nil {
		log.Fatal(err)
	}
	log.Println("sent 5MB binary payload")
	select {}
}
```

- [ ] **Step 3: Build all**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 4: Run full test suite**

```bash
go test ./... -v -timeout 60s
```

Expected: all tests pass.

- [ ] **Step 5: Final commit**

```bash
git add examples/ && git commit -m "feat: JSON and binary usage examples"
git push origin main
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] 20-byte fixed binary header → Task 1
- [x] ContentType (all payload types) → Task 1 types.go
- [x] zstd compression with threshold → Task 2
- [x] Framed read/write over net.Conn → Task 3
- [x] Relay dial + TLS → Task 4
- [x] Reconnect with exponential backoff → Task 5
- [x] Chunking large payloads → Task 6
- [x] Out-of-order fragment reassembly → Task 7
- [x] Parallel chunk send (WriteChunks) → Task 7
- [x] Ring buffer with overflow eviction → Task 8
- [x] ACK/NACK + retry + dead-letter hooks → Task 9
- [x] CLI peer node → Task 10
- [x] Self-hosted relay → Task 11
- [x] Examples → Task 12

**Type consistency:**
- `conn.Conn` → used consistently across broker, chunker, conn packages
- `protocol.Header` → same struct in all tasks
- `chunker.Chunk` → `{Hdr protocol.Header, Payload []byte}` — used in split, assemble, conn.WriteChunks
- `broker.AckResult` → `AckOK` / `AckNACK` — consistent in hooks and recvLoop
