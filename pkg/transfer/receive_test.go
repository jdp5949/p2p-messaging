package transfer

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

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
		p, _, e := Receive(sendB, a2b, dst, ow, nil)
		res <- rr{p, e}
	}()
	_, sendErr := Send(sendA, b2a, paths, nil)
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

	_, _, err := Receive(func(protocol.ContentType, []byte) error { return nil }, in, dst, nil, nil)
	if err == nil {
		t.Fatal("expected integrity error")
	}
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
	in <- Msg{protocol.ContentBinary, encodeChunk(5, full[5:])}
	in <- Msg{protocol.ContentBinary, encodeChunk(0, full[0:5])}
	in <- Msg{protocol.ContentBinary, encodeChunk(5, full[5:])} // dup
	in <- Msg{protocol.ContentJSON, marshalTrailer(Trailer{SHA256: sum, Total: int64(len(full))})}
	close(in)

	saved, _, err := Receive(func(protocol.ContentType, []byte) error { return nil }, in, dst, func(string) bool { return true }, nil)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	got, _ := os.ReadFile(saved)
	if !bytes.Equal(got, full) {
		t.Fatalf("got %q want %q", got, full)
	}
}

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

func TestHeaderMarshalIsJSON(t *testing.T) {
	var h Header
	if err := json.Unmarshal(marshalHeader(Header{Kind: "file"}), &h); err != nil {
		t.Fatal(err)
	}
	if h.T != "header" {
		t.Fatalf("t=%q", h.T)
	}
}
