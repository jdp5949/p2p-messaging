package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

func TestSendTimesOutWithoutAck(t *testing.T) {
	old := ackTimeout
	ackTimeout = 50 * time.Millisecond
	defer func() { ackTimeout = old }()

	dir := t.TempDir()
	path := filepath.Join(dir, "f.txt")
	if err := os.WriteFile(path, []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Receiver never sends DONE.
	in := make(chan Msg) // never delivers
	_, err := Send(func(protocol.ContentType, []byte) error { return nil }, in, []string{path}, nil)
	if err == nil {
		t.Fatal("expected timeout error when peer never acknowledges")
	}
}

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
	done := make(chan Msg, 1)
	done <- Msg{protocol.ContentJSON, marshalDone()}
	close(done)

	if _, err := Send(send, done, []string{path}, nil); err != nil {
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
