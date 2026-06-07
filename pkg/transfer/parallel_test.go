package transfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
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
