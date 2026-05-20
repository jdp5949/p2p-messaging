package chunker

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/compress"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

func mustEncoder(t *testing.T) *compress.Encoder {
	t.Helper()
	enc, err := compress.NewEncoder()
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

func mustDecoder(t *testing.T) *compress.Decoder {
	t.Helper()
	dec, err := compress.NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	return dec
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b) //nolint:gosec
	return b
}

// TestSmallPayload: single fragment, no FlagFragmented.
func TestSmallPayload(t *testing.T) {
	enc := mustEncoder(t)
	dec := mustDecoder(t)
	payload := []byte("hello world")

	msgs, err := Split(1, protocol.ContentText, payload, enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 msg, got %d", len(msgs))
	}
	if msgs[0].Header.Flags&protocol.FlagFragmented != 0 {
		t.Error("should not have FlagFragmented for single fragment")
	}
	if msgs[0].Header.FragTotal != 1 {
		t.Errorf("FragTotal want 1, got %d", msgs[0].Header.FragTotal)
	}

	asm := NewAssembler()
	got, ok, err := asm.Add(msgs[0], dec)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("want complete")
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch: got %q want %q", got, payload)
	}
}

// TestLargePayload: >512KB, multiple fragments, reassemble matches original.
func TestLargePayload(t *testing.T) {
	enc := mustEncoder(t)
	dec := mustDecoder(t)
	payload := randBytes(ChunkSize + 100*1024) // 612 KB

	msgs, err := Split(42, protocol.ContentBinary, payload, enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) < 2 {
		t.Fatalf("want multiple msgs, got %d", len(msgs))
	}
	for _, m := range msgs {
		if m.Header.Flags&protocol.FlagFragmented == 0 {
			t.Error("FlagFragmented must be set on multi-fragment messages")
		}
	}

	asm := NewAssembler()
	var result []byte
	for _, m := range msgs {
		got, ok, err := asm.Add(m, dec)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			result = got
		}
	}
	if result == nil {
		t.Fatal("never got complete payload")
	}

	// Decompress expected if compression was applied.
	expected := payload
	if msgs[0].Header.Flags&protocol.FlagCompressed != 0 {
		// Already decompressed by Assembler; compare original.
	}
	if !bytes.Equal(result, expected) {
		t.Errorf("payload round-trip mismatch: len got=%d want=%d", len(result), len(expected))
	}
}

// TestOutOfOrder: fragments arrive reversed, assembly still works.
func TestOutOfOrder(t *testing.T) {
	enc := mustEncoder(t)
	dec := mustDecoder(t)
	payload := randBytes(ChunkSize*3 + 1024)

	msgs, err := Split(99, protocol.ContentBinary, payload, enc)
	if err != nil {
		t.Fatal(err)
	}

	// Reverse order.
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}

	asm := NewAssembler()
	var result []byte
	for _, m := range msgs {
		got, ok, err := asm.Add(m, dec)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			result = got
		}
	}
	if result == nil {
		t.Fatal("never got complete payload")
	}
	if !bytes.Equal(result, payload) {
		t.Errorf("out-of-order round-trip mismatch: len got=%d want=%d", len(result), len(payload))
	}
}

// TestPurge: missing fragment removed after TTL.
func TestPurge(t *testing.T) {
	enc := mustEncoder(t)
	dec := mustDecoder(t)
	payload := randBytes(ChunkSize + 1024)

	msgs, err := Split(7, protocol.ContentBinary, payload, enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) < 2 {
		t.Skip("not enough fragments")
	}

	asm := NewAssembler()
	// Add all but last fragment.
	for _, m := range msgs[:len(msgs)-1] {
		_, ok, err := asm.Add(m, dec)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("should not be complete yet")
		}
	}

	asm.mu.Lock()
	count := len(asm.streams)
	asm.mu.Unlock()
	if count != 1 {
		t.Fatalf("want 1 in-progress stream, got %d", count)
	}

	// Purge with very short TTL — stream should be removed.
	asm.Purge(time.Nanosecond)

	asm.mu.Lock()
	count = len(asm.streams)
	asm.mu.Unlock()
	if count != 0 {
		t.Fatalf("want 0 streams after purge, got %d", count)
	}
}

// TestCompressedFragmented: compressed + fragmented round-trip.
func TestCompressedFragmented(t *testing.T) {
	enc := mustEncoder(t)
	dec := mustDecoder(t)
	// Compressible data (repeated pattern).
	chunk := bytes.Repeat([]byte("abcdefgh"), 1024)
	payload := bytes.Repeat(chunk, 80) // ~640KB, highly compressible

	msgs, err := Split(55, protocol.ContentRaw, payload, enc)
	if err != nil {
		t.Fatal(err)
	}

	asm := NewAssembler()
	var result []byte
	for _, m := range msgs {
		got, ok, err := asm.Add(m, dec)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			result = got
		}
	}
	if result == nil {
		t.Fatal("never complete")
	}
	if !bytes.Equal(result, payload) {
		t.Errorf("compressed+fragmented round-trip fail: len got=%d want=%d", len(result), len(payload))
	}
}

// TestVariousSizes: table-driven round-trip for multiple payload sizes.
func TestVariousSizes(t *testing.T) {
	enc := mustEncoder(t)
	dec := mustDecoder(t)

	sizes := []int{
		0,
		1,
		256,
		ChunkSize,      // exactly 512KB
		1 * 1024 * 1024, // 1MB
		5 * 1024 * 1024, // 5MB
	}

	for _, size := range sizes {
		size := size
		t.Run("", func(t *testing.T) {
			t.Parallel()
			var payload []byte
			if size > 0 {
				payload = randBytes(size)
			} else {
				payload = []byte{}
			}

			msgs, err := Split(uint64(size+1), protocol.ContentBinary, payload, enc)
			if err != nil {
				t.Fatal(err)
			}

			asm := NewAssembler()
			var result []byte
			for _, m := range msgs {
				got, ok, err := asm.Add(m, dec)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					result = got
				}
			}
			if !bytes.Equal(result, payload) {
				t.Errorf("size=%d round-trip fail: got len=%d want len=%d", size, len(result), len(payload))
			}
		})
	}
}

// TestMalformedFragment: bad fragment returns error.
func TestMalformedFragment(t *testing.T) {
	dec := mustDecoder(t)
	asm := NewAssembler()

	bad := protocol.Message{
		Header: protocol.Header{
			MsgID:      100,
			FragTotal:  0, // invalid
			FragIndex:  0,
			PayloadLen: 0,
		},
		Payload: nil,
	}
	_, _, err := asm.Add(bad, dec)
	if err == nil {
		t.Error("expected error for FragTotal=0")
	}
}
