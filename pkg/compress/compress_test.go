package compress

import (
	"bytes"
	"math/rand"
	"testing"
)

func setup(t *testing.T) (*Encoder, *Decoder) {
	t.Helper()
	enc, err := NewEncoder()
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	dec, err := NewDecoder()
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	return enc, dec
}

func TestSmallPayloadNotCompressed(t *testing.T) {
	enc, _ := setup(t)
	src := []byte("short")
	out, was, err := enc.Compress(src)
	if err != nil {
		t.Fatal(err)
	}
	if was {
		t.Error("expected wasCompressed=false for small payload")
	}
	if !bytes.Equal(out, src) {
		t.Error("expected original bytes returned unchanged")
	}
}

func TestEmptyPayloadNotCompressed(t *testing.T) {
	enc, _ := setup(t)
	out, was, err := enc.Compress([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if was {
		t.Error("expected wasCompressed=false for empty payload")
	}
	if len(out) != 0 {
		t.Error("expected empty output")
	}
}

func TestLargePayloadRoundTrip(t *testing.T) {
	enc, dec := setup(t)
	src := bytes.Repeat([]byte("hello world "), 30)
	if len(src) <= ThresholdBytes {
		t.Fatalf("test data too small: %d bytes", len(src))
	}

	compressed, was, err := enc.Compress(src)
	if err != nil {
		t.Fatal(err)
	}
	if !was {
		t.Error("expected wasCompressed=true for large payload")
	}

	restored, err := dec.Decompress(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, src) {
		t.Error("round-trip mismatch")
	}
}

func TestCompressibleVsIncompressible(t *testing.T) {
	enc, dec := setup(t)

	compressible := bytes.Repeat([]byte{0xAB}, 512)
	compressed, was, err := enc.Compress(compressible)
	if err != nil {
		t.Fatal(err)
	}
	if !was {
		t.Error("expected compression for compressible data")
	}
	if len(compressed) >= len(compressible) {
		t.Logf("note: compressible data did not shrink (len=%d vs %d)", len(compressed), len(compressible))
	}
	restored, err := dec.Decompress(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, compressible) {
		t.Error("round-trip mismatch for compressible data")
	}

	rng := rand.New(rand.NewSource(42))
	incompressible := make([]byte, 512)
	rng.Read(incompressible)
	compressed2, was2, err := enc.Compress(incompressible)
	if err != nil {
		t.Fatal(err)
	}
	if !was2 {
		t.Error("expected wasCompressed=true for large incompressible data (above threshold)")
	}
	restored2, err := dec.Decompress(compressed2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored2, incompressible) {
		t.Error("round-trip mismatch for incompressible data")
	}
}
