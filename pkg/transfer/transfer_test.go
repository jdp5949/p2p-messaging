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

func TestChatHelloKind(t *testing.T) {
	m := Msg{protocol.ContentJSON, ChatHello()}
	if got := Kind(m); got != "chat" {
		t.Fatalf("Kind(ChatHello) = %q, want chat", got)
	}
	// Kind must match classify for all kinds (it's the exported alias).
	if Kind(Msg{protocol.ContentBinary, make([]byte, 8)}) != "data" {
		t.Fatal("Kind data mismatch")
	}
	if Kind(Msg{protocol.ContentText, []byte("hi")}) != "other" {
		t.Fatal("Kind text should be other")
	}
}
