package codephrase

import (
	"regexp"
	"strings"
	"testing"
)

var phraseRE = regexp.MustCompile(`^[0-9]+-[a-z]+-[a-z]+-[a-z]+$`)

func TestGenerateFormat(t *testing.T) {
	for i := 0; i < 100; i++ {
		p := Generate()
		if !phraseRE.MatchString(p) {
			t.Fatalf("bad phrase format: %q", p)
		}
		if len(strings.Split(p, "-")) != 4 {
			t.Fatalf("want 4 segments, got %q", p)
		}
	}
}

func TestSessionIDStableAndHex(t *testing.T) {
	a := SessionID("4-brave-tiger-comet")
	b := SessionID("4-brave-tiger-comet")
	if a != b {
		t.Fatalf("SessionID not stable: %q vs %q", a, b)
	}
	if len(a) != 32 { // 16 bytes hex-encoded
		t.Fatalf("SessionID len = %d, want 32", len(a))
	}
	if SessionID("other") == a {
		t.Fatal("SessionID collision for different inputs")
	}
}
