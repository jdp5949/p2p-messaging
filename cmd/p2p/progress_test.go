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
