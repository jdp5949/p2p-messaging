package main

import (
	"strings"
	"testing"
)

func TestFormatProgress(t *testing.T) {
	s := formatProgress(5*1024*1024, 10*1024*1024)
	if !strings.Contains(s, "50.0%") {
		t.Fatalf("want percent, got %q", s)
	}
	if !strings.Contains(s, "5.0 MB") || !strings.Contains(s, "10.0 MB") {
		t.Fatalf("want humanized sizes, got %q", s)
	}
	u := formatProgress(2048, 0)
	if !strings.Contains(u, "2.0 KB") {
		t.Fatalf("want humanized byte count, got %q", u)
	}
}
