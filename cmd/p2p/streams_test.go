package main

import "testing"

func TestNegotiateMin(t *testing.T) {
	if got := minInt(4, 2); got != 2 {
		t.Fatalf("minInt=%d", got)
	}
	if got := minInt(1, 5); got != 1 {
		t.Fatalf("minInt=%d", got)
	}
}
