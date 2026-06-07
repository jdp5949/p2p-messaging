package main

import "testing"

func TestParseSizes(t *testing.T) {
	got, err := parseSizes("1KB,1MB,10MB")
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1024, 1024 * 1024, 10 * 1024 * 1024}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%d want %d", i, got[i], want[i])
		}
	}
	if _, err := parseSizes("1KB,nope"); err == nil {
		t.Fatal("expected error")
	}
}
