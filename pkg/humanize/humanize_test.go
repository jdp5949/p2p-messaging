package humanize

import (
	"testing"
	"time"
)

func TestBytes(t *testing.T) {
	cases := map[int64]string{
		0: "0 B", 512: "512 B", 1023: "1023 B",
		1024: "1.0 KB", 1536: "1.5 KB",
		5 * 1024 * 1024:        "5.0 MB",
		3 * 1024 * 1024 * 1024: "3.0 GB",
	}
	for n, want := range cases {
		if got := Bytes(n); got != want {
			t.Errorf("Bytes(%d)=%q want %q", n, got, want)
		}
	}
}

func TestRate(t *testing.T) {
	if got := Rate(1024*1024, time.Second); got != "1.0 MB/s" {
		t.Errorf("Rate=%q want 1.0 MB/s", got)
	}
	if got := Rate(100, 0); got != "—" {
		t.Errorf("Rate(_,0)=%q want —", got)
	}
}

func TestDur(t *testing.T) {
	cases := map[time.Duration]string{
		850 * time.Millisecond:  "850ms",
		4200 * time.Millisecond: "4.2s",
		63 * time.Second:        "1m3s",
	}
	for d, want := range cases {
		if got := Dur(d); got != want {
			t.Errorf("Dur(%v)=%q want %q", d, got, want)
		}
	}
}

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"512": 512, "1KB": 1024, "10MB": 10 * 1024 * 1024,
		"1.5MB": 1572864, "2GB": 2 * 1024 * 1024 * 1024, "256B": 256,
	}
	for s, want := range cases {
		got, err := ParseSize(s)
		if err != nil || got != want {
			t.Errorf("ParseSize(%q)=%d,%v want %d", s, got, err, want)
		}
	}
	if _, err := ParseSize("abc"); err == nil {
		t.Error("expected error for bad size")
	}
}
