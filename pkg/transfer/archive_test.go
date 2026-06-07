package transfer

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestTarRoundTrip(t *testing.T) {
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("beta"), 0o600); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := writeTar(&buf, []string{src}); err != nil {
		t.Fatalf("writeTar: %v", err)
	}

	dst := t.TempDir()
	if err := extractTar(&buf, dst); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	base := filepath.Base(src)
	got, err := os.ReadFile(filepath.Join(dst, base, "a.txt"))
	if err != nil || string(got) != "alpha" {
		t.Fatalf("a.txt = %q err=%v", got, err)
	}
	got, err = os.ReadFile(filepath.Join(dst, base, "sub", "b.txt"))
	if err != nil || string(got) != "beta" {
		t.Fatalf("b.txt = %q err=%v", got, err)
	}
	fi, _ := os.Stat(filepath.Join(dst, base, "sub", "b.txt"))
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("perm = %o, want 600", fi.Mode().Perm())
	}
}

func TestExtractTarRejectsZipSlip(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "../evil.txt", Mode: 0o644, Size: 4, Typeflag: tar.TypeReg})
	tw.Write([]byte("evil"))
	tw.Close()

	dst := t.TempDir()
	if err := extractTar(&buf, dst); err == nil {
		t.Fatal("expected zip-slip rejection")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dst), "evil.txt")); err == nil {
		t.Fatal("zip-slip wrote outside dest")
	}
}
