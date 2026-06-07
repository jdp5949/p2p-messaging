package transfer

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// writeTar streams a tar of paths into w, preserving relative structure
// (relative to each path's parent) and file modes. Directories recurse.
func writeTar(w io.Writer, paths []string) error {
	tw := tar.NewWriter(w)
	for _, p := range paths {
		clean := filepath.Clean(p)
		base := filepath.Dir(clean)
		err := filepath.Walk(clean, func(file string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(base, file)
			if err != nil {
				return err
			}
			hdr, err := tar.FileInfoHeader(fi, "")
			if err != nil {
				return err
			}
			hdr.Name = filepath.ToSlash(rel)
			if fi.IsDir() {
				hdr.Name += "/"
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			if fi.Mode().IsRegular() {
				f, err := os.Open(file)
				if err != nil {
					return err
				}
				_, err = io.Copy(tw, f)
				f.Close()
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return tw.Close()
}

// extractTar unpacks r into destDir, rejecting any entry that escapes destDir
// (zip-slip) and skipping symlinks/special files.
func extractTar(r io.Reader, destDir string) error {
	tr := tar.NewReader(r)
	cleanDest := filepath.Clean(destDir)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target := filepath.Join(cleanDest, hdr.Name)
		if target != cleanDest &&
			!strings.HasPrefix(filepath.Clean(target)+string(os.PathSeparator), cleanDest+string(os.PathSeparator)) {
			return fmt.Errorf("transfer: unsafe path in archive: %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil { //nolint:gosec
				f.Close()
				return err
			}
			f.Close()
		default:
			// skip symlinks and special files for safety
		}
	}
}
