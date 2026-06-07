package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// Receive consumes a transfer from in, writing into destDir. It verifies the
// SHA-256 from the trailer, then for "file" places the result at
// destDir/<name> (consulting overwrite when it exists, keeping "<name>.part" if
// declined), or for "archive" unpacks the tar into destDir. On success it sends
// a DONE ack via send and returns the saved path.
func Receive(send SendFunc, in <-chan Msg, destDir string, overwrite OverwriteFn, progress ProgressFn) (string, error) {
	first, ok := <-in
	if !ok {
		return "", fmt.Errorf("transfer: stream closed before header")
	}
	if classify(first) != "header" {
		return "", fmt.Errorf("transfer: expected header, got %s", classify(first))
	}
	var hdr Header
	if err := json.Unmarshal(first.Payload, &hdr); err != nil {
		return "", fmt.Errorf("transfer: bad header: %w", err)
	}

	tmp, err := os.CreateTemp(destDir, ".p2p-recv-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	removeTmp := func() { _ = tmp.Close(); _ = os.Remove(tmpName) }

	var trailer *Trailer
	for m := range in {
		switch classify(m) {
		case "data":
			off, data, derr := decodeChunk(m.Payload)
			if derr != nil {
				removeTmp()
				return "", derr
			}
			if _, werr := tmp.WriteAt(data, off); werr != nil {
				removeTmp()
				return "", werr
			}
			if progress != nil {
				progress(off+int64(len(data)), hdr.Size)
			}
		case "trailer":
			var tr Trailer
			if err := json.Unmarshal(m.Payload, &tr); err != nil {
				removeTmp()
				return "", err
			}
			trailer = &tr
		}
		if trailer != nil {
			break
		}
	}
	if trailer == nil {
		removeTmp()
		return "", fmt.Errorf("transfer: stream ended before trailer")
	}
	if err := tmp.Sync(); err != nil {
		removeTmp()
		return "", err
	}
	_ = tmp.Close()

	sum, total, err := hashFile(tmpName)
	if err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	if sum != trailer.SHA256 || total != trailer.Total {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("transfer: integrity check failed")
	}

	var saved string
	if hdr.Kind == "archive" {
		f, oerr := os.Open(tmpName)
		if oerr != nil {
			_ = os.Remove(tmpName)
			return "", oerr
		}
		err = extractTar(f, destDir)
		_ = f.Close()
		_ = os.Remove(tmpName)
		if err != nil {
			return "", err
		}
		saved = destDir
	} else {
		final := filepath.Join(destDir, filepath.Base(hdr.Name))
		if _, statErr := os.Stat(final); statErr == nil &&
			(overwrite == nil || !overwrite(filepath.Base(hdr.Name))) {
			final += ".part"
		}
		if hdr.Mode != 0 {
			_ = os.Chmod(tmpName, os.FileMode(hdr.Mode))
		}
		if err := os.Rename(tmpName, final); err != nil {
			_ = os.Remove(tmpName)
			return "", err
		}
		saved = final
	}

	if send != nil {
		_ = send(protocol.ContentJSON, marshalDone())
	}
	return saved, nil
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
