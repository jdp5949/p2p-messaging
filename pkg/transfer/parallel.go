package transfer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Stream is one framed, ordered, reliable message channel (a crypto.Session in
// production). SendParallel/ReceiveParallel use several of them at once.
type Stream interface {
	WriteMsg(p []byte) error
	ReadMsg() ([]byte, error)
	Close() error
}

// 1-byte message tags for the parallel protocol (Streams carry both binary data
// and JSON control, so every message is tagged).
const (
	tagHeader  = 'H'
	tagData    = 'D' // followed by encodeChunk(offset, bytes)
	tagEOS     = 'E' // end of a stream's range
	tagTrailer = 'T'
	tagDone    = 'O'
)

// splitRanges divides [0,total) into n contiguous [lo,hi) ranges.
func splitRanges(total int64, n int) [][2]int64 {
	out := make([][2]int64, n)
	for i := 0; i < n; i++ {
		lo := int64(i) * total / int64(n)
		hi := int64(i+1) * total / int64(n)
		if i == n-1 {
			hi = total
		}
		out[i] = [2]int64{lo, hi}
	}
	return out
}

// SendParallel sends paths across len(streams) streams. streams[0] is control.
func SendParallel(streams []Stream, paths []string, progress ProgressFn) (Stats, error) {
	start := time.Now()
	m := len(streams)
	if m == 0 {
		return Stats{}, fmt.Errorf("transfer: no streams")
	}

	// Resolve the payload to a single file on disk (tar dirs/multi to temp).
	srcPath, hdr, cleanup, err := prepareSource(paths)
	if err != nil {
		return Stats{}, err
	}
	defer cleanup()

	sum, size, err := hashFile(srcPath)
	if err != nil {
		return Stats{}, err
	}
	hdr.SHA256, hdr.Size, hdr.Streams = sum, size, m

	if err := streams[0].WriteMsg(tagged(tagHeader, marshalHeader(hdr))); err != nil {
		return Stats{}, err
	}

	f, err := os.Open(srcPath)
	if err != nil {
		return Stats{}, err
	}
	defer f.Close()

	ranges := splitRanges(size, m)
	var sent int64
	errc := make(chan error, m)
	for i := 0; i < m; i++ {
		go func(i int, lo, hi int64) {
			buf := make([]byte, ChunkSize)
			off := lo
			for off < hi {
				n := int64(len(buf))
				if rem := hi - off; rem < n {
					n = rem
				}
				if _, e := f.ReadAt(buf[:n], off); e != nil {
					errc <- e
					return
				}
				if e := streams[i].WriteMsg(tagged(tagData, encodeChunk(off, buf[:n]))); e != nil {
					errc <- e
					return
				}
				off += n
				if progress != nil {
					progress(atomic.AddInt64(&sent, n), size)
				}
			}
			errc <- streams[i].WriteMsg([]byte{tagEOS})
		}(i, ranges[i][0], ranges[i][1])
	}
	for i := 0; i < m; i++ {
		if e := <-errc; e != nil {
			return Stats{}, e
		}
	}

	if err := streams[0].WriteMsg(tagged(tagTrailer, marshalTrailer(Trailer{SHA256: sum, Total: size}))); err != nil {
		return Stats{}, err
	}

	// Wait for DONE on stream0 (with timeout).
	donec := make(chan error, 1)
	go func() {
		for {
			mb, e := streams[0].ReadMsg()
			if e != nil {
				donec <- e
				return
			}
			if len(mb) > 0 && mb[0] == tagDone {
				donec <- nil
				return
			}
		}
	}()
	select {
	case e := <-donec:
		if e != nil {
			return Stats{}, fmt.Errorf("transfer: waiting for ack: %w", e)
		}
	case <-time.After(ackTimeout):
		return Stats{}, fmt.Errorf("transfer: peer did not acknowledge within %s", ackTimeout)
	}
	return Stats{Bytes: size, Duration: time.Since(start)}, nil
}

// ReceiveParallel receives a parallel transfer. streams[0] is control.
func ReceiveParallel(streams []Stream, destDir string, overwrite OverwriteFn, progress ProgressFn) (string, Stats, error) {
	start := time.Now()
	m := len(streams)

	// HEADER on stream0.
	hb, err := streams[0].ReadMsg()
	if err != nil {
		return "", Stats{}, err
	}
	if len(hb) == 0 || hb[0] != tagHeader {
		return "", Stats{}, fmt.Errorf("transfer: expected header")
	}
	var hdr Header
	if err := json.Unmarshal(hb[1:], &hdr); err != nil {
		return "", Stats{}, fmt.Errorf("transfer: bad header: %w", err)
	}

	tmp, err := os.CreateTemp(destDir, ".p2p-recv-*")
	if err != nil {
		return "", Stats{}, err
	}
	tmpName := tmp.Name()
	if err := tmp.Truncate(hdr.Size); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", Stats{}, err
	}
	cleanup := func() { tmp.Close(); os.Remove(tmpName) }

	// Reader per stream: write DATA at offset until EOS. stream0 also yields
	// TRAILER (after its EOS) — capture it.
	var written int64
	var trailer atomic.Value // Trailer
	errc := make(chan error, m)
	var mu sync.Mutex // guards WriteAt (os.File.WriteAt is safe concurrently on most platforms, but lock to be safe)
	for i := 0; i < m; i++ {
		go func(i int) {
			eosSeen := false
			for {
				mb, e := streams[i].ReadMsg()
				if e != nil {
					errc <- e
					return
				}
				switch mb[0] {
				case tagData:
					off, data, de := decodeChunk(mb[1:])
					if de != nil {
						errc <- de
						return
					}
					mu.Lock()
					_, we := tmp.WriteAt(data, off)
					mu.Unlock()
					if we != nil {
						errc <- we
						return
					}
					if progress != nil {
						progress(atomic.AddInt64(&written, int64(len(data))), hdr.Size)
					}
				case tagEOS:
					eosSeen = true
					if i != 0 {
						errc <- nil
						return
					}
					// stream0: keep reading for TRAILER.
				case tagTrailer:
					var tr Trailer
					if e := json.Unmarshal(mb[1:], &tr); e != nil {
						errc <- e
						return
					}
					trailer.Store(tr)
					errc <- nil
					return
				}
				_ = eosSeen
			}
		}(i)
	}
	for i := 0; i < m; i++ {
		if e := <-errc; e != nil {
			cleanup()
			return "", Stats{}, e
		}
	}

	tr, _ := trailer.Load().(Trailer)
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", Stats{}, err
	}
	tmp.Close()

	sum, total, err := hashFile(tmpName)
	if err != nil {
		os.Remove(tmpName)
		return "", Stats{}, err
	}
	if sum != tr.SHA256 || total != tr.Total {
		os.Remove(tmpName)
		return "", Stats{}, fmt.Errorf("transfer: integrity check failed")
	}

	saved, err := finalize(tmpName, destDir, hdr, overwrite)
	if err != nil {
		return "", Stats{}, err
	}
	_ = streams[0].WriteMsg([]byte{tagDone})
	return saved, Stats{Bytes: total, Duration: time.Since(start)}, nil
}

// tagged prefixes a 1-byte tag.
func tagged(tag byte, body []byte) []byte {
	out := make([]byte, 1+len(body))
	out[0] = tag
	copy(out[1:], body)
	return out
}

// prepareSource resolves paths to a single on-disk file to send. A lone regular
// file is used directly; a dir or multiple paths are tarred to a temp file.
func prepareSource(paths []string) (srcPath string, hdr Header, cleanup func(), err error) {
	cleanup = func() {}
	if len(paths) == 0 {
		return "", Header{}, cleanup, fmt.Errorf("transfer: no paths")
	}
	if len(paths) == 1 {
		fi, e := os.Stat(paths[0])
		if e != nil {
			return "", Header{}, cleanup, e
		}
		if fi.Mode().IsRegular() {
			return paths[0], Header{Kind: "file", Name: filepath.Base(paths[0]), Mode: uint32(fi.Mode())}, cleanup, nil
		}
	}
	// archive
	tf, e := os.CreateTemp("", "p2p-tar-*")
	if e != nil {
		return "", Header{}, cleanup, e
	}
	name := "bundle.tar"
	if len(paths) == 1 {
		name = filepath.Base(filepath.Clean(paths[0])) + ".tar"
	}
	if e := writeTar(tf, paths); e != nil {
		tf.Close()
		os.Remove(tf.Name())
		return "", Header{}, cleanup, e
	}
	tf.Close()
	tmpName := tf.Name()
	return tmpName, Header{Kind: "archive", Name: name}, func() { os.Remove(tmpName) }, nil
}

// finalize places the verified temp file as the final file, or unpacks an
// archive into destDir. Mirrors the single-stream Receive end-game.
func finalize(tmpName, destDir string, hdr Header, overwrite OverwriteFn) (string, error) {
	if hdr.Kind == "archive" {
		f, e := os.Open(tmpName)
		if e != nil {
			os.Remove(tmpName)
			return "", e
		}
		e = extractTar(f, destDir)
		f.Close()
		os.Remove(tmpName)
		if e != nil {
			return "", e
		}
		return destDir, nil
	}
	final := filepath.Join(destDir, filepath.Base(hdr.Name))
	if _, statErr := os.Stat(final); statErr == nil && (overwrite == nil || !overwrite(filepath.Base(hdr.Name))) {
		final += ".part"
	}
	if hdr.Mode != 0 {
		_ = os.Chmod(tmpName, os.FileMode(hdr.Mode))
	}
	if e := os.Rename(tmpName, final); e != nil {
		os.Remove(tmpName)
		return "", e
	}
	return final, nil
}
