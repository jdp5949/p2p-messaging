package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// ackTimeout bounds how long Send waits for the receiver's DONE acknowledgement
// after the trailer, so an old/incompatible peer cannot hang the sender forever.
// Var (not const) so tests can shorten it.
var ackTimeout = 30 * time.Second

// Send transmits paths via send. A single regular file is streamed raw; a
// directory or multiple paths are streamed as a tar archive. After the trailer,
// Send blocks until the receiver returns a DONE message on in, guaranteeing the
// data was saved+verified before the caller exits.
func Send(send SendFunc, in <-chan Msg, paths []string, progress ProgressFn) error {
	if len(paths) == 0 {
		return fmt.Errorf("transfer: no paths")
	}

	var (
		hdr     Header
		source  io.Reader
		size    int64
		closeFn = func() error { return nil }
	)

	single := false
	if len(paths) == 1 {
		fi, err := os.Stat(paths[0])
		if err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			single = true
			f, err := os.Open(paths[0])
			if err != nil {
				return err
			}
			source, closeFn, size = f, f.Close, fi.Size()
			hdr = Header{Kind: "file", Name: filepath.Base(paths[0]), Size: size, Mode: uint32(fi.Mode())}
		}
	}

	if !single {
		pr, pw := io.Pipe()
		go func() { pw.CloseWithError(writeTar(pw, paths)) }()
		source, closeFn = pr, pr.Close
		name := "bundle.tar"
		if len(paths) == 1 {
			name = filepath.Base(filepath.Clean(paths[0])) + ".tar"
		}
		hdr = Header{Kind: "archive", Name: name}
	}
	defer closeFn()

	if err := send(protocol.ContentJSON, marshalHeader(hdr)); err != nil {
		return err
	}

	h := sha256.New()
	tee := io.TeeReader(source, h)
	buf := make([]byte, ChunkSize)
	var offset int64
	for {
		n, rerr := tee.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if err := send(protocol.ContentBinary, encodeChunk(offset, chunk)); err != nil {
				return err
			}
			offset += int64(n)
			if progress != nil {
				progress(offset, size)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}

	tr := Trailer{SHA256: hex.EncodeToString(h.Sum(nil)), Total: offset}
	if err := send(protocol.ContentJSON, marshalTrailer(tr)); err != nil {
		return err
	}

	// Wait for the receiver's DONE ack, but don't hang forever if the peer
	// never acknowledges (e.g. it is running an old/incompatible p2p that does
	// not understand the transfer protocol).
	timer := time.NewTimer(ackTimeout)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-in:
			if !ok {
				return fmt.Errorf("transfer: peer closed before acknowledging")
			}
			if classify(m) == "done" {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("transfer: peer did not acknowledge within %s — is the other side running the latest p2p?", ackTimeout)
		}
	}
}
