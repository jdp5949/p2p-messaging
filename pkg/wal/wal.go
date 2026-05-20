// Package wal provides an append-only write-ahead log for unacked message persistence.
// It gives at-least-once delivery semantics across process restarts.
package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	OpSend uint8 = 0x01
	OpAck  uint8 = 0x02

	headerSize = 1 + 4 + 8 // op(1) + length(4) + msgID(8)
)

// Entry is one logical record read from the WAL.
type Entry struct {
	MsgID   uint64
	Payload []byte
}

// WAL is a write-ahead log of unacked messages.
type WAL struct {
	path   string
	file   *os.File
	mu     sync.Mutex
	fsync  bool
}

// Open creates or opens a WAL file at path.
// fsync controls whether file.Sync() is called after every write.
func Open(path string, fsync bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &WAL{path: path, file: f, fsync: fsync}, nil
}

// Append writes an OpSend record. Thread-safe.
func (w *WAL) Append(msgID uint64, payload []byte) error {
	return w.writeRecord(OpSend, msgID, payload)
}

// Ack writes an OpAck record. Thread-safe.
func (w *WAL) Ack(msgID uint64) error {
	return w.writeRecord(OpAck, msgID, nil)
}

// writeRecord serialises and appends one record to the file.
func (w *WAL) writeRecord(op uint8, msgID uint64, payload []byte) error {
	payLen := uint32(len(payload))
	buf := make([]byte, headerSize+int(payLen))
	buf[0] = op
	binary.BigEndian.PutUint32(buf[1:5], payLen)
	binary.BigEndian.PutUint64(buf[5:13], msgID)
	copy(buf[headerSize:], payload)

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("wal: write record: %w", err)
	}
	if w.fsync {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal: fsync: %w", err)
		}
	}
	return nil
}

// Replay reads the entire WAL and returns all unacked entries.
// Called at startup before resuming normal operation.
func (w *WAL) Replay() ([]Entry, error) {
	f, err := os.Open(w.path)
	if err != nil {
		return nil, fmt.Errorf("wal: replay open: %w", err)
	}
	defer f.Close()

	pending := make(map[uint64][]byte) // msgID -> payload
	order := []uint64{}                // insertion order for stable output

	hdr := make([]byte, headerSize)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("wal: replay read header: %w", err)
		}
		op := hdr[0]
		payLen := binary.BigEndian.Uint32(hdr[1:5])
		msgID := binary.BigEndian.Uint64(hdr[5:13])

		var payload []byte
		if payLen > 0 {
			payload = make([]byte, payLen)
			if _, err := io.ReadFull(f, payload); err != nil {
				// Treat partial trailing record as lost (crash mid-write).
				break
			}
		}

		switch op {
		case OpSend:
			if _, seen := pending[msgID]; !seen {
				order = append(order, msgID)
			}
			pending[msgID] = payload
		case OpAck:
			delete(pending, msgID)
		}
	}

	entries := make([]Entry, 0, len(pending))
	for _, id := range order {
		if payload, ok := pending[id]; ok {
			entries = append(entries, Entry{MsgID: id, Payload: payload})
		}
	}
	return entries, nil
}

// Compact rewrites the file keeping only unacked entries.
// Uses temp file + atomic rename for crash safety.
func (w *WAL) Compact() error {
	entries, err := w.Replay()
	if err != nil {
		return fmt.Errorf("wal: compact replay: %w", err)
	}

	tmp := w.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: compact create tmp: %w", err)
	}

	for _, e := range entries {
		payLen := uint32(len(e.Payload))
		buf := make([]byte, headerSize+int(payLen))
		buf[0] = OpSend
		binary.BigEndian.PutUint32(buf[1:5], payLen)
		binary.BigEndian.PutUint64(buf[5:13], e.MsgID)
		copy(buf[headerSize:], e.Payload)
		if _, werr := tf.Write(buf); werr != nil {
			tf.Close()
			os.Remove(tmp)
			return fmt.Errorf("wal: compact write: %w", werr)
		}
	}

	if serr := tf.Sync(); serr != nil {
		tf.Close()
		os.Remove(tmp)
		return fmt.Errorf("wal: compact sync: %w", serr)
	}
	tf.Close()

	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current write handle, rename, reopen.
	w.file.Close()
	if rerr := os.Rename(tmp, w.path); rerr != nil {
		// Best-effort reopen original.
		w.file, _ = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		return fmt.Errorf("wal: compact rename: %w", rerr)
	}

	w.file, err = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: compact reopen: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
