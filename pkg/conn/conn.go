package conn

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/crypto"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

const MaxPayloadSize = 16 * 1024 * 1024 // 16 MB

const defaultBatchBufSize = 64 * 1024 // 64 KB internal bufio size when BatchSize==0

// Config holds dial and timeout settings for Conn.
type Config struct {
	DialFunc     func() (net.Conn, error)
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PingInterval time.Duration

	// HandshakeCfg: if set, wraps the raw net.Conn in an encrypted crypto.Session.
	// nil = plaintext (backward compatible).
	HandshakeCfg *crypto.HandshakeConfig

	// BatchSize: if > 0, WriteMsg accumulates frames in a buffer and flushes
	// when the buffer reaches BatchSize bytes or when BatchTimeout elapses.
	// 0 = no batching (current behavior, immediate write per call).
	BatchSize int

	// BatchTimeout: flush interval for the background batcher goroutine.
	// Only used when BatchSize > 0. Defaults to 5ms when BatchSize > 0.
	BatchTimeout time.Duration
}

// Conn wraps a net.Conn with framed read/write and reconnect support.
//
// mu protects raw/transport field access (connection replacement).
// writeMu serializes concurrent writers without blocking readers.
// When batching is enabled, bw is a bufio.Writer wrapping transport;
// all WriteMsg calls go through bw and writeMu guards it.
type Conn struct {
	cfg       Config
	mu        sync.Mutex // guards raw, transport, bw fields
	writeMu   sync.Mutex // serializes WriteMsg / Flush calls during I/O
	raw       net.Conn
	transport io.ReadWriteCloser // raw conn or encrypted session
	bw        *bufio.Writer      // non-nil only when batching enabled
	stopPing  chan struct{}
	stopBatch chan struct{} // closed to stop background flush goroutine
}

// New dials immediately and returns a ready Conn.
func New(cfg Config) (*Conn, error) {
	raw, err := cfg.DialFunc()
	if err != nil {
		return nil, err
	}
	transport, err := buildTransport(raw, cfg.HandshakeCfg)
	if err != nil {
		raw.Close()
		return nil, err
	}
	c := &Conn{
		cfg:       cfg,
		raw:       raw,
		transport: transport,
		stopPing:  make(chan struct{}),
		stopBatch: make(chan struct{}),
	}
	if cfg.BatchSize > 0 {
		c.bw = bufio.NewWriterSize(transport, cfg.BatchSize)
		timeout := cfg.BatchTimeout
		if timeout <= 0 {
			timeout = 5 * time.Millisecond
		}
		go c.batchFlushLoop(timeout)
	}
	if cfg.PingInterval > 0 {
		go c.pingLoop()
	}
	return c, nil
}

// batchFlushLoop periodically flushes the bufio.Writer.
func (c *Conn) batchFlushLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopBatch:
			return
		case <-ticker.C:
			c.writeMu.Lock()
			if c.bw != nil {
				_ = c.bw.Flush()
			}
			c.writeMu.Unlock()
		}
	}
}

// buildTransport sets TCP_NODELAY and wraps raw with a crypto.Session if cfg is non-nil.
func buildTransport(raw net.Conn, hsCfg *crypto.HandshakeConfig) (io.ReadWriteCloser, error) {
	if tc, ok := raw.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if hsCfg == nil {
		return raw, nil
	}
	return crypto.Handshake(raw, *hsCfg)
}

// WriteMsg encodes and writes a framed message. Thread-safe.
//
// Batching mode (BatchSize > 0): writes header+payload into an internal
// bufio.Writer. The background flush goroutine drains it every BatchTimeout.
// If buffered bytes >= BatchSize after write, flushes immediately.
//
// Non-batching mode:
//   - TCP plaintext: single writev syscall via net.Buffers (1 syscall, no Nagle).
//   - Other transports: two-write path.
func (c *Conn) WriteMsg(msg protocol.Message) error {
	msg.Header.PayloadLen = uint32(len(msg.Payload))
	hdr := protocol.EncodeHeader(msg.Header)

	// Snapshot transport reference under mu (non-blocking for reads).
	c.mu.Lock()
	raw := c.raw
	transport := c.transport
	bw := c.bw
	isTCP := c.cfg.HandshakeCfg == nil
	c.mu.Unlock()

	// Serialize writes under writeMu (I/O can now run without blocking ReadMsg).
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.cfg.WriteTimeout > 0 {
		_ = raw.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	}

	// Batching path: write into bufio.Writer, flush when buffer is full.
	if bw != nil {
		if _, err := bw.Write(hdr[:]); err != nil {
			return err
		}
		if len(msg.Payload) > 0 {
			if _, err := bw.Write(msg.Payload); err != nil {
				return err
			}
		}
		if bw.Buffered() >= c.cfg.BatchSize {
			return bw.Flush()
		}
		return nil
	}

	// TCP plaintext fast path: net.Buffers gives OS-level writev (1 syscall).
	if _, ok := raw.(*net.TCPConn); ok && isTCP {
		var bufs net.Buffers
		if len(msg.Payload) > 0 {
			bufs = net.Buffers{hdr[:], msg.Payload}
		} else {
			bufs = net.Buffers{hdr[:]}
		}
		_, err := bufs.WriteTo(transport.(net.Conn))
		return err
	}

	// All other paths (encrypted session, net.Pipe, tests): two-write path.
	// For encrypted Session: each Write is one frame; header and payload must be
	// separate writes so ReadMsg can ReadFull each frame independently.
	if _, err := transport.Write(hdr[:]); err != nil {
		return err
	}
	if len(msg.Payload) > 0 {
		if _, err := transport.Write(msg.Payload); err != nil {
			return err
		}
	}
	return nil
}

// Flush forces any buffered writes to the underlying transport.
// No-op when BatchSize == 0.
func (c *Conn) Flush() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.bw != nil {
		return c.bw.Flush()
	}
	return nil
}

// ReadMsg reads one framed message. Single-goroutine reader only.
func (c *Conn) ReadMsg() (protocol.Message, error) {
	c.mu.Lock()
	if c.cfg.ReadTimeout > 0 {
		_ = c.raw.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
	}
	transport := c.transport
	c.mu.Unlock()

	var hdrBuf [protocol.HeaderSize]byte
	if _, err := io.ReadFull(transport, hdrBuf[:]); err != nil {
		return protocol.Message{}, err
	}

	hdr := protocol.DecodeHeader(hdrBuf)
	if hdr.PayloadLen > MaxPayloadSize {
		return protocol.Message{}, fmt.Errorf("conn: payload size %d exceeds limit %d", hdr.PayloadLen, MaxPayloadSize)
	}
	msg := protocol.Message{Header: hdr}

	if hdr.PayloadLen > 0 {
		msg.Payload = make([]byte, hdr.PayloadLen)
		if _, err := io.ReadFull(transport, msg.Payload); err != nil {
			return protocol.Message{}, err
		}
	}
	return msg, nil
}

// Reconnect dials again and replaces the internal connection.
// Resets the batching bufio.Writer to point at the new transport.
func (c *Conn) Reconnect() error {
	raw, err := c.cfg.DialFunc()
	if err != nil {
		return err
	}
	transport, err := buildTransport(raw, c.cfg.HandshakeCfg)
	if err != nil {
		raw.Close()
		return err
	}
	c.writeMu.Lock()
	c.mu.Lock()
	oldTransport := c.transport
	c.raw = raw
	c.transport = transport
	if c.bw != nil {
		c.bw.Reset(transport)
	}
	c.mu.Unlock()
	c.writeMu.Unlock()
	_ = oldTransport.Close()
	return nil
}

// Close shuts down the connection and stops background goroutines.
// Flushes any buffered data before closing.
func (c *Conn) Close() error {
	select {
	case <-c.stopPing:
	default:
		close(c.stopPing)
	}
	select {
	case <-c.stopBatch:
	default:
		close(c.stopBatch)
	}

	// Set a short write deadline so the flush can't block indefinitely,
	// then flush any buffered data, then close the raw conn.
	c.mu.Lock()
	raw := c.raw
	c.mu.Unlock()
	if c.bw != nil {
		_ = raw.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	}
	_ = c.Flush()
	_ = raw.SetWriteDeadline(time.Time{}) // clear deadline

	_ = raw.Close()

	c.mu.Lock()
	defer c.mu.Unlock()
	// For encrypted sessions, transport.Close() may differ from raw.Close().
	return c.transport.Close()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.raw.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.raw.RemoteAddr()
}

func (c *Conn) pingLoop() {
	ticker := time.NewTicker(c.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopPing:
			return
		case <-ticker.C:
			ping := protocol.Message{Header: protocol.Header{MsgType: protocol.MsgPing}}
			if err := c.WriteMsg(ping); err != nil {
				return
			}
		}
	}
}
