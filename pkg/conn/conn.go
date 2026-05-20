package conn

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/crypto"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

const MaxPayloadSize = 16 * 1024 * 1024 // 16 MB

// Config holds dial and timeout settings for Conn.
type Config struct {
	DialFunc     func() (net.Conn, error)
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PingInterval time.Duration

	// HandshakeCfg: if set, wraps the raw net.Conn in an encrypted crypto.Session.
	// nil = plaintext (backward compatible).
	HandshakeCfg *crypto.HandshakeConfig
}

// Conn wraps a net.Conn with framed read/write and reconnect support.
type Conn struct {
	cfg       Config
	mu        sync.Mutex
	raw       net.Conn
	transport io.ReadWriteCloser // raw conn or encrypted session
	stopPing  chan struct{}
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
	}
	if cfg.PingInterval > 0 {
		go c.pingLoop()
	}
	return c, nil
}

// buildTransport wraps raw with a crypto.Session if cfg is non-nil.
func buildTransport(raw net.Conn, hsCfg *crypto.HandshakeConfig) (io.ReadWriteCloser, error) {
	if hsCfg == nil {
		return raw, nil
	}
	return crypto.Handshake(raw, *hsCfg)
}

// WriteMsg encodes and writes a framed message. Thread-safe.
func (c *Conn) WriteMsg(msg protocol.Message) error {
	msg.Header.PayloadLen = uint32(len(msg.Payload))
	hdr := protocol.EncodeHeader(msg.Header)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cfg.WriteTimeout > 0 {
		_ = c.raw.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	}

	if _, err := c.transport.Write(hdr[:]); err != nil {
		return err
	}
	if len(msg.Payload) > 0 {
		if _, err := c.transport.Write(msg.Payload); err != nil {
			return err
		}
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
	c.mu.Lock()
	oldTransport := c.transport
	c.raw = raw
	c.transport = transport
	c.mu.Unlock()
	_ = oldTransport.Close()
	return nil
}

// Close shuts down the connection and stops background goroutines.
func (c *Conn) Close() error {
	select {
	case <-c.stopPing:
	default:
		close(c.stopPing)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
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
