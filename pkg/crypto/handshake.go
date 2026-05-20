package crypto

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/flynn/noise"
)

// cipherSuite is the shared Noise cipher suite: X25519 + AES-256-GCM + SHA-256.
var cipherSuite = noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)

// HandshakeConfig configures a Noise handshake.
type HandshakeConfig struct {
	Identity   *Identity
	KnownPeers *KnownPeers
	PeerName   string // required for KK (repeat connect)
	PAKECode   string // PSK for XX (first connect); empty if peer already known
	Initiator  bool
}

// Session wraps a net.Conn with Noise-encrypted framed I/O.
type Session struct {
	conn        net.Conn
	send        *noise.CipherState // initiator→responder
	recv        *noise.CipherState // responder→initiator
	remotePub   ed25519.PublicKey
	mu          sync.Mutex
}

// Handshake performs XX (first connect, PSK) or KK (repeat connect) over conn.
// On first connect it pins the remote static key in KnownPeers.
func Handshake(conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	if cfg.PAKECode != "" {
		return handshakeXX(conn, cfg)
	}
	return handshakeKK(conn, cfg)
}

// derivePSK derives a 32-byte PSK from a human-readable PAKE code.
func derivePSK(code string) []byte {
	h := sha256.Sum256([]byte("p2p-psk:" + code))
	return h[:]
}

func handshakeXX(conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	psk := derivePSK(cfg.PAKECode)

	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           cipherSuite,
		Pattern:               noise.HandshakeXX,
		Initiator:             cfg.Initiator,
		StaticKeypair:         cfg.Identity.NoiseKey,
		PresharedKey:          psk,
		PresharedKeyPlacement: 0,
	})
	if err != nil {
		return nil, fmt.Errorf("new handshake state: %w", err)
	}

	cs0, cs1, err := runHandshake(conn, hs, cfg.Initiator)
	if err != nil {
		return nil, err
	}

	remotePub := ed25519.PublicKey(hs.PeerStatic())

	if cfg.PeerName != "" && cfg.KnownPeers != nil {
		if err := cfg.KnownPeers.Add(cfg.PeerName, remotePub); err != nil {
			return nil, fmt.Errorf("pin peer key: %w", err)
		}
	}

	return newSession(conn, cs0, cs1, cfg.Initiator, remotePub), nil
}

func handshakeKK(conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	if cfg.KnownPeers == nil {
		return nil, errors.New("handshake: KnownPeers required for KK pattern")
	}
	if cfg.PeerName == "" {
		return nil, errors.New("handshake: PeerName required for KK pattern")
	}

	peerPub, ok := cfg.KnownPeers.Get(cfg.PeerName)
	if !ok {
		return nil, fmt.Errorf("handshake: peer %q not in known_peers, use PSK for first connect", cfg.PeerName)
	}

	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   cipherSuite,
		Pattern:       noise.HandshakeKK,
		Initiator:     cfg.Initiator,
		StaticKeypair: cfg.Identity.NoiseKey,
		PeerStatic:    []byte(peerPub),
	})
	if err != nil {
		return nil, fmt.Errorf("new handshake state: %w", err)
	}

	cs0, cs1, err := runHandshake(conn, hs, cfg.Initiator)
	if err != nil {
		return nil, fmt.Errorf("KK handshake: %w", err)
	}

	remotePub := ed25519.PublicKey(hs.PeerStatic())
	return newSession(conn, cs0, cs1, cfg.Initiator, remotePub), nil
}

// runHandshake drives the Noise handshake message exchange over the wire.
// Returns (send, recv) CipherStates from the initiator's perspective.
func runHandshake(conn net.Conn, hs *noise.HandshakeState, initiator bool) (*noise.CipherState, *noise.CipherState, error) {
	var cs0, cs1 *noise.CipherState

	for cs0 == nil {
		if hs.MessageIndex()%2 == 0 && initiator || hs.MessageIndex()%2 == 1 && !initiator {
			// our turn to write
			msg, c0, c1, err := hs.WriteMessage(nil, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("write handshake msg: %w", err)
			}
			if err := writeFrame(conn, msg); err != nil {
				return nil, nil, err
			}
			cs0, cs1 = c0, c1
		} else {
			// our turn to read
			frame, err := readFrame(conn)
			if err != nil {
				return nil, nil, err
			}
			_, c0, c1, err := hs.ReadMessage(nil, frame)
			if err != nil {
				return nil, nil, fmt.Errorf("read handshake msg: %w", err)
			}
			cs0, cs1 = c0, c1
		}
	}

	return cs0, cs1, nil
}

// newSession builds a Session with correct send/recv assignment.
// Noise Split() returns (initiator-send / responder-recv, responder-send / initiator-recv).
func newSession(conn net.Conn, cs0, cs1 *noise.CipherState, initiator bool, remotePub ed25519.PublicKey) *Session {
	if initiator {
		return &Session{conn: conn, send: cs0, recv: cs1, remotePub: remotePub}
	}
	return &Session{conn: conn, send: cs1, recv: cs0, remotePub: remotePub}
}

// Write encrypts and sends a frame: [4-byte big-endian length][ciphertext].
func (s *Session) Write(plaintext []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ct, err := s.send.Encrypt(nil, nil, plaintext)
	if err != nil {
		return 0, fmt.Errorf("encrypt: %w", err)
	}
	if err := writeFrame(s.conn, ct); err != nil {
		return 0, err
	}
	return len(plaintext), nil
}

// Read receives and decrypts one frame into buf.
func (s *Session) Read(buf []byte) (int, error) {
	frame, err := readFrame(s.conn)
	if err != nil {
		return 0, err
	}
	pt, err := s.recv.Decrypt(nil, nil, frame)
	if err != nil {
		return 0, fmt.Errorf("decrypt: %w", err)
	}
	n := copy(buf, pt)
	if n < len(pt) {
		return n, io.ErrShortBuffer
	}
	return n, nil
}

// RemotePubKey returns the authenticated remote static public key.
func (s *Session) RemotePubKey() ed25519.PublicKey {
	return s.remotePub
}

// Close closes the underlying connection.
func (s *Session) Close() error {
	return s.conn.Close()
}

// writeFrame writes a length-prefixed frame (4-byte big-endian length + data).
func writeFrame(w io.Writer, data []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write frame body: %w", err)
	}
	return nil
}

// readFrame reads a length-prefixed frame.
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("read frame header: %w", err)
	}
	size := binary.BigEndian.Uint32(hdr[:])
	if size > 8*1024*1024 { // 8 MB guard
		return nil, fmt.Errorf("frame too large: %d bytes", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read frame body: %w", err)
	}
	return buf, nil
}
