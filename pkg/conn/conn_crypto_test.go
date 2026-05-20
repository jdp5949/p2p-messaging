package conn

import (
	"bytes"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/crypto"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

// makeTestIdentity creates a temp-file-backed identity for tests.
func makeTestIdentity(t *testing.T) *crypto.Identity {
	t.Helper()
	path := filepath.Join(t.TempDir(), "id_ed25519")
	id, err := crypto.LoadOrGenerateIdentity(path)
	if err != nil {
		t.Fatalf("LoadOrGenerateIdentity: %v", err)
	}
	return id
}

// makeTestKnownPeers creates a temp-file-backed KnownPeers for tests.
func makeTestKnownPeers(t *testing.T) *crypto.KnownPeers {
	t.Helper()
	path := filepath.Join(t.TempDir(), "known_peers")
	kp, err := crypto.LoadKnownPeers(path)
	if err != nil {
		t.Fatalf("LoadKnownPeers: %v", err)
	}
	return kp
}

// cryptoPipePair creates two Conn instances connected via net.Pipe with
// encrypted sessions (XX handshake using the given PSK).
func cryptoPipePair(t *testing.T, psk string) (*Conn, *Conn) {
	t.Helper()

	idA := makeTestIdentity(t)
	idB := makeTestIdentity(t)
	kpA := makeTestKnownPeers(t)
	kpB := makeTestKnownPeers(t)

	rawA, rawB := net.Pipe()

	type result struct {
		c   *Conn
		err error
	}

	chA := make(chan result, 1)
	chB := make(chan result, 1)

	go func() {
		c, err := New(Config{
			DialFunc: func() (net.Conn, error) { return rawA, nil },
			HandshakeCfg: &crypto.HandshakeConfig{
				Identity:   idA,
				KnownPeers: kpA,
				PeerName:   "bob",
				PAKECode:   psk,
				Initiator:  true,
			},
		})
		chA <- result{c, err}
	}()

	go func() {
		c, err := New(Config{
			DialFunc: func() (net.Conn, error) { return rawB, nil },
			HandshakeCfg: &crypto.HandshakeConfig{
				Identity:   idB,
				KnownPeers: kpB,
				PeerName:   "alice",
				PAKECode:   psk,
				Initiator:  false,
			},
		})
		chB <- result{c, err}
	}()

	rA := <-chA
	rB := <-chB

	if rA.err != nil {
		t.Fatalf("initiator conn.New: %v", rA.err)
	}
	if rB.err != nil {
		t.Fatalf("responder conn.New: %v", rB.err)
	}

	t.Cleanup(func() {
		rA.c.Close()
		rB.c.Close()
	})

	return rA.c, rB.c
}

// TestCryptoConnRoundTrip verifies WriteMsg/ReadMsg works over an encrypted session.
func TestCryptoConnRoundTrip(t *testing.T) {
	cA, cB := cryptoPipePair(t, "test-psk-roundtrip")

	want := protocol.Message{
		Header:  protocol.Header{MsgID: 7, MsgType: protocol.MsgData},
		Payload: []byte("encrypted hello"),
	}

	errCh := make(chan error, 1)
	go func() { errCh <- cA.WriteMsg(want) }()

	got, err := cB.ReadMsg()
	if err != nil {
		t.Fatalf("ReadMsg: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	if got.Header.MsgID != want.Header.MsgID {
		t.Errorf("MsgID: got %d want %d", got.Header.MsgID, want.Header.MsgID)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("Payload mismatch: got %q want %q", got.Payload, want.Payload)
	}
}

// TestCryptoPinsPubKeys verifies that both sides pin each other's public keys
// after an XX (PSK) handshake via the KnownPeers store.
func TestCryptoPinsPubKeys(t *testing.T) {
	idA := makeTestIdentity(t)
	idB := makeTestIdentity(t)
	kpA := makeTestKnownPeers(t)
	kpB := makeTestKnownPeers(t)
	psk := "test-pin-psk"

	rawA, rawB := net.Pipe()

	type result struct {
		c   *Conn
		err error
	}
	chA := make(chan result, 1)
	chB := make(chan result, 1)

	go func() {
		c, err := New(Config{
			DialFunc: func() (net.Conn, error) { return rawA, nil },
			HandshakeCfg: &crypto.HandshakeConfig{
				Identity:   idA,
				KnownPeers: kpA,
				PeerName:   "bob",
				PAKECode:   psk,
				Initiator:  true,
			},
		})
		chA <- result{c, err}
	}()
	go func() {
		c, err := New(Config{
			DialFunc: func() (net.Conn, error) { return rawB, nil },
			HandshakeCfg: &crypto.HandshakeConfig{
				Identity:   idB,
				KnownPeers: kpB,
				PeerName:   "alice",
				PAKECode:   psk,
				Initiator:  false,
			},
		})
		chB <- result{c, err}
	}()

	rA := <-chA
	rB := <-chB
	if rA.err != nil {
		t.Fatalf("A: %v", rA.err)
	}
	if rB.err != nil {
		t.Fatalf("B: %v", rB.err)
	}
	defer rA.c.Close()
	defer rB.c.Close()

	// A pinned bob, B pinned alice.
	if _, ok := kpA.Get("bob"); !ok {
		t.Error("kpA should have pinned 'bob' after XX handshake")
	}
	if _, ok := kpB.Get("alice"); !ok {
		t.Error("kpB should have pinned 'alice' after XX handshake")
	}
}

// TestCryptoKKRehandshake verifies that after XX pinning, a KK re-handshake
// (no PSK) succeeds and messages flow correctly.
func TestCryptoKKRehandshake(t *testing.T) {
	idA := makeTestIdentity(t)
	idB := makeTestIdentity(t)
	kpA := makeTestKnownPeers(t)
	kpB := makeTestKnownPeers(t)
	psk := "test-kk-psk"

	// First connect: XX to pin keys.
	raw1A, raw1B := net.Pipe()

	type result struct {
		c   *Conn
		err error
	}
	ch1A := make(chan result, 1)
	ch1B := make(chan result, 1)

	go func() {
		c, err := New(Config{
			DialFunc: func() (net.Conn, error) { return raw1A, nil },
			HandshakeCfg: &crypto.HandshakeConfig{
				Identity:   idA,
				KnownPeers: kpA,
				PeerName:   "bob",
				PAKECode:   psk,
				Initiator:  true,
			},
		})
		ch1A <- result{c, err}
	}()
	go func() {
		c, err := New(Config{
			DialFunc: func() (net.Conn, error) { return raw1B, nil },
			HandshakeCfg: &crypto.HandshakeConfig{
				Identity:   idB,
				KnownPeers: kpB,
				PeerName:   "alice",
				PAKECode:   psk,
				Initiator:  false,
			},
		})
		ch1B <- result{c, err}
	}()

	r1A := <-ch1A
	r1B := <-ch1B
	if r1A.err != nil || r1B.err != nil {
		t.Fatalf("XX handshake failed: A=%v B=%v", r1A.err, r1B.err)
	}
	r1A.c.Close()
	r1B.c.Close()

	// Second connect: KK (no PSK) — keys already pinned.
	raw2A, raw2B := net.Pipe()
	ch2A := make(chan result, 1)
	ch2B := make(chan result, 1)

	go func() {
		c, err := New(Config{
			DialFunc: func() (net.Conn, error) { return raw2A, nil },
			HandshakeCfg: &crypto.HandshakeConfig{
				Identity:   idA,
				KnownPeers: kpA,
				PeerName:   "bob",
				// No PAKECode → KK pattern
				Initiator: true,
			},
		})
		ch2A <- result{c, err}
	}()
	go func() {
		c, err := New(Config{
			DialFunc: func() (net.Conn, error) { return raw2B, nil },
			HandshakeCfg: &crypto.HandshakeConfig{
				Identity:   idB,
				KnownPeers: kpB,
				PeerName:   "alice",
				Initiator:  false,
			},
		})
		ch2B <- result{c, err}
	}()

	r2A := <-ch2A
	r2B := <-ch2B
	if r2A.err != nil {
		t.Fatalf("KK handshake initiator failed: %v", r2A.err)
	}
	if r2B.err != nil {
		t.Fatalf("KK handshake responder failed: %v", r2B.err)
	}
	defer r2A.c.Close()
	defer r2B.c.Close()

	// Verify messages flow over KK session.
	want := []byte("kk-reconnect-ok")
	errCh := make(chan error, 1)
	go func() {
		errCh <- r2A.c.WriteMsg(protocol.Message{
			Header:  protocol.Header{MsgID: 1, MsgType: protocol.MsgData},
			Payload: want,
		})
	}()

	got, err := r2B.c.ReadMsg()
	if err != nil {
		t.Fatalf("ReadMsg KK: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteMsg KK: %v", err)
	}
	if !bytes.Equal(got.Payload, want) {
		t.Errorf("KK payload mismatch: got %q want %q", got.Payload, want)
	}
}

// TestPlaintextBackwardCompat verifies nil HandshakeCfg keeps existing behavior.
func TestPlaintextBackwardCompat(t *testing.T) {
	// nil HandshakeCfg — must behave exactly as before.
	raw1, raw2 := net.Pipe()
	cA, err := New(Config{DialFunc: func() (net.Conn, error) { return raw1, nil }})
	if err != nil {
		t.Fatal(err)
	}
	cB, err := New(Config{DialFunc: func() (net.Conn, error) { return raw2, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()
	defer cB.Close()

	want := protocol.Message{
		Header:  protocol.Header{MsgID: 99, MsgType: protocol.MsgData},
		Payload: []byte("plaintext"),
	}
	errCh := make(chan error, 1)
	go func() { errCh <- cA.WriteMsg(want) }()

	got, err := cB.ReadMsg()
	if err != nil {
		t.Fatalf("ReadMsg: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Error("plaintext payload mismatch")
	}

	_ = time.Second // keep import used
}
