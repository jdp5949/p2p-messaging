package crypto

import (
	"bytes"
	"crypto/ed25519"
	"net"
	"os"
	"path/filepath"
	"testing"
)

// makeKnownPeers creates a temporary KnownPeers backed by a temp file.
func makeKnownPeers(t *testing.T) *KnownPeers {
	t.Helper()
	path := filepath.Join(t.TempDir(), "known_peers")
	kp, err := LoadKnownPeers(path)
	if err != nil {
		t.Fatalf("LoadKnownPeers: %v", err)
	}
	return kp
}

// makeIdentity creates a temporary Identity.
func makeIdentity(t *testing.T) *Identity {
	t.Helper()
	path := filepath.Join(t.TempDir(), "id_ed25519")
	id, err := LoadOrGenerateIdentity(path)
	if err != nil {
		t.Fatalf("LoadOrGenerateIdentity: %v", err)
	}
	return id
}

// pipeHandshake runs Handshake on both sides of a net.Pipe concurrently.
// If either side errors, both connections are closed to unblock the other.
func pipeHandshake(t *testing.T, initiatorCfg, responderCfg HandshakeConfig) (*Session, *Session, error) {
	t.Helper()
	a, b := net.Pipe()

	type result struct {
		sess *Session
		err  error
	}
	ch := make(chan result, 2)

	go func() {
		s, err := Handshake(a, initiatorCfg)
		if err != nil {
			a.Close()
		}
		ch <- result{s, err}
	}()
	go func() {
		s, err := Handshake(b, responderCfg)
		if err != nil {
			b.Close()
		}
		ch <- result{s, err}
	}()

	r0 := <-ch
	r1 := <-ch

	if r0.err != nil {
		if r1.sess != nil {
			r1.sess.Close()
		} else {
			b.Close()
		}
		return nil, nil, r0.err
	}
	if r1.err != nil {
		if r0.sess != nil {
			r0.sess.Close()
		} else {
			a.Close()
		}
		return nil, nil, r1.err
	}

	t.Cleanup(func() { a.Close(); b.Close() })
	return r0.sess, r1.sess, nil
}

// TestXXHandshakePinsRemotePub tests first-connect XX with PSK pins both sides.
func TestXXHandshakePinsRemotePub(t *testing.T) {
	idA, idB := makeIdentity(t), makeIdentity(t)
	kpA, kpB := makeKnownPeers(t), makeKnownPeers(t)
	psk := "apple-tiger-7291"

	sessA, sessB, err := pipeHandshake(t,
		HandshakeConfig{Identity: idA, KnownPeers: kpA, PeerName: "bob", PAKECode: psk, Initiator: true},
		HandshakeConfig{Identity: idB, KnownPeers: kpB, PeerName: "alice", PAKECode: psk, Initiator: false},
	)
	if err != nil {
		t.Fatalf("handshake failed: %v", err)
	}
	defer sessA.Close()
	defer sessB.Close()

	if len(sessA.RemotePubKey()) == 0 {
		t.Error("sessA: empty remote pub key")
	}
	if len(sessB.RemotePubKey()) == 0 {
		t.Error("sessB: empty remote pub key")
	}

	// Bob pinned alice, alice pinned bob.
	if _, ok := kpA.Get("bob"); !ok {
		t.Error("kpA should have pinned 'bob'")
	}
	if _, ok := kpB.Get("alice"); !ok {
		t.Error("kpB should have pinned 'alice'")
	}
}

// TestKKHandshakeKnownPeers tests second-connect KK with pinned keys.
func TestKKHandshakeKnownPeers(t *testing.T) {
	idA, idB := makeIdentity(t), makeIdentity(t)
	kpA, kpB := makeKnownPeers(t), makeKnownPeers(t)
	psk := "apple-tiger-7291"

	// First connect: XX
	s1, s2, err := pipeHandshake(t,
		HandshakeConfig{Identity: idA, KnownPeers: kpA, PeerName: "bob", PAKECode: psk, Initiator: true},
		HandshakeConfig{Identity: idB, KnownPeers: kpB, PeerName: "alice", PAKECode: psk, Initiator: false},
	)
	if err != nil {
		t.Fatalf("XX handshake failed: %v", err)
	}
	s1.Close()
	s2.Close()

	// Second connect: KK (no PSK).
	s3, s4, err := pipeHandshake(t,
		HandshakeConfig{Identity: idA, KnownPeers: kpA, PeerName: "bob", Initiator: true},
		HandshakeConfig{Identity: idB, KnownPeers: kpB, PeerName: "alice", Initiator: false},
	)
	if err != nil {
		t.Fatalf("KK handshake failed: %v", err)
	}
	defer s3.Close()
	defer s4.Close()
}

// TestWrongPSKFails verifies that mismatched PSK causes handshake failure.
func TestWrongPSKFails(t *testing.T) {
	idA, idB := makeIdentity(t), makeIdentity(t)

	_, _, err := pipeHandshake(t,
		HandshakeConfig{Identity: idA, KnownPeers: makeKnownPeers(t), PeerName: "bob", PAKECode: "correct-code", Initiator: true},
		HandshakeConfig{Identity: idB, KnownPeers: makeKnownPeers(t), PeerName: "alice", PAKECode: "wrong-code", Initiator: false},
	)
	if err == nil {
		t.Error("expected handshake to fail with wrong PSK, but it succeeded")
	}
}

// TestKKWrongPubKeyFails verifies MITM detection when pinned key doesn't match.
func TestKKWrongPubKeyFails(t *testing.T) {
	idA, idB, idEvil := makeIdentity(t), makeIdentity(t), makeIdentity(t)
	kpA, kpB := makeKnownPeers(t), makeKnownPeers(t)
	psk := "apple-tiger-7291"

	// First connect: XX — A and B pin each other.
	s1, s2, err := pipeHandshake(t,
		HandshakeConfig{Identity: idA, KnownPeers: kpA, PeerName: "bob", PAKECode: psk, Initiator: true},
		HandshakeConfig{Identity: idB, KnownPeers: kpB, PeerName: "alice", PAKECode: psk, Initiator: false},
	)
	if err != nil {
		t.Fatalf("setup XX failed: %v", err)
	}
	s1.Close()
	s2.Close()

	// A has pinned bob's real key. Substitute evil identity on bob's side.
	// KK should fail because evil's static key != what A pinned.
	kpEvilB := makeKnownPeers(t)
	_ = kpEvilB.Add("alice", ed25519.PublicKey(kpA.peers["bob"])) // evil knows alice

	_, _, err = pipeHandshake(t,
		HandshakeConfig{Identity: idA, KnownPeers: kpA, PeerName: "bob", Initiator: true},
		HandshakeConfig{Identity: idEvil, KnownPeers: kpEvilB, PeerName: "alice", Initiator: false},
	)
	if err == nil {
		t.Error("expected KK to fail when remote presents wrong key (MITM), but it succeeded")
	}
}

// TestEncryptedRoundTrip tests plaintext → Write → Read → matches.
func TestEncryptedRoundTrip(t *testing.T) {
	idA, idB := makeIdentity(t), makeIdentity(t)
	kpA, kpB := makeKnownPeers(t), makeKnownPeers(t)

	sessA, sessB, err := pipeHandshake(t,
		HandshakeConfig{Identity: idA, KnownPeers: kpA, PeerName: "bob", PAKECode: "test-psk", Initiator: true},
		HandshakeConfig{Identity: idB, KnownPeers: kpB, PeerName: "alice", PAKECode: "test-psk", Initiator: false},
	)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer sessA.Close()
	defer sessB.Close()

	msg := []byte("hello encrypted world")
	done := make(chan error, 1)

	go func() {
		buf := make([]byte, 1024)
		n, err := sessB.Read(buf)
		if err != nil {
			done <- err
			return
		}
		if !bytes.Equal(buf[:n], msg) {
			done <- nil
			t.Errorf("got %q, want %q", buf[:n], msg)
			return
		}
		done <- nil
	}()

	if _, err := sessA.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Read: %v", err)
	}
}

// TestLargeMessageRoundTrip tests 1 MB round-trip.
func TestLargeMessageRoundTrip(t *testing.T) {
	idA, idB := makeIdentity(t), makeIdentity(t)
	kpA, kpB := makeKnownPeers(t), makeKnownPeers(t)

	sessA, sessB, err := pipeHandshake(t,
		HandshakeConfig{Identity: idA, KnownPeers: kpA, PeerName: "bob", PAKECode: "big-msg-psk", Initiator: true},
		HandshakeConfig{Identity: idB, KnownPeers: kpB, PeerName: "alice", PAKECode: "big-msg-psk", Initiator: false},
	)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer sessA.Close()
	defer sessB.Close()

	big := make([]byte, 1<<20) // 1 MB
	for i := range big {
		big[i] = byte(i % 251)
	}

	recv := make(chan []byte, 1)
	go func() {
		buf := make([]byte, len(big))
		n, err := sessB.Read(buf)
		if err != nil {
			t.Errorf("Read: %v", err)
		}
		recv <- buf[:n]
	}()

	if _, err := sessA.Write(big); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := <-recv
	if !bytes.Equal(got, big) {
		t.Errorf("large message mismatch: got len %d want %d", len(got), len(big))
	}
}

// TestPSKDerivation verifies same code yields same PSK on both sides.
func TestPSKDerivation(t *testing.T) {
	code := "apple-tiger-7291"
	psk1 := derivePSK(code)
	psk2 := derivePSK(code)

	if !bytes.Equal(psk1, psk2) {
		t.Error("same code must produce same PSK")
	}
	if len(psk1) != 32 {
		t.Errorf("PSK must be 32 bytes, got %d", len(psk1))
	}

	other := derivePSK("different-code")
	if bytes.Equal(psk1, other) {
		t.Error("different codes must produce different PSKs")
	}
}

// TestIdentityLoadOrGenerate tests file-backed identity persistence.
func TestIdentityLoadOrGenerate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "id_ed25519")

	id1, err := LoadOrGenerateIdentity(path)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	id2, err := LoadOrGenerateIdentity(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if !bytes.Equal(id1.PubKey, id2.PubKey) {
		t.Error("loaded identity differs from generated one")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("identity file perm: got %o, want 0600", perm)
	}
}

// TestKnownPeersPersistence tests save/load round-trip.
func TestKnownPeersPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "known_peers")

	kp1, _ := LoadKnownPeers(path)
	pub := ed25519.PublicKey(make([]byte, 32))
	pub[0] = 0xAB
	_ = kp1.Add("alice", pub)

	kp2, err := LoadKnownPeers(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, ok := kp2.Get("alice")
	if !ok {
		t.Fatal("alice not found after reload")
	}
	if !bytes.Equal(got, pub) {
		t.Errorf("pubkey mismatch after reload")
	}
}
