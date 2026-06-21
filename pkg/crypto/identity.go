package crypto

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/flynn/noise"
)

const (
	DefaultKeyPath        = "~/.p2p/id_ed25519"
	DefaultKnownPeersPath = "~/.p2p/known_peers"
)

// Identity holds an Ed25519 signing keypair and a persistent Noise X25519 static keypair.
// The Noise keypair is what gets exchanged and pinned during handshakes.
// Ed25519 is kept for future signing use (e.g. challenge/response extensions).
type Identity struct {
	PrivKey   ed25519.PrivateKey
	PubKey    ed25519.PublicKey
	NoiseKey  noise.DHKey // persistent X25519 static keypair for Noise handshakes
}

// LoadOrGenerateIdentity loads identity from path, generating and saving one if absent.
func LoadOrGenerateIdentity(path string) (*Identity, error) {
	expanded, err := expandPath(path)
	if err != nil {
		return nil, fmt.Errorf("expand path: %w", err)
	}

	data, err := os.ReadFile(expanded)
	if err == nil {
		return parseIdentityFile(data)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read identity: %w", err)
	}

	return generateAndSave(expanded)
}

func generateAndSave(path string) (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ed25519 key: %w", err)
	}

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	noiseKey, err := cs.GenerateKeypair(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate noise keypair: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	content := formatIdentityFile(priv, pub, noiseKey)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		return nil, fmt.Errorf("write identity: %w", err)
	}

	return &Identity{PrivKey: priv, PubKey: pub, NoiseKey: noiseKey}, nil
}

func formatIdentityFile(priv ed25519.PrivateKey, pub ed25519.PublicKey, noiseKey noise.DHKey) string {
	return fmt.Sprintf(
		"ed25519-private %s\ned25519-public %s\nnoise-private %s\nnoise-public %s\n",
		base64.StdEncoding.EncodeToString(priv),
		base64.StdEncoding.EncodeToString(pub),
		base64.StdEncoding.EncodeToString(noiseKey.Private),
		base64.StdEncoding.EncodeToString(noiseKey.Public),
	)
}

func parseIdentityFile(data []byte) (*Identity, error) {
	fields := make(map[string][]byte)

	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid identity line: %q", line)
		}
		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", parts[0], err)
		}
		fields[parts[0]] = decoded
	}

	privKey := ed25519.PrivateKey(fields["ed25519-private"])
	pubKey := ed25519.PublicKey(fields["ed25519-public"])
	noisePriv := fields["noise-private"]
	noisePub := fields["noise-public"]

	if privKey == nil || pubKey == nil || noisePriv == nil || noisePub == nil {
		return nil, errors.New("incomplete identity file")
	}

	return &Identity{
		PrivKey:  privKey,
		PubKey:   pubKey,
		NoiseKey: noise.DHKey{Private: noisePriv, Public: noisePub},
	}, nil
}


// KnownPeers manages pinned Ed25519 pubkeys (like SSH known_hosts).
//
// It is safe for concurrent use: parallel file transfers run several Noise
// handshakes at once, each of which pins (Add) the peer's static key, so the
// in-memory map and the on-disk file must be guarded against concurrent access.
type KnownPeers struct {
	path  string
	mu    sync.Mutex
	peers map[string]ed25519.PublicKey
}

// LoadKnownPeers loads known peers from path, creating empty store if absent.
func LoadKnownPeers(path string) (*KnownPeers, error) {
	expanded, err := expandPath(path)
	if err != nil {
		return nil, fmt.Errorf("expand path: %w", err)
	}

	kp := &KnownPeers{path: expanded, peers: make(map[string]ed25519.PublicKey)}

	data, err := os.ReadFile(expanded)
	if errors.Is(err, os.ErrNotExist) {
		return kp, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read known_peers: %w", err)
	}

	return kp, kp.parse(data)
}

func (k *KnownPeers) parse(data []byte) error {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 3 || parts[1] != "ed25519" {
			return fmt.Errorf("invalid known_peers line: %q", line)
		}
		decoded, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			return fmt.Errorf("decode pubkey for %s: %w", parts[0], err)
		}
		k.peers[parts[0]] = ed25519.PublicKey(decoded)
	}
	return nil
}

// Get returns the pinned pubkey for a peer, if known.
func (k *KnownPeers) Get(name string) (ed25519.PublicKey, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	pub, ok := k.peers[name]
	return pub, ok
}

// Add pins a new pubkey for name and persists the store.
func (k *KnownPeers) Add(name string, pub ed25519.PublicKey) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.peers[name] = pub
	return k.save()
}

// Save writes the known_peers file.
func (k *KnownPeers) Save() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.save()
}

// save serialises the in-memory map to disk. The caller must hold k.mu.
func (k *KnownPeers) save() error {
	if err := os.MkdirAll(filepath.Dir(k.path), 0700); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	var sb strings.Builder
	for name, pub := range k.peers {
		fmt.Fprintf(&sb, "%s ed25519 %s\n", name, base64.StdEncoding.EncodeToString(pub))
	}

	return os.WriteFile(k.path, []byte(sb.String()), 0600)
}

func expandPath(path string) (string, error) {
	if !strings.HasPrefix(path, "~") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, path[1:]), nil
}
