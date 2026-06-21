package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestKnownPeersConcurrentAdd exercises concurrent pinning, which happens when
// several parallel file-transfer streams each complete a Noise XX handshake and
// pin the peer's static key at the same time. Without synchronisation this
// panics with "concurrent map iteration and map write" (run with -race to also
// catch the data race). The store must remain consistent afterwards.
func TestKnownPeersConcurrentAdd(t *testing.T) {
	kp, err := LoadKnownPeers(filepath.Join(t.TempDir(), "known_peers"))
	if err != nil {
		t.Fatalf("LoadKnownPeers: %v", err)
	}

	const n = 16
	keys := make([]ed25519.PublicKey, n)
	for i := range keys {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = pub
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := kp.Add(fmt.Sprintf("peer-%d", i), keys[i]); err != nil {
				t.Errorf("Add peer-%d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		got, ok := kp.Get(fmt.Sprintf("peer-%d", i))
		if !ok {
			t.Errorf("peer-%d missing after concurrent Add", i)
			continue
		}
		if !got.Equal(keys[i]) {
			t.Errorf("peer-%d key mismatch", i)
		}
	}
}
