package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/codephrase"
	"github.com/jdp5949/p2p-messaging/pkg/crypto"
	"github.com/jdp5949/p2p-messaging/pkg/rendezvous"
	"github.com/jdp5949/p2p-messaging/pkg/transfer"
)

const streamReadBuf = 1 << 20 // 1 MB: max framed message the adapter reads

// sessionStream adapts a *crypto.Session to transfer.Stream.
type sessionStream struct{ s *crypto.Session }

func (a *sessionStream) WriteMsg(p []byte) error { _, e := a.s.Write(p); return e }
func (a *sessionStream) ReadMsg() ([]byte, error) {
	buf := make([]byte, streamReadBuf)
	n, e := a.s.Read(buf)
	if e != nil {
		return nil, e
	}
	return buf[:n], nil
}
func (a *sessionStream) Close() error { return a.s.Close() }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// streamConfig carries what openSession needs.
type streamConfig struct {
	relayAddr string
	useTLS    bool
	tlsConfig *tls.Config
	code      string
	id        *crypto.Identity
	known     *crypto.KnownPeers
	initiator bool
}

// openSession opens one stream i: rendezvous + Noise handshake. Every parallel
// data stream uses its own session id "<code>#<i>" — distinct from the plain
// SessionID(code) the chat/single-stream broker path uses, so the two never
// collide on the relay.
func openSession(cfg streamConfig, i int) (*crypto.Session, error) {
	sid := codephrase.SessionID(fmt.Sprintf("%s#%d", cfg.code, i))
	ctx, cancel := contextWithTimeout(15 * time.Second)
	defer cancel()
	res, err := rendezvous.Dial(ctx, rendezvous.Options{
		RelayAddr: cfg.relayAddr, SessionID: sid, TLS: cfg.useTLS, TLSConfig: cfg.tlsConfig, PunchTimeout: 6 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	sess, err := crypto.Handshake(res.Conn, crypto.HandshakeConfig{
		Identity: cfg.id, KnownPeers: cfg.known, PeerName: sid, PAKECode: cfg.code, Initiator: cfg.initiator,
	})
	if err != nil {
		res.Conn.Close()
		return nil, err
	}
	return sess, nil
}

// ctrlMsg is the small JSON used to negotiate the stream count on stream0.
type ctrlMsg struct {
	T string `json:"t"`
	N int    `json:"n,omitempty"`
	K int    `json:"k,omitempty"`
}

// openStreams establishes up to want streams and negotiates the agreed count m.
// Returns the transfer.Streams (len m, index 0 = control). Caller closes them.
func openStreams(cfg streamConfig, want int) ([]transfer.Stream, error) {
	// Stream 0 (control) must succeed.
	s0, err := openSession(cfg, 0)
	if err != nil {
		return nil, fmt.Errorf("control stream: %w", err)
	}
	ctrl := &sessionStream{s: s0}

	// Negotiate N (initiator announces; joiner learns).
	if cfg.initiator {
		b, _ := json.Marshal(ctrlMsg{T: "want", N: want})
		if err := ctrl.WriteMsg(b); err != nil {
			ctrl.Close()
			return nil, err
		}
	} else {
		mb, err := ctrl.ReadMsg()
		if err != nil {
			ctrl.Close()
			return nil, err
		}
		var cm ctrlMsg
		if json.Unmarshal(mb, &cm) != nil || cm.T != "want" {
			ctrl.Close()
			return nil, fmt.Errorf("expected want, got %q", mb)
		}
		want = cm.N
	}

	// Open streams 1..want-1 concurrently.
	sessions := make([]*crypto.Session, want)
	sessions[0] = s0
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 1; i < want; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := openSession(cfg, i)
			if err == nil {
				mu.Lock()
				sessions[i] = s
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	kSelf := 0
	for _, s := range sessions {
		if s != nil {
			kSelf++
		}
	}

	// Exchange opened counts; both compute min.
	myCount, _ := json.Marshal(ctrlMsg{T: "opened", K: kSelf})
	if err := ctrl.WriteMsg(myCount); err != nil {
		closeAll(sessions)
		return nil, err
	}
	mb, err := ctrl.ReadMsg()
	if err != nil {
		closeAll(sessions)
		return nil, err
	}
	var peer ctrlMsg
	if json.Unmarshal(mb, &peer) != nil || peer.T != "opened" {
		closeAll(sessions)
		return nil, fmt.Errorf("expected opened, got %q", mb)
	}
	m := minInt(kSelf, peer.K)

	// Compact the first m non-nil contiguous sessions; close the rest.
	// (Both sides opened the same indices set on success; index 0 always present.
	//  Use the first m sessions by index that are non-nil — they match because
	//  rendezvous#i pairs deterministically; if asymmetric, m already accounts.)
	out := make([]transfer.Stream, 0, m)
	for i := 0; i < want && len(out) < m; i++ {
		if sessions[i] != nil {
			out = append(out, &sessionStream{s: sessions[i]})
			sessions[i] = nil
		}
	}
	closeAll(sessions) // close leftovers
	return out, nil
}

func closeAll(ss []*crypto.Session) {
	for _, s := range ss {
		if s != nil {
			s.Close()
		}
	}
}

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
