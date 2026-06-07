package main

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/rendezvous"
)

// directReconnectTimeout bounds the best-effort direct dial on reconnect so it
// cannot starve the re-rendezvous fallback that follows it.
const directReconnectTimeout = 2 * time.Second

// sessionDialer produces net.Conns for conn.Conn. The first call performs a
// full relay rendezvous + hole-punch. On reconnect it makes a quick best-effort
// direct dial to the remembered partner address (succeeds only if that peer
// happens to be directly reachable, e.g. on a LAN or with a public listener),
// then falls back to a fresh re-rendezvous — which is the reliable path through
// NAT, since neither peer keeps a listener open after the initial punch.
type sessionDialer struct {
	relayAddr    string
	sessionID    string
	useTLS       bool
	tlsConfig    *tls.Config
	punchTimeout time.Duration
	dialTimeout  time.Duration

	mu            sync.Mutex
	established   bool
	partnerPublic string
	lastDirect    bool // true if the most recent connect was a direct punch
}

// LastDirect reports whether the most recent successful connect was a direct
// P2P (hole-punched) connection rather than a relay bridge.
func (d *sessionDialer) LastDirect() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastDirect
}

// DialFunc is the function handed to conn.New.
func (d *sessionDialer) DialFunc() (net.Conn, error) {
	d.mu.Lock()
	established := d.established
	d.mu.Unlock()

	if established {
		// Best-effort direct reconnect on its own short budget; usually fails
		// fast (connection refused) and we fall through to re-rendezvous.
		dctx, dcancel := context.WithTimeout(context.Background(), directReconnectTimeout)
		c, err := d.dialDirect(dctx)
		dcancel()
		if err == nil {
			d.mu.Lock()
			d.lastDirect = true
			d.mu.Unlock()
			return c, nil
		}
	}

	// Re-rendezvous gets its own full budget, independent of the direct attempt.
	ctx, cancel := context.WithTimeout(context.Background(), d.attemptBudget())
	defer cancel()
	return d.rendezvous(ctx)
}

func (d *sessionDialer) attemptBudget() time.Duration {
	if d.dialTimeout > 0 {
		return d.dialTimeout
	}
	return 12 * time.Second
}

// dialDirect connects straight to the remembered partner public address.
func (d *sessionDialer) dialDirect(ctx context.Context) (net.Conn, error) {
	d.mu.Lock()
	addr := d.partnerPublic
	d.mu.Unlock()
	var dl net.Dialer
	return dl.DialContext(ctx, "tcp", addr)
}

// rendezvous performs a full relay rendezvous + hole-punch.
func (d *sessionDialer) rendezvous(ctx context.Context) (net.Conn, error) {
	res, err := rendezvous.Dial(ctx, rendezvous.Options{
		RelayAddr:    d.relayAddr,
		SessionID:    d.sessionID,
		TLS:          d.useTLS,
		TLSConfig:    d.tlsConfig,
		PunchTimeout: d.punchTimeout,
	})
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.established = true
	d.lastDirect = !res.UsedFallback
	if res.Partner.PublicAddr != "" {
		d.partnerPublic = res.Partner.PublicAddr
	}
	d.mu.Unlock()
	return res.Conn, nil
}
