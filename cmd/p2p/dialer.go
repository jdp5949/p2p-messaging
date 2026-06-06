package main

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/rendezvous"
)

// sessionDialer produces net.Conns for conn.Conn. The first call performs a
// full rendezvous; later calls try the remembered partner directly first.
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
}

// DialFunc is the function handed to conn.New.
func (d *sessionDialer) DialFunc() (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.attemptBudget())
	defer cancel()

	d.mu.Lock()
	established := d.established
	d.mu.Unlock()

	if established {
		if c, err := d.dialDirect(ctx); err == nil {
			return c, nil
		}
	}
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
	if res.Partner.PublicAddr != "" {
		d.partnerPublic = res.Partner.PublicAddr
	}
	d.mu.Unlock()
	return res.Conn, nil
}
