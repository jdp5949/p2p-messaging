// Package rendezvous is the client half of the relay protocol: it connects to
// the relay, exchanges endpoint info with a partner sharing the same session
// ID, attempts a direct NAT hole-punch, and falls back to the relay byte-bridge.
package rendezvous

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/holepunch"
)

// Options configures a rendezvous dial.
type Options struct {
	RelayAddr    string
	SessionID    string
	TLS          bool
	TLSConfig    *tls.Config
	PunchTimeout time.Duration
}

// Result is the outcome of a successful rendezvous.
type Result struct {
	Conn         net.Conn
	Partner      holepunch.Info
	UsedFallback bool
}

type peerInfo struct {
	LocalAddrs []string `json:"local_addrs,omitempty"`
}

// Dial performs the full rendezvous and returns a usable connection.
func Dial(ctx context.Context, opt Options) (*Result, error) {
	if opt.PunchTimeout <= 0 {
		opt.PunchTimeout = holepunch.DefaultTimeout
	}

	localPort, err := holepunch.FreePort()
	if err != nil {
		return nil, fmt.Errorf("pick local port: %w", err)
	}

	raw, err := holepunch.DialReuse(ctx, localPort, opt.RelayAddr)
	if err != nil {
		return nil, fmt.Errorf("dial relay: %w", err)
	}

	var relayConn net.Conn = raw
	if opt.TLS {
		tlsCfg := opt.TLSConfig
		if tlsCfg == nil {
			host, _, _ := net.SplitHostPort(opt.RelayAddr)
			tlsCfg = &tls.Config{ServerName: host}
		}
		tc := tls.Client(raw, tlsCfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			raw.Close()
			return nil, fmt.Errorf("relay tls handshake: %w", err)
		}
		relayConn = tc
	}

	info := peerInfo{LocalAddrs: localAddrs(localPort)}
	infoJSON, _ := json.Marshal(info)
	if _, err := fmt.Fprintf(relayConn, "%s\n%s\n", opt.SessionID, infoJSON); err != nil {
		relayConn.Close()
		return nil, fmt.Errorf("send rendezvous lines: %w", err)
	}

	r := bufio.NewReader(relayConn)

	partnerLine, err := r.ReadString('\n')
	if err != nil {
		relayConn.Close()
		return nil, fmt.Errorf("read partner info: %w", err)
	}
	var partner holepunch.Info
	if err := json.Unmarshal([]byte(strings.TrimRight(partnerLine, "\r\n")), &partner); err != nil {
		relayConn.Close()
		return nil, fmt.Errorf("parse partner info: %w", err)
	}

	startLine, err := r.ReadString('\n')
	if err != nil || strings.TrimRight(startLine, "\r\n") != "START" {
		relayConn.Close()
		return nil, fmt.Errorf("expected START, got %q (err=%v)", startLine, err)
	}

	direct, perr := holepunch.AttemptPunch(partner, localPort, opt.PunchTimeout)
	if perr == nil {
		fmt.Fprint(relayConn, "PUNCH_OK\n")
		relayConn.Close()
		return &Result{Conn: direct, Partner: partner, UsedFallback: false}, nil
	}

	fmt.Fprint(relayConn, "PUNCH_FAIL\n")
	return &Result{Conn: relayConn, Partner: partner, UsedFallback: true}, nil
}

// localAddrs enumerates non-loopback local IPv4 addresses with localPort,
// plus loopback so same-host tests can punch.
func localAddrs(localPort int) []string {
	var out []string
	ifaces, err := net.InterfaceAddrs()
	if err != nil {
		return out
	}
	for _, a := range ifaces {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		ip4 := ipnet.IP.To4()
		if ip4 == nil {
			continue
		}
		out = append(out, fmt.Sprintf("%s:%d", ip4.String(), localPort))
	}
	out = append(out, fmt.Sprintf("127.0.0.1:%d", localPort))
	return out
}
