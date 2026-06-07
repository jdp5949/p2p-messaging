// Package rendezvous is the client half of the relay protocol: it connects to
// the relay, exchanges endpoint info with a partner sharing the same session
// ID, attempts a direct NAT hole-punch, and falls back to the relay byte-bridge.
package rendezvous

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/holepunch"
)

// punchMagic is exchanged on a freshly hole-punched connection to confirm it is
// genuinely bidirectional before either side commits to using it. A fixed
// length (read with io.ReadFull) keeps the stream clean for the Noise handshake
// that follows.
var punchMagic = []byte("P2PPUNCH")

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

	// Attempt a direct hole-punch, then VALIDATE it is truly bidirectional.
	// A simultaneous-open can yield a half-open / mismatched pair that "dials
	// ok" but cannot carry traffic both ways; validation catches that.
	var direct net.Conn
	if c, perr := holepunch.AttemptPunch(partner, localPort, opt.PunchTimeout); perr == nil {
		if verr := validatePunch(c); verr == nil {
			direct = c
		} else {
			c.Close()
		}
	}

	if direct != nil {
		fmt.Fprint(relayConn, "PUNCH_OK\n")
	} else {
		fmt.Fprint(relayConn, "PUNCH_FAIL\n")
	}

	// The relay decides unanimously: DIRECT only if BOTH peers validated their
	// direct link; otherwise BRIDGE (relay copies bytes — always bidirectional).
	decLine, err := r.ReadString('\n')
	if err != nil {
		if direct != nil {
			direct.Close()
		}
		relayConn.Close()
		return nil, fmt.Errorf("read relay decision: %w", err)
	}
	switch strings.TrimRight(decLine, "\r\n") {
	case "DIRECT":
		relayConn.Close()
		return &Result{Conn: direct, Partner: partner, UsedFallback: false}, nil
	case "BRIDGE":
		if direct != nil {
			direct.Close()
		}
		// Wrap with the bufio reader so any bytes it already buffered (the relay
		// starts copying immediately after BRIDGE) are not lost.
		return &Result{Conn: &bufConn{Conn: relayConn, r: r}, Partner: partner, UsedFallback: true}, nil
	default:
		if direct != nil {
			direct.Close()
		}
		relayConn.Close()
		return nil, fmt.Errorf("unexpected relay decision %q", decLine)
	}
}

// validatePunch confirms a hole-punched conn carries traffic in both directions
// by exchanging a fixed magic. Both peers write then read, so neither blocks the
// other. A short deadline bounds the check.
func validatePunch(c net.Conn) error {
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	defer c.SetDeadline(time.Time{}) //nolint:errcheck
	if _, err := c.Write(punchMagic); err != nil {
		return err
	}
	buf := make([]byte, len(punchMagic))
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}
	if !bytes.Equal(buf, punchMagic) {
		return errors.New("rendezvous: punch validation magic mismatch")
	}
	return nil
}

// bufConn is a net.Conn whose reads come from a bufio.Reader (preserving bytes
// buffered before the conn was handed back), while writes/close/deadlines go to
// the underlying conn.
type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufConn) Read(p []byte) (int, error) { return b.r.Read(p) }

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
