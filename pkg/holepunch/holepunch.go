// Package holepunch implements TCP NAT hole-punching between two peers.
//
// Both peers connect to a relay, exchange endpoint Info via the relay's
// control protocol, then call AttemptPunch simultaneously to establish a
// direct TCP connection. If the punch fails the caller falls back to the
// relay-bridged connection.
package holepunch

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// DefaultTimeout is the punch attempt deadline if the caller passes 0.
const DefaultTimeout = 5 * time.Second

// Info describes a peer's known endpoints as reported by/to the relay.
type Info struct {
	PublicAddr string   `json:"public_addr"`            // e.g. "203.0.113.5:54321"
	LocalAddrs []string `json:"local_addrs,omitempty"` // LAN addrs e.g. "192.168.1.10:9000"
}

// Result is the outcome of a hole-punch attempt.
type Result struct {
	Conn         net.Conn // direct connection if succeeded; nil on failure
	UsedFallback bool     // true when falling back to relay-bridged conn
}

// reuseControl is a net.ListenConfig.Control function that sets
// SO_REUSEADDR and SO_REUSEPORT so that a local port can be both
// listened on and dialled from simultaneously.
func reuseControl(network, address string, c syscall.RawConn) error {
	var setSockOptErr error
	err := c.Control(func(fd uintptr) {
		if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
			setSockOptErr = fmt.Errorf("SO_REUSEADDR: %w", err)
			return
		}
		if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
			setSockOptErr = fmt.Errorf("SO_REUSEPORT: %w", err)
		}
	})
	if err != nil {
		return err
	}
	return setSockOptErr
}

// listenConfig returns a net.ListenConfig with SO_REUSEPORT enabled.
func listenConfig() net.ListenConfig {
	return net.ListenConfig{Control: reuseControl}
}

// dialDirect dials remoteAddr from localPort using SO_REUSEPORT.
func dialDirect(ctx context.Context, localPort int, remoteAddr string) (net.Conn, error) {
	d := net.Dialer{
		LocalAddr: &net.TCPAddr{Port: localPort},
		Control:   reuseControl,
	}
	return d.DialContext(ctx, "tcp", remoteAddr)
}

// listenDirect listens on localPort with SO_REUSEPORT and accepts one conn.
func listenDirect(ctx context.Context, localPort int) (net.Conn, error) {
	lc := listenConfig()
	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", localPort))
	if err != nil {
		return nil, fmt.Errorf("listen :%d: %w", localPort, err)
	}
	defer ln.Close()

	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := ln.Accept()
		ch <- result{conn, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		return r.conn, r.err
	}
}

// AttemptPunch tries to establish a direct TCP connection to remote.
//
// Strategy:
//  1. Simultaneously dial remote.PublicAddr and all remote.LocalAddrs
//  2. Also listen on localPort for an incoming direct connection
//  3. First success wins; all other attempts are abandoned
//  4. Returns error if nothing succeeds within timeout
//
// localPort is the port the caller used during its relay connection so the
// NAT mapping is already in place.
func AttemptPunch(remote Info, localPort int, timeout time.Duration) (net.Conn, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	type res struct {
		conn net.Conn
		err  error
	}
	results := make(chan res, 8)

	targets := buildTargets(remote)
	total := len(targets) + 1 // +1 for listener

	// Listener goroutine.
	go func() {
		conn, err := listenDirect(ctx, localPort)
		results <- res{conn, err}
	}()

	// Dialler goroutines.
	for _, addr := range targets {
		addr := addr
		go func() {
			conn, err := dialDirect(ctx, localPort, addr)
			results <- res{conn, err}
		}()
	}

	var firstErr error
	for i := 0; i < total; i++ {
		r := <-results
		if r.err == nil {
			cancel() // stop remaining goroutines
			// drain so goroutines can exit
			go func() {
				for j := i + 1; j < total; j++ {
					if extra := <-results; extra.conn != nil {
						extra.conn.Close()
					}
				}
			}()
			return r.conn, nil
		}
		if firstErr == nil {
			firstErr = r.err
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}
	return nil, fmt.Errorf("holepunch: all %d attempts failed", total)
}

// buildTargets returns the ordered list of addresses to dial.
func buildTargets(remote Info) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(a string) {
		if a != "" && !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	add(remote.PublicAddr)
	for _, a := range remote.LocalAddrs {
		add(a)
	}
	return out
}
