//go:build windows

package holepunch

import "syscall"

// reuseControl is a no-op on Windows. SO_REUSEPORT does not exist on Windows;
// SO_REUSEADDR has different semantics. TCP simultaneous-open from a reused
// port is not supported. Caller falls back to relay bridge.
func reuseControl(network, address string, c syscall.RawConn) error {
	return nil
}
