//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package holepunch

import (
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

// reuseControl sets SO_REUSEADDR + SO_REUSEPORT so a local port can be
// listened on and dialled from simultaneously (required for TCP simultaneous-open).
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
