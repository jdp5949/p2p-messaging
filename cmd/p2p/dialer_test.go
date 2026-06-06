package main

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestSessionDialerDirectReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	d := &sessionDialer{
		relayAddr:     "unused",
		sessionID:     "sid",
		punchTimeout:  time.Second,
		partnerPublic: ln.Addr().String(),
		established:   true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := d.dialDirect(ctx)
	if err != nil {
		t.Fatalf("dialDirect: %v", err)
	}
	c.Close()
}
