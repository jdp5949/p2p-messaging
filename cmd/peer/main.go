package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/broker"
	"github.com/jaypatel/p2p-messaging/pkg/conn"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

func main() {
	addr := flag.String("addr", "", "remote peer address to dial (e.g. localhost:9000)")
	listen := flag.String("listen", "", "local address to listen on (e.g. :9000)")
	mode := flag.String("mode", "text", "text or binary")
	ping := flag.Duration("ping", 10*time.Second, "ping interval")
	timeout := flag.Duration("timeout", 30*time.Second, "ACK timeout")
	flag.Parse()

	if *addr == "" && *listen == "" {
		fmt.Fprintln(os.Stderr, "peer: must set -addr or -listen")
		os.Exit(1)
	}

	netConn, err := dial(*listen, *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "peer: connect: %v\n", err)
		os.Exit(1)
	}

	c, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return netConn, nil },
		ReadTimeout:  *timeout,
		WriteTimeout: *timeout,
		PingInterval: *ping,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "peer: conn.New: %v\n", err)
		os.Exit(1)
	}

	b, err := broker.New(broker.Config{
		Conn:       c,
		ACKTimeout: *timeout,
		OnInbound: func(m broker.InboundMsg) {
			fmt.Printf("[RECV msgID=%d] %s\n", m.MsgID, m.Payload)
		},
		OnDead: func(msgID uint64, e error) {
			fmt.Fprintf(os.Stderr, "[DEAD msgID=%d] %v\n", msgID, e)
		},
		PingInterval: *ping,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "peer: broker.New: %v\n", err)
		os.Exit(1)
	}

	done := make(chan struct{})
	go readStdin(b, *mode, done)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	_ = b.Close()
	<-done
}

func dial(listenAddr, remoteAddr string) (net.Conn, error) {
	if listenAddr != "" {
		ln, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return nil, err
		}
		defer ln.Close()
		return ln.Accept()
	}
	return net.Dial("tcp", remoteAddr)
}

func readStdin(b *broker.Broker, mode string, done chan struct{}) {
	defer close(done)
	if mode == "binary" {
		readBinary(b)
	} else {
		readText(b)
	}
}

func readText(b *broker.Broker) {
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := sc.Bytes()
		if _, err := b.Send(protocol.ContentText, protocol.PriorityNormal, line); err != nil {
			fmt.Fprintf(os.Stderr, "peer: send: %v\n", err)
		}
	}
}

func readBinary(b *broker.Broker) {
	buf := make([]byte, 4096)
	for {
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if _, serr := b.Send(protocol.ContentBinary, protocol.PriorityNormal, chunk); serr != nil {
				fmt.Fprintf(os.Stderr, "peer: send: %v\n", serr)
			}
		}
		if err != nil {
			return
		}
	}
}
