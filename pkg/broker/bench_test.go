package broker

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// benchBroker creates a broker backed by a loopback pipe with an auto-ACK server.
func benchBroker(b *testing.B) (*Broker, func()) {
	b.Helper()
	return benchBrokerCfg(b, conn.Config{})
}

// benchBrokerCfg creates a broker with a custom conn.Config.
func benchBrokerCfg(b *testing.B, connCfg conn.Config) (*Broker, func()) {
	b.Helper()
	clientRaw, srvRaw := net.Pipe()
	connCfg.DialFunc = func() (net.Conn, error) { return clientRaw, nil }
	c, err := conn.New(connCfg)
	if err != nil {
		b.Fatal(err)
	}

	// Server: read fragment, send ACK, repeat.
	stop := make(chan struct{})
	go func() {
		var hdrBuf [protocol.HeaderSize]byte
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := io.ReadFull(srvRaw, hdrBuf[:]); err != nil {
				return
			}
			hdr := protocol.DecodeHeader(hdrBuf)
			if hdr.PayloadLen > 0 {
				payload := make([]byte, hdr.PayloadLen)
				if _, err := io.ReadFull(srvRaw, payload); err != nil {
					return
				}
			}
			// Send ACK.
			ack := protocol.EncodeHeader(protocol.Header{
				MsgID:   hdr.MsgID,
				MsgType: protocol.MsgACK,
			})
			_, _ = srvRaw.Write(ack[:])
		}
	}()

	br, err := New(Config{Conn: c})
	if err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		close(stop)
		_ = br.Close()
		_ = srvRaw.Close()
	}
	return br, cleanup
}

func BenchmarkSend1KB(b *testing.B) {
	br, cleanup := benchBroker(b)
	defer cleanup()
	payload := make([]byte, 1024)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.Send(protocol.ContentRaw, protocol.PriorityNormal, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSendBatched1KB(b *testing.B) {
	br, cleanup := benchBrokerCfg(b, conn.Config{
		BatchSize:    64 * 1024,
		BatchTimeout: 5 * time.Millisecond,
	})
	defer cleanup()
	payload := make([]byte, 1024)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.Send(protocol.ContentRaw, protocol.PriorityNormal, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSend4KB(b *testing.B) {
	br, cleanup := benchBroker(b)
	defer cleanup()
	payload := make([]byte, 4096)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.Send(protocol.ContentRaw, protocol.PriorityNormal, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSend64KB(b *testing.B) {
	br, cleanup := benchBroker(b)
	defer cleanup()
	payload := make([]byte, 64*1024)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.Send(protocol.ContentRaw, protocol.PriorityNormal, payload); err != nil {
			b.Fatal(err)
		}
	}
}
