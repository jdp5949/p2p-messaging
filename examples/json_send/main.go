package main

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/broker"
	"github.com/jaypatel/p2p-messaging/pkg/conn"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

func main() {
	pipeA, pipeB := net.Pipe()

	connA, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return pipeA, nil },
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	connB, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return pipeB, nil },
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	received := make(chan struct{}, 3)

	brokerB, err := broker.New(broker.Config{
		Conn:        connB,
		ACKTimeout:  10 * time.Second,
		MaxRetries:  3,
		OnInbound: func(msg broker.InboundMsg) {
			fmt.Printf("[B] received msg %d: %s\n", msg.MsgID, string(msg.Payload))
			received <- struct{}{}
		},
	})
	if err != nil {
		panic(err)
	}

	brokerA, err := broker.New(broker.Config{
		Conn:       connA,
		ACKTimeout: 10 * time.Second,
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	messages := []string{
		`{"event":"hello","seq":1}`,
		`{"event":"hello","seq":2}`,
		`{"event":"hello","seq":3}`,
	}

	go func() {
		for _, payload := range messages {
			_, _ = brokerA.Send(protocol.ContentJSON, protocol.PriorityNormal, []byte(payload))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			<-received
		}
	}()

	wg.Wait()

	// Close B first so A's retryLoop sees a closed pipe cleanly.
	_ = brokerB.Close()
	_ = brokerA.Close()
	fmt.Println("[done] 3 JSON messages delivered")
}
