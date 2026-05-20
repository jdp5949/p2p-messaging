package main

import (
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/broker"
	"github.com/jaypatel/p2p-messaging/pkg/conn"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

const twoMB = 2 * 1024 * 1024

func main() {
	pipeA, pipeB := net.Pipe()

	connA, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return pipeA, nil },
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	connB, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return pipeB, nil },
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	done := make(chan struct{})

	brokerB, err := broker.New(broker.Config{
		Conn:       connB,
		ACKTimeout: 60 * time.Second,
		MaxRetries: 3,
		OnInbound: func(msg broker.InboundMsg) {
			if len(msg.Payload) == twoMB {
				fmt.Println("[OK] received 2MB binary payload")
			} else {
				fmt.Printf("[FAIL] expected %d bytes, got %d\n", twoMB, len(msg.Payload))
			}
			close(done)
		},
	})
	if err != nil {
		panic(err)
	}

	brokerA, err := broker.New(broker.Config{
		Conn:       connA,
		ACKTimeout: 60 * time.Second,
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	payload := make([]byte, twoMB)
	if _, err := rand.Read(payload); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := brokerA.Send(protocol.ContentBinary, protocol.PriorityNormal, payload)
		if err != nil {
			fmt.Printf("[A] send error: %v\n", err)
		}
	}()

	<-done
	wg.Wait()

	_ = brokerA.Close()
	_ = brokerB.Close()
}
