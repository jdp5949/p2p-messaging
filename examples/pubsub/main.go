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

const numMessages = 5

func makeConnPair() (*conn.Conn, *conn.Conn) {
	a, b := net.Pipe()
	connA, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return a, nil },
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	connB, err := conn.New(conn.Config{
		DialFunc:     func() (net.Conn, error) { return b, nil },
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	return connA, connB
}

func makeBrokerPair(label string, wg *sync.WaitGroup) (*broker.Broker, *broker.Broker) {
	pubConn, subConn := makeConnPair()

	sub, err := broker.New(broker.Config{
		Conn:       subConn,
		ACKTimeout: 30 * time.Second,
		MaxRetries: 3,
		OnInbound: func(msg broker.InboundMsg) {
			fmt.Printf("[%s] msg %d: %s\n", label, msg.MsgID, string(msg.Payload))
			wg.Done()
		},
	})
	if err != nil {
		panic(err)
	}

	pub, err := broker.New(broker.Config{
		Conn:       pubConn,
		ACKTimeout: 30 * time.Second,
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	return pub, sub
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2 * numMessages) // 2 subscribers x 5 messages each

	pub1, sub1 := makeBrokerPair("sub-1", &wg)
	pub2, sub2 := makeBrokerPair("sub-2", &wg)

	publishers := []*broker.Broker{pub1, pub2}
	subscribers := []*broker.Broker{sub1, sub2}

	go func() {
		for i := 1; i <= numMessages; i++ {
			payload := []byte(fmt.Sprintf(`{"topic":"news","seq":%d}`, i))
			for _, pub := range publishers {
				_, err := pub.Send(protocol.ContentJSON, protocol.PriorityNormal, payload)
				if err != nil {
					fmt.Printf("[publisher] send error: %v\n", err)
				}
			}
		}
	}()

	wg.Wait()

	for _, b := range publishers {
		_ = b.Close()
	}
	for _, b := range subscribers {
		_ = b.Close()
	}

	fmt.Println("[done] pub/sub complete")
}
