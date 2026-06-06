package rendezvous

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func startTestRelay(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	var mu sync.Mutex
	pending := map[string]net.Conn{}
	pendingInfo := map[string]string{}

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				r := newLineReader(c)
				sid, _ := r.line()
				info, _ := r.line()
				mu.Lock()
				other, ok := pending[sid]
				if !ok {
					pending[sid] = c
					pendingInfo[sid] = info
					mu.Unlock()
					return
				}
				otherInfo := pendingInfo[sid]
				delete(pending, sid)
				delete(pendingInfo, sid)
				mu.Unlock()

				other.Write([]byte(info + "\n"))
				c.Write([]byte(otherInfo + "\n"))
				other.Write([]byte("START\n"))
				c.Write([]byte("START\n"))
			}(c)
		}
	}()
	return ln.Addr().String()
}

func TestRendezvousDirectPunch(t *testing.T) {
	relay := startTestRelay(t)
	const sid = "testsession00000"

	var wg sync.WaitGroup
	var aConn, bConn net.Conn
	var aErr, bErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := Dial(ctx, Options{RelayAddr: relay, SessionID: sid, PunchTimeout: 6 * time.Second})
		if err == nil {
			aConn = res.Conn
		}
		aErr = err
	}()
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := Dial(ctx, Options{RelayAddr: relay, SessionID: sid, PunchTimeout: 6 * time.Second})
		if err == nil {
			bConn = res.Conn
		}
		bErr = err
	}()
	wg.Wait()

	if aErr != nil || bErr != nil {
		t.Fatalf("dial errors: a=%v b=%v", aErr, bErr)
	}
	defer aConn.Close()
	defer bConn.Close()

	go aConn.Write([]byte("ping"))
	buf := make([]byte, 4)
	bConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := bConn.Read(buf); err != nil {
		t.Fatalf("read on punched conn: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("got %q want ping", buf)
	}
}
