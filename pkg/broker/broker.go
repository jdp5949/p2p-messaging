package broker

import (
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaypatel/p2p-messaging/pkg/chunker"
	"github.com/jaypatel/p2p-messaging/pkg/compress"
	"github.com/jaypatel/p2p-messaging/pkg/conn"
	"github.com/jaypatel/p2p-messaging/pkg/protocol"
)

const (
	DefaultRetryBuffer = 10_000
	DefaultACKTimeout  = 30 * time.Second
	DefaultMaxRetries  = 5
)

// InboundMsg is a fully reassembled message delivered to the app.
type InboundMsg struct {
	MsgID       uint64
	ContentType protocol.ContentType
	Payload     []byte
}

// Config for Broker.
type Config struct {
	Conn         *conn.Conn
	RetryBuffer  int
	ACKTimeout   time.Duration
	MaxRetries   int
	OnInbound    func(InboundMsg)
	OnDead       func(msgID uint64, err error)
	PingInterval time.Duration
}

// slot tracks one in-flight message in the ring buffer.
type slot struct {
	msgID    uint64
	frags    []protocol.Message
	sendTime time.Time
	retries  int
	dead     bool
	active   bool
}

// Broker is the public API.
type Broker struct {
	cfg    Config
	conn   *conn.Conn
	enc    *compress.Encoder
	dec    *compress.Decoder
	assemb *chunker.Assembler

	mu      sync.Mutex
	ring    []slot
	ringLen int

	msgIDCounter uint64

	stop chan struct{}
	wg   sync.WaitGroup
}

// New creates and starts a Broker.
func New(cfg Config) (*Broker, error) {
	if cfg.Conn == nil {
		return nil, errors.New("broker: Conn is required")
	}
	if cfg.RetryBuffer <= 0 {
		cfg.RetryBuffer = DefaultRetryBuffer
	}
	if cfg.ACKTimeout <= 0 {
		cfg.ACKTimeout = DefaultACKTimeout
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}

	enc, err := compress.NewEncoder()
	if err != nil {
		return nil, err
	}
	dec, err := compress.NewDecoder()
	if err != nil {
		return nil, err
	}

	b := &Broker{
		cfg:     cfg,
		conn:    cfg.Conn,
		enc:     enc,
		dec:     dec,
		assemb:  chunker.NewAssembler(),
		ring:    make([]slot, cfg.RetryBuffer),
		ringLen: cfg.RetryBuffer,
		stop: make(chan struct{}),
	}

	b.wg.Add(1)
	go b.readLoop()
	b.wg.Add(1)
	go b.retryLoop()

	return b, nil
}

// Send enqueues a message for delivery.
func (b *Broker) Send(ct protocol.ContentType, priority protocol.Priority, payload []byte) (uint64, error) {
	msgID := atomic.AddUint64(&b.msgIDCounter, 1)

	frags, err := chunker.Split(msgID, ct, payload, b.enc)
	if err != nil {
		return 0, err
	}
	for i := range frags {
		frags[i].Header.Priority = priority
	}

	if err := b.enqueue(msgID, frags); err != nil {
		return 0, err
	}

	for _, f := range frags {
		if err := b.conn.WriteMsg(f); err != nil {
			// Free slot so caller can retry cleanly without duplicate in-flight.
			b.freeSlot(msgID)
			return 0, err
		}
	}
	return msgID, nil
}

// Close shuts down the broker.
func (b *Broker) Close() error {
	select {
	case <-b.stop:
	default:
		close(b.stop)
	}
	// Close conn first so readLoop unblocks from ReadMsg, then wait.
	err := b.conn.Close()
	b.wg.Wait()
	return err
}

// enqueue finds a free slot in the ring buffer (blocks until one is free).
func (b *Broker) enqueue(msgID uint64, frags []protocol.Message) error {
	for {
		b.mu.Lock()
		for i := range b.ring {
			if !b.ring[i].active {
				b.ring[i] = slot{
					msgID:    msgID,
					frags:    frags,
					sendTime: time.Now(),
					retries:  0,
					dead:     false,
					active:   true,
				}
				b.mu.Unlock()
				return nil
			}
		}
		b.mu.Unlock()

		// ring full — wait a bit
		select {
		case <-b.stop:
			return errors.New("broker: closed")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// freeSlot marks the slot for msgID as free.
func (b *Broker) freeSlot(msgID uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range b.ring {
		if b.ring[i].active && b.ring[i].msgID == msgID {
			b.ring[i].active = false
			return
		}
	}
}

// readLoop reads messages from the connection.
func (b *Broker) readLoop() {
	defer b.wg.Done()
	reconnectDelays := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}

	for {
		select {
		case <-b.stop:
			return
		default:
		}

		msg, err := b.conn.ReadMsg()
		if err != nil {
			select {
			case <-b.stop:
				return
			default:
			}

			if isConnError(err) {
				b.handleReconnect(reconnectDelays)
			}
			continue
		}

		b.dispatch(msg)
	}
}

func isConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return false
}

// handleReconnect attempts reconnect with backoff, then replays unacked slots.
func (b *Broker) handleReconnect(delays []time.Duration) {
	for _, d := range delays {
		select {
		case <-b.stop:
			return
		case <-time.After(d):
		}
		if err := b.conn.Reconnect(); err == nil {
			b.replayUnacked()
			return
		}
	}
}

// replayUnacked resends all active (unacked) slots after reconnect.
// Resets sendTime so the retry timer gives each message a fresh window.
func (b *Broker) replayUnacked() {
	now := time.Now()
	b.mu.Lock()
	var toReplay [][]protocol.Message
	for i := range b.ring {
		s := &b.ring[i]
		if s.active && !s.dead {
			cp := make([]protocol.Message, len(s.frags))
			copy(cp, s.frags)
			toReplay = append(toReplay, cp)
			s.sendTime = now
			s.retries = 0
		}
	}
	b.mu.Unlock()

	for _, frags := range toReplay {
		for _, f := range frags {
			_ = b.conn.WriteMsg(f)
		}
	}
}

// dispatch routes a received message to the right handler.
func (b *Broker) dispatch(msg protocol.Message) {
	switch msg.Header.MsgType {
	case protocol.MsgACK:
		b.freeSlot(msg.Header.MsgID)

	case protocol.MsgNACK:
		msgID := msg.Header.MsgID
		b.freeSlot(msgID)
		if b.cfg.OnDead != nil {
			b.cfg.OnDead(msgID, errors.New("broker: NACK received"))
		}

	case protocol.MsgData:
		payload, complete, err := b.assemb.Add(msg, b.dec)
		if err != nil || !complete {
			return
		}
		if b.cfg.OnInbound != nil {
			b.cfg.OnInbound(InboundMsg{
				MsgID:       msg.Header.MsgID,
				ContentType: msg.Header.ContentType,
				Payload:     payload,
			})
		}
		// Send ACK back asynchronously so readLoop doesn't block.
		msgID := msg.Header.MsgID
		go func() {
			ack := protocol.Message{
				Header: protocol.Header{MsgID: msgID, MsgType: protocol.MsgACK},
			}
			_ = b.conn.WriteMsg(ack)
		}()

	case protocol.MsgPing:
		pingID := msg.Header.MsgID
		go func() {
			pong := protocol.Message{
				Header: protocol.Header{MsgID: pingID, MsgType: protocol.MsgPong},
			}
			_ = b.conn.WriteMsg(pong)
		}()
	}
}

// retryLoop checks for timed-out slots and retries or marks dead.
func (b *Broker) retryLoop() {
	defer b.wg.Done()
	interval := b.cfg.ACKTimeout / 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	if interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			b.checkTimeouts()
		}
	}
}

func (b *Broker) checkTimeouts() {
	now := time.Now()
	b.mu.Lock()
	type action struct {
		idx  int
		dead bool
	}
	var actions []action

	for i := range b.ring {
		s := &b.ring[i]
		if !s.active || s.dead {
			continue
		}
		backoff := backoffDuration(s.retries, b.cfg.ACKTimeout)
		deadline := s.sendTime.Add(backoff)
		if now.Before(deadline) {
			continue
		}
		if s.retries >= b.cfg.MaxRetries {
			actions = append(actions, action{i, true})
		} else {
			actions = append(actions, action{i, false})
		}
	}

	// apply actions under lock
	var toSend [][]protocol.Message
	var toDead []uint64

	for _, a := range actions {
		s := &b.ring[a.idx]
		if a.dead {
			s.dead = true
			s.active = false
			toDead = append(toDead, s.msgID)
		} else {
			s.retries++
			s.sendTime = now
			cp := make([]protocol.Message, len(s.frags))
			copy(cp, s.frags)
			toSend = append(toSend, cp)
		}
	}
	b.mu.Unlock()

	for _, frags := range toSend {
		for _, f := range frags {
			_ = b.conn.WriteMsg(f)
		}
	}

	if b.cfg.OnDead != nil {
		for _, id := range toDead {
			b.cfg.OnDead(id, errors.New("broker: max retries exhausted"))
		}
	}
}

func backoffDuration(retries int, base time.Duration) time.Duration {
	multiplier := math.Pow(2, float64(retries))
	d := time.Duration(float64(base) * multiplier)
	cap := 30 * time.Second
	if d > cap {
		return cap
	}
	return d
}
