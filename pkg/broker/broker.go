package broker

import (
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/chunker"
	"github.com/jdp5949/p2p-messaging/pkg/compress"
	"github.com/jdp5949/p2p-messaging/pkg/conn"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
	"github.com/jdp5949/p2p-messaging/pkg/wal"
)

const (
	DefaultRetryBuffer = 10_000
	DefaultACKTimeout  = 30 * time.Second
	DefaultMaxRetries  = 5
)

// DefaultReconnectDelays spans ~60s before giving up.
var DefaultReconnectDelays = []time.Duration{
	1 * time.Second, 2 * time.Second, 4 * time.Second,
	8 * time.Second, 15 * time.Second, 30 * time.Second,
}

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

	// ReconnectDelays is the backoff schedule for reconnect attempts after a
	// connection drop. When exhausted without success, OnReconnectFailed is
	// invoked and the read loop stops. nil = DefaultReconnectDelays.
	ReconnectDelays []time.Duration

	// OnReconnectFailed is called once when all ReconnectDelays are exhausted.
	OnReconnectFailed func()

	// WAL: optional write-ahead log for crash-recovery of unacked outbound messages.
	// nil = in-memory only (backward compatible).
	WAL *wal.WAL
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
	cond    *sync.Cond // signals when a slot becomes free
	ring    []slot
	ringLen int
	freeIdx []int // stack of free slot indices (O(1) enqueue/free)

	msgIDCounter uint64
	ackCount     uint64 // counts acks for WAL compact trigger

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
	if cfg.ReconnectDelays == nil {
		cfg.ReconnectDelays = DefaultReconnectDelays
	}

	enc, err := compress.NewEncoder()
	if err != nil {
		return nil, err
	}
	dec, err := compress.NewDecoder()
	if err != nil {
		return nil, err
	}

	freeIdx := make([]int, cfg.RetryBuffer)
	for i := range freeIdx {
		freeIdx[i] = i
	}
	b := &Broker{
		cfg:     cfg,
		conn:    cfg.Conn,
		enc:     enc,
		dec:     dec,
		assemb:  chunker.NewAssembler(),
		ring:    make([]slot, cfg.RetryBuffer),
		ringLen: cfg.RetryBuffer,
		freeIdx: freeIdx,
		stop:    make(chan struct{}),
	}
	b.cond = sync.NewCond(&b.mu)

	if cfg.WAL != nil {
		if err := b.replayWAL(); err != nil {
			return nil, err
		}
		b.wg.Add(1)
		go b.walCompactLoop()
	}

	b.wg.Add(1)
	go b.readLoop()
	b.wg.Add(1)
	go b.retryLoop()

	return b, nil
}

// replayWAL re-enqueues unacked entries from the WAL into the ring buffer.
func (b *Broker) replayWAL() error {
	entries, err := b.cfg.WAL.Replay()
	if err != nil {
		return err
	}
	for _, e := range entries {
		ct, priority, payload := unpackWALPayload(e.Payload)
		frags, err := chunker.Split(e.MsgID, ct, payload, b.enc)
		if err != nil {
			continue
		}
		for i := range frags {
			frags[i].Header.Priority = priority
		}
		_ = b.enqueue(e.MsgID, frags)
		// Advance counter so new sends don't reuse replayed IDs.
		if e.MsgID > atomic.LoadUint64(&b.msgIDCounter) {
			atomic.StoreUint64(&b.msgIDCounter, e.MsgID)
		}
	}
	return nil
}

// walCompactLoop runs WAL.Compact() every minute.
func (b *Broker) walCompactLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			_ = b.cfg.WAL.Compact()
		}
	}
}

// packWALPayload encodes [ContentType:1][Priority:1][payload...] for WAL storage.
func packWALPayload(ct protocol.ContentType, priority protocol.Priority, payload []byte) []byte {
	buf := make([]byte, 2+len(payload))
	buf[0] = byte(ct)
	buf[1] = byte(priority)
	copy(buf[2:], payload)
	return buf
}

// unpackWALPayload decodes a packed WAL payload.
func unpackWALPayload(packed []byte) (protocol.ContentType, protocol.Priority, []byte) {
	if len(packed) < 2 {
		return protocol.ContentText, protocol.PriorityNormal, packed
	}
	return protocol.ContentType(packed[0]), protocol.Priority(packed[1]), packed[2:]
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

	// WAL append BEFORE network write for crash safety.
	if b.cfg.WAL != nil {
		packed := packWALPayload(ct, priority, payload)
		if err := b.cfg.WAL.Append(msgID, packed); err != nil {
			b.freeSlot(msgID)
			return 0, err
		}
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
	// Wake any goroutines blocked in enqueue waiting for a free slot.
	b.cond.Broadcast()
	// Close conn first so readLoop unblocks from ReadMsg, then wait.
	err := b.conn.Close()
	b.wg.Wait()
	return err
}

// enqueue claims a free slot via freeIdx stack (O(1)). Waits via sync.Cond if full.
func (b *Broker) enqueue(msgID uint64, frags []protocol.Message) error {
	b.mu.Lock()
	for len(b.freeIdx) == 0 {
		// Check stop without releasing permanently — use a goroutine signal trick.
		// We release mu temporarily to check stop channel.
		b.mu.Unlock()
		select {
		case <-b.stop:
			return errors.New("broker: closed")
		default:
		}
		b.mu.Lock()
		if len(b.freeIdx) == 0 {
			b.cond.Wait()
		}
	}
	idx := b.freeIdx[len(b.freeIdx)-1]
	b.freeIdx = b.freeIdx[:len(b.freeIdx)-1]
	b.ring[idx] = slot{
		msgID:    msgID,
		frags:    frags,
		sendTime: time.Now(),
		active:   true,
	}
	b.mu.Unlock()
	return nil
}

// freeSlot marks the slot for msgID as free and signals waiting enqueue callers.
func (b *Broker) freeSlot(msgID uint64) {
	b.mu.Lock()
	for i := range b.ring {
		if b.ring[i].active && b.ring[i].msgID == msgID {
			b.ring[i].active = false
			b.freeIdx = append(b.freeIdx, i)
			if b.cfg.WAL != nil {
				_ = b.cfg.WAL.Ack(msgID)
				atomic.AddUint64(&b.ackCount, 1)
			}
			b.mu.Unlock()
			b.cond.Signal()
			return
		}
	}
	b.mu.Unlock()
}

// readLoop reads messages from the connection.
func (b *Broker) readLoop() {
	defer b.wg.Done()
	reconnectDelays := b.cfg.ReconnectDelays

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
				if !b.handleReconnect(reconnectDelays) {
					// Distinguish a clean shutdown (Close closed b.stop) from
					// a genuine reconnect-window exhaustion. Only the latter
					// is a real peer drop.
					select {
					case <-b.stop:
						return
					default:
					}
					if b.cfg.OnReconnectFailed != nil {
						b.cfg.OnReconnectFailed()
					}
					return
				}
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
// Returns true if a reconnect succeeded.
func (b *Broker) handleReconnect(delays []time.Duration) bool {
	if b.conn == nil {
		return false
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for _, d := range delays {
		timer.Reset(d)
		select {
		case <-b.stop:
			return false
		case <-timer.C:
		}
		if err := b.conn.Reconnect(); err == nil {
			b.replayUnacked()
			return true
		}
	}
	return false
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

	freed := 0
	for _, a := range actions {
		s := &b.ring[a.idx]
		if a.dead {
			s.dead = true
			s.active = false
			b.freeIdx = append(b.freeIdx, a.idx)
			freed++
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
	for i := 0; i < freed; i++ {
		b.cond.Signal()
	}

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
