package chunker

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/jdp5949/p2p-messaging/pkg/compress"
	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

const (
	ChunkSize         = 512 * 1024 // 512 KB
	MaxStreams         = 8
	DefaultMaxStreams  = 1024
)

// Split splits payload into fragment protocol.Messages ready to send.
func Split(msgID uint64, ct protocol.ContentType, payload []byte, enc *compress.Encoder) ([]protocol.Message, error) {
	data, wasCompressed, err := enc.Compress(payload)
	if err != nil {
		return nil, err
	}

	chunks := splitBytes(data)
	if len(chunks) > math.MaxUint16 {
		return nil, errors.New("chunker: payload too large (exceeds 65535 fragments)")
	}
	total := uint16(len(chunks))

	msgs := make([]protocol.Message, len(chunks))
	for i, chunk := range chunks {
		flags := buildFlags(total > 1, wasCompressed)
		msgs[i] = protocol.Message{
			Header: protocol.Header{
				MsgID:       msgID,
				MsgType:     protocol.MsgData,
				ContentType: ct,
				Flags:       flags,
				Priority:    protocol.PriorityNormal,
				FragIndex:   uint16(i),
				FragTotal:   total,
				PayloadLen:  uint32(len(chunk)),
			},
			Payload: chunk,
		}
	}
	return msgs, nil
}

func splitBytes(data []byte) [][]byte {
	if len(data) == 0 {
		return [][]byte{data}
	}
	var chunks [][]byte
	for len(data) > 0 {
		n := ChunkSize
		if n > len(data) {
			n = len(data)
		}
		chunks = append(chunks, data[:n])
		data = data[n:]
	}
	return chunks
}

func buildFlags(fragmented, compressed bool) protocol.Flags {
	var f protocol.Flags
	if compressed {
		f |= protocol.FlagCompressed
	}
	if fragmented {
		f |= protocol.FlagFragmented
	}
	return f
}

// streamState holds in-progress fragment reassembly for one MsgID.
type streamState struct {
	frags      [][]byte
	total      uint16
	received   uint16
	compressed bool
	arrivedAt  time.Time
}

// Assembler reassembles fragments into complete payloads.
type Assembler struct {
	mu         sync.Mutex
	streams    map[uint64]*streamState
	maxStreams  int
}

// NewAssembler returns a new Assembler.
func NewAssembler() *Assembler {
	return &Assembler{streams: make(map[uint64]*streamState), maxStreams: DefaultMaxStreams}
}

// Add adds a fragment. Returns (payload, true, nil) when complete.
func (a *Assembler) Add(msg protocol.Message, dec *compress.Decoder) ([]byte, bool, error) {
	h := msg.Header
	if err := validateFragment(h, msg.Payload); err != nil {
		return nil, false, err
	}

	// Single-fragment message — no assembly needed.
	if h.FragTotal == 1 {
		return decompress(msg.Payload, h.Flags, dec)
	}

	a.mu.Lock()
	st, err2 := a.getOrCreate(h)
	if err2 != nil {
		a.mu.Unlock()
		return nil, false, err2
	}
	if st.frags[h.FragIndex] != nil {
		a.mu.Unlock()
		return nil, false, nil // duplicate, ignore
	}
	chunk := make([]byte, len(msg.Payload))
	copy(chunk, msg.Payload)
	st.frags[h.FragIndex] = chunk
	st.received++
	complete := st.received == st.total
	var assembled []byte
	if complete {
		assembled = joinChunks(st.frags)
		delete(a.streams, h.MsgID)
	}
	a.mu.Unlock()

	if !complete {
		return nil, false, nil
	}
	return decompress(assembled, h.Flags, dec)
}

// Purge removes stale in-progress messages older than ttl.
func (a *Assembler) Purge(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, st := range a.streams {
		if st.arrivedAt.Before(cutoff) {
			delete(a.streams, id)
		}
	}
}

func (a *Assembler) getOrCreate(h protocol.Header) (*streamState, error) {
	st, ok := a.streams[h.MsgID]
	if !ok {
		if len(a.streams) >= a.maxStreams {
			return nil, errors.New("chunker: too many in-flight streams")
		}
		st = &streamState{
			frags:      make([][]byte, h.FragTotal),
			total:      h.FragTotal,
			compressed: h.Flags&protocol.FlagCompressed != 0,
			arrivedAt:  time.Now(),
		}
		a.streams[h.MsgID] = st
	}
	return st, nil
}

func validateFragment(h protocol.Header, payload []byte) error {
	if h.FragTotal == 0 {
		return errors.New("chunker: FragTotal is 0")
	}
	if h.FragIndex >= h.FragTotal {
		return errors.New("chunker: FragIndex >= FragTotal")
	}
	if uint32(len(payload)) != h.PayloadLen {
		return errors.New("chunker: payload length mismatch")
	}
	return nil
}

func joinChunks(frags [][]byte) []byte {
	total := 0
	for _, f := range frags {
		total += len(f)
	}
	out := make([]byte, 0, total)
	for _, f := range frags {
		out = append(out, f...)
	}
	return out
}

func decompress(data []byte, flags protocol.Flags, dec *compress.Decoder) ([]byte, bool, error) {
	if flags&protocol.FlagCompressed == 0 {
		return data, true, nil
	}
	out, err := dec.Decompress(data)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}
