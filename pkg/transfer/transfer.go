// Package transfer implements croc-style file/dir transfer over an abstract
// reliable, ordered, encrypted message channel (provided by the broker). It is
// deliberately decoupled from the broker so it can be tested with plain
// channels.
//
// Protocol (each item is one channel message):
//
//	HEADER  ContentJSON  {"t":"header","kind":"file|archive","name":..,"size":..,"mode":..}
//	DATA    ContentBinary [8-byte big-endian offset][bytes]   (repeated)
//	TRAILER ContentJSON  {"t":"trailer","sha256":hex,"total":N}
//	DONE    ContentJSON  {"t":"done"}     (receiver -> sender, after save+verify)
package transfer

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/jdp5949/p2p-messaging/pkg/protocol"
)

// ChunkSize is the DATA payload size (excluding the 8-byte offset prefix).
const ChunkSize = 512 * 1024

// Msg is one message on the transfer channel.
type Msg struct {
	ContentType protocol.ContentType
	Payload     []byte
}

// SendFunc transmits one message. In production it wraps broker.Send.
type SendFunc func(ct protocol.ContentType, payload []byte) error

// ProgressFn reports bytes transferred; total is 0 when unknown (archives).
type ProgressFn func(done, total int64)

// OverwriteFn is consulted when a destination file already exists.
type OverwriteFn func(name string) bool

// Header is the first message of a transfer.
type Header struct {
	T    string `json:"t"`
	Kind string `json:"kind"` // "file" | "archive"
	Name string `json:"name"`
	Size int64  `json:"size"`
	Mode uint32 `json:"mode"`
}

// Trailer ends the data stream.
type Trailer struct {
	T      string `json:"t"`
	SHA256 string `json:"sha256"`
	Total  int64  `json:"total"`
}

func marshalHeader(h Header) []byte   { h.T = "header"; b, _ := json.Marshal(h); return b }
func marshalTrailer(t Trailer) []byte { t.T = "trailer"; b, _ := json.Marshal(t); return b }
func marshalDone() []byte             { b, _ := json.Marshal(map[string]string{"t": "done"}); return b }

// ChatHello is the control message a chat initiator sends immediately on
// connect, so the joining peer instantly knows the session is interactive chat
// (not a file transfer) and can show "connected" and accept typed input right
// away — even if the initiator never sends anything. It is a ContentJSON
// payload; pair it with protocol.ContentJSON when sending.
func ChatHello() []byte { b, _ := json.Marshal(map[string]string{"t": "chat"}); return b }

// Kind classifies an inbound message into one of: "header", "trailer", "done",
// "data", "chat", or "other" (plain chat text falls under "other").
func Kind(m Msg) string { return classify(m) }

// encodeChunk prefixes data with an 8-byte big-endian offset.
func encodeChunk(offset int64, data []byte) []byte {
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[:8], uint64(offset))
	copy(buf[8:], data)
	return buf
}

// decodeChunk splits the 8-byte offset prefix from the data.
func decodeChunk(b []byte) (int64, []byte, error) {
	if len(b) < 8 {
		return 0, nil, errors.New("transfer: chunk too short")
	}
	return int64(binary.BigEndian.Uint64(b[:8])), b[8:], nil
}

// classify returns "header","trailer","done","data", or "other".
func classify(m Msg) string {
	if m.ContentType == protocol.ContentBinary {
		return "data"
	}
	if m.ContentType == protocol.ContentJSON {
		var probe struct {
			T string `json:"t"`
		}
		if json.Unmarshal(m.Payload, &probe) == nil && probe.T != "" {
			return probe.T
		}
	}
	return "other"
}
