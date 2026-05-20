package protocol

import (
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		h    Header
	}{
		{
			name: "zero header",
			h:    Header{},
		},
		{
			name: "max values",
			h: Header{
				MsgID:       ^uint64(0),
				MsgType:     MsgType(^uint8(0)),
				ContentType: ContentType(^uint8(0)),
				Flags:       Flags(^uint8(0)),
				Priority:    Priority(^uint8(0)),
				FragIndex:   ^uint16(0),
				FragTotal:   ^uint16(0),
				PayloadLen:  ^uint32(0),
			},
		},
		{
			name: "data json normal priority",
			h: Header{
				MsgID:       0xDEADBEEFCAFEBABE,
				MsgType:     MsgData,
				ContentType: ContentJSON,
				Flags:       FlagCompressed,
				Priority:    PriorityNormal,
				FragIndex:   3,
				FragTotal:   10,
				PayloadLen:  1024,
			},
		},
		{
			name: "ping fragmented high priority",
			h: Header{
				MsgID:       1,
				MsgType:     MsgPing,
				ContentType: ContentBinary,
				Flags:       FlagFragmented | FlagPriority,
				Priority:    PriorityHigh,
				FragIndex:   0,
				FragTotal:   1,
				PayloadLen:  0,
			},
		},
		{
			name: "ack protobuf",
			h: Header{
				MsgID:       42,
				MsgType:     MsgACK,
				ContentType: ContentProtobuf,
				Flags:       0,
				Priority:    PriorityLow,
				FragIndex:   100,
				FragTotal:   200,
				PayloadLen:  512,
			},
		},
		{
			name: "nack avro",
			h: Header{
				MsgID:       9999,
				MsgType:     MsgNACK,
				ContentType: ContentAvro,
				Flags:       FlagCompressed | FlagFragmented,
				Priority:    PriorityHigh,
				FragIndex:   7,
				FragTotal:   8,
				PayloadLen:  4096,
			},
		},
		{
			name: "pong raw",
			h: Header{
				MsgID:       0xFFFF,
				MsgType:     MsgPong,
				ContentType: ContentRaw,
				Flags:       FlagPriority,
				Priority:    PriorityNormal,
				FragIndex:   0,
				FragTotal:   0,
				PayloadLen:  256,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeHeader(tt.h)
			got := DecodeHeader(encoded)
			if got != tt.h {
				t.Errorf("round-trip mismatch\n  want: %+v\n  got:  %+v", tt.h, got)
			}
		})
	}
}

func TestHeaderSize(t *testing.T) {
	var b [HeaderSize]byte
	_ = b
	if HeaderSize != 20 {
		t.Fatalf("HeaderSize must be 20, got %d", HeaderSize)
	}
}

func TestEncodeBigEndian(t *testing.T) {
	h := Header{
		MsgID:      0x0102030405060708,
		PayloadLen: 0x090A0B0C,
		FragIndex:  0x0D0E,
		FragTotal:  0x0F10,
	}
	b := EncodeHeader(h)
	want := [HeaderSize]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x00, 0x00, 0x00, 0x00,
		0x0D, 0x0E, 0x0F, 0x10,
		0x09, 0x0A, 0x0B, 0x0C,
	}
	if b != want {
		t.Errorf("big-endian encoding wrong\n  want: %v\n  got:  %v", want, b)
	}
}
