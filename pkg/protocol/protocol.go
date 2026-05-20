package protocol

import "encoding/binary"

const HeaderSize = 20

type MsgType uint8

const (
	MsgData MsgType = iota
	MsgACK
	MsgNACK
	MsgPing
	MsgPong
)

type ContentType uint8

const (
	ContentJSON ContentType = iota
	ContentBinary
	ContentText
	ContentProtobuf
	ContentAvro
	ContentRaw
)

type Flags uint8

const (
	FlagCompressed Flags = 1 << iota
	FlagFragmented
	FlagPriority
)

type Priority uint8

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 1
	PriorityHigh   Priority = 2
)

type Header struct {
	MsgID       uint64
	MsgType     MsgType
	ContentType ContentType
	Flags       Flags
	Priority    Priority
	FragIndex   uint16
	FragTotal   uint16
	PayloadLen  uint32
}

func EncodeHeader(h Header) [HeaderSize]byte {
	var b [HeaderSize]byte
	binary.BigEndian.PutUint64(b[0:8], h.MsgID)
	b[8] = uint8(h.MsgType)
	b[9] = uint8(h.ContentType)
	b[10] = uint8(h.Flags)
	b[11] = uint8(h.Priority)
	binary.BigEndian.PutUint16(b[12:14], h.FragIndex)
	binary.BigEndian.PutUint16(b[14:16], h.FragTotal)
	binary.BigEndian.PutUint32(b[16:20], h.PayloadLen)
	return b
}

func DecodeHeader(b [HeaderSize]byte) Header {
	return Header{
		MsgID:       binary.BigEndian.Uint64(b[0:8]),
		MsgType:     MsgType(b[8]),
		ContentType: ContentType(b[9]),
		Flags:       Flags(b[10]),
		Priority:    Priority(b[11]),
		FragIndex:   binary.BigEndian.Uint16(b[12:14]),
		FragTotal:   binary.BigEndian.Uint16(b[14:16]),
		PayloadLen:  binary.BigEndian.Uint32(b[16:20]),
	}
}

type Message struct {
	Header  Header
	Payload []byte
}
