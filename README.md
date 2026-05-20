# p2p-messaging

Direct peer-to-peer messaging library in Go. No broker in the hot path. Like Solace or TIBCO — but P2P, minimal setup, MIT licensed.

## Architecture

```
  Peer A                              Peer B
  ------                              ------
  broker (ring buffer, ACK/NACK)      broker
      |                                   |
  chunker (512 KB fragments)          chunker
      |                                   |
  conn (framed TCP, reconnect)        conn
      |                                   |
  compress (zstd, threshold 256 B)    compress
      |                                   |
  protocol (20-byte binary header)    protocol
      |                                   |
      +-------- TCP / relay --------------|
                    |
               cmd/relay
           (rendezvous only)
```

Packages:

| Package | Role |
|---|---|
| `pkg/protocol` | 20-byte binary wire header: MsgID, MsgType, ContentType, Flags, Priority, FragIndex, FragTotal, PayloadLen |
| `pkg/compress` | zstd compression, threshold 256 bytes, pool-based encoder/decoder |
| `pkg/conn` | Framed TCP read/write, auto-reconnect, ping loop, 16 MB payload guard |
| `pkg/chunker` | 512 KB fragment split/reassemble, out-of-order delivery, 1024 concurrent stream cap |
| `pkg/broker` | Ring buffer 10 K slots, ACK/NACK per message, exponential backoff retry, reconnect replay |

Commands:

| Command | Role |
|---|---|
| `cmd/peer` | CLI peer node |
| `cmd/relay` | TCP rendezvous relay (public IP required on one side) |
| `cmd/bench` | Throughput benchmark tool |

## Quick Start

### Install binary (pre-built)

Download the latest release from [GitHub Releases](https://github.com/jdp5949/p2p-messaging/releases):

```sh
# Linux (amd64)
curl -Lo peer https://github.com/jdp5949/p2p-messaging/releases/latest/download/peer-linux-amd64
curl -Lo relay https://github.com/jdp5949/p2p-messaging/releases/latest/download/relay-linux-amd64
chmod +x peer relay

# macOS (arm64)
curl -Lo peer https://github.com/jdp5949/p2p-messaging/releases/latest/download/peer-darwin-arm64
curl -Lo relay https://github.com/jdp5949/p2p-messaging/releases/latest/download/relay-darwin-arm64
chmod +x peer relay
```

### Build from source

Requires Go 1.21+.

```sh
git clone https://github.com/jdp5949/p2p-messaging
cd p2p-messaging
go build -o peer   ./cmd/peer
go build -o relay  ./cmd/relay
go build -o bench  ./cmd/bench
```

### Run relay (public host)

```sh
./relay --addr :9000
```

### Run peers

```sh
# Peer A (sender)
./peer --relay relay.example.com:9000 --room myroom --send

# Peer B (receiver)
./peer --relay relay.example.com:9000 --room myroom --recv
```

## Using the Library

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/jaypatel/p2p-messaging/pkg/broker"
    "github.com/jaypatel/p2p-messaging/pkg/conn"
)

func main() {
    // Dial remote peer
    c, err := conn.Dial(context.Background(), "peer.example.com:9001")
    if err != nil {
        log.Fatal(err)
    }

    // Wrap with broker for ACK/retry guarantees
    b := broker.New(c, broker.Options{
        RingSize:   10_000,
        MaxRetries: 5,
    })
    defer b.Close()

    // Send a message — broker handles chunking, compression, ACK
    msgID, err := b.Send(context.Background(), []byte("hello from peer A"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("sent", msgID)

    // Receive
    msg, err := b.Recv(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("received: %s\n", msg.Payload)
}
```

## Wire Format

Every message frame begins with a 20-byte binary header (big-endian):

```
 0       4       6       8      9       10      12      14      18
 +-------+-------+-------+------+-------+-------+-------+-------+
 | MsgID |MsgType| CTent |Flags | Prio  |FragIdx|FragTot|PayLen |
 | uint32| uint16| uint16|uint8 | uint8 |uint16 |uint16 |uint32 |
 +-------+-------+-------+------+-------+-------+-------+-------+
```

- `MsgID` — unique message identifier (sender-assigned, monotonic)
- `MsgType` — DATA / ACK / NACK / PING / PONG
- `ContentType` — JSON / BINARY / TEXT / PROTO / AVRO / RAW
- `Flags` — compressed bit, fragmented bit
- `Priority` — 0 (low) to 255 (high)
- `FragIndex / FragTotal` — fragment position within a chunked message
- `PayloadLen` — byte length of the payload that follows

## Delivery Guarantees

| Event | Behaviour |
|---|---|
| Message sent | ACK expected within 30 s |
| No ACK received | Exponential backoff retry, up to 5 attempts |
| 5th retry exhausted | Message marked DEAD, caller notified |
| Connection drop | Auto-reconnect; unacked messages replayed from ring buffer |
| NACK received | Immediate retry (counts against max retries) |
| Ring buffer full | Oldest unacked slot evicted; caller receives ErrBufferFull |

## Benchmarks

Measured peer-to-peer on Mac to Oracle Cloud (public internet, ~80 ms RTT):

| Message Size | Throughput | Bandwidth |
|---|---|---|
| 256 B | 32,715 msg/s | 8.38 MB/s |
| 1 KB | 7,291 msg/s | 7.47 MB/s |
| 8 KB | 21,904 msg/s | 179 MB/s |
| 64 KB | 15,506 msg/s | 1,016 MB/s |

Run your own benchmark:

```sh
./bench --addr peer.example.com:9001 --size 8192 --duration 10s
```

## Configuration

### conn.Options

| Field | Default | Description |
|---|---|---|
| `MaxPayloadBytes` | 16 MB | Hard limit; frames larger than this are rejected |
| `PingInterval` | 15 s | Keep-alive ping cadence |
| `ReconnectDelay` | 1 s | Initial reconnect wait (doubles on each attempt) |

### broker.Options

| Field | Default | Description |
|---|---|---|
| `RingSize` | 10,000 | In-flight message slots |
| `MaxRetries` | 5 | Per-message retry limit before DEAD |
| `ACKTimeout` | 30 s | Deadline before first retry |

### chunker.Options

| Field | Default | Description |
|---|---|---|
| `FragmentSize` | 512 KB | Split threshold |
| `MaxStreams` | 1,024 | Concurrent reassembly streams |

### compress.Options

| Field | Default | Description |
|---|---|---|
| `Threshold` | 256 B | Payloads smaller than this skip compression |
| `Level` | zstd.SpeedDefault | zstd compression level |

## Limitations

- No end-to-end encryption (TLS planned; PRs welcome)
- No built-in peer discovery; relay required for NAT traversal
- Max single message payload: 16 MB (conn layer hard limit)
- Max fragment count per message: 65,535 (~32 GB theoretical)
- Ring buffer cap: 10,000 in-flight messages; overflow drops oldest

## License

MIT
