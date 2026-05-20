# p2p-messaging

Direct peer-to-peer messaging library in Go. No broker in the hot path. Like Solace or TIBCO — but P2P, minimal setup, MIT licensed.

Now with **E2E encryption** (Noise Protocol), **WAL persistence**, and **NAT hole-punch**.

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
  crypto (Noise XX/KK + AES-256-GCM)  crypto
      |                                   |
  compress (zstd, threshold 256 B)    compress
      |                                   |
  protocol (20-byte binary header)    protocol
      |                                   |
      +--- direct TCP (hole-punch) --------|
            OR relay byte-bridge
                    |
               cmd/relay
         (rendezvous + hole-punch)
```

## Packages

| Package | Role |
|---|---|
| `pkg/protocol` | 20-byte binary wire header: MsgID, MsgType, ContentType, Flags, Priority, FragIndex, FragTotal, PayloadLen |
| `pkg/compress` | zstd compression, threshold 256 bytes, pool-based encoder/decoder |
| `pkg/conn` | Framed TCP read/write, auto-reconnect, ping loop, 16 MB payload guard |
| `pkg/chunker` | 512 KB fragment split/reassemble, out-of-order delivery, 1024 concurrent stream cap |
| `pkg/broker` | Ring buffer 10 K slots, ACK/NACK per message, exponential backoff retry, reconnect replay |
| `pkg/crypto` | Noise XX/KK handshake, Ed25519 identity keys, AES-256-GCM session encryption |
| `pkg/wal` | Append-only WAL for at-least-once delivery across crashes |
| `pkg/holepunch` | TCP simultaneous-open NAT hole-punch with relay-assisted coordination |

## Commands

| Command | Role |
|---|---|
| `cmd/peer` | CLI peer node |
| `cmd/relay` | TCP rendezvous relay + hole-punch coordinator (public IP required on one side) |
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

### Run peers (plaintext)

```sh
# Peer A (sender)
./peer --relay relay.example.com:9000 --room myroom --send

# Peer B (receiver)
./peer --relay relay.example.com:9000 --room myroom --recv
```

### Run peers (E2E encrypted, first connect with PAKE)

```sh
# Peer B — listen, share one-time code "test-2026" out-of-band
./peer --relay relay.example.com:9000 --room myroom --recv \
  -id ~/.p2p/id_ed25519 -known ~/.p2p/known_peers -pake test-2026

# Peer A — dial, uses same one-time code
./peer --relay relay.example.com:9000 --room myroom --send \
  -id ~/.p2p/id_ed25519 -known ~/.p2p/known_peers \
  -peer-name bob -pake test-2026
```

After the first connect, both peers pin each other's Ed25519 public key in `known_peers`. Subsequent connections use the faster KK pattern (no PAKE needed).

### Run peers with WAL persistence

```sh
./peer --relay relay.example.com:9000 --room myroom --send \
  -id ~/.p2p/id_ed25519 -wal /var/lib/p2p/send.wal
```

On restart, the peer replays unacked messages from the WAL before sending new ones.

## Using the Library

### Basic connection

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

### Encrypted connection with WAL

```go
package main

import (
    "context"
    "log"

    "github.com/jaypatel/p2p-messaging/pkg/broker"
    "github.com/jaypatel/p2p-messaging/pkg/conn"
    "github.com/jaypatel/p2p-messaging/pkg/crypto"
    "github.com/jaypatel/p2p-messaging/pkg/wal"
)

func main() {
    // Load or generate identity key
    id, err := crypto.LoadOrGenerateIdentity("~/.p2p/id_ed25519")
    if err != nil {
        log.Fatal(err)
    }

    // Load known peers (SSH known_hosts style)
    peers, err := crypto.LoadKnownPeers("~/.p2p/known_peers")
    if err != nil {
        log.Fatal(err)
    }

    // Dial with Noise XX + PAKE (first connect)
    c, err := conn.DialEncrypted(context.Background(), "peer.example.com:9001",
        crypto.Options{
            Identity:   id,
            KnownPeers: peers,
            PeerName:   "bob",
            PAKECode:   "test-2026", // empty string = KK pattern (known peer)
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    // Open WAL for crash-safe delivery
    w, err := wal.Open("/var/lib/p2p/send.wal")
    if err != nil {
        log.Fatal(err)
    }
    defer w.Close()

    // Replay unacked messages from previous run
    unacked, _ := w.Replay()
    for _, msg := range unacked {
        log.Printf("replaying unacked msgID=%d", msg.MsgID)
    }

    b := broker.New(c, broker.Options{
        RingSize:   10_000,
        MaxRetries: 5,
        WAL:        w,
    })
    defer b.Close()
}
```

## Security

### Identity keys

Each peer generates an Ed25519 key pair on first run. The private key is stored at `~/.p2p/id_ed25519` (mode 0600).

```
~/.p2p/
  id_ed25519        # Ed25519 private key (0600)
  id_ed25519.pub    # Ed25519 public key (0644)
  known_peers       # pinned remote keys (SSH known_hosts format)
```

`known_peers` format:

```
alice ed25519 base64encodedPublicKey==
bob   ed25519 base64encodedPublicKey==
```

### Noise Protocol handshake

p2p-messaging uses the [Noise Protocol Framework](https://noiseprotocol.org/) (`github.com/flynn/noise`), the same foundation used by WireGuard and the Lightning Network.

**First connect — pattern XX with PAKE code:**

```
Alice                              Bob
 |--- one-time code: "test-2026" (shared out-of-band) ---|
 |--> -> e                                                |
 |<-- <- e, ee, s, es                                     |
 |--> -> s, se                                            |
 |   (both derive shared session key from X25519 ECDH    |
 |    + PAKE code as PSK, then pin remote static key)    |
 |<======== AES-256-GCM encrypted frames ===============>|
```

**Subsequent connect — pattern KK (both pubkeys known, no PAKE):**

```
Alice                              Bob
 |--> -> e, es, ss                                        |
 |<-- <- e, ee, se                                        |
 |<======== AES-256-GCM encrypted frames ===============>|
```

Key exchange uses **X25519 ECDH**. Session encryption uses **AES-256-GCM**. After the first handshake, the remote peer's static public key is pinned in `known_peers` — TOFU with a PAKE bootstrap.

### CLI flags (`cmd/peer`)

| Flag | Description |
|---|---|
| `-id <path>` | Identity key path (default: `~/.p2p/id_ed25519`) |
| `-known <path>` | known_peers file path (default: `~/.p2p/known_peers`) |
| `-peer-name <name>` | Remote peer name for known_peers lookup |
| `-pake <code>` | One-time PAKE code for first connect (XX pattern) |
| `-no-crypto` | Disable encryption (plaintext TCP) |
| `-wal <path>` | WAL file path (empty = no persistence) |

### CLI flags (`cmd/bench`)

| Flag | Description |
|---|---|
| `-id <path>` | Identity key path |
| `-known <path>` | known_peers file path |
| `-peer-name <name>` | Remote peer name |
| `-pake <code>` | One-time PAKE code |
| `-crypto` | Enable encryption for benchmark |
| `-wal <path>` | WAL file path |

## Persistence (WAL)

`pkg/wal` provides an append-only write-ahead log for at-least-once delivery across process crashes.

**File format** (binary, big-endian):

```
[Op:1][Length:4][MsgID:8][Payload:N]
```

| Op | Value | Meaning |
|---|---|---|
| `OpSend` | 0x01 | Message written to WAL before network send |
| `OpAck` | 0x02 | ACK received; entry eligible for compaction |

**Lifecycle:**

1. `WAL.Append()` — called before each network write (crash-safe ordering)
2. `WAL.Ack()` — called when ACK received (marks for compaction)
3. `WAL.Replay()` — called on startup; returns unacked messages for re-queue
4. `WAL.Compact()` — runs every 60 s; rewrites file keeping only unacked entries

**Semantics:** at-least-once delivery. Duplicate suppression is the receiver's responsibility (use `MsgID` for deduplication).

## NAT Traversal

### Hole-punch (`pkg/holepunch` + `cmd/relay`)

Both peers connect to the relay first. The relay exchanges public and local addresses (JSON `Info` messages) and sends a `START` signal to both simultaneously.

```
Peer A                  Relay                   Peer B
  |---- register -------->|<------ register ----|
  |<--- Info(B addrs) ----|------ Info(A addrs)->|
  |<========= START simultaneously ============>|
  |------- TCP simultaneous-open (SO_REUSEPORT) --|
  |<============ direct TCP link ===============>|
         (fallback: relay byte-bridge after 5 s)
```

The relay sends `PUNCH_OK` when a direct path is confirmed. If the 5-second window expires without success, traffic falls back through the relay byte-bridge automatically.

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
| Process crash | WAL replay re-queues unacked messages on next startup |

## Benchmarks

Measured Mac to Oracle Cloud VM (public internet, ~50 ms RTT), 5,000 x 1024-byte messages:

| Mode | Throughput | Bandwidth |
|---|---|---|
| Plaintext TCP | 23,059 msg/s | 23.6 MB/s |
| Noise XX + AES-256-GCM | 7,053 msg/s | 7.2 MB/s |

Historical single-stream numbers (various message sizes, ~80 ms RTT):

| Message Size | Throughput | Bandwidth |
|---|---|---|
| 256 B | 32,715 msg/s | 8.38 MB/s |
| 1 KB | 7,291 msg/s | 7.47 MB/s |
| 8 KB | 21,904 msg/s | 179 MB/s |
| 64 KB | 15,506 msg/s | 1,016 MB/s |

Run your own benchmark:

```sh
# Plaintext
./bench --addr peer.example.com:9001 --size 1024 --count 5000

# Encrypted
./bench --addr peer.example.com:9001 --size 1024 --count 5000 \
  -crypto -id ~/.p2p/id_ed25519 -known ~/.p2p/known_peers \
  -peer-name bob -pake test-2026
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

### crypto.Options

| Field | Default | Description |
|---|---|---|
| `Identity` | — | Ed25519 identity loaded from disk |
| `KnownPeers` | — | Pinned remote keys (SSH known_hosts style) |
| `PeerName` | — | Name to look up in known_peers |
| `PAKECode` | `""` | One-time code for XX pattern; empty uses KK pattern |

### wal.Options

| Field | Default | Description |
|---|---|---|
| `Path` | — | File path for the WAL |
| `CompactInterval` | 60 s | How often to run compaction |

## License

MIT
