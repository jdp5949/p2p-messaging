# p2p-messaging — Build Plan

Direct P2P messaging between two hosts. No broker in hot path. Like Solace/TIBCO but P2P. Built in Go on top of [croc](https://github.com/schollz/croc).

## What it does

- Two peers connect via croc relay (rendezvous + PAKE + TLS), then communicate directly
- Bidirectional messaging with explicit ACK/NACK per message
- Large messages chunked and sent over parallel streams (like croc does for files)
- Auto-reconnect with unacked message replay from in-memory ring buffer
- Mixed payload: JSON, binary, text, protobuf, Avro, raw bytes
- zstd compression for payloads > 256 bytes

## Wire format

Fixed 20-byte binary header + raw payload. Single `io.ReadFull` parse. No encoding library overhead.

```
[MsgID: 8][MsgType: 1][ContentType: 1][Flags: 1][Priority: 1][FragIndex: 2][FragTotal: 2][PayloadLen: 4]
```

## Delivery guarantees

- Sender tracks in-flight messages in ring buffer (default 10K slots)
- ACK → slot freed
- NACK → error callback, slot freed
- Timeout (30s) → exponential backoff retry (max 5 attempts)
- Dead → `OnDead` callback

## Build order

- [ ] `pkg/protocol` — header encode/decode, types, constants
- [ ] `pkg/compress` — zstd wrap with threshold logic
- [ ] `pkg/conn` — TLS conn wrap, framed read/write, reconnect loop
- [ ] `pkg/chunker` — split/assemble large messages + parallel streams
- [ ] `pkg/broker` — public API: send queue, ACK/NACK tracker, retry buffer
- [ ] `cmd/peer` — CLI to start a peer node
- [ ] `cmd/relay` — optional self-hosted relay server
- [ ] `examples/` — JSON, binary, pubsub-style usage examples

## Dependencies

```
github.com/schollz/croc/v10       # relay + PAKE + TLS
github.com/klauspost/compress     # zstd
```

## Key limits

| Param | Default |
|---|---|
| Chunk size | 512 KB |
| Parallel streams | 8 |
| Max message size | ~32 GB (65535 chunks) |
| Retry buffer | 10,000 slots |
| ACK timeout | 30s |
| Max retries | 5 |
