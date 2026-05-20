# p2p-messaging

Direct peer-to-peer messaging between two hosts. No broker. No middleware. Built in Go on top of [croc](https://github.com/schollz/croc).

Like Solace or TIBCO — but P2P, minimal setup, MIT licensed.

## Features

- Direct TLS connection (croc relay for rendezvous only)
- ACK/NACK per message with automatic retry
- Large message chunking with parallel streams
- Mixed payload: JSON, binary, text, protobuf, Avro, raw bytes
- Auto-reconnect with unacked message replay
- zstd compression

## Status

Early development. See [plan.md](plan.md) for build roadmap.

## License

MIT
