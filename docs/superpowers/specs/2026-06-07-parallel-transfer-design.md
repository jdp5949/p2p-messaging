# Design: parallel multi-stream file transfer

Date: 2026-06-07
Status: Approved (pending spec review)

Speed up file transfer by sending over N parallel connections. A single TCP
stream can't fill a real internet path (measured: ~18–34 Mbps single vs ~46 Mbps
with 4 streams to the same host). Chat is unchanged.

## Decisions (locked)
| Topic | Decision |
|---|---|
| Default streams | 4; `-streams N` override (1 = today's single-stream path) |
| Failure | shortfall → use `min(opened)` (down to 1); mid-transfer drop → abort with clear error (SHA guards integrity) |
| Scope | files/dirs only; chat single-stream as today |
| Crypto | each stream its own Noise (PAKE from the same code) |

## Stream abstraction
`pkg/transfer` gains a tiny interface so it doesn't depend on crypto directly:

```go
type Stream interface {
    WriteMsg(p []byte) error      // sends one framed message
    ReadMsg() ([]byte, error)     // reads one framed message
    Close() error
}
```
`cmd/p2p` adapts `*crypto.Session` to `Stream` (WriteMsg = Session.Write; ReadMsg
reads into a 1 MB buffer via Session.Read; both are already frame-oriented).

## Connection setup (cmd/p2p)
From one code, derive `sessionID_i = codephrase.SessionID(code + "#" + i)`.
1. **Stream 0** (control): `rendezvous.Dial(sessionID_0)` → `crypto.Handshake`
   (PAKE=code, PeerName=sessionID_0). Same as today; if this fails, abort.
2. Initiator writes control `{"t":"want","n":N}` on stream0; joiner reads N.
3. Both open streams `1..N-1` concurrently, each `rendezvous.Dial(sessionID_i)` +
   `crypto.Handshake` (PAKE=code, PeerName=sessionID_i), with a 12s deadline.
   `k_self` = number successfully opened (including stream0).
4. Exchange counts on stream0: initiator `{"t":"opened","k":k_init}`, joiner
   `{"t":"opened","k":k_join}`. Both compute `m = min(k_init, k_join)` (≥1).
5. Keep `streams[0..m-1]`, close the rest.
6. If `m == 1`: fall back to the existing single-stream `transfer.Send/Receive`
   over stream0 (no behavior change). Else run the parallel protocol below.

This negotiation makes the two sides agree on a usable stream count even if one
side opened fewer (asymmetry), and degrades cleanly to 1.

## Parallel transfer protocol (`pkg/transfer`)
`SendParallel(streams []Stream, paths []string, progress ProgressFn) (Stats, error)`
`ReceiveParallel(streams []Stream, destDir string, overwrite OverwriteFn, progress ProgressFn) (string, Stats, error)`

Wire (control on `streams[0]`; data on all):
- `streams[0]` HEADER (json): `{kind:file|archive, name, size, mode, sha256, streams:m}`.
  - single file: `size`, `mode`, whole-file `sha256` (sender reads the file once
    to hash, then again to send — two passes; acceptable). archive: tar to a temp
    file first, hash it, then send; `size` = tar size.
- **Range split:** the payload (file or temp tar) of length `L` is split into `m`
  contiguous ranges `r_i = [i*L/m, (i+1)*L/m)`. Stream `i` sends its range as
  `DATA` messages: `[8-byte absolute offset][bytes]`, ~512 KB each. Uses
  `ReadAt` so streams read their range independently.
- Each stream sends an `EOS` (json `{t:eos}`) after its range.
- After all `m` streams finish sending, `streams[0]` sends TRAILER (json
  `{t:trailer, total:L}`).
- Receiver pre-creates `name.part`, truncates to `size`, then for each stream
  reads DATA → `WriteAt(offset)`. It knows `m` from HEADER; it waits for `m`
  EOS + the TRAILER. Then it re-reads the temp file, verifies `sha256`, applies
  overwrite policy + rename (file) or unpacks tar (archive), and sends `DONE` on
  `streams[0]`. Sender waits for `DONE` (30s ack timeout, as today).

Reassembly is offset-based (`WriteAt`), so cross-stream interleaving is correct
by construction. Progress = sum of bytes written across streams.

## Error handling
- Any stream read/write error mid-transfer → abort the whole transfer with a
  clear error; receiver discards `name.part`. (Re-run; SHA guarantees no silent
  corruption.)
- `m == 1` → single-stream fallback path (unchanged reliability + 60s reconnect).
- Multi-stream path has **no** per-stream reconnect in v1 (documented).

## CLI / bench
- `cmd/p2p`: `-streams N` (default 4) on `send`. The receiver (`p2p <code>`)
  needs no flag — it learns `m` via negotiation. `-streams 1` forces today's path.
- `p2p bench` uses the parallel path too (so the table reflects multi-stream),
  honoring `-streams`.
- `-debug` prints the negotiated stream count and per-stream connect path.

## Files
- Create `pkg/transfer/parallel.go` (+ `parallel_test.go`) — Stream interface,
  SendParallel, ReceiveParallel, range split/merge.
- Modify `cmd/p2p/main.go` — `-streams`, multi-session setup + negotiation,
  Stream adapter, dispatch parallel vs single.
- Create `cmd/p2p/streams.go` (+ test) — session opening, min-negotiation,
  `sessionStream` adapter (crypto.Session → transfer.Stream).
- Modify `cmd/p2p/bench.go` — use the parallel path.
- Modify README + docs/site — document `-streams`.

## Testing
- `pkg/transfer` parallel: in-memory `Stream` pairs (channel-backed) — N=3
  roundtrip of a multi-MB blob, assert bytes + sha; range split covers exact +
  remainder; out-of-order WriteAt correctness; m=1 equals single-stream.
- `cmd/p2p`: min-negotiation unit (min of two counts); sessionStream adapter
  read/write roundtrip over a net.Pipe + crypto handshake.
- Real ec2: `p2p bench -sizes 10MB,50MB` single (`-streams 1`) vs 4-stream;
  expect higher throughput. 100 MB file sha-match over 4 streams.

## Out of scope (v1)
- Per-stream reconnect/resume.
- Dynamic/adaptive stream count.
- Parallelizing chat.
