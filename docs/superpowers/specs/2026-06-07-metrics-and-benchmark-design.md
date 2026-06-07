# Design: human-readable sizes, speed metrics, debug mode, benchmark

Date: 2026-06-07
Status: Approved (pending spec review)

Adds observability to the p2p app: human-readable sizes everywhere, transfer
speed summaries, a `-debug` mode (chat latency, connect time, path), and a
`p2p bench` subcommand that measures the real connection across file sizes.

## Decisions (locked)
| Topic | Decision |
|---|---|
| Sizes/rates | Human-readable (KB/MB/GB, 1024-based), **always on** |
| Transfer speed | Printed **always** after each transfer |
| Latency / connect-time / path detail | Only under `-debug` |
| Benchmark | New `p2p bench` over the **real** relay/punch path (cross-machine) |

## Part 1 — `pkg/humanize`
Pure, dependency-free, unit-tested.

```go
func Bytes(n int64) string      // 1023->"1023 B", 1024->"1.0 KB", ->"12.3 MB","1.5 GB"
func Rate(n int64, d time.Duration) string // ->"2.9 MB/s"; d<=0 -> "—"
func Dur(d time.Duration) string // <1s->"850ms"; <60s->"4.2s"; else->"1m3s"
```
- 1024-based units: B, KB, MB, GB, TB. One decimal for KB+ (e.g. `1.0 KB`), no
  decimal for bytes.
- `Rate` = `Bytes(int64(float64(n)/d.Seconds()))+"/s"` with d>0 guard.

## Part 2 — transfer Stats
`transfer.Send` and `transfer.Receive` return transfer statistics so the CLI can
report speed.

```go
type Stats struct {
    Bytes    int64         // payload bytes transferred (offset total)
    Duration time.Duration // from first HEADER send/recv to final DONE
}

func Send(send SendFunc, in <-chan Msg, paths []string, progress ProgressFn) (Stats, error)
func Receive(send SendFunc, in <-chan Msg, destDir string, overwrite OverwriteFn, progress ProgressFn) (string, Stats, error)
```
- Send: start timer before HEADER; Bytes = final offset; Duration = until DONE.
- Receive: start timer at first message (the HEADER passed in); Bytes =
  trailer.Total; Duration = until DONE sent.
- Existing callers/tests updated for the new return arity.

## Part 3 — CLI metrics (`cmd/p2p`)
- New flag: `-debug` (bool).
- Progress bar uses `humanize.Bytes`: `[====      ] 43.7% (5.2 MB / 12.0 MB)`;
  for unknown total (archive) `5.2 MB`.
- After send: `✓ sent 12.3 MB in 4.2s (2.9 MB/s)`.
- After receive: `✓ saved movie.mp4 — 12.3 MB in 4.2s (2.9 MB/s), sha256 ok`.
- Under `-debug`:
  - Connect: `connected in 380ms (direct P2P)` — time from `conn.New` start to
    return; path from `dialer.LastDirect()`.
  - Chat latency: broker calls a new `OnAck(msgID, rtt)`; CLI prints
    `✓ delivered (12 ms)` per acked line (stderr, so it doesn't mingle with the
    chat transcript).

### broker change
Add to `broker.Config`:
```go
// OnAck is called when an ACK is received for a sent message, with the
// round-trip time from send to ACK. Optional.
OnAck func(msgID uint64, rtt time.Duration)
```
In `dispatch` on `MsgACK`, before/while freeing the slot, if `OnAck != nil`
compute `rtt = time.Since(slot.sendTime)` and invoke it. Must read `sendTime`
under the same lock that guards the ring; call `OnAck` outside the lock.

## Part 4 — `p2p bench`
A subcommand measuring the live connection across payload sizes.

Usage:
```
p2p bench                 # initiator: prints code, runs the matrix
p2p bench <code>          # responder
p2p bench -sizes 1KB,1MB,10MB,100MB   # override matrix (initiator side)
```
Default matrix: `1KB,64KB,1MB,10MB,50MB`.

Flow (initiator drives):
1. Connect like normal (`-relay`, TLS, crypto), record connect time + path.
2. Initiator sends a **bench plan** control message (`{"t":"bench","sizes":[...]}`,
   ContentJSON) so the responder knows the sizes and iteration count.
3. Latency probe: initiator sends one tiny ping and measures RTT to its ACK
   (via broker `OnAck`) — reported as connection latency.
4. For each size: initiator writes a temp file of that many random bytes and
   runs `transfer.Send`; responder runs `transfer.Receive` into a temp dir
   (overwrite=true, progress=nil). Both capture `Stats`. After each, initiator
   prints a row.
5. Initiator sends a **bench done** control (`{"t":"benchdone"}`); responder
   loop sees it and exits; both clean up temp files/dirs.

Reuse: parsing sizes (`parseSize("10MB")->int64`) lives in `pkg/humanize`
(inverse of `Bytes`, accepts `B/KB/MB/GB`, case-insensitive, integer or
decimal). The bench transfer reuses `pkg/transfer` over temp files, so it
exercises the exact production path (chunking, sha256, ACK, punch/bridge).

Report (stderr/stdout), example:
```
Relay     : 129.153.24.33.nip.io:9009
Connected : 372ms (direct P2P)
Latency   : 28 ms (round-trip)

  SIZE        TIME        THROUGHPUT
  1.0 KB      31ms        32 KB/s
  64.0 KB     45ms        1.4 MB/s
  1.0 MB      410ms       2.5 MB/s
  10.0 MB     3.6s        2.8 MB/s
  50.0 MB     17.9s       2.8 MB/s
```

Responder prints a short "bench complete" line.

### bench responder detection
The responder is just `p2p bench <code>`. The initiator's first message is the
bench plan (`t:"bench"`). The responder loop: read first inbound; if
`t:"bench"` → enter bench-receive loop (repeatedly: peek next; `header`→
`transfer.Receive`; `benchdone`→stop). This is isolated to the bench code path;
normal `p2p send`/`p2p <code>` is unchanged.

## Files
- Create `pkg/humanize/humanize.go` (+ test) — Bytes, Rate, Dur, parseSize/ParseSize.
- Modify `pkg/transfer/send.go`, `receive.go` — return `Stats`; add `Stats` type
  in `transfer.go`. Update `pkg/transfer/*_test.go`.
- Modify `pkg/broker/broker.go` (+ test) — `OnAck` callback.
- Modify `cmd/p2p/main.go` — `-debug`, speed summaries, humanized progress,
  connect timing, chat latency; update `transfer.Send/Receive` call sites.
- Modify `cmd/p2p/progress.go` — humanized `formatProgress`.
- Create `cmd/p2p/bench.go` (+ small test) — bench subcommand + size matrix.

## Error handling
- `Rate`/`Dur` guard zero/negative durations (`—`).
- `ParseSize` returns an error on bad input; bench prints usage and exits 2.
- Bench: any transfer error aborts the run with the size that failed; temp files
  always cleaned via `defer`.

## Testing
- `humanize`: table tests for Bytes (1023, 1024, 1.5MB, GB, TB), Rate, Dur,
  ParseSize round-trips and bad input.
- `transfer`: existing roundtrip tests assert `Stats.Bytes` equals payload size
  and `Duration > 0`.
- `broker`: OnAck fires once per acked message with rtt ≥ 0 (pipe-backed conn
  test).
- `cmd/p2p`: `formatProgress` shows MB; `ParseSize` wiring; bench size parsing.
- Manual/real: `p2p bench` mac↔ec2 (direct + `P2P_FORCE_RELAY=1` bridge),
  confirm the table prints sane throughput and the file-send speed summary.

## Out of scope (v1)
- Persisted/CSV bench output (stdout table only).
- Bidirectional simultaneous bench (one direction: initiator→responder).
- Graphs.
