# Design: croc-style file transfer + one-line installer

Date: 2026-06-06
Status: Approved (pending spec review)

Two independent features shipped in one batch:
1. **File transfer** — `p2p send <path...>` sends files/dirs croc-style over our existing reliable encrypted stack; receiver auto-saves, verifies SHA-256, shows progress.
2. **Installer scripts** — one-line `install.sh` (macOS/Linux) + `install.ps1` (Windows) that detect OS/arch, download the right release binary, and put `p2p` on PATH.

---

## Part 1 — File transfer

### Decisions (locked)
| Topic | Decision |
|---|---|
| Approach | croc-*style* on our own stack (broker/chunker/crypto/relay) — no croc code/dep |
| Scope | files + multiple files + directories (full) |
| Integrity | streaming whole-payload SHA-256, verified at end |
| Resume | none in v1 (drop ⇒ restart) |
| Overwrite | prompt `overwrite X? [y/N]` |
| End state | after transfer both sides exit (croc-like) |

### CLI
```
p2p send report.pdf              # single file
p2p send a.txt b.jpg pics/       # multiple paths + a directory
p2p <code>                       # receiver: auto-save + verify + progress
p2p send                         # (unchanged) interactive chat
```
`send` with ≥1 path → file mode; `send` with no path → existing chat. Receiver
auto-detects mode from the session HEADER (no extra flag).

### New package `pkg/transfer`
Two roles, both driven by an existing `*broker.Broker`:
- `Send(b *broker.Broker, paths []string, progress ProgressFn) error`
- `Receive(b *broker.Broker, destDir string, prompt OverwriteFn, progress ProgressFn) (string, error)`

`pkg/transfer` does NOT know about relays/crypto — it only speaks broker
messages. This keeps it unit-testable with an in-memory broker pair.

### Wire protocol (application framing over broker messages)
Three message kinds, distinguished by `protocol.ContentType` + a JSON `t` field:

1. **HEADER** — `ContentJSON`:
   ```json
   {"t":"header","kind":"file","name":"report.pdf","size":12345,"mode":420}
   ```
   `kind` is `"file"` (single regular file) or `"archive"` (tar stream for
   dirs / multiple paths). `size` is bytes when known (single file from
   `os.Stat`); `0`/omitted for archives (indeterminate). `mode` is the unix
   file mode for a single file.

2. **DATA** — `ContentBinary`: payload = `[8-byte big-endian offset][chunk bytes]`,
   chunk ≈ 512 KB. Receiver does `f.WriteAt(chunk, offset)` → reassembly is
   order-independent and idempotent under reconnect-replay (duplicate chunk just
   rewrites the same offset).

3. **TRAILER** — `ContentJSON`:
   ```json
   {"t":"trailer","sha256":"<hex>","total":12345}
   ```
   Sent after all DATA. Signals end-of-transfer.

Sender streams chunks via `broker.Send`; the broker ring (10 K slots) blocks
`Send` when full, giving natural backpressure so memory stays bounded for
multi-GB transfers (no whole-file buffering).

### Files vs directories vs multiple paths
- Exactly one path that is a regular file → `kind:"file"`, streamed raw, `name`
  = base name, `mode` preserved.
- A directory, or more than one path → `kind:"archive"`: sender writes a **tar**
  stream (POSIX tar via `archive/tar`) covering all paths (preserving relative
  paths + perms) and sends it as the byte stream. `name` = `"<first-base>.tar"`
  or `"bundle.tar"` for multiple. Receiver unpacks after verify.

### Integrity
Sender wraps its byte source in an `io.TeeReader` into a `sha256.Hash`; the
final hex digest goes in the TRAILER. Receiver maintains its own running
`sha256` over received bytes (written in offset order; for v1 chunks are sent
sequentially so the running hash is computed as chunks arrive in order — see
note). At TRAILER, receiver compares digests and byte counts. Mismatch →
delete the temp file, return an error, exit non-zero.

> Note on hashing + WriteAt: chunks are *sent* sequentially, so in the common
> path they arrive in order and the receiver hashes as it writes. To stay
> correct even if a reconnect-replay reorders a chunk, the receiver computes the
> SHA-256 by re-reading the finished temp file once at TRAILER time (single
> extra pass) rather than relying on arrival order. This is simple and robust.

### Receiver UX
- Writes to `name.part` in the destination dir (cwd by default).
- On TRAILER + hash OK: if final `name` exists → call `prompt("overwrite name? [y/N]")`;
  if declined, keep the `.part` as `name.part` and tell the user. Else atomic
  `os.Rename(name.part, name)`. For archives: unpack tar into dest with
  path-safety, then remove the temp tar.
- Progress: a `ProgressFn(done, total int64)` callback; CLI renders a bar with
  `%` when `total>0`, else a running byte count. Final line:
  `✓ saved report.pdf (12345 bytes, sha256 verified)`.

### Path safety (zip-slip)
When unpacking the tar, reject any entry whose cleaned path escapes the
destination: compute `filepath.Join(dest, entry)`, then verify
`strings.HasPrefix(filepath.Clean(joined)+sep, filepath.Clean(dest)+sep)`.
Reject absolute paths and any `..` traversal. Skip symlinks (or reject) to avoid
link-based escapes.

### CLI wiring (`cmd/p2p`)
`main` branches: if `args[0]=="send"` and `len(args)>1` → file-send mode:
build conn+broker as today, then `transfer.Send(b, paths, progressBar)`, then
exit. Receiver path (`p2p <code>`): after broker is up, read the first inbound;
if it is a transfer HEADER, run `transfer.Receive(...)`; otherwise fall into the
existing chat loop. (HEADER arriving first disambiguates file vs chat.)

### Error handling
- Unreadable source path → error before connecting.
- Hash/byte mismatch → delete temp, error.
- Disk full / write error → error, delete temp.
- Connection lost mid-transfer → broker's 60 s reconnect applies; if it gives up,
  transfer fails with "peer lost", temp removed.
- Overwrite declined → keep `.part`, non-fatal message.

### Testing
- `pkg/transfer` roundtrip over an in-memory broker pair (`net.Pipe`-backed
  conn, like existing broker tests): send a file, receive into a temp dir,
  assert bytes + name + mode identical.
- Directory roundtrip: tar of a small tree, unpack, assert tree + perms match.
- SHA-256 mismatch: corrupt one byte in transit (or force bad trailer) → receiver
  errors and removes temp.
- Zip-slip: craft a tar with a `../evil` entry → unpack rejects it.
- Offset reassembly: deliver chunks out of order / duplicated → file still
  correct (WriteAt idempotent).

### Out of scope (v1)
- Resume / partial restart.
- Compression of the stream (broker already zstd-compresses per message).
- Per-chunk hashing.
- Symlink preservation (skipped/rejected for safety).

---

## Part 2 — Installer scripts

### `install.sh` (POSIX sh; macOS + Linux)
Behavior:
1. `OS=$(uname -s)` → `Darwin`→`darwin`, `Linux`→`linux`; anything else → error
   with a pointer to releases.
2. `ARCH=$(uname -m)` → `x86_64|amd64`→`amd64`, `aarch64|arm64`→`arm64`; else error.
3. `ASSET=p2p-${OS}-${ARCH}`; `URL=https://github.com/jdp5949/p2p-messaging/releases/latest/download/${ASSET}`.
4. Download to a temp file with `curl -fsSL` (fallback `wget -qO`).
5. `chmod +x`.
6. Install dir: prefer `/usr/local/bin` if writable; else if `$(id -u)=0` use it;
   else try `sudo mv` when a tty is present; else fall back to `~/.local/bin`
   (create it) and print a PATH hint if it is not already on `PATH`.
7. macOS only: `xattr -d com.apple.quarantine <dest>` (ignore errors) so Gatekeeper
   does not block.
8. Verify: run `p2p` (expect usage, exit 2) or `command -v p2p`; print
   `✓ installed — run: p2p send`.

Idempotent, no interactive prompts beyond a possible `sudo` password. Usable as:
```sh
curl -fsSL https://raw.githubusercontent.com/jdp5949/p2p-messaging/main/install.sh | sh
```

### `install.ps1` (Windows PowerShell)
1. `$arch = amd64` (only build today).
2. Download `p2p-windows-amd64.exe` to `$env:LOCALAPPDATA\p2p\p2p.exe` (create dir).
3. Add `$env:LOCALAPPDATA\p2p` to the **user** PATH (`[Environment]::SetEnvironmentVariable('Path', ..., 'User')`) if not present.
4. Print `installed — open a new terminal, then: p2p send`.

Usage:
```powershell
irm https://raw.githubusercontent.com/jdp5949/p2p-messaging/main/install.ps1 | iex
```

### README
Replace the manual `curl`/`chmod`/`mv` block with the two one-liners (sh + ps1)
as the primary install path; keep `go install` and prebuilt-asset table as
alternatives.

### Testing (installer)
- Lint `install.sh` with `sh -n install.sh` (syntax) and, if available,
  `shellcheck`.
- Dry exercise the OS/arch mapping logic via a small `case` self-test in CI is
  optional; primary verification is a manual run on macOS arm64 + Linux amd64
  (download + `p2p` works).
- `install.ps1`: `pwsh -NoProfile -Command "$PSScriptRoot"` parse check if pwsh
  present; otherwise manual note.

> Scripts live on the `main` branch so the raw.githubusercontent URLs resolve
> only after this work merges. The one-liners are documented to point at `main`.
```
