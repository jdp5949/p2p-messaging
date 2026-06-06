# Design: `p2p` croc-style CLI + hosted relay

Date: 2026-06-06
Status: Approved (pending spec review)

## Goal

Make this project as simple to use as `croc`. Today the `peer` binary only does
manual `-addr` / `-listen` direct TCP dialing, and the README documents
`--relay`/`--room`/`--send` flags that do not exist. We want:

```sh
# sender
p2p send
# -> code = 4-brave-tiger-comet

# receiver
p2p 4-brave-tiger-comet
```

After both sides connect, an interactive two-way terminal chat opens. The
connection survives short drops (retry for 60s) and reuses pinned crypto
credentials on reconnect.

## Decisions (locked)

| Topic | Decision |
|---|---|
| Relay | Our own `cmd/relay` (rendezvous + holepunch already built), hosted by us |
| Relay host | Oracle VM `129.153.24.33`, DNS `129.153.24.33.nip.io` |
| Relay port | `9009` (already open in iptables; croc's old port, nothing running) |
| TLS | Yes — TLS inside relay using existing Let's Encrypt nip.io cert |
| CLI name | `p2p` (`cmd/p2p`), installed on PATH via `go install` |
| Payload | Interactive text chat (matches "messaging" purpose) |
| Code phrase | `N-word-word-word`, doubles as relay sessionID + PAKE password |
| Reconnect | Remember partner ip:port + pinned keys; retry direct→relay 60s; then drop |

Default relay address baked into `cmd/p2p`: `129.153.24.33.nip.io:9009`,
overridable with `-relay host:port`.

## Architecture

```
  p2p send                                  p2p <code>
  --------                                  ----------
  generate code phrase                      parse code phrase
       |                                         |
  pkg/rendezvous (relay client)  <--TLS-->  pkg/rendezvous
       |   sessionID = code                      |
       |   exchange peer Info (ip:port)          |
       v                                         v
  holepunch.AttemptPunch  <==direct TCP==>  holepunch.AttemptPunch
       |   (fallback: relay byte-bridge)         |
       v                                         v
  crypto: PAKE(code) -> Noise XX -> pin     crypto (mirror)
       |   key in known_peers                    |
       v                                         v
  broker (ACK/retry) <--- chat lines --->   broker
       |                                         |
  reconnect manager (60s)                   reconnect manager (60s)
```

The relay only brokers the rendezvous (control channel, now TLS) and an optional
byte-bridge fallback. The actual chat path is direct P2P when hole-punch
succeeds, and is always Noise/AES end-to-end encrypted regardless of path.

## Components

### 1. `pkg/rendezvous` (new — the missing client)

The relay *server* (`cmd/relay`) already exists and matches two peers by a
sessionID line, exchanges JSON `peerInfo`, signals hole-punch, and bridges on
failure. There is **no client** for this protocol today. This package implements
it:

- `Dial(ctx, relayAddr, sessionID string, opts) (net.Conn, Info, error)`
- Opens TLS connection to relay (`tls.Dial`, ServerName = nip.io host).
- Writes line 1 = sessionID, line 2 = JSON `peerInfo{LocalAddrs:[...]}`.
- Reads partner `Info`, runs `holepunch.AttemptPunch(partner, localPort, timeout)`.
- On punch success: returns the direct `net.Conn`.
- On punch failure: signals `PUNCH_FAIL`, falls back to the relay byte-bridge
  conn (relay already implements this fallback server-side).
- Returns which path was used so the reconnect manager can prefer direct first.

**Dependency:** `pkg/holepunch` (existing `AttemptPunch`), `crypto/tls`.

### 2. `cmd/p2p` (new CLI)

Verbs:
- `p2p send` — generate code phrase, rendezvous, crypto handshake, open chat.
- `p2p <code>` — join with code, mirror.
- `p2p relay` — convenience to run the relay server locally (thin wrapper /
  alias of `cmd/relay`) for self-hosters. (Optional, low priority.)

Flags: `-relay` (default baked-in), `-id`, `-known` (reuse existing key/known
paths), `-no-crypto` (debug).

Output on `send` is croc-like:
```
Code is: 4-brave-tiger-comet
On the other computer run:  p2p 4-brave-tiger-comet
```

### 3. Code phrase

- `pkg/codephrase`: `Generate() string` -> `"<digit>-<word>-<word>-<word>"`
  from a built-in wordlist (e.g. EFF short list subset, ~256 words).
- `sessionID = sha256(codephrase)[:16]` so the raw words are not sent to the
  relay in clear; the relay only sees the hash. (croc-like privacy.)
- PAKE password = the full code phrase (shared secret both sides typed).

### 4. Crypto (reuse existing `pkg/crypto`)

- First connect: PAKE using the code phrase establishes a shared secret; run
  Noise XX; pin each side's Ed25519 public key into `known_peers`.
- Reconnect within the 60s window: use Noise KK with the pinned keys — no code
  phrase needed again.
- This matches the existing README "first connect with PAKE, later KK" behavior,
  now wired through the `p2p` flow automatically.

### 5. Reconnect manager (new, in `cmd/p2p` or `pkg/session`)

State remembered after first successful connect:
- partner last-known direct `ip:port`
- partner pinned Ed25519 key (from `known_peers`)
- the sessionID (for relay re-rendezvous)

On connection loss:
1. Retry **direct** dial to remembered `ip:port` (fast path) with backoff.
2. If direct fails, retry **relay re-rendezvous** (same sessionID).
3. Keep trying for **60 seconds total**.
4. On success: KK handshake with pinned key, resume chat, replay any unacked
   messages via broker WAL semantics.
5. After 60s of failure: print "peer lost, dropping", exit non-zero.

### 6. Relay TLS (modify `cmd/relay`)

- Add `-tls-cert` / `-tls-key` flags (or `-tls` to auto-load from
  `/etc/letsencrypt/live/129.153.24.33.nip.io/`).
- When set, wrap the listener in `tls.NewListener`. `c.RemoteAddr()` on a TLS
  conn still returns the real client IP:port, so hole-punch info stays correct.
- Plain TCP remains supported when flags are absent (local dev / tests).
- Cert renewal: certbot renews the file; a `systemd` reload or periodic restart
  picks up the new cert. (Phase: simple daily restart timer is acceptable v1.)

### 7. Deployment

Server Go is 1.13 (too old). Build on Mac, ship the binary:

```sh
GOOS=linux GOARCH=amd64 go build -o relay-linux ./cmd/relay
scp -i ssh-key relay-linux ubuntu@129.153.24.33:/home/ubuntu/p2p-relay/relay
```

- Install a `systemd` unit `p2p-relay.service` running:
  `relay -addr :9009 -tls -tls-cert <nip.io fullchain> -tls-key <nip.io privkey>`
- Port 9009 already accepted in iptables; verify Oracle security list allows it,
  otherwise open via OCI console (manual, user action).
- Smoke test: from Mac, `p2p send` + `p2p <code>` across two terminals using the
  public relay; confirm direct punch, then simulate drop to confirm 60s reconnect.

## Error handling

- Relay unreachable: clear message, suggest `-relay` or retry.
- No partner within relay wait timeout: "no peer joined with that code", exit.
- PAKE mismatch (wrong/typo code): "code did not match, check the phrase", exit.
- Hole-punch fail: transparent fallback to relay bridge (log once at debug).
- 60s reconnect exhausted: "peer lost", exit non-zero.

## Testing

- `pkg/codephrase`: unit — format, determinism of hash, wordlist size.
- `pkg/rendezvous`: integration with a local in-process relay (plain TCP) —
  two clients match, exchange info, bridge fallback path.
- `cmd/relay` TLS: integration — start TLS relay with a self-signed cert, client
  `tls.Dial` with `InsecureSkipVerify` in test, confirm RemoteAddr preserved.
- Reconnect manager: unit with a fake transport that drops then recovers within
  60s, and one that never recovers (asserts drop after 60s).
- Manual E2E: two terminals against the deployed public relay.

## Out of scope (YAGNI for v1)

- File transfer (chat only for now; can add `p2p send <file>` later).
- Multi-peer rooms (1:1 only).
- nginx/Cloudflare fronting (TLS lives in relay directly).
- Cert auto-reload without restart (daily restart timer is fine v1).
- Replacing/removing the old `cmd/peer` (leave it; `p2p` is the new entry point).

## README / docs follow-up

- Replace the false `--relay/--room/--send` Quick Start with the real `p2p`
  flow.
- Document `go install github.com/jdp5949/p2p-messaging/cmd/p2p@latest`.
- Note the public relay address and how to self-host (`p2p relay` / `cmd/relay`).
