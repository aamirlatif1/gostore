# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`gostore` is a simplified, S3-compatible distributed object storage system (early/in-progress). It has two largely independent subsystems that are not yet wired together:

- **`internal/cluster`** — peer-to-peer networking between nodes. Defines a pluggable `Transport` abstraction with a concrete TCP implementation.
- **`internal/store`** — local disk persistence using a content-addressable storage (CAS) layout.

`main.go` currently only exercises the cluster transport (starts a TCP listener on `:4000` and prints consumed RPCs). The store is not yet invoked from `main`.

## Commands

```bash
make build        # go build -o bin/gostore
make run          # build then ./bin/gostore (starts TCP listener on :4000)
make lint         # golangci-lint run
make test         # runs lint first, then `go test ./... -v`
```

`make test` runs `lint` as a prerequisite, so a lint failure blocks tests. To run tests without linting, or to target a single test:

```bash
go test ./internal/store/ -v
go test ./internal/cluster/ -run TestTCPTransport -v
```

## Architecture

### Cluster transport (`internal/cluster`)

The design separates the network mechanism from message handling via interfaces in `transport.go`:

- `Transport` — `ListenAndAccept()` + `Consume() <-chan RPC`. The intent is that UDP/websocket implementations can replace TCP without touching callers.
- `Peer` — a handle to a remote node (currently just `Close()`).
- `RPC` — the unit delivered over `Consume()`: `From net.Addr` + `Payload []byte`.

`TCPTransport` (`tcp_transport.go`) is the concrete implementation, configured via `TCPTransportOpts`:
- `HandshakeFunc` — per-connection handshake hook (`NOOPHandshakeFunc` is the default no-op in `handshake.go`).
- `Decoder` — turns bytes off the wire into an `RPC` (`encoding.go`). `DefaultDecoder` does a raw 1024-byte read; `GOBDecoder` uses `encoding/gob`.
- `OnPeer func(Peer) error` — callback after a successful handshake; returning an error drops the connection.

Connection flow: `ListenAndAccept` spawns `startAccptLoop`, which `go handleConnection`s each accepted conn. `handleConnection` runs handshake → `OnPeer` → a read loop that decodes RPCs and pushes them onto `rpcChan`. The loop exits cleanly on `net.ErrClosed`.

### Store (`internal/store`)

`Store` (`store.go`) persists blobs to disk with a pluggable `PathTransformFunc(string) PathKey` that maps a key to a directory path + filename:
- `CASPathTransformFunc` — SHA-1 hashes the key and splits the hex digest into 5-char path segments, so content is sharded across nested directories (content-addressable). This is the intended production layout.
- `PathKey.FullPath()` joins `Pathname/Filename`.

API: `WriteStream`, `Read`, `Delete`, `Has`. I/O is stream-based (`io.Reader`).

## Notes

- Go 1.26.2 (`go.mod`); module path `github.com/aamirlatif1/gostore`.
- `cmd/cli` and `config` directories exist but are empty placeholders.
- When adding a new transport, implement the `Transport`/`Peer` interfaces rather than extending `TCPTransport`.
