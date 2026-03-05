# sparsync

`sparsync` is a flagship showcase for the `spargio` runtime and companion protocol crates.

Goal: become the fastest lightweight Rust rsync-style sync tool for large trees and high-concurrency transfer workloads.

## What It Implements Now

- Fast parallel directory walk on top of `spargio::fs::read_dir`
- Full-file hashing with BLAKE3 + persistent source hash cache
- Parallel file transfer over `spargio-quic` (`QuicEndpoint`)
- Protocol compatibility handshake (`Hello` frame version check)
- Resume support with partial-file restart offsets
- Binary protocol framing with `rkyv` control messages (no JSON wire headers)
- Batched init + small-file multi-upload streams (fewer round trips)
- Batched chunk upload streams for resumed/large-file paths
- Direct initialized-file batching for medium files (reduced first-sync stream churn)
- Optional per-chunk compression (Zstd)
- Optional mTLS client auth (`serve --client-ca`, `push --client-cert/--client-key`)
- Prefix-scoped authorization policies (`auth issue-client --allow-prefix`)
- Auth CLI for mTLS lifecycle (`auth init-server|issue-client|revoke-client|rotate|status`)
- Structured auth audit logs for allow/deny and per-connection bytes transferred
- SSH bootstrap + one-command local->remote sync (`sync /src user@host:/dest`)
- Synthetic benchmark harness for scan/task/I/O pressure

## Quick Start

1. Build:

```bash
cargo build --release
```

2. Generate cert/key:

```bash
./target/release/sparsync gen-cert \
  --cert ./certs/server.cert.der \
  --key ./certs/server.key.der \
  --name localhost
```

3. Start receiver:

```bash
./target/release/sparsync serve \
  --bind 0.0.0.0:7844 \
  --destination /data/replica \
  --cert ./certs/server.cert.der \
  --key ./certs/server.key.der
```
Add `--preserve-metadata` for rsync-like mode/mtime preservation.

4. Push source tree:

```bash
./target/release/sparsync push \
  --source /data/source \
  --server 127.0.0.1:7844 \
  --server-name localhost \
  --ca ./certs/server.cert.der \
  --scan-workers 16 \
  --hash-workers 32 \
  --connections 1 \
  --parallel-files 64 \
  --chunk-size 1048576 \
  --compression-level 3
```

Re-running `push` skips already complete files via persisted hashes.
You can save and reuse connection credentials with `--profile <name>`.

Use `--cold-start` for the experimental cold-copy fast path (optimized for initial empty-target syncs).
Set `SPARSYNC_PROFILE=1` to emit per-push transfer counters/timers.

## Secure mTLS Workflow

Initialize server-side auth materials:

```bash
./target/release/sparsync auth init-server --dir ~/.config/sparsync/auth --server-name sync-host
```

Issue a client certificate and authorize it:

```bash
./target/release/sparsync auth issue-client \
  --dir ~/.config/sparsync/auth \
  --client-id alice-laptop \
  --allow-prefix team-a/inbox
```

Use `--allow-prefix /` to grant access to the full server destination root.

Run server with client cert enforcement and authz policy:

```bash
./target/release/sparsync serve \
  --bind 0.0.0.0:7844 \
  --destination /data/replica \
  --cert ~/.config/sparsync/auth/server.cert.der \
  --key ~/.config/sparsync/auth/server.key.der \
  --client-ca ~/.config/sparsync/auth/client-ca.cert.der \
  --authz ~/.config/sparsync/auth/authz.json
```

Push using mTLS credentials:

```bash
./target/release/sparsync push \
  --source /data/source \
  --server 10.0.0.10:7844 \
  --server-name sync-host \
  --ca ~/.config/sparsync/auth/server.cert.der \
  --client-cert ~/.config/sparsync/auth/clients/alice-laptop.cert.der \
  --client-key ~/.config/sparsync/auth/clients/alice-laptop.key.der
```

Audit events are emitted on `sparsync::audit` (allow/deny decisions, authorized client identity,
destination path denials, and bytes written per connection).

## One-Command SSH Bootstrap Sync

For local->remote sync, `sync` defaults to:

- `--bootstrap ssh`
- `--transport quic`
- `--install auto`

Example:

```bash
./target/release/sparsync sync /data/source user@sync-host:/data/replica
```

This flow bootstraps remote auth material, issues client credentials, launches a one-shot remote
receiver, transfers over QUIC+mTLS, and stores a local profile for reuse.

## Scan Command

```bash
./target/release/sparsync scan \
  --source /data/source \
  --scan-workers 16 \
  --hash-workers 32 \
  --chunk-size 1048576 \
  --output /tmp/manifest.json
```

## Benchmark Command

Baseline synthetic profile:

```bash
./target/release/sparsync bench --files 1000000 --tasks 500000 --io-ops 100000 --in-flight 8192
```

With external comparisons:

```bash
./target/release/sparsync bench \
  --files 1000000 --tasks 500000 --io-ops 100000 --in-flight 8192 \
  --rsync-command "rsync -a --delete /data/source/ /data/rsync-dst/" \
  --syncthing-command "./scripts/run_syncthing_bench.sh"
```

## Remote Rsync Comparison

Use the built-in remote benchmark harness (rsync daemon mode over TCP). It will prefer
`./target/release/sparsync` automatically when available:

```bash
./scripts/bench_remote_rsync_vs_sparsync.sh
```

Latest measured summary is tracked in [BENCHMARKS.md](./BENCHMARKS.md).
Profiling findings and optimization log are tracked in [PERFORMANCE.md](./PERFORMANCE.md).
Ongoing optimization roadmap is tracked in [PERF_PLAN.md](./PERF_PLAN.md).

Useful overrides:

```bash
SMALL_DIRS=20 SMALL_FILES_PER_DIR=200 LARGE_FILES=16 CHANGED_FILES=200 \
SPARSYNC_CONNECTIONS=1 SPARSYNC_PARALLEL_FILES=32 \
SPARSYNC_SCAN_WORKERS=16 SPARSYNC_HASH_WORKERS=32 SPARSYNC_FIRST_COLD=0 \
SPARSYNC_BATCH_WRITE_CONCURRENCY=48 \
RSYNC_ARGS="-a --delete" \
./scripts/bench_remote_rsync_vs_sparsync.sh
```

`SPARSYNC_BATCH_WRITE_CONCURRENCY` is optional; by default the server auto-tunes write fan-out per batch.
`RSYNC_TRANSPORT` controls comparison mode: `daemon` (default, unencrypted) or `ssh` (encrypted).
`SPARSYNC_PROFILE=1` enables client/server transfer profiling counters in logs.
`SPARSYNC_SMALL_FILE_MAX_BYTES`, `SPARSYNC_DIRECT_FILE_MAX_BYTES`, and `SPARSYNC_AUTO_CONNECTIONS` are optional tuning knobs for throughput experiments.

Encrypted comparison example:

```bash
RSYNC_TRANSPORT=ssh ./scripts/bench_remote_rsync_vs_sparsync.sh
```

Median report helper (defaults to `RUNS=5`, both transports):

```bash
./scripts/bench_remote_rsync_vs_sparsync_median.sh
```

## Architecture

- `src/scan.rs`: parallel scan + file hashing
- `src/transfer.rs`: client push pipeline + batched upload streams
- `src/server.rs`: QUIC receiver + batched chunk ingest
- `src/state.rs`: resume/complete state persistence
- `src/protocol.rs`: `rkyv` control frame schema + binary payload encoding
- `src/compression.rs`: Zstd per-chunk compression
- `src/bench.rs`: synthetic and external benchmark runner

## Roadmap to “Fastest”

- Add rolling delta signatures to avoid full-file retransmit on modified files
- Stream multiple chunks per stream to reduce QUIC stream-open overhead
- Upstream `spargio-quic`: add long-lived framed stream APIs for lower control/stream overhead
- Batch state persistence and add WAL for lower metadata overhead
- Add cross-file chunk dedup cache and send-side content-addressed reuse
- Add reproducible benchmark suites against tuned `rsync` and `syncthing`
