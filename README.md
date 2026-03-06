# sparsync
`rsync`-compatible high-performance remote file synchronization utility (and optional server) built on QUIC and the [`spargio`](https://github.com/soumyarupsarkar/spargio) `io_uring` runtime, using `rkyv` for zero-copy metadata serialization.

sparsync is optimized for synchronizing large file trees with extremely high concurrency (not multiplexing over an SSH session). It works well for backups to a NAS. It is Linux-only for now.

For such workloads, our benchmarks show **20% faster performance than rsync-over-SSH for initial syncs, and up to 10x faster subsequent syncs.**

## Overview
`sparsync` is a high-performance file synchronization tool, protocol, and server focused on syncing very large directory trees. It provides rsync-like CLI ergonomics, but is designed around QUIC for transport and `io_uring` for IO operations.

The primary optimization target is sync workloads with many files and high metadata churn, especially repeated syncs where planning, comparison, and control-plane overhead dominate. `sparsync` also supports local copy, SSH stdio transport, and a [QUIC transport path (`sparsync://`) with optional mTLS authorization](./PROTOCOL.md).

## Quick example
```bash
# rsync-style invocation (implicit `sync` command)
sparsync -av --delete ./src user@host:/srv/data
```

```bash
# explicit QUIC path with SSH bootstrap on first contact
# this initializes a sparsync server on a host, enrolls the client, and connects via QUIC+mTLS
sparsync -av \
  --delete \
  --transport quic \
  --bootstrap ssh \
  --ssh-target user@host \
  ./src sparsync://host:7844/srv/data
```

```bash
# stop the remote server if you'd like when done
sparsync server stop user@host
```

## Why sparsync?
Most file sync tools were designed for earlier machine profiles. Current systems often have many cores, NVMe-backed storage, and (most importantly) very large trees of many small files. In these workloads, sync time can be dominated by metadata traversal, scheduling overhead, and per-file coordination.

`sparsync` is designed to reduce those bottlenecks by batching control work, parallelizing scan/hash/transfer stages, and keeping transport overhead low on repeat runs.

## Why not just rsync?
`rsync` is mature, widely deployed, portable, and generally excellent software. The ability to specify any remote shell composably enables it to leverage any protocol. It should remain the default.

However, sometimes I kick off a sync for a large file tree (e.g., home dir backup) between two systems, it takes hours, and I get very impatient. `sparsync` is noticeably faster.

## When shouldn't I use sparsync?
For maximum performance, `sparsync` requires running a daemon on the server and connecting through UDP (QUIC) ports. If this isn't possible, `sparsync` still works but it likely won't be faster than just using `rsync`.

## Disclaimer
Like `spargio`, this is an experimental crate, with contributions from Codex that haven't been fully audited yet. Keep backups and do not use in production.

## Features
- rsync-style primary CLI: `sparsync [OPTIONS] SRC DEST`
- Transport auto-selection by endpoint (`user@host:/path` and `ssh://...` use SSH stdio; `sparsync://host[:port]/path` uses QUIC), with support for local copies without remote transport
- Optional mTLS and prefix-scoped authorization for QUIC server mode
- Benchmark scripts and CI benchmark gate for reproducible performance tracking after release

Additionally (like rsync),
- Resume support for interrupted transfers
- Include/exclude filtering, `--delete`, `--dry-run` (`-n`), `-u`, and `--bwlimit`
- Optional compression (`-z`) and metadata/xattr preservation (`-a`, `-X`)
- Parallel directory scan and hashing with persistent hash cache
- Batched init and upload control flow for reduced per-file overhead

## Architecture overview
At a high level, `sparsync` runs as a sync pipeline:

1. Directory scan and file enumeration.
2. Metadata and hash collection.
3. Source-vs-destination initialization/comparison.
4. Transfer scheduling (batch init, small-file batch upload, streamed chunk upload).
5. Optional metadata/xattr sync and delete plan application.

Runtime and protocol notes:
- Uses `spargio` runtime primitives for async I/O and task execution.
- Uses io_uring-backed nonblocking paths where available.
- Uses work-stealing task scheduling for scan/hash/transfer fan-out.
- Uses binary framed protocol messages (`rkyv`) plus explicit wire preamble checks.
- Performs protocol compatibility validation (`version`, `codec`, `endianness`, binary version).

## Benchmarks
Note that performance depends heavily on dataset shape, storage characteristics, CPU, network path, and transport mode.

Current published snapshots are in [`BENCHMARKS.md`](./BENCHMARKS.md). The latest section (March 6, 2026) includes medians from repeated localhost-loopback runs on a personal laptop.

### Latest published snapshot (from `BENCHMARKS.md`)
Dataset in that run:
- `1008` files
- `20,873,216` bytes total (~20 MB)
- Changed-set phase modifies `100` small files

Baseline harness (`RUNS=3`, QUIC vs rsync over SSH):

| Metric | sparsync QUIC (ms) | rsync over SSH (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 416 | 520 | 0.80x |
| Second sync (no changes) | 26 | 245 | 0.11x |
| Changed sync | 51 | 263 | 0.19x |

## Reproducing the benchmark
Build:
```bash
cargo build --release
```

Single-run comparison harness:
```bash
./scripts/bench_remote_rsync_vs_sparsync.sh
```

SSH transport comparison:
```bash
RSYNC_TRANSPORT=ssh ./scripts/bench_remote_rsync_vs_sparsync.sh
```

Median helper:
```bash
./scripts/bench_remote_rsync_vs_sparsync_median.sh
```

CI-style gate:
```bash
RUNS=3 TRANSPORTS="ssh" ./scripts/bench_ci_gate.sh
```

Synthetic runtime pressure benchmark:
```bash
sparsync bench --files 1000000 --tasks 500000 --io-ops 100000 --in-flight 8192
```

## When sparsync shines
- Large trees with many files and repeated sync cycles
- Workloads where no-change/changed-set latency is more important than first-copy simplicity
- Deployments that can use `sparsync://` QUIC transport directly
- Teams that want runtime-level profiling and iteration inside a Rust codebase

## When sparsync isn't the best
- As mentioned above, scenarios where QUIC is blocked and SSH fallback performance is the primary requirement
- Small, infrequent sync jobs where setup overhead dominates
- Environments requiring maximal rsync flag/protocol compatibility today

## Installation
From crates.io:
```bash
cargo install sparsync
```

From source:
```bash
git clone https://github.com/soumyarupsarkar/sparsync.git
cd sparsync
cargo install --path .
# or: cargo build --release
```

Binary path:
```bash
sparsync --help
# or: ./target/release/sparsync --help
```

## Project status
`sparsync` is experimental with further performance and compatibility work pending.

Current state:
- Core sync paths and benchmark harnesses are present.
- QUIC path is the primary performance path.
- SSH stdio path exists for compatibility/fallback use, with active optimization work.
- Interface and operational ergonomics are still evolving.

## Roadmap
- Improve transport and control-plane efficiency on SSH mode
- Expand compatibility coverage for commonly used rsync flags and edge cases
- Continue protocol/runtime profiling and benchmark-driven optimization
- Improve bootstrap/enrollment ergonomics and operational hardening

## Why Spargio?
Using `sparsync` exercises:
- io_uring-native async file/network paths
- high-volume task scheduling and work stealing
- framed protocol handling under concurrent sync pressure
- benchmark and profiling workflows for real sync workloads

Spargio offers these capabilities.

## Contributing
Contributions are welcome, especially around performance investigation, protocol correctness, compatibility, and reproducible benchmark methodology.

Please open an issue or PR with:
- clear repro steps
- expected vs. observed behavior
- benchmark data for performance improvements

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `sparsync` by you shall be licensed as MIT, without any
additional terms or conditions.

