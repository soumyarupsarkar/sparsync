# sparsync

`sparsync` is a Linux-first, rsync-style file synchronization tool optimized for very large directory trees and repeated syncs.

It keeps familiar `rsync`-like CLI ergonomics, but adds a dedicated high-performance path built around QUIC, io_uring, and the [`spargio`](https://github.com/soumyarupsarkar/spargio) runtime, and uses `rkyv` for zero-copy metadata serialization.

In our benchmarks (involving lots of little files), `sparsync` is about **30% faster than rsync-over-SSH on initial syncs and up to 10x faster on subsequent syncs.**

## Quick start

Familiar rsync-style invocation:

```bash
sparsync -avzP --delete -u ./src user@host:/srv/data
```

Dedicated high-performance QUIC path leveraging the [`sparsync://` protocol](./PROTOCOL.md):

```bash
sparsync -avzP --delete -u \
  --transport quic --bootstrap ssh --ssh-target user@host \
  ./src sparsync://host:7844/srv/data
```

Use the first form for compatibility.  
Use the second form when you want the fastest path.

Optionally, use the server CLI to manage the remote QUIC server. For example, to stop it:

```bash
sparsync server stop user@host
```

## Why use sparsync?

Use `sparsync` when:

- you sync large trees with many small files
- you run the same sync repeatedly
- metadata traversal, comparison, and per-file coordination dominate runtime
- you can run a dedicated remote server path for maximum performance

`sparsync` is especially aimed at workloads like home directory backups, NAS sync, and other cases where `rsync` works, but feels too slow on modern multi-core machines.

## When not to use sparsync

Don't use if you can't open UDP (QUIC) ports: for best performance, `sparsync` uses a remote daemon and QUIC over UDP. If you cannot use that deployment model, `sparsync` still supports SSH stdio transport, but likely won't be faster than plain `rsync`.

Don't use if you work across heterogeneous machines (e.g., Mac and Linux). sparsync trades off portability for performance: it is Linux-only (kernel version 6.0+ recommended for io_uring/msg_ring support), and strictly enforces compatibility between clients and servers (matching protocol expectations including endianness and binary version). This is often the case when transferring files between machines, but when it isn't, sparsync will fail early with an explicit error message without initiating data transfer instead of falling back to a slower compatibility mode.

Finally, it's not the fastest if you're mainly making small changes to very large files. Take a look at [Content Defined Chunking tools](https://github.com/google/cdc-file-transfer) instead for these workloads.

## What makes it different?

- **rsync-style CLI** with support for local copy, SSH stdio transport, and `sparsync://` QUIC transport
- **parallel scan, hash, and transfer pipeline**
- **persistent hash cache** to accelerate repeated syncs
- **batched control flow** to reduce per-file overhead
- optional **mTLS** authorization for QUIC server mode
- built on **Spargio** and `io_uring` for high concurrency on Linux

## Note: why not just rsync?
`rsync` is mature, widely deployed, portable, and generally excellent software. The ability to specify any remote shell composably enables it to leverage any protocol. It should remain the default.

However, when syncing a large file tree (e.g., home dir backup) between two systems, it can take hours; `sparsync` is noticeably faster.

`sparsync` is designed to reduce bottlenecks when syncing lots of little files (metadata traversal, scheduling overhead, and per-file coordination) by batching control work, parallelizing scan/hash/transfer stages, and keeping transport overhead low on repeat runs.

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

Current published snapshots are in [`BENCHMARKS.md`](./BENCHMARKS.md). The latest section (March 7, 2026) includes medians from repeated localhost-loopback runs on a personal laptop.

### Latest published snapshot (from `BENCHMARKS.md`)
Dataset in that run:
- `1008` files
- `20,873,216` bytes total (~20 MB)
- Changed-set phase modifies `100` small files

Baseline harness (`RUNS=3`, QUIC vs rsync over SSH):

| Metric | sparsync QUIC (ms) | rsync over SSH (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 404 | 582 | 0.69x |
| Second sync (no changes) | 29 | 245 | 0.12x |
| Changed sync | 52 | 262 | 0.20x |

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
`sparsync` is currently Linux-only and experimental. Do not treat it as production-hardened software yet.

Further performance and compatibility work is pending.

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
