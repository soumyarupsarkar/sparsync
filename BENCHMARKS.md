# Benchmark Summary

## Latest Snapshot (March 7, 2026)

After planner quick-check + hash parallelism + blocking-path cleanup, median benchmark reruns were executed with:

- `RUNS=3 TRANSPORTS=ssh ./scripts/bench_remote_rsync_vs_sparsync_median.sh`

Dataset (median across runs):

- `files=1008`
- `bytes=20873216`

### SSH Transport (`RUNS=3`)

| Metric | sparsync (ms) | rsync over SSH (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 404 | 582 | 0.69x |
| Second sync (no changes) | 29 | 245 | 0.12x |
| Changed sync | 52 | 262 | 0.20x |

## Previous Snapshot (March 6, 2026)

After QUIC no-change overhead reductions (`7f3137c`), median benchmark reruns were executed with:

- `RUNS=3 TRANSPORTS=ssh ./scripts/bench_remote_rsync_vs_sparsync_median.sh`

Dataset (median across runs):

- `files=1008`
- `bytes=20873216`

### Baseline Harness (`RUNS=3`, QUIC vs rsync over SSH)

| Metric | sparsync QUIC (ms) | rsync over SSH (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 416 | 520 | 0.80x |
| Second sync (no changes) | 26 | 245 | 0.11x |
| Changed sync | 51 | 263 | 0.19x |

### Unified 3-Mode Comparison (`RUNS=3`, same per-run dataset for all modes)

| Mode | Initial sync (ms) | Second sync (ms) | Changed sync (ms) |
|---|---:|---:|---:|
| rsync over SSH | 514 | 247 | 261 |
| sparsync over QUIC | 436 | 29 | 51 |
| sparsync over SSH stdio | 896 | 490 | 511 |

Ratios vs `rsync over SSH` in the unified run:

- `sparsync over QUIC`: `0.85x` (first), `0.12x` (second), `0.20x` (changed)
- `sparsync over SSH stdio`: `1.74x` (first), `1.98x` (second), `1.96x` (changed)

## Post-Compat Snapshot (March 5, 2026)

After implementing `RSYNC_COMPAT` flows (rsync-style CLI, `enroll`, `server` lifecycle, local/SSH/QUIC paths, reverse direction orchestration), median benchmark reruns were executed with:

- `RUNS=3 TRANSPORTS=ssh ./scripts/bench_remote_rsync_vs_sparsync_median.sh`
- `RUNS=3 TRANSPORTS=daemon ./scripts/bench_remote_rsync_vs_sparsync_median.sh`

Dataset (median across runs):

- `files=1008`
- `bytes=20873216`

### SSH Transport (`RUNS=3`)

| Metric | sparsync (ms) | rsync over SSH (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 403 | 510 | 0.79x |
| Second sync (no changes) | 34 | 244 | 0.14x |
| Changed sync | 56 | 260 | 0.22x |

### Daemon Transport (`RUNS=3`)

| Metric | sparsync (ms) | rsync daemon (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 396 | 228 | 1.74x |
| Second sync (no changes) | 31 | 137 | 0.23x |
| Changed sync | 57 | 156 | 0.37x |

## Historical Snapshot (March 4, 2026)

Remote-style comparison was run with:

- `RUNS=5 TRANSPORTS=daemon ./scripts/bench_remote_rsync_vs_sparsync_median.sh`
- `RUNS=5 TRANSPORTS=ssh ./scripts/bench_remote_rsync_vs_sparsync_median.sh`
- `SPARSYNC_BIN=./target/release/sparsync` (auto-selected by script)
- Host: `nproc=16` logical cores
- Default dataset/profile:
  - `SMALL_DIRS=10`
  - `SMALL_FILES_PER_DIR=100`
  - `SMALL_FILE_SIZE=4096`
  - `LARGE_FILES=8`
  - `LARGE_FILE_SIZE=2097152`
  - `CHANGED_FILES=100`
  - `SPARSYNC_FIRST_COLD=0` (normal path for all three runs)
  - `RSYNC_ARGS="-a --delete"`

Dataset:

- `files=1008`
- `bytes=20873216`
- Layout:
  - `small/dir_0..9/file_0..99.bin` (`1000` files total, `4096` bytes each, random `/dev/urandom` payloads)
  - `large/chunk_0..7.bin` (`8` files total, `2097152` bytes each, random `/dev/urandom` payloads)

Changed phase (`CHANGED_FILES=100`):

- Benchmark appends one line (`delta-<n>`) to `100` files selected in lexical order from `find <src>/small -type f | sort`.
- With default dataset naming, this means all files under `small/dir_0` are modified in lexical filename order.
- The mutation pass is done separately for the `sparsync` source tree and the `rsync` source tree so both tools process equivalent churn.

Results below are medians of 5 consecutive runs per transport mode after integrating long-lived framed streams over QUIC (`read_chunk` incremental decode + stream reuse for multi-request transfer paths).

### Daemon Transport (`RSYNC_TRANSPORT=daemon`)

| Metric | sparsync (ms) | rsync (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 402 | 230 | 1.75x |
| Second sync (no changes) | 29 | 136 | 0.21x |
| Changed sync | 54 | 155 | 0.35x |

### SSH Transport (`RSYNC_TRANSPORT=ssh`)

| Metric | sparsync (ms) | rsync over SSH (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 419 | 552 | 0.76x |
| Second sync (no changes) | 28 | 246 | 0.11x |
| Changed sync | 50 | 270 | 0.19x |

Interpretation:

- In daemon mode (unencrypted rsync), `sparsync` is still slower on initial cold copy.
- In daemon mode, `sparsync` remains faster on warm/churned runs.
- In SSH mode (encrypted rsync), `sparsync` is faster in all three phases in this sample.

Experimental notes:

- `--cold-start` is implemented. In a 3-run sample on this dataset, first-sync median was `491ms` (`sparsync_first_ms`) versus `229ms` (`rsync_remote_first_ms`), so it remains slower than the normal path here.
- Server write fan-out can be tuned with `SPARSYNC_BATCH_WRITE_CONCURRENCY` (auto-tuned by default).
- Latest profiled first-sync pass shows `control_frames=5` and `streams_opened=4` (from `SPARSYNC_PROFILE=1`), reflecting one fewer client stream open than prior snapshots due to long-lived framed stream reuse.

## Compatibility Refactor Smoke (March 5, 2026)

Quick post-`RSYNC_COMPAT` implementation sanity run:

- Command:
  - `SMALL_DIRS=2 SMALL_FILES_PER_DIR=20 LARGE_FILES=2 CHANGED_FILES=10 RSYNC_TRANSPORT=ssh ./scripts/bench_remote_rsync_vs_sparsync.sh`
- Dataset:
  - `files=42`
  - `bytes=4358144`

| Metric | sparsync (ms) | rsync over SSH (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 128 | 343 | 0.37x |
| Second sync (no changes) | 17 | 237 | 0.07x |
| Changed sync | 27 | 236 | 0.11x |

Note: this is a reduced-size smoke dataset to validate behavior after major CLI/transport compatibility changes, not a replacement for full benchmark medians.

## Profiling Notes (March 4, 2026)

- Profiling used `valgrind` (`callgrind` and `cachegrind`) plus targeted `perf stat` and `strace -c` passes.
- Top instruction consumers were memory initialization/copy (`memset`/`memcpy`) and QUIC/TLS crypto paths (`ring`/`quinn`), indicating first-sync is currently dominated by payload movement + encrypted transport overhead rather than scan/hashing.
- Scan/hashing is not the main bottleneck in the benchmark profile: first push logs consistently show single-digit to low-double-digit millisecond scan phases versus ~400ms+ total push elapsed.
- Full profiling/optimization log: [PERFORMANCE_LOG.md](./PERFORMANCE_LOG.md)

## Reproduce

```bash
cargo build --release
./scripts/bench_remote_rsync_vs_sparsync.sh
```

Encrypted rsync comparison:

```bash
RSYNC_TRANSPORT=ssh ./scripts/bench_remote_rsync_vs_sparsync.sh
```

For multiple runs and median:

```bash
./scripts/bench_remote_rsync_vs_sparsync_median.sh
```
