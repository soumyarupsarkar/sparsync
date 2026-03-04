# Benchmark Summary

## Latest Snapshot (March 4, 2026)

Remote-style comparison was run with:

- `./scripts/bench_remote_rsync_vs_sparsync.sh`
- `SPARSYNC_BIN=./target/release/sparsync` (auto-selected by script)
- `RSYNC_TRANSPORT=daemon` and `RSYNC_TRANSPORT=ssh`
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

Results below are medians of 5 consecutive runs per transport mode.

### Daemon Transport (`RSYNC_TRANSPORT=daemon`)

| Metric | sparsync (ms) | rsync (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 416 | 232 | 1.79x |
| Second sync (no changes) | 29 | 135 | 0.21x |
| Changed sync | 52 | 177 | 0.29x |

### SSH Transport (`RSYNC_TRANSPORT=ssh`)

| Metric | sparsync (ms) | rsync over SSH (ms) | sparsync / rsync |
|---|---:|---:|---:|
| Initial sync | 451 | 546 | 0.83x |
| Second sync (no changes) | 31 | 247 | 0.13x |
| Changed sync | 55 | 261 | 0.21x |

Interpretation:

- In daemon mode (unencrypted rsync), `sparsync` is still slower on initial cold copy.
- In daemon mode, `sparsync` remains faster on warm/churned runs.
- In SSH mode (encrypted rsync), `sparsync` is faster in all three phases in this sample.

Experimental notes:

- `--cold-start` is implemented. In a 3-run sample on this dataset, first-sync median was `491ms` (`sparsync_first_ms`) versus `229ms` (`rsync_remote_first_ms`), so it remains slower than the normal path here.
- Server write fan-out can be tuned with `SPARSYNC_BATCH_WRITE_CONCURRENCY` (auto-tuned by default).
- Latest profiled first-sync pass reduced client control stream count from `11` to `5` via direct-file initialized batching plus larger payload continuity in those paths.

## Profiling Notes (March 4, 2026)

- Profiling used `valgrind` (`callgrind` and `cachegrind`) plus targeted `perf stat` and `strace -c` passes.
- Top instruction consumers were memory initialization/copy (`memset`/`memcpy`) and QUIC/TLS crypto paths (`ring`/`quinn`), indicating first-sync is currently dominated by payload movement + encrypted transport overhead rather than scan/hashing.
- Scan/hashing is not the main bottleneck in the benchmark profile: first push logs consistently show single-digit to low-double-digit millisecond scan phases versus ~450ms+ total push elapsed.
- Full profiling/optimization log: [PERFORMANCE.md](./PERFORMANCE.md)
- Ongoing execution roadmap: [PERF_PLAN.md](./PERF_PLAN.md)

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
