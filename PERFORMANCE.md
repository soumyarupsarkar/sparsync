# Performance And Profiling Notes

## Scope

This note captures the cold-start optimization work and profiling results for `sparsync` as of **March 4, 2026**.
Forward-looking execution plan is in [PERF_PLAN.md](./PERF_PLAN.md).

Primary goal in this pass:

- Reduce initial sync (empty target) time without regressing warm/churned sync latency.

## Benchmark Baseline

Benchmark harness:

- `./scripts/bench_remote_rsync_vs_sparsync.sh`

Dataset/profile used in this pass:

- `SMALL_DIRS=10`
- `SMALL_FILES_PER_DIR=100`
- `SMALL_FILE_SIZE=4096`
- `LARGE_FILES=8`
- `LARGE_FILE_SIZE=2097152`
- `CHANGED_FILES=100`
- `SPARSYNC_FIRST_COLD=0` (default path) unless explicitly testing `--cold-start`
- `RSYNC_ARGS="-a --delete"`
- `RSYNC_TRANSPORT=daemon` for these reported numbers

Representative medians from the latest 5-run sample (normal path):

- `sparsync_first_ms=466`
- `sparsync_second_ms=32`
- `sparsync_changed_ms=56`
- `rsync_remote_first_ms=229`
- `rsync_remote_second_ms=136`
- `rsync_remote_changed_ms=149`

Interpretation:

- Initial sync is still slower than remote rsync in this profile.
- Warm and changed syncs remain faster than remote rsync.

## Profiling Setup

Environment limitations in this workspace:

- `perf`: unavailable
- `strace`: unavailable
- `valgrind`: available

Tools used:

- `valgrind --tool=callgrind`
- `valgrind --tool=cachegrind`
- `callgrind_annotate`
- `cg_annotate`

Profiling was run on reduced datasets to keep valgrind runtime practical.

## Profiler Findings

Top consumers from callgrind/cachegrind were dominated by:

- Memory initialization/copy (`memset` / `memcpy`)
- QUIC/TLS crypto + protocol machinery (`ring` / `quinn`)

What this means:

- First-sync time is currently dominated by payload movement and encrypted transport overhead.
- Directory scan/hash is not the primary bottleneck in this benchmark profile.
  - Push logs consistently show scan phases in single-digit to low-double-digit milliseconds.

## Optimizations Implemented

### Data-path copy reduction

- Added `maybe_decode` returning `Cow<[u8]>` for zero-copy uncompressed decode paths.
  - File: `src/compression.rs`
- Reworked compression path to reuse owned buffers (`maybe_compress_vec`) instead of avoidable cloning.
  - File: `src/compression.rs`

### Protocol framing overhead reduction

- Added header-only encoder (`encode_header`) and switched send path to write header + payload separately.
  - Avoids one extra request buffer concatenation copy for payload-heavy frames.
  - Files: `src/protocol.rs`, `src/transfer.rs`

### Batch response/control-plane simplification

- Changed batch response matching to request-order semantics.
  - Removed per-result path echo for:
    - `InitBatchResult`
    - `UploadSmallFileResult`
    - `UploadColdFileResult`
  - Removed client-side hashmap reconciliation for these responses.
  - Files: `src/protocol.rs`, `src/server.rs`, `src/transfer.rs`

### Server-side batching improvements

- Removed per-file payload `to_vec()` copies in small/cold batch handlers (process slices directly).
  - File: `src/server.rs`
- Small-file direct-write completions now batch into `complete_files_batch`.
  - Avoids repeated per-file state persistence calls in batch.
  - Files: `src/server.rs`, `src/state.rs`
- Added adaptive write fan-out for batch handlers with manual override:
  - `SPARSYNC_BATCH_WRITE_CONCURRENCY=<n>`
  - File: `src/server.rs`
- Avoided stale-partial cleanup syscalls when partial tree is known empty.
  - File: `src/server.rs`

### Cold-start path updates

- Implemented/retained explicit cold path (`--cold-start`) with cold-batch protocol frames.
- Added multi-batch scheduling support across available connections in cold path.
  - File: `src/transfer.rs`

## Current Outcome

- The above changes improved hot-path efficiency and preserved strong warm/churned performance.
- On this benchmark profile, initial sync is still not faster than remote rsync.
- `--cold-start` remains experimental and is currently slower than the tuned default path on this dataset.

## Reproduce

Build:

```bash
cargo build --release
```

Run benchmark harness once:

```bash
./scripts/bench_remote_rsync_vs_sparsync.sh
```

Run repeated sample:

```bash
for i in 1 2 3 4 5; do ./scripts/bench_remote_rsync_vs_sparsync.sh; done
```

Optional server write-fanout tuning:

```bash
SPARSYNC_BATCH_WRITE_CONCURRENCY=48 ./scripts/bench_remote_rsync_vs_sparsync.sh
```

## Next Profiling Targets

- Instrument frame-size and stream-count distributions during first sync.
- Measure time split between:
  - Client encode/compress/copy
  - Network/crypto
  - Server decode/write/state commit
- Evaluate protocol changes that reduce first-sync stream churn (larger contiguous data streams and fewer control round trips).
