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

Representative medians from the latest 5-run sample:

- Daemon mode (`RSYNC_TRANSPORT=daemon`):
  - `sparsync_first_ms=406`
  - `sparsync_second_ms=30`
  - `sparsync_changed_ms=55`
  - `rsync_remote_first_ms=229`
  - `rsync_remote_second_ms=137`
  - `rsync_remote_changed_ms=149`
- SSH mode (`RSYNC_TRANSPORT=ssh`):
  - `sparsync_first_ms=410`
  - `sparsync_second_ms=31`
  - `sparsync_changed_ms=55`
  - `rsync_ssh_first_ms=571`
  - `rsync_ssh_second_ms=248`
  - `rsync_ssh_changed_ms=264`

Interpretation:

- Initial sync is still slower than unencrypted rsync daemon.
- Across encrypted comparison (`rsync` over SSH), `sparsync` is faster in all measured phases.
- Warm and changed syncs remain substantially faster in both comparison modes.

## Profiling Setup

Environment limitations in this workspace:

- `perf`: installed (`/usr/bin/perf` wrapper; kernel-matched binary available via linux-tools)
- `strace`: available
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

### Long-lived framed stream protocol (this wave)

- Added incremental frame-length parsing helper (`frame_total_len`) in protocol framing layer.
- Client transfer paths now reuse a single bidirectional stream session for multi-request sequences:
  - large-file init batches
  - small-batch init + upload
  - per-file chunk upload batch loops
- Server stream handler now processes multiple framed requests on one stream using incremental `read_chunk` decode instead of one-shot `read_to_end`.
- This reduces stream setup churn and avoids buffering whole stream payloads before decode.
- Files: `src/protocol.rs`, `src/transfer.rs`, `src/server.rs`

### Batch response/control-plane simplification

- Changed batch response matching to request-order semantics.
  - Removed per-result path echo for:
    - `InitBatchResult`
    - `UploadSmallFileResult`
    - `UploadColdFileResult`
  - Removed client-side hashmap reconciliation for these responses.
  - Files: `src/protocol.rs`, `src/server.rs`, `src/transfer.rs`

### Large-file control-path reduction

- Replaced per-file init round-trips for large files with batched init requests.
- Upload workers now consume precomputed `InitFileResponse` state from batch init.
- Files: `src/transfer.rs`

### Direct-file initialized batching and payload assembly

- Added direct-file batch path for initialized non-resumed files up to `SPARSYNC_DIRECT_FILE_MAX_BYTES` (default `4 MiB`), reusing cold-batch upload frames to reduce per-file stream churn.
- Added pipelined direct-batch scheduling with bounded in-flight upload batches.
- Reworked chunked upload batch assembly to encode chunk entries in-place, removing one extra per-batch payload re-copy.
- Files: `src/transfer.rs`, `src/protocol.rs`

### Instrumentation and reproducibility

- Added client transfer profiling counters/timers gated by `SPARSYNC_PROFILE=1`.
  - Includes control frame count, streams opened, request/response bytes, disk read/encode/roundtrip timings.
  - File: `src/transfer.rs`
- Added server profiling counters/timers under the same `SPARSYNC_PROFILE=1` flag.
  - Includes stream read/decode/process/encode/write timing and batch split/write/state-commit timing.
  - File: `src/server.rs`
- Added median report harness:
  - `./scripts/bench_remote_rsync_vs_sparsync_median.sh`
  - Supports repeated runs across daemon and ssh transports.

### Transport tuning outcomes

- Added optional `SPARSYNC_SMALL_FILE_MAX_BYTES` tuning knob and evaluated larger thresholds.
- For this benchmark profile, larger small-file thresholds did not improve median first-sync time and sometimes regressed changed-sync latency, so default remained conservative (`128 KiB`).
- Added optional `SPARSYNC_AUTO_CONNECTIONS=1` path for experimentation; default remains off to preserve warm/churn guardrails.

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
- Initial sync is still behind unencrypted rsync daemon on this profile.
- Encrypted comparison (`rsync` over SSH) now shows `sparsync` faster across first/warm/changed phases in the latest sample.
- `--cold-start` remains experimental and is currently slower than the tuned default path on this dataset.
- Profile counters on this dataset now show lower first-sync control/stream churn (`control_frames=5`, `streams_opened=4` in latest profiled pass, down from `11`/`11` in prior snapshots).

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

Median helper (both transports, `RUNS=5` by default):

```bash
./scripts/bench_remote_rsync_vs_sparsync_median.sh
```

Enable push transfer profile counters:

```bash
SPARSYNC_PROFILE=1 ./target/release/sparsync push --source ... --server ... --ca ...
```

Run a profiled daemon benchmark pass:

```bash
SPARSYNC_PROFILE=1 RSYNC_TRANSPORT=daemon ./scripts/bench_remote_rsync_vs_sparsync.sh
```

Optional server write-fanout tuning:

```bash
SPARSYNC_BATCH_WRITE_CONCURRENCY=48 ./scripts/bench_remote_rsync_vs_sparsync.sh
```

Optional direct-file batching threshold tuning:

```bash
SPARSYNC_DIRECT_FILE_MAX_BYTES=$((8*1024*1024)) ./scripts/bench_remote_rsync_vs_sparsync.sh
```

## Next Profiling Targets

- Add periodic/summary export of server profile counters to a machine-readable artifact for automated regression checks.
- Measure time split between:
  - Client encode/compress/copy
  - Network/crypto
  - Server decode/write/state commit
- Upstream `spargio-quic`: optimize encrypted transport hot paths (buffer reuse, copy reduction, pacing/ACK behavior) and retest first-sync medians.
