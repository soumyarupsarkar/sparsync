# Performance Plan

## Goals

- Beat `rsync` on the existing benchmark for initial sync while preserving warm/churn latency wins.
- Provide fair encrypted and unencrypted comparisons in benchmark reporting.
- Keep protocol/runtime design lightweight and maintainable for the `spargio` showcase.

## Current Status (March 4, 2026)

Implemented in this wave:

- Dual benchmark tracks in canonical harness (`RSYNC_TRANSPORT=daemon|ssh`).
- Median report helper script (`scripts/bench_remote_rsync_vs_sparsync_median.sh`).
- First-sync control path reduction via batched large-file init.
- Client-side transfer instrumentation (`SPARSYNC_PROFILE=1`) with counters/timers.
- Initialized direct-file batching for non-resumed medium files to reduce first-sync stream churn.
- In-place chunk batch payload assembly to remove an extra transfer-path copy.
- Long-lived framed stream reuse across client/server request loops (incremental `read_chunk` framing).

Still pending:

- Transport-level overhead reduction in `spargio-quic` (crypto/memory-movement dominated first-sync cost) remains the largest open lever.
- No additional in-repo big-ticket blockers identified from this plan; optional WAL work is parked unless profiling shows state persistence becomes a dominant cost.

## Execution Sequence Status

1. Establish dual benchmark tracks (`daemon` and `ssh`) and lock reporting format.
Status: completed.
2. Run baseline snapshots on both tracks.
Status: completed and refreshed via median helper script.
3. Execute Phase 1 and Phase 2 optimizations with per-change benchmark checks.
Status: completed for this wave (copy reductions + large-file batched init + direct-file initialized batching + control-path simplification).
4. Execute Phase 3 and Phase 4 tuning with guardrails.
Status: completed for in-repo scope (state batching + transport knobs + benchmark tuning; WAL intentionally deferred pending a state-heavy bottleneck signal).
5. Publish updated benchmark + profiling notes after each optimization wave.
Status: completed (BENCHMARKS.md and PERFORMANCE.md updated after this wave).

## Post-Plan Upstream Follow-up (`spargio-quic`)

- Reduce encrypted transport overhead further: packet/buffer reuse, fewer allocs/copies, and scheduler pacing/ACK tuning.
- Continue true zero-copy-ish flow across file/network boundaries where runtime and QUIC APIs permit.
- Re-profile first-sync after each transport change with the same benchmark harness and median methodology.

## Remaining Big-Ticket Opportunities (Ranked, March 4, 2026)

1. `spargio-quic` hot-path allocation/copy reduction (highest expected impact)
  - Prioritize packet/stream buffer reuse and avoid per-op realloc/copy churn in send/recv loops.
  - Goal: reduce first-sync instruction share in `memcpy`/`memset` and TLS/QUIC glue overhead.
2. Long-lived streaming upload protocol (incremental write/decode, fewer framed batch round trips)
  - Move more transfer paths from request/response batch framing to sustained stream flow.
  - Goal: cut control-frame parsing/serialization and stream setup overhead further.
3. Optional trusted-LAN non-crypto transport mode (only if beating `rsync://` is strict)
  - `rsync://` daemon baseline is unencrypted while QUIC/TLS is encrypted.
  - Goal: enable a fair unencrypted fast path for explicit trusted environments.
4. Deeper transport tuning in `spargio-quic`
  - Focus on pacing/ACK behavior, scheduler wakeups, send quantum/coalescing, and flow-control defaults.
  - Goal: improve first-sync without regressing warm/changed medians.
5. End-to-end zero-copy-ish file/network data path improvements
  - Reduce remaining file-buffer to QUIC-payload and QUIC-payload to file-buffer copies.
  - Goal: lower memory-movement cost across large payloads.

Lower expected ROI for current benchmark profile:

- Additional scan/hash optimization (current profiles show transport + memory movement dominate first sync).

## Benchmark Fairness Plan

### Track A: Unencrypted baseline

- Keep current daemon benchmark (`rsync://`) as the unencrypted ceiling.
- Purpose: isolate raw transfer + protocol/runtime overhead without encryption.

### Track B: Encrypted apples-to-apples

- Add `rsync` over SSH benchmark path and report separately.
- Compare to `sparsync` QUIC/TLS results (always encrypted).
- Publish both sets of numbers:
  - `sparsync vs rsync_remote` (daemon)
  - `sparsync vs rsync_ssh` (encrypted)

### Reporting rules

- Report medians from at least 5 consecutive runs.
- Always include dataset shape and CPU/core settings with results.
- Keep one canonical benchmark script and avoid ad-hoc one-off commands in docs.

## Overhead Elimination Plan

### Phase 1: Data movement and allocation

- Remove remaining avoidable buffer copies in upload/decode/write paths.
- Reuse large buffers across files/chunks where safe.
- Track allocation churn (`malloc/free` hotspots) with callgrind/cachegrind deltas.

Success criteria:

- Lower instruction share in `memcpy`/`memset`.
- No regression in `sparsync_second_ms` and `sparsync_changed_ms`.

### Phase 2: Protocol/control path

- Collapse first-sync round trips further (fewer control messages per file group).
- Increase payload continuity per stream to reduce stream-open churn.
- Keep response semantics index-ordered to avoid map reconciliation overhead.

Success criteria:

- Fewer control frames and stream opens per transfer.
- Improved `sparsync_first_ms` at identical dataset/settings.

### Phase 3: State and metadata path

- Batch state commits aggressively in both small and cold paths.
- Minimize per-file metadata syscalls where cached state is definitive.
- Evaluate optional lightweight WAL for amortized persistence.

Success criteria:

- Reduced state write frequency per completed batch.
- No correctness regressions under resume/restart scenarios.

### Phase 4: Transport tuning

- Tune connection count, stream payload sizing, and write fan-out by mode:
  - first sync (throughput biased)
  - warm/changed sync (latency biased)
- Maintain conservative defaults; allow explicit env override for bench exploration.

Success criteria:

- Better first-sync median without harming warm/churn medians.
- Stable behavior across repeated benchmark runs.

## Instrumentation Plan

- Keep and expand phase timers:
  - scan/enumeration
  - hash
  - encode/compress
  - network transfer
  - server decode/write/state commit
- Add counters:
  - streams opened
  - control frames sent
  - bytes copied/encoded (where measurable)
- Use:
  - `strace` for syscall distribution checks
  - `valgrind` for instruction/memory profile snapshots
  - `perf` when kernel support is available

## Guardrails

- Any optimization that improves first sync but regresses warm/changed by >10% median is rejected or behind an explicit mode flag.
- No protocol changes that weaken correctness guarantees (resume safety, file integrity, metadata handling).
- Keep benchmark script reproducible and self-describing.

## Execution Sequence

1. Establish dual benchmark tracks (`daemon` and `ssh`) and lock reporting format.
2. Run baseline snapshots on both tracks.
3. Execute Phase 1 and Phase 2 optimizations with per-change benchmark checks.
4. Execute Phase 3 and Phase 4 tuning with guardrails.
5. Publish updated benchmark + profiling notes after each optimization wave.
