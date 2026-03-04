# Performance Plan

## Goals

- Beat `rsync` on the existing benchmark for initial sync while preserving warm/churn latency wins.
- Provide fair encrypted and unencrypted comparisons in benchmark reporting.
- Keep protocol/runtime design lightweight and maintainable for the `spargio` showcase.

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
