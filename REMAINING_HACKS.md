# Remaining Hacks Before General Release

This file tracks temporary implementation shortcuts that should be removed or hardened before calling `sparsync` generally available.

Status legend:

- `OPEN`: not addressed
- `IN_PROGRESS`: partially addressed
- `DONE`: fully addressed

## P0 Release Blockers

1. `OPEN` Remove remote `python3` dependency for delete/list flows  
Current hack: remote file listing and remote prune are executed via inline Python snippets over SSH in [src/main.rs](/workspace/spargio-showcase/sparsync/src/main.rs).  
Why this is a blocker: adds undeclared runtime dependency and portability/security risk.  
Fix plan: add native `sparsync` control operations for listing/prune (or protocol RPC equivalents) and remove Python snippets entirely.  
Acceptance: no `python3 - <<'PY'` usage remains in repository code paths.

2. `OPEN` Replace shell-script remote orchestration with protocolized control RPC  
Current hack: many remote operations are built from shell strings + quoting in [src/bootstrap.rs](/workspace/spargio-showcase/sparsync/src/bootstrap.rs) and [src/main.rs](/workspace/spargio-showcase/sparsync/src/main.rs).  
Why this is a blocker: fragile quoting, harder auditing, potential command construction bugs.  
Fix plan: add a typed control plane over SSH stdio (or QUIC control stream) for enroll/start/stop/status/prune operations.  
Acceptance: no multi-command shell scripts are required for core control operations.

3. `OPEN` Harden SSH host key policy  
Current hack: SSH commands default to `StrictHostKeyChecking=accept-new`.  
Why this is a blocker: first-contact trust is implicit and can hide onboarding mistakes.  
Fix plan: default to strict verification using known hosts; require explicit opt-in flag for trust-on-first-use behavior.  
Acceptance: default sync/enroll/server flows fail if host key is unknown or mismatched unless user opts in.

4. `OPEN` Remove bootstrap-wide `allow-prefix /` default  
Current hack: bootstrap enrollment issues client auth with full-root access (`--allow-prefix /`).  
Why this is a blocker: over-broad authorization by default.  
Fix plan: scope issued prefix to destination path unless explicitly overridden.  
Acceptance: bootstrap-generated authz entry is least-privilege by default.

5. `OPEN` Replace reverse-tunnel pull architecture with native remote->local protocol  
Current hack: remote->local sync starts a local temporary server + reverse SSH tunnel + remote push.  
Why this is a blocker: extra moving parts, extra cert generation, higher complexity, more failure modes.  
Fix plan: implement explicit pull/download protocol path (remote source streams to local sink without reverse tunnel).  
Acceptance: remote->local runs without creating a local temporary QUIC server or SSH reverse port forwarding.

## P1 High Priority (Post-Blocker)

1. `OPEN` Remove `/tmp` destination sentinel in server status/stop parsing  
Current hack: status/stop target resolution uses dummy path values to parse SSH targets.  
Fix plan: add explicit endpoint parser for host-only server-management targets.  
Acceptance: no fake path sentinel logic remains.

2. `OPEN` Dry-run parity for remote modes  
Current state: dry-run is implemented but not yet full rsync-grade action simulation for all transfer/delete cases.  
Fix plan: return structured plan from both sides (copy/skip/delete counts and paths) before execution.  
Acceptance: `-n` reports deterministic action plan for local/remote directions with delete/include/exclude.

3. `OPEN` Complete true `-P` parity (`--partial` + `--progress`)  
Current state: resume/partial behavior is wired, but full rsync-like `-P` parity (including explicit `--partial` and `--progress` controls and stable progress UX) is incomplete.  
Fix plan: make `-P` a strict shorthand for `--partial --progress`, expose standalone long flags, and add consistent progress output across local/SSH/QUIC paths.  
Acceptance: `-P`, `--partial`, and `--progress` are all parsed and tested; progress output is deterministic and available during transfers.

4. `OPEN` Filter grammar parity with rsync  
Current state: include/exclude uses simple globset semantics, not rsync filter grammar parity.  
Fix plan: implement rsync-like filter rule parser/order semantics.  
Acceptance: compatibility tests for common rsync include/exclude cases pass.

5. `OPEN` Archive mode parity gaps  
Current state: metadata preservation is partial; full ownership/group/xattrs/symlink parity is incomplete.  
Fix plan: add explicit feature matrix and implement missing archive semantics with platform gating.  
Acceptance: documented matrix + tests across supported OS targets.

6. `OPEN` Protocol-level delete planning  
Current state: delete is currently composed via keep-list + out-of-band remote prune helper.  
Fix plan: integrate delete plan into transfer protocol for atomic, auditable prune behavior.  
Acceptance: delete actions are emitted/applied through typed protocol messages.

7. `OPEN` Rate-limiting quality (`--bwlimit`)  
Current state: implemented with coarse sleep-based limiter around frame sending/local copies.  
Fix plan: replace with central token-bucket limiter integrated with runtime I/O scheduling.  
Acceptance: sustained rate stays within tolerance and is validated by benchmark tests.

8. `OPEN` Default stdio framing path to nonblocking io_uring mode  
Current state: stdio nonblocking is behind `SPARSYNC_STDIO_NONBLOCK`; default behavior can still run with blocking fd semantics.  
Fix plan: make nonblocking stdio the default for both client and server stdio framing paths, with env opt-out only for debugging.  
Acceptance: stdio transport is nonblocking by default and partial-write/`EAGAIN` paths remain covered.

9. `OPEN` Remove blocking local-copy engine from async command flow  
Current state: local->local sync uses blocking `std::fs` plus `std::thread::sleep` in [src/local_copy.rs](/workspace/spargio-showcase/sparsync/src/local_copy.rs), invoked from async command handling.  
Fix plan: port local copy/prune to `spargio::fs` + `spargio::sleep` (or isolate blocking work on a dedicated blocking executor outside runtime shards).  
Acceptance: no blocking file/sleep operations remain in local-copy hot paths under runtime execution.

10. `OPEN` De-block SSH bootstrap/control-plane orchestration  
Current state: bootstrap and helper flows use blocking `std::process::Command`, blocking temp-file ops, and blocking readiness loops (`TcpStream::connect_timeout` + `thread::sleep`).  
Fix plan: use nonblocking pipe-based process I/O and runtime-driven timers/readiness checks; move temp-file operations to async FS where practical.  
Acceptance: bootstrap/enroll/start/stop flows avoid runtime-thread blocking waits and blocking network probe loops.

11. `OPEN` Remove blocking thread watchdog from `serve --once`  
Current state: `serve --once` starts a `thread::spawn` + `thread::sleep` watchdog that may call `process::exit`.  
Fix plan: rely on runtime timeout/cancellation in the accept loop instead of an external blocking watchdog thread.  
Acceptance: once-idle timeout behavior works without a dedicated blocking watchdog thread.

12. `OPEN` Top-level `--version` support is missing  
Current loose end: `sparsync --version` currently errors as unknown argument.  
Fix plan: enable clap version flag at top-level command and test it in CLI integration suite.  
Acceptance: `sparsync --version` exits `0` and prints binary version.

13. `OPEN` Top-level short help (`-h`) is missing/ambiguous  
Current loose end: `-h` is reserved for rsync-style human-readable in `sync`, so `sparsync -h` currently errors.  
Fix plan: decide explicit contract:
- either support top-level `-h` and keep `sync -h` via compatibility parser rules, or
- keep `-h` sync-only but provide a clear top-level error hint (`use --help`).
Acceptance: behavior is documented, deterministic, and covered by tests.

14. `OPEN` `help` subcommand is intercepted by implicit `sync` insertion  
Current loose end: `sparsync help sync` currently routes to `sync` positional parsing and fails with unrelated file-path errors.  
Fix plan: update implicit-subcommand detection to preserve clap help subcommand routing.  
Acceptance: `sparsync help`, `sparsync help sync`, and `sparsync sync --help` all work correctly.

15. `OPEN` Error-message quality pass for CLI compatibility mode  
Current loose end: some parse/dispatch failures bubble low-context messages (for example path canonicalization errors caused by mistaken help invocation).  
Fix plan: add explicit top-level validation and user-facing hints for common mistakes (transport mismatch, endpoint format mismatch, missing `--ssh-target`, etc.).  
Acceptance: common user mistakes produce actionable errors that mention fix-up flags/usage.

## P2 Quality / Operability

1. `OPEN` Service management robustness  
Current state: remote persistent server is managed via `nohup` + pid file shell logic.  
Fix plan: provide managed service units (systemd user/system), install/status integration, and log routing.  
Acceptance: long-lived service lifecycle can be managed without manual pid file logic.

2. `OPEN` Secret storage backend expansion  
Current state: filesystem permissions are enforced, but platform key-store integration is not implemented.  
Fix plan: add optional key-store backends with headless file fallback.  
Acceptance: selectable secret backend with migration path and tests.

3. `OPEN` Benchmark gate automation in CI  
Current state: benchmark qualification is manual/script-driven.  
Fix plan: add CI benchmark profiles with regression thresholds and artifact retention.  
Acceptance: CI reports pass/fail against defined latency/throughput guardrails.

4. `OPEN` CLI compatibility regression tests  
Current state: implicit `sync` mode and compatibility flags are implemented but need deeper matrix testing.  
Fix plan: add integration tests for short-flag bundles, unknown flags, endpoint permutations, and transport auto-selection.  
Acceptance: release CI includes compatibility suite with reproducible fixtures.

5. `OPEN` Remove top-level `futures::executor::block_on` bridge  
Current state: CLI entry currently uses `futures::executor::block_on` before entering `spargio::run_with`.  
Fix plan: move to a direct spargio-owned top-level runtime bootstrap path (or provide a small internal wrapper without external executor semantics).  
Acceptance: runtime entry is explicitly spargio-driven end-to-end, with no separate executor bridge in `main`.

## Recommended Execution Order

1. Finish P0 items 1-5.
2. Land protocol/control-plane replacements before adding more compatibility surface.
3. Close P1 runtime-consistency items first (`-P`, stdio nonblocking default, local-copy de-blocking, bootstrap/control-plane de-blocking).
4. Close remaining P1 parity items (`-n`, filters, archive) with tests.
5. Close P2 operability and CI gates.
6. Re-run benchmark qualification and update `BENCHMARKS.md`.
