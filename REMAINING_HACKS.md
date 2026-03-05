# Remaining Hacks Before General Release

This file tracks temporary implementation shortcuts that should be removed or hardened before calling `sparsync` generally available.

Status legend:

- `OPEN`: not addressed
- `IN_PROGRESS`: partially addressed
- `DONE`: fully addressed

## P0 Release Blockers

1. `DONE` Remove remote `python3` dependency for delete/list flows  
Current state: replaced Python snippets with native internal commands (`stream-source`, `prune-destination`) over SSH.  
Why this is a blocker: adds undeclared runtime dependency and portability/security risk.  
Fix plan: add native `sparsync` control operations for listing/prune (or protocol RPC equivalents) and remove Python snippets entirely.  
Acceptance: no `python3 - <<'PY'` usage remains in repository code paths.

2. `DONE` Replace shell-script remote orchestration with protocolized control RPC  
Current state: multi-command shell scripts were removed for core operations; delete planning now uses typed protocol frames (`DeletePlanRequest/Response`) over SSH stdio and QUIC instead of shelling out to remote prune scripts.  
Why this is a blocker: fragile quoting, harder auditing, potential command construction bugs.  
Fix plan: add a typed control plane over SSH stdio (or QUIC control stream) for enroll/start/stop/status/prune operations.  
Acceptance: no multi-command shell scripts are required for core control operations.

3. `DONE` Harden SSH host key policy  
Current state: default now uses strict host key checking; TOFU is explicit opt-in (`SPARSYNC_SSH_TOFU=1`, or explicit `SPARSYNC_SSH_STRICT_HOST_KEY_CHECKING`).  
Why this is a blocker: first-contact trust is implicit and can hide onboarding mistakes.  
Fix plan: default to strict verification using known hosts; require explicit opt-in flag for trust-on-first-use behavior.  
Acceptance: default sync/enroll/server flows fail if host key is unknown or mismatched unless user opts in.

4. `DONE` Remove bootstrap-wide `allow-prefix /` default  
Current state: bootstrap enrollment now scopes issued client authz to the requested destination path.
Why this is a blocker: over-broad authorization by default.  
Fix plan: scope issued prefix to destination path unless explicitly overridden.  
Acceptance: bootstrap-generated authz entry is least-privilege by default.

5. `DONE` Replace reverse-tunnel pull architecture with native remote->local protocol  
Current state: remote->local now uses SSH stream pull (`stream-source`), with no reverse tunnel and no local temporary QUIC server.
Why this is a blocker: extra moving parts, extra cert generation, higher complexity, more failure modes.  
Fix plan: implement explicit pull/download protocol path (remote source streams to local sink without reverse tunnel).  
Acceptance: remote->local runs without creating a local temporary QUIC server or SSH reverse port forwarding.

## P1 High Priority (Post-Blocker)

1. `DONE` Remove `/tmp` destination sentinel in server status/stop parsing  
Current hack: status/stop target resolution uses dummy path values to parse SSH targets.  
Fix plan: add explicit endpoint parser for host-only server-management targets.  
Acceptance: no fake path sentinel logic remains.

2. `IN_PROGRESS` Dry-run parity for remote modes  
Current state: remote dry-run now includes protocol-based delete simulation for both SSH stdio and QUIC paths, but still does not emit full per-path action plans in rsync-style detail.  
Fix plan: return structured plan from both sides (copy/skip/delete counts and paths) before execution.  
Acceptance: `-n` reports deterministic action plan for local/remote directions with delete/include/exclude.

3. `DONE` Complete true `-P` parity (`--partial` + `--progress`)  
Current state: `-P`, `--partial`, and `--progress` are parsed and wired; progress output is emitted in local/SSH/QUIC transfer paths.
Fix plan: make `-P` a strict shorthand for `--partial --progress`, expose standalone long flags, and add consistent progress output across local/SSH/QUIC paths.  
Acceptance: `-P`, `--partial`, and `--progress` are all parsed and tested; progress output is deterministic and available during transfers.

4. `IN_PROGRESS` Filter grammar parity with rsync  
Current state: include/exclude now uses rule-based matching (include-first rule evaluation, basename and directory suffix normalization), but full rsync filter rule grammar/order controls are still incomplete.  
Fix plan: implement rsync-like filter rule parser/order semantics.  
Acceptance: compatibility tests for common rsync include/exclude cases pass.

5. `OPEN` Archive mode parity gaps  
Current state: metadata preservation is partial; full ownership/group/xattrs/symlink parity is incomplete.  
Fix plan: add explicit feature matrix and implement missing archive semantics with platform gating.  
Acceptance: documented matrix + tests across supported OS targets.

6. `DONE` Protocol-level delete planning  
Current state: delete planning is implemented as typed wire frames (`DeletePlanRequest/Response`) and applied by QUIC server and SSH stdio server handlers.
Fix plan: integrate delete plan into transfer protocol for atomic, auditable prune behavior.  
Acceptance: delete actions are emitted/applied through typed protocol messages.

7. `DONE` Rate-limiting quality (`--bwlimit`)  
Current state: switched to token-bucket limiters in transfer/local copy paths.
Fix plan: replace with central token-bucket limiter integrated with runtime I/O scheduling.  
Acceptance: sustained rate stays within tolerance and is validated by benchmark tests.

8. `DONE` Default stdio framing path to nonblocking io_uring mode  
Current state: stdio framing defaults to nonblocking; `SPARSYNC_STDIO_NONBLOCK=0` is opt-out for debugging.
Fix plan: make nonblocking stdio the default for both client and server stdio framing paths, with env opt-out only for debugging.  
Acceptance: stdio transport is nonblocking by default and partial-write/`EAGAIN` paths remain covered.

9. `DONE` Remove blocking local-copy engine from async command flow  
Current state: local->local copy/prune path now uses `spargio::fs` and async sleeps for rate limiting.
Fix plan: port local copy/prune to `spargio::fs` + `spargio::sleep` (or isolate blocking work on a dedicated blocking executor outside runtime shards).  
Acceptance: no blocking file/sleep operations remain in local-copy hot paths under runtime execution.

10. `IN_PROGRESS` De-block SSH bootstrap/control-plane orchestration  
Current state: major blocking entry points are now offloaded to dedicated blocking threads from async call sites; bootstrap internals still contain blocking subprocess/poll loops.
Fix plan: use nonblocking pipe-based process I/O and runtime-driven timers/readiness checks; move temp-file operations to async FS where practical.  
Acceptance: bootstrap/enroll/start/stop flows avoid runtime-thread blocking waits and blocking network probe loops.

11. `DONE` Remove blocking thread watchdog from `serve --once`  
Current state: `serve --once` uses runtime timeout logic only.
Fix plan: rely on runtime timeout/cancellation in the accept loop instead of an external blocking watchdog thread.  
Acceptance: once-idle timeout behavior works without a dedicated blocking watchdog thread.

12. `DONE` Top-level `--version` support is missing  
Current loose end: `sparsync --version` currently errors as unknown argument.  
Fix plan: enable clap version flag at top-level command and test it in CLI integration suite.  
Acceptance: `sparsync --version` exits `0` and prints binary version.

13. `DONE` Top-level short help (`-h`) is missing/ambiguous  
Current loose end: `-h` is reserved for rsync-style human-readable in `sync`, so `sparsync -h` currently errors.  
Fix plan: decide explicit contract:
- either support top-level `-h` and keep `sync -h` via compatibility parser rules, or
- keep `-h` sync-only but provide a clear top-level error hint (`use --help`).
Acceptance: behavior is documented, deterministic, and covered by tests.

14. `DONE` `help` subcommand is intercepted by implicit `sync` insertion  
Current loose end: `sparsync help sync` currently routes to `sync` positional parsing and fails with unrelated file-path errors.  
Fix plan: update implicit-subcommand detection to preserve clap help subcommand routing.  
Acceptance: `sparsync help`, `sparsync help sync`, and `sparsync sync --help` all work correctly.

15. `IN_PROGRESS` Error-message quality pass for CLI compatibility mode  
Current state: unknown-flag errors now fail fast with explicit help hints and top-level `-h` gives an actionable compatibility message; additional transport/endpoint validation hints are still being expanded.  
Fix plan: add explicit top-level validation and user-facing hints for common mistakes (transport mismatch, endpoint format mismatch, missing `--ssh-target`, etc.).  
Acceptance: common user mistakes produce actionable errors that mention fix-up flags/usage.

## P2 Quality / Operability

1. `IN_PROGRESS` Service management robustness  
Current state: added native `service-daemon` start/stop/status management (PID/log aware), early-exit detection on daemon start, and remote binary discovery for stop/status instead of fixed PATH assumptions.  
Fix plan: provide managed service units (systemd user/system), install/status integration, and log routing.  
Acceptance: long-lived service lifecycle can be managed without manual pid file logic.

2. `OPEN` Secret storage backend expansion  
Current state: filesystem permissions are enforced, but platform key-store integration is not implemented.  
Fix plan: add optional key-store backends with headless file fallback.  
Acceptance: selectable secret backend with migration path and tests.

3. `DONE` Benchmark gate automation in CI  
Current state: added `scripts/bench_ci_gate.sh` and `.github/workflows/benchmark-gate.yml` with ratio thresholds and artifact upload retention.
Fix plan: add CI benchmark profiles with regression thresholds and artifact retention.  
Acceptance: CI reports pass/fail against defined latency/throughput guardrails.

4. `IN_PROGRESS` CLI compatibility regression tests  
Current state: added targeted coverage for unknown-flag hinting, `-h` compatibility hint, and auto-transport selection; full end-to-end matrix integration tests remain.
Fix plan: add integration tests for short-flag bundles, unknown flags, endpoint permutations, and transport auto-selection.  
Acceptance: release CI includes compatibility suite with reproducible fixtures.

5. `DONE` Remove top-level `futures::executor::block_on` bridge  
Current state: entry now uses `spargio::__private::block_on` instead of direct `futures::executor::block_on` at top-level.
Fix plan: move to a direct spargio-owned top-level runtime bootstrap path (or provide a small internal wrapper without external executor semantics).  
Acceptance: runtime entry is explicitly spargio-driven end-to-end, with no separate executor bridge in `main`.

## Recommended Execution Order

1. Finish P0 items 1-5.
2. Land protocol/control-plane replacements before adding more compatibility surface.
3. Close P1 runtime-consistency items first (`-P`, stdio nonblocking default, local-copy de-blocking, bootstrap/control-plane de-blocking).
4. Close remaining P1 parity items (`-n`, filters, archive) with tests.
5. Close P2 operability and CI gates.
6. Re-run benchmark qualification and update `BENCHMARKS.md`.

## New Findings (Post-Implementation)

1. `OPEN` Full typed control stream is still missing  
Current state: orchestration uses typed internal commands over SSH, but not a single long-lived RPC control stream.

2. `OPEN` Bootstrap internals still contain blocking subprocess/network waits  
Current state: async callers now offload to blocking threads, but `bootstrap.rs` internals remain synchronous.

3. `OPEN` Remote->local QUIC pull path still depends on SSH bootstrap channel  
Current state: pull no longer uses reverse tunnels, but still streams over SSH (`stream-source`) rather than direct QUIC download RPC.

4. `OPEN` Delete-plan keep-list currently uses single-frame payloads  
Current state: protocolized delete planning is in place, but very large keep-lists can hit `max_stream_payload` limits before chunked request support lands.
