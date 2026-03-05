# RSYNC Compatibility Plan

## Goal

Make `sparsync` a familiar, safe, practical `rsync`-style tool with:

- rsync-like invocation (`sparsync [OPTIONS] SRC DEST`)
- Local copy (no remote connection)
- SSH endpoint support (spargio-over-SSH stdio transport and SSH bootstrap control plane)
- `sparsync://` QUIC + mTLS fast path
- Bidirectional copy semantics (local->remote and remote->local)
- Repeat-run performance after enrollment that matches current direct `sparsync over QUIC` benchmarks (within noise budget)

Non-goal: full wire/protocol compatibility with `rsync`.

## Key Design Decisions

1. CLI Compatibility Model
- Decision: support `sparsync [OPTIONS] SRC DEST` as the primary UX; keep subcommands for admin/debug.
- Decision: preserve explicit flags where behavior differs from rsync (`--transport`, `--bootstrap`, `--profile`, `--enroll`).
- Decision: initial rsync-like flag set must include: `-a`, `-v`, `-z`, `-h`, `-P`, `--delete`, `--dry-run`/`-n`, `--exclude`, `--include`, `-u`, `--bwlimit`.
- Decision: unknown/unsupported flags must fail fast with clear error text (no silent ignore, no best-effort reinterpretation).

2. Endpoint and Transport Semantics
- Decision: endpoint grammar:
  - Local: `/path` or relative path
  - SSH-style remote: `[user@]host:/path` and `ssh://[user@]host[:port]/path`
  - QUIC remote: `sparsync://host[:port]/path`
- Decision: default transport mode is `auto`:
  - For `sparsync://`: force QUIC (service expected to be running).
  - For SSH-style remote:
    - default to SSH stdio data plane (rsync-like expectation, no implicit service start).
    - QUIC is used only when explicitly requested.
- Decision: keep explicit override via `--transport quic|ssh|local|auto`.
- Decision: QUIC bootstrap over SSH is explicit opt-in:
  - `--bootstrap ssh --ssh-target user@host` when destination is `sparsync://...`
  - or one-time `enroll` command before `sparsync://...` use.

3. Copy Direction Semantics (rsync-like)
- Decision: support both:
  - local -> remote
  - remote -> local
- Decision: source side remains the scanner/sender role.
- Decision: for remote->local with QUIC, local orchestrator may start a local receiver and ask remote sender to push to it (SSH control plane can coordinate this).
- Decision: remote->remote is out of initial scope; can be added later via relay/orchestration mode.

4. Enrollment and Reuse
- Decision: separate first-time enrollment from steady-state transfer.
- Decision: first connection may auto-enroll only for QUIC mode (`sparsync://...` with explicit bootstrap policy), not for default SSH-style endpoint behavior.
- Decision: repeat runs must skip full bootstrap by default and reuse:
  - profile
  - CA/cert/key bundle
  - remote server identity
- Decision: cert renewal only near expiry/revocation/version mismatch.

5. Server Lifecycle
- Decision: explicit server management commands:
  - `sparsync server start <host-or-profile>`
  - `sparsync server stop <host-or-profile>`
  - `sparsync server status <host-or-profile>`
- Decision: QUIC fast path should target a long-lived server (service-managed), not per-sync `serve --once`.
- Decision: `serve --once` remains for bootstrap and controlled one-shot flows.
- Decision: document clearly when servers are started:
  - `sparsync://...`: expects running server.
  - `--bootstrap ssh` on `sparsync://...`: may initialize and/or start remote server.
  - SSH-style endpoint default path does not start persistent server.
  - `--transport ssh`: no persistent server required (stdio session model).

6. Secret Management
- Decision: split config from secrets.
- Decision: store non-secret config in XDG config (`$XDG_CONFIG_HOME/sparsync` or `~/.config/sparsync`).
- Decision: store private keys and enrollment secrets in dedicated secrets path (`$XDG_DATA_HOME/sparsync/secrets` or `~/.local/share/sparsync/secrets`) with strict perms.
- Decision: enforce permissions:
  - secrets dir `0700`
  - private key files `0600`
  - fail closed (or warn+repair with `--fix-perms`) if too open.
- Decision: optional key-store integration for user keys (macOS Keychain, Windows DPAPI/CredMan, libsecret/pass) with file-backed fallback for headless/server.
- Decision: never print secret contents; avoid logging secret paths at info level.

7. SSH Overhead Minimization
- Decision: treat SSH as control plane after first enrollment, not steady-state data plane for QUIC path.
- Decision: reduce bootstrap RTTs by collapsing setup into one remote bootstrap command.
- Decision: use SSH multiplexing (`ControlMaster`/`ControlPersist`) when available.
- Decision: avoid per-run binary upload:
  - version/hash probe first
  - persistent install/cache by default for trusted hosts
  - ephemeral install remains opt-in for high-assurance environments.
- Decision: remove fixed startup sleeps; replace with readiness probes.

8. Performance Targets
- Decision: post-enrollment QUIC path must be benchmarked separately from enrollment/bootstrap overhead.
- Decision: acceptance target:
  - repeat runs after enrollment: within 5% of direct `sparsync over QUIC` benchmark medians.
  - first run includes enrollment cost and is reported separately.
- Decision: SSH stdio mode remains a compatibility/fallback transport; optimize but do not let it constrain QUIC fast path design.

## Proposed UX

Primary:

```bash
sparsync [OPTIONS] SRC DEST
```

Examples:

```bash
# local copy
sparsync -a /src /dst

# SSH endpoint (rsync-like): SSH data plane, no remote service start
sparsync -a /src user@host:/dst

# force SSH stdio data plane
sparsync -a --transport ssh /src user@host:/dst

# explicit QUIC endpoint (expects running server)
sparsync -a /src sparsync://host:7844/dst

# one-time enrollment/bootstrap for QUIC endpoint
sparsync enroll user@host:/dst

# QUIC endpoint with explicit bootstrap over SSH when needed
sparsync -a /src sparsync://host:7844/dst --bootstrap ssh --ssh-target user@host

# reverse direction (remote -> local)
sparsync -a user@host:/src /dst
```

Admin/ops:

```bash
sparsync enroll user@host:/dst
sparsync server start user@host
sparsync server status user@host
sparsync server stop user@host
```

## Sample Workflow (Proposed)

First-time QUIC setup over SSH (one-time enrollment):

```bash
sparsync enroll user@sync-host:/data/team
```

Steady-state fast path (repeat runs):

```bash
sparsync -a /local/src sparsync://sync-host:7844/data/team
```

Reverse direction (remote -> local):

```bash
sparsync -a sparsync://sync-host:7844/data/team /local/restore
```

SSH fallback when QUIC is blocked:

```bash
sparsync -a --transport ssh /local/src user@sync-host:/data/team
```

Optional one-shot bootstrap directly from a QUIC endpoint invocation:

```bash
sparsync -a /local/src sparsync://sync-host:7844/data/team \
  --bootstrap ssh --ssh-target user@sync-host
```

Server lifecycle:

```bash
sparsync server status user@sync-host
sparsync server stop user@sync-host
sparsync server start user@sync-host
```

## Flag Compatibility (Initial Scope)

Required initial rsync-compatible flags:

- `-a` archive mode (recursive + metadata preservation policy)
- `-v` verbose output
- `-z` compression
- `-h` human-readable output
- `-P` shorthand for partial transfer + progress
- `--delete` remove destination files not present at source
- `--dry-run` and `-n` (no writes/deletes; show plan)
- `--exclude <pattern>`
- `--include <pattern>`
- `-u` skip files newer on receiver
- `--bwlimit <rate>`

Additional flags to consider next:

- `-r` recursive (if separated from archive mode behavior)
- `-t`, `-p` explicit mtime/perm toggles
- `-o`, `-g` owner/group preservation toggles (with clear non-root behavior)
- `-c` checksum mode (force content-based change detection policy)
- `-l`, `-L`, `-K` symlink policy controls
- `--partial` and `--progress` as standalone controls (for parity with `-P`)
- `--inplace`
- `--append` and `--append-verify`
- `--delete-excluded`
- `--delete-after` and `--delete-during`
- `--prune-empty-dirs`
- `--files-from`
- `--filter`
- `--compress-level`
- `--size-only`
- `--ignore-existing`
- `--existing`
- `--mkpath`

Unknown flag handling:

- Any unrecognized flag must terminate immediately with non-zero exit.
- Error output should identify the unknown flag and suggest `--help`.
- Compatibility mode must never silently drop a flag.
- Combined short flags must be parsed atomically (`-avzhP`), and any unknown short option in the bundle must fail the entire invocation.
- Unknown long flags and unknown option values must fail before any scan/transfer work begins.
- No passthrough of unknown rsync flags to transport or remote side unless explicitly implemented.

## Enrollment Lifecycle

1. Detect profile
- If valid profile exists, use it directly.
- If missing/invalid/expired, enter enrollment path.

2. Enrollment path
- Verify remote identity over SSH.
- Verify/install compatible `sparsync` binary remotely.
- Initialize server auth material if missing.
- Issue or renew client cert with scoped prefix authorization.
- Store local profile and secrets in proper locations/permissions.
- Optionally start/enable remote QUIC server service.

3. Steady-state path
- Direct QUIC+mTLS transfer using cached profile.
- Background/periodic cert validity checks.
- Renewal only when required.

## Implementation Phases

Phase 1: CLI and Endpoint Compatibility
- Add implicit `sync` mode (`sparsync SRC DEST`).
- Add transport `auto` decision engine.
- Support remote->local orchestration path.
- Ensure SSH-style endpoints default to SSH data plane; no implicit QUIC service start.

Phase 2: Secrets and Profile Hardening
- Introduce config vs secrets directory split.
- Add permission enforcement and migration from existing `.config` secrets.
- Add optional key-store provider abstraction.

Phase 3: Enrollment Engine
- Implement explicit `enroll`.
- Add auto-enroll policy only for QUIC endpoints (`sparsync://...`) when explicitly enabled.
- Add cert expiry/revocation/version checks and renewal logic.

Phase 4: Server Lifecycle Management
- Implement remote server `start/stop/status`.
- Add persistent service templates (systemd user/system) and docs.
- Keep one-shot bootstrap mode as fallback.

Phase 5: SSH Overhead Reduction
- Single-shot bootstrap RPC over SSH.
- Multiplexed SSH sessions for setup steps.
- Binary version/hash probe + cached install.
- Remove fixed sleeps and replace with readiness checks.
- Keep SSH data-plane path separate from QUIC bootstrap path so rsync-like SSH usage remains predictable.

Phase 6: Performance Qualification
- Split benchmark suites:
  - enrollment (first-run)
  - steady-state repeat runs
- Gate on:
  - repeat QUIC after enrollment ~= direct QUIC baseline
  - no regressions in no-change and changed-set medians
  - acceptable SSH fallback latency vs rsync-over-SSH baseline.

## Safety Defaults

- mTLS required for `sparsync://` by default.
- Prefix-scoped authorization required for issued clients.
- Host key verification on SSH bootstrap.
- No destructive delete unless explicitly requested (`--delete`).
- Clear audit logs for authz decisions and connection identity.

## Risks and Mitigations

- Risk: bootstrap still dominates user-perceived latency.
  - Mitigation: one-time enrollment, cached profiles, service-managed server.
- Risk: secrets leakage from weak file permissions.
  - Mitigation: strict permission enforcement and migration checks.
- Risk: transport fallback confusion.
  - Mitigation: explicit `--transport` override and verbose mode explaining chosen path.
- Risk: remote->local orchestration complexity.
  - Mitigation: implement in controlled steps with integration tests for each direction/transport.

## Definition of Done

This effort is not complete until all of the following are implemented and validated:

- `sparsync [OPTIONS] SRC DEST` works for local, SSH-style remote, and `sparsync://` endpoints.
- Both directions are supported: local->remote and remote->local.
- SSH-style endpoints are rsync-like by default (SSH data plane, no implicit persistent service start).
- `sparsync://` supports:
  - pre-enrolled direct QUIC+mTLS use
  - one-time enrollment flow (`enroll`)
  - optional explicit SSH bootstrap on invocation (`--bootstrap ssh --ssh-target ...`)
- Remote server lifecycle commands (`server start|stop|status`) are implemented.
- Initial rsync-compatible flag set is implemented:
  - `-a -v -z -h -P --delete --dry-run/-n --exclude --include -u --bwlimit`
- Unknown flags fail fast with clear errors.
- Unknown option values and invalid short-flag bundles fail fast before transfer starts.
- Secret management hardening is in place (config/secrets split + permission enforcement).
- Benchmark gates pass:
  - repeat runs after enrollment are within target range of direct `sparsync over QUIC` baseline
  - no unacceptable regressions in SSH fallback or local copy modes.

## Implementation Status (Current)

Implemented:

- Primary invocation: `sparsync [OPTIONS] SRC DEST` (implicit `sync` mode)
- Endpoint parsing: local, SSH-style, `sparsync://`
- Transport auto-selection (`ssh` for SSH-style, `quic` for `sparsync://`)
- Directions:
  - local->local (native local copy path)
  - local->remote
  - remote->local (SSH reverse-tunnel orchestrated push path)
- Commands:
  - `enroll`
  - `server start|stop|status`
- Secret/config split:
  - config in XDG config root
  - secrets in XDG data root with strict permissions
- Supported initial flag behavior:
  - `-a -v -z -h -P --dry-run/-n --exclude --include -u --bwlimit`
  - `--delete` for local->local, local->remote, and remote->local
- Unknown flags and invalid flag bundles fail fast.

Benchmark qualification reruns have been executed post-change and documented in `BENCHMARKS.md`.
