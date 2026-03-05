# mTLS Ergonomics Roadmap

## Goal

Make secure `sparsync` usage feel as easy as `rsync over ssh`, while keeping QUIC as the fast data path.

Target outcome:

- Users run one command to sync a local source path to a remote destination.
- Initialization, access grants, enrollment, and key exchange are hidden behind SSH bootstrap.
- Actual transfer runs over QUIC with mTLS authorization.

## Implementation Status (March 4, 2026)

Implemented in this repo:

- Protocol compatibility handshake (`Hello` frame version check).
- mTLS server/client flags:
  - `serve --client-ca`
  - `push --client-cert --client-key`
- Server-side authz policy enforcement from fingerprint allowlist (`--authz`).
- Prefix-scoped authorization (`--allow-prefix`) enforced across init/upload frames.
- Auth lifecycle CLI:
  - `auth init-server`
  - `auth issue-client`
  - `auth revoke-client`
  - `auth rotate`
  - `auth status`
- Structured audit logging (`sparsync::audit`) for allow/deny decisions and bytes transferred.
- Profile persistence and reuse for push (`push --profile`).
- One-command local->remote sync with SSH bootstrap and QUIC transfer:
  - `sync /src user@host:/dest`
  - defaults: `--bootstrap ssh --transport quic --install ephemeral`
- Default remote install mode is ephemeral upload/cleanup per sync.
- Optional persistent install path for `--install auto` / `--install upload-local-binary` is `~/.local/bin/sparsync`.
- One-shot remote serve session mode (`serve --once`) used by bootstrap sync.

## Desired End-State UX

Primary command:

```bash
sparsync sync /src/path user@host:/dest/path --transport quic --bootstrap ssh --install ephemeral
```

First-run behavior:

1. SSH to remote host.
2. Verify/install matching `sparsync` binary if needed.
3. Initialize server trust material if not already configured.
4. Issue or refresh caller client cert.
5. Launch an ephemeral receive session scoped to `/dest/path`.
6. Transfer over QUIC+mTLS.
7. Persist profile locally for future pushes.

Repeat-run behavior:

1. Reuse local profile and trust bundle.
2. Refresh short-lived cert only when near expiry.
3. Reuse remote service/session setup when possible.
4. Transfer over QUIC+mTLS with minimal bootstrap overhead.

## Roadmap

### Phase 0: Foundations

- Add explicit protocol version handshake to avoid silent compatibility issues.
- Add profile storage (`~/.config/sparsync/profiles.toml`).
- Define stable identity fields for authz (`client_id`, SAN URI, cert fingerprint).

Exit criteria:

- Client and server reject incompatible protocol versions with clear errors.
- `push --profile <name>` resolves endpoint + trust settings.

### Phase 1: mTLS Core

- Server:
  - Require client certificates with `--client-ca`.
  - Enforce authz policy file mapping identity to destination prefix allowlist.
- Client:
  - Present client cert/key via `--client-cert` and `--client-key`.
- Add audit logging:
  - Peer identity, destination path, decision allow/deny, bytes transferred.

Exit criteria:

- Unauthorized certs are rejected before transfer starts.
- Authorized certs can only write to allowed destination prefixes.

### Phase 2: Auth CLI (Manual but Simple)

- Add `sparsync auth` commands:
  - `auth init-server`
  - `auth issue-client`
  - `auth revoke-client`
  - `auth rotate`
- Produce/install profile bundles from auth commands.
- Add `auth status` for certificate and policy visibility.

Exit criteria:

- End-to-end mTLS setup can be done without raw OpenSSL commands.
- Admin can grant and revoke client access using only `sparsync`.

### Phase 3: SSH Bootstrap Control Plane

- Add `--bootstrap ssh` path for `push`.
- Bootstrap actions over SSH:
  - Detect remote `sparsync`.
  - Optionally install/upgrade binary.
  - Initialize remote server auth state if missing.
  - Issue/fetch short-lived client cert and trust bundle.
- Add remote helper subcommand (hidden/internal), for example:
  - `sparsync agent bootstrap`
  - `sparsync serve-session --once`

Exit criteria:

- A new authorized caller can run one SSH-assisted command and complete a push without manual cert file copying.

### Phase 4: One-Command Operational UX

- Support rsync-like endpoint syntax in `push`/`sync`:
  - `/local/path`
  - `[user@]host:/remote/path`
- Make SSH bootstrap automatic by default when remote endpoint syntax is used.
- Add `--install` modes:
  - `ephemeral` (default)
  - `auto`
  - `off`
  - `upload-local-binary`
  - `package-manager`

Exit criteria:

- `sparsync push /src user@host:/dest` works for first-time and repeat pushes with no manual PKI steps.

### Phase 5: Hardening and Ops

- Certificate lifecycle:
  - short-lived client certs
  - automatic renewal
  - explicit revocation and denylist support
- Binary provenance:
  - checksum/signature verification for remote install
- Safety controls:
  - destination-path guardrails
  - optional approval prompts for first bootstrap/install
  - structured logs for SIEM ingestion

Exit criteria:

- Security and operability are acceptable for internet-exposed deployments with SSH access controls.

## Transport Strategy

- Keep QUIC+mTLS as the performance path.
- Optionally add `--transport ssh` later as a compatibility/fallback mode when UDP is blocked.
- Do not attempt QUIC-over-standard-SSH forwarding as primary design.

## Performance Guardrails

- Existing direct QUIC flow must remain unchanged when bootstrap is disabled.
- One-command bootstrap overhead must be paid mainly on first run.
- Repeat-run bootstrap overhead target: low single-digit seconds worst case, typically much lower.
- Continue benchmark reporting in:
  - `BENCHMARKS.md`
  - `PERFORMANCE.md`

## Implementation Notes (Current Repo)

- Likely touch points:
  - CLI and command dispatch: `src/main.rs`
  - TLS config and cert loading: `src/certs.rs`
  - Client transfer orchestration: `src/transfer.rs`
  - Server authn/authz enforcement: `src/server.rs`
  - Optional profile persistence: new profile module + config file handling
- Keep existing benchmark harnesses unchanged for perf comparison continuity.

## Open Questions

- Should first-run bootstrap default to ephemeral per-push sessions or persistent daemon setup?
- Should authorization be identity-to-prefix only, or include operation quotas/rate limits?
- Should binary install default to uploaded local binary or remote package channel?
- How strict should remote version matching be (`exact`, `same-major`, negotiated)?

## Update (March 4, 2026, Follow-up)

Additional implemented items since the initial roadmap draft:

- Prefix-scoped authorization is now enforced by the server for init/upload operations.
  - `auth issue-client --allow-prefix <prefix>` configures per-client destination scope.
  - `--allow-prefix /` grants full destination-root access.
- Structured auth audit logs are emitted on `sparsync::audit`.
  - Connection allow/deny decisions include client identity + fingerprint.
  - Path authorization denials are logged with attempted path and allowed prefixes.
  - Per-connection transferred bytes are logged on connection close.
- SSH bootstrap flow now issues client certs with explicit prefix argument (`--allow-prefix /`).

Transport status clarification:

- `sparsync over ssh` as a data plane transport via stdio is now implemented.
- `sync --transport ssh` uses SSH stdio with remote `serve-stdio` and can run with `--bootstrap none` (if `sparsync` already exists remotely) or `--bootstrap ssh` (default `--install ephemeral`).
- Current implementation is functional but intentionally conservative (single SSH data session, no stream multiplexing), so performance is expected to trail QUIC mode.

Benchmark snapshot (March 5, 2026, localhost loopback):

- Dataset:
  - `1008` files total
  - `20,873,216` bytes total
  - Changed-set run modifies `100` small files
  - Release build, same dataset shape and worker settings across all modes
- Timings (ms):
  - `rsync over ssh`: first `375.49`, second `222.95`, changed `231.89`
  - `sparsync over ssh stdio`: first `947.82`, second `556.05`, changed `617.72`
  - `sparsync over quic (no mTLS)`: first `413.72`, second `30.73`, changed `59.44`
  - `sparsync over quic (mTLS)`: first `390.52`, second `32.28`, changed `68.97`
- Ratios vs `rsync over ssh`:
  - `sparsync over ssh stdio`: `2.52x`, `2.49x`, `2.66x` slower (first/second/changed)
  - `sparsync over quic (no mTLS)`: `1.10x` slower first, then `~7.3x` faster second and `~3.9x` faster changed
  - `sparsync over quic (mTLS)`: `1.04x` slower first, then `~6.9x` faster second and `~3.4x` faster changed

## SSH Stdio Performance Remediation Plan (March 5, 2026)

Goal:

- Reduce `sparsync --transport ssh` latency enough to be competitive with `rsync -e ssh`, while preserving current QUIC fast path behavior.

Current gap summary:

- SSH stdio mode is currently functional but slower due to per-file request/response churn, no batching parity with QUIC, and conservative sequential flow.

Execution sequence:

1. Add SSH transport profiling and stage timing (first)
   - Add `SPARSYNC_PROFILE=1` counters specific to SSH mode:
     - frame encode/decode time
     - round-trip count/time
     - read/write syscalls and bytes
     - scan/hash/init/upload phase timing
   - Exit criteria:
     - One benchmark run can attribute SSH time to at least: scan/hash, init control-plane, upload data-plane, and stdio framing overhead.

2. Bring protocol-path parity with QUIC for init/small/cold paths
   - Implement `InitBatchRequest/InitBatchResponse` in `serve-stdio` and SSH client.
   - Implement `UploadSmallBatchRequest/Response` and `UploadColdBatchRequest/Response` in SSH mode.
   - Replace per-file `InitFile` flow in SSH client with batched init + batched small/direct uploads.
   - Exit criteria:
     - SSH mode uses same high-level batching strategy as QUIC for unchanged/small/direct cases.
     - No functional regressions vs current SSH and QUIC behavior.

3. Add upload pipelining/windowing for SSH large-file batches
   - Move from strict request/response serialization to bounded in-flight batch window (same model as QUIC path).
   - Keep ordering/ack validation semantics consistent.
   - Exit criteria:
     - Large-file SSH upload path supports configurable in-flight window.
     - Throughput improves on first-sync large-file component without correctness regressions.

4. Reduce stdio framing and copy overhead
   - Reuse frame buffers (request/response) across calls.
   - Avoid per-frame temporary allocations where possible.
   - Avoid unnecessary flushes; flush at frame boundaries only where required.
   - Add buffered read/write wrappers with stable capacity sizing.
   - Exit criteria:
     - Lower CPU time in encode/decode + stdio read/write stages in profiling output.

5. Optimize no-change and small-change behavior
   - Ensure no-change path is dominated by batched init decisions, not per-file round-trips.
   - Revisit changed-set handling to minimize control traffic and unnecessary rework.
   - Exit criteria:
     - Significant reduction in second-sync and changed-sync latency for SSH mode.

6. Optional advanced delta path (post-core fixes)
   - Evaluate rsync-style rolling delta only after steps 1-5 are complete.
   - Gate by complexity/perf tradeoff and impact on first-sync path.
   - Exit criteria:
     - Clear win in changed-set benchmarks or feature deferred with documented rationale.

Benchmark and acceptance gates:

- Use the existing four-way benchmark matrix:
  - `sparsync --transport ssh`
  - `rsync -e ssh`
  - `sparsync` QUIC (no mTLS)
  - `sparsync` QUIC + mTLS
- Keep dataset and changed-set definitions fixed per run and report first/second/changed metrics.
- Primary target:
  - Bring SSH stdio mode much closer to `rsync -e ssh` (especially second/changed syncs), while avoiding regressions in QUIC modes.

## Update (March 5, 2026, SSH Remediation Execution)

Execution status for the sequence above:

1. Added SSH transport profiling and stage timing.
2. Added protocol-path parity for `InitBatch`, `UploadSmallBatch`, and `UploadColdBatch` in SSH stdio mode.
3. Added upload pipelining/windowing for SSH large-file batch uploads.
4. Reduced stdio framing/copy overhead (buffer reuse + buffered stdin/stdout session).
5. Fixed no-change/small-change behavior regression:
   - Root cause: resume state persistence used a stride and `serve-stdio` exited without a forced flush, so many completed-file entries were not written before next session.
   - Fix: added explicit `StateStore::flush()` and called it on `serve-stdio` shutdown; also flush on QUIC `serve --once` shutdown.
6. Optional advanced delta path: deferred.
   - Rationale: with step 5 fixed, changed/no-change SSH runs are now near parity with `rsync -e ssh` on the current dataset, so rolling-delta complexity is not justified as the next immediate SSH win.

Profile + benchmark snapshot (localhost loopback, same dataset shape used elsewhere):

- Dataset:
  - files: `1008`
  - bytes: `20,873,216`
  - changed set: append one line to first `100` lexicographically sorted files under `small/`
- Timings (ms):
  - `sparsync over ssh stdio`: first `502`, second `217`, changed `235`
  - `rsync -e ssh`: first `386`, second `218`, changed `234`
- Ratios (`sparsync / rsync`):
  - first: `1.30x`
  - second: `1.00x`
  - changed: `1.00x`

SSH profile highlights (`SPARSYNC_PROFILE=1`):

- First sync: `frames_sent=6`, `frames_received=6`
- Second sync (no changes): `frames_sent=3`, `frames_received=3`, `synced files=0 skipped=1008`
- Changed sync: `frames_sent=4`, `frames_received=4`, `synced files=100 skipped=908`
