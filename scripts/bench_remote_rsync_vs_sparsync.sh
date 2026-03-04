#!/usr/bin/env bash
set -euo pipefail

if [ -z "${SPARSYNC_BIN:-}" ]; then
  if [ -x "./target/release/sparsync" ]; then
    SPARSYNC_BIN="./target/release/sparsync"
  else
    SPARSYNC_BIN="./target/debug/sparsync"
  fi
fi
SPARSYNC_PORT=${SPARSYNC_PORT:-8970}
RSYNC_PORT=${RSYNC_PORT:-28730}
SMALL_DIRS=${SMALL_DIRS:-10}
SMALL_FILES_PER_DIR=${SMALL_FILES_PER_DIR:-100}
SMALL_FILE_SIZE=${SMALL_FILE_SIZE:-4096}
LARGE_FILES=${LARGE_FILES:-8}
LARGE_FILE_SIZE=${LARGE_FILE_SIZE:-2097152}
CHANGED_FILES=${CHANGED_FILES:-100}
SPARSYNC_PARALLEL_FILES=${SPARSYNC_PARALLEL_FILES:-16}
SPARSYNC_CONNECTIONS=${SPARSYNC_CONNECTIONS:-1}
SPARSYNC_SCAN_WORKERS=${SPARSYNC_SCAN_WORKERS:-8}
SPARSYNC_HASH_WORKERS=${SPARSYNC_HASH_WORKERS:-16}
SPARSYNC_FIRST_COLD=${SPARSYNC_FIRST_COLD:-0}
KEEP_TMP=${KEEP_TMP:-0}
RSYNC_ARGS=${RSYNC_ARGS:-"-a --delete"}
RSYNC_TRANSPORT=${RSYNC_TRANSPORT:-daemon}
RSYNC_SSH_PORT=${RSYNC_SSH_PORT:-2222}
RSYNC_SSH_USER=${RSYNC_SSH_USER:-root}

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync not found" >&2
  exit 1
fi

if [ ! -x "$SPARSYNC_BIN" ]; then
  echo "sparsync binary not found at $SPARSYNC_BIN" >&2
  echo "build first: cargo build --release" >&2
  exit 1
fi

ROOT=$(mktemp -d)
BASE="$ROOT/base"
SRC_SP="$ROOT/src_sparsync"
SRC_RS_DAEMON="$ROOT/src_rsync_daemon"
SRC_RS_SSH="$ROOT/src_rsync_ssh"
DST_SP="$ROOT/dst_sparsync"
DST_RS_DAEMON="$ROOT/dst_rsync_daemon"
DST_RS_SSH="$ROOT/dst_rsync_ssh"
CERT_DIR="$ROOT/certs"
RSYNC_CONF="$ROOT/rsyncd.conf"
SSH_DIR="$ROOT/ssh"
mkdir -p "$BASE" "$SRC_SP" "$SRC_RS_DAEMON" "$SRC_RS_SSH" "$DST_SP" "$DST_RS_DAEMON" "$DST_RS_SSH" "$CERT_DIR" "$SSH_DIR"

cleanup() {
  if [ -n "${SP_PID:-}" ]; then
    kill "$SP_PID" 2>/dev/null || true
    wait "$SP_PID" 2>/dev/null || true
  fi
  if [ -n "${RS_PID:-}" ]; then
    kill "$RS_PID" 2>/dev/null || true
    wait "$RS_PID" 2>/dev/null || true
  fi
  if [ -n "${SSH_PID:-}" ]; then
    kill "$SSH_PID" 2>/dev/null || true
    wait "$SSH_PID" 2>/dev/null || true
  fi
  if [ "$KEEP_TMP" != "1" ]; then
    rm -rf "$ROOT"
  else
    echo "kept temp dir: $ROOT"
  fi
}
trap cleanup EXIT

mkdir -p "$BASE/small" "$BASE/large"
for d in $(seq 0 $((SMALL_DIRS - 1))); do
  mkdir -p "$BASE/small/dir_$d"
  for f in $(seq 0 $((SMALL_FILES_PER_DIR - 1))); do
    head -c "$SMALL_FILE_SIZE" /dev/urandom > "$BASE/small/dir_$d/file_${f}.bin"
  done
done
for f in $(seq 0 $((LARGE_FILES - 1))); do
  head -c "$LARGE_FILE_SIZE" /dev/urandom > "$BASE/large/chunk_${f}.bin"
done

cp -a "$BASE/." "$SRC_SP/"
cp -a "$BASE/." "$SRC_RS_DAEMON/"
cp -a "$BASE/." "$SRC_RS_SSH/"

FILES=$(find "$BASE" -type f | wc -l | tr -d ' ')
BYTES=$(du -sb "$BASE" | awk '{print $1}')

"$SPARSYNC_BIN" gen-cert \
  --cert "$CERT_DIR/server.cert.der" \
  --key "$CERT_DIR/server.key.der" \
  --name localhost >/dev/null

"$SPARSYNC_BIN" serve \
  --bind "127.0.0.1:${SPARSYNC_PORT}" \
  --destination "$DST_SP" \
  --cert "$CERT_DIR/server.cert.der" \
  --key "$CERT_DIR/server.key.der" >/tmp/sparsync-bench-server.log 2>&1 &
SP_PID=$!
sleep 1

ms_now() { date +%s%N; }

SPARSYNC_FIRST_ARGS=()
if [ "$SPARSYNC_FIRST_COLD" = "1" ]; then
  SPARSYNC_FIRST_ARGS+=(--cold-start)
fi

sp1_start=$(ms_now)
"$SPARSYNC_BIN" push \
  --source "$SRC_SP" \
  --server "127.0.0.1:${SPARSYNC_PORT}" \
  --server-name localhost \
  --ca "$CERT_DIR/server.cert.der" \
  --parallel-files "$SPARSYNC_PARALLEL_FILES" \
  --connections "$SPARSYNC_CONNECTIONS" \
  --scan-workers "$SPARSYNC_SCAN_WORKERS" \
  --hash-workers "$SPARSYNC_HASH_WORKERS" \
  "${SPARSYNC_FIRST_ARGS[@]}" \
  --compression-level 0 >/tmp/sparsync-bench-push1.log
sp1_end=$(ms_now)

sp2_start=$(ms_now)
"$SPARSYNC_BIN" push \
  --source "$SRC_SP" \
  --server "127.0.0.1:${SPARSYNC_PORT}" \
  --server-name localhost \
  --ca "$CERT_DIR/server.cert.der" \
  --parallel-files "$SPARSYNC_PARALLEL_FILES" \
  --connections "$SPARSYNC_CONNECTIONS" \
  --scan-workers "$SPARSYNC_SCAN_WORKERS" \
  --hash-workers "$SPARSYNC_HASH_WORKERS" \
  --compression-level 0 >/tmp/sparsync-bench-push2.log
sp2_end=$(ms_now)

changed=0
while IFS= read -r path; do
  printf 'delta-%s\n' "$changed" >> "$path"
  changed=$((changed + 1))
  [ "$changed" -ge "$CHANGED_FILES" ] && break
done < <(find "$SRC_SP/small" -type f | sort)

sp3_start=$(ms_now)
"$SPARSYNC_BIN" push \
  --source "$SRC_SP" \
  --server "127.0.0.1:${SPARSYNC_PORT}" \
  --server-name localhost \
  --ca "$CERT_DIR/server.cert.der" \
  --parallel-files "$SPARSYNC_PARALLEL_FILES" \
  --connections "$SPARSYNC_CONNECTIONS" \
  --scan-workers "$SPARSYNC_SCAN_WORKERS" \
  --hash-workers "$SPARSYNC_HASH_WORKERS" \
  --compression-level 0 >/tmp/sparsync-bench-push3.log
sp3_end=$(ms_now)

cat > "$RSYNC_CONF" <<CFG
pid file = $ROOT/rsyncd.pid
use chroot = false
port = $RSYNC_PORT
[syncdst]
path = $DST_RS_DAEMON
read only = false
list = yes
uid = root
gid = root
CFG

run_rsync_daemon() {
  rsync --daemon --no-detach --config "$RSYNC_CONF" >/tmp/rsync-bench-daemon.log 2>&1 &
  RS_PID=$!
  sleep 1

  rs1_start=$(ms_now)
  rsync $RSYNC_ARGS "$SRC_RS_DAEMON/" "rsync://127.0.0.1:${RSYNC_PORT}/syncdst/" >/tmp/rsync-bench1.log
  rs1_end=$(ms_now)

  rs2_start=$(ms_now)
  rsync $RSYNC_ARGS "$SRC_RS_DAEMON/" "rsync://127.0.0.1:${RSYNC_PORT}/syncdst/" >/tmp/rsync-bench2.log
  rs2_end=$(ms_now)

  changed=0
  while IFS= read -r path; do
    printf 'delta-%s\n' "$changed" >> "$path"
    changed=$((changed + 1))
    [ "$changed" -ge "$CHANGED_FILES" ] && break
  done < <(find "$SRC_RS_DAEMON/small" -type f | sort)

  rs3_start=$(ms_now)
  rsync $RSYNC_ARGS "$SRC_RS_DAEMON/" "rsync://127.0.0.1:${RSYNC_PORT}/syncdst/" >/tmp/rsync-bench3.log
  rs3_end=$(ms_now)
}

run_rsync_ssh() {
  if ! command -v ssh >/dev/null 2>&1; then
    echo "ssh client not found for RSYNC_TRANSPORT=ssh" >&2
    exit 1
  fi
  if [ ! -x /usr/sbin/sshd ]; then
    echo "sshd not found for RSYNC_TRANSPORT=ssh (install openssh-server)" >&2
    exit 1
  fi

  ssh-keygen -t ed25519 -N "" -f "$SSH_DIR/bench_id_ed25519" >/dev/null
  cp "$SSH_DIR/bench_id_ed25519.pub" "$SSH_DIR/authorized_keys"
  chmod 600 "$SSH_DIR/authorized_keys"
  ssh-keygen -t ed25519 -N "" -f "$SSH_DIR/ssh_host_ed25519_key" >/dev/null

  cat > "$SSH_DIR/sshd_config" <<CFG
Port $RSYNC_SSH_PORT
ListenAddress 127.0.0.1
HostKey $SSH_DIR/ssh_host_ed25519_key
PidFile $SSH_DIR/sshd.pid
AuthorizedKeysFile $SSH_DIR/authorized_keys
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
PubkeyAuthentication yes
PermitRootLogin yes
UsePAM no
StrictModes no
LogLevel ERROR
Subsystem sftp internal-sftp
CFG

  /usr/sbin/sshd -f "$SSH_DIR/sshd_config" -E /tmp/rsync-bench-sshd.log -D &
  SSH_PID=$!
  sleep 1

  ssh_cmd="ssh -i $SSH_DIR/bench_id_ed25519 -p $RSYNC_SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

  rs1_start=$(ms_now)
  rsync $RSYNC_ARGS -e "$ssh_cmd" "$SRC_RS_SSH/" "${RSYNC_SSH_USER}@127.0.0.1:$DST_RS_SSH/" >/tmp/rsync-bench1.log
  rs1_end=$(ms_now)

  rs2_start=$(ms_now)
  rsync $RSYNC_ARGS -e "$ssh_cmd" "$SRC_RS_SSH/" "${RSYNC_SSH_USER}@127.0.0.1:$DST_RS_SSH/" >/tmp/rsync-bench2.log
  rs2_end=$(ms_now)

  changed=0
  while IFS= read -r path; do
    printf 'delta-%s\n' "$changed" >> "$path"
    changed=$((changed + 1))
    [ "$changed" -ge "$CHANGED_FILES" ] && break
  done < <(find "$SRC_RS_SSH/small" -type f | sort)

  rs3_start=$(ms_now)
  rsync $RSYNC_ARGS -e "$ssh_cmd" "$SRC_RS_SSH/" "${RSYNC_SSH_USER}@127.0.0.1:$DST_RS_SSH/" >/tmp/rsync-bench3.log
  rs3_end=$(ms_now)
}

RSYNC_LABEL=""
case "$RSYNC_TRANSPORT" in
  daemon)
    RSYNC_LABEL="rsync_remote"
    run_rsync_daemon
    ;;
  ssh)
    RSYNC_LABEL="rsync_ssh"
    run_rsync_ssh
    ;;
  *)
    echo "invalid RSYNC_TRANSPORT='$RSYNC_TRANSPORT' (expected 'daemon' or 'ssh')" >&2
    exit 1
    ;;
esac

rm -rf "$DST_SP/.sparsync"
diff -qr "$SRC_SP" "$DST_SP" >/dev/null
if [ "$RSYNC_TRANSPORT" = "daemon" ]; then
  diff -qr "$SRC_RS_DAEMON" "$DST_RS_DAEMON" >/dev/null
else
  diff -qr "$SRC_RS_SSH" "$DST_RS_SSH" >/dev/null
fi

SP1_MS=$(( (sp1_end - sp1_start) / 1000000 ))
SP2_MS=$(( (sp2_end - sp2_start) / 1000000 ))
SP3_MS=$(( (sp3_end - sp3_start) / 1000000 ))
RS1_MS=$(( (rs1_end - rs1_start) / 1000000 ))
RS2_MS=$(( (rs2_end - rs2_start) / 1000000 ))
RS3_MS=$(( (rs3_end - rs3_start) / 1000000 ))

python3 - <<PY
sp1=$SP1_MS
sp2=$SP2_MS
sp3=$SP3_MS
rs1=$RS1_MS
rs2=$RS2_MS
rs3=$RS3_MS
label="$RSYNC_LABEL"
files=$FILES
bytes_=$BYTES
print(f"dataset_files={files}")
print(f"dataset_bytes={bytes_}")
print(f"sparsync_first_ms={sp1}")
print(f"sparsync_second_ms={sp2}")
print(f"sparsync_changed_ms={sp3}")
print(f"{label}_first_ms={rs1}")
print(f"{label}_second_ms={rs2}")
print(f"{label}_changed_ms={rs3}")
print(f"ratio_first_sparsync_over_rsync={sp1/rs1:.2f}x" if rs1 else "ratio_first_sparsync_over_rsync=inf")
print(f"ratio_second_sparsync_over_rsync={sp2/rs2:.2f}x" if rs2 else "ratio_second_sparsync_over_rsync=inf")
print(f"ratio_changed_sparsync_over_rsync={sp3/rs3:.2f}x" if rs3 else "ratio_changed_sparsync_over_rsync=inf")
PY
