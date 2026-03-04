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
KEEP_TMP=${KEEP_TMP:-0}
RSYNC_ARGS=${RSYNC_ARGS:-"-a --delete"}

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
SRC_RS="$ROOT/src_rsync"
DST_SP="$ROOT/dst_sparsync"
DST_RS="$ROOT/dst_rsync"
CERT_DIR="$ROOT/certs"
RSYNC_CONF="$ROOT/rsyncd.conf"
mkdir -p "$BASE" "$SRC_SP" "$SRC_RS" "$DST_SP" "$DST_RS" "$CERT_DIR"

cleanup() {
  if [ -n "${SP_PID:-}" ]; then
    kill "$SP_PID" 2>/dev/null || true
    wait "$SP_PID" 2>/dev/null || true
  fi
  if [ -n "${RS_PID:-}" ]; then
    kill "$RS_PID" 2>/dev/null || true
    wait "$RS_PID" 2>/dev/null || true
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
cp -a "$BASE/." "$SRC_RS/"

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
path = $DST_RS
read only = false
list = yes
uid = root
gid = root
CFG

rsync --daemon --no-detach --config "$RSYNC_CONF" >/tmp/rsync-bench-daemon.log 2>&1 &
RS_PID=$!
sleep 1

rs1_start=$(ms_now)
rsync $RSYNC_ARGS "$SRC_RS/" "rsync://127.0.0.1:${RSYNC_PORT}/syncdst/" >/tmp/rsync-bench1.log
rs1_end=$(ms_now)

rs2_start=$(ms_now)
rsync $RSYNC_ARGS "$SRC_RS/" "rsync://127.0.0.1:${RSYNC_PORT}/syncdst/" >/tmp/rsync-bench2.log
rs2_end=$(ms_now)

changed=0
while IFS= read -r path; do
  printf 'delta-%s\n' "$changed" >> "$path"
  changed=$((changed + 1))
  [ "$changed" -ge "$CHANGED_FILES" ] && break
done < <(find "$SRC_RS/small" -type f | sort)

rs3_start=$(ms_now)
rsync $RSYNC_ARGS "$SRC_RS/" "rsync://127.0.0.1:${RSYNC_PORT}/syncdst/" >/tmp/rsync-bench3.log
rs3_end=$(ms_now)

rm -rf "$DST_SP/.sparsync"
diff -qr "$SRC_SP" "$DST_SP" >/dev/null
diff -qr "$SRC_RS" "$DST_RS" >/dev/null

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
files=$FILES
bytes_=$BYTES
print(f"dataset_files={files}")
print(f"dataset_bytes={bytes_}")
print(f"sparsync_first_ms={sp1}")
print(f"sparsync_second_ms={sp2}")
print(f"sparsync_changed_ms={sp3}")
print(f"rsync_remote_first_ms={rs1}")
print(f"rsync_remote_second_ms={rs2}")
print(f"rsync_remote_changed_ms={rs3}")
print(f"ratio_first_sparsync_over_rsync={sp1/rs1:.2f}x" if rs1 else "ratio_first_sparsync_over_rsync=inf")
print(f"ratio_second_sparsync_over_rsync={sp2/rs2:.2f}x" if rs2 else "ratio_second_sparsync_over_rsync=inf")
print(f"ratio_changed_sparsync_over_rsync={sp3/rs3:.2f}x" if rs3 else "ratio_changed_sparsync_over_rsync=inf")
PY
