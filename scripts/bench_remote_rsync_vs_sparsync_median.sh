#!/usr/bin/env bash
set -euo pipefail

RUNS=${RUNS:-5}
TRANSPORTS=${TRANSPORTS:-"daemon ssh"}
START_SPARSYNC_PORT=${START_SPARSYNC_PORT:-11000}
START_RSYNC_PORT=${START_RSYNC_PORT:-31000}
START_RSYNC_SSH_PORT=${START_RSYNC_SSH_PORT:-24000}

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [ "$RUNS" -lt 1 ]; then
  echo "RUNS must be a positive integer" >&2
  exit 1
fi

tmp=$(mktemp)
cleanup() { rm -f "$tmp"; }
trap cleanup EXIT

sp_port=$START_SPARSYNC_PORT
rs_port=$START_RSYNC_PORT
ssh_port=$START_RSYNC_SSH_PORT

for transport in $TRANSPORTS; do
  for run in $(seq 1 "$RUNS"); do
    out=$(
      RSYNC_TRANSPORT="$transport" \
      SPARSYNC_PORT="$sp_port" \
      RSYNC_PORT="$rs_port" \
      RSYNC_SSH_PORT="$ssh_port" \
      ./scripts/bench_remote_rsync_vs_sparsync.sh
    )
    printf "transport=%s run=%s\n%s\n" "$transport" "$run" "$out" >> "$tmp"
    sp_port=$((sp_port + 1))
    rs_port=$((rs_port + 1))
    ssh_port=$((ssh_port + 1))
  done
done

python3 - "$tmp" <<'PY'
import re
import statistics
import sys
from collections import defaultdict

path = sys.argv[1]
rows = defaultdict(lambda: defaultdict(list))
current = None

with open(path, "r", encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        m = re.match(r"transport=(\w+)\s+run=\d+", line)
        if m:
            current = m.group(1)
            continue
        if current is None:
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if value.endswith("x"):
            continue
        try:
            rows[current][key].append(float(value))
        except ValueError:
            pass

for transport in sorted(rows):
    data = rows[transport]
    print(f"transport={transport}")
    for key in sorted(data):
        med = statistics.median(data[key])
        if med.is_integer():
            med = int(med)
        print(f"{key}_median={med}")
    print()
PY
