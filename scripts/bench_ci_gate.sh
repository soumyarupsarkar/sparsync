#!/usr/bin/env bash
set -euo pipefail

RUNS=${RUNS:-3}
TRANSPORTS=${TRANSPORTS:-"ssh"}
OUT_DIR=${OUT_DIR:-"bench-artifacts"}
mkdir -p "$OUT_DIR"

RAW_OUT="$OUT_DIR/bench_median.txt"
SUMMARY_JSON="$OUT_DIR/bench_summary.json"

echo "benchmark gate: runs=$RUNS transports=$TRANSPORTS"
RUNS="$RUNS" TRANSPORTS="$TRANSPORTS" ./scripts/bench_remote_rsync_vs_sparsync_median.sh | tee "$RAW_OUT"

python3 - "$RAW_OUT" "$SUMMARY_JSON" <<'PY'
import json
import os
import sys

raw_path = sys.argv[1]
summary_path = sys.argv[2]

def env_float(name: str, default: float) -> float:
    value = os.environ.get(name, "")
    if not value:
        return default
    return float(value)

thresholds = {
    "ssh": {
        "first": env_float("MAX_SSH_FIRST_RATIO", 1.30),
        "second": env_float("MAX_SSH_SECOND_RATIO", 1.20),
        "changed": env_float("MAX_SSH_CHANGED_RATIO", 1.20),
    },
    "daemon": {
        "first": env_float("MAX_DAEMON_FIRST_RATIO", 3.00),
        "second": env_float("MAX_DAEMON_SECOND_RATIO", 1.30),
        "changed": env_float("MAX_DAEMON_CHANGED_RATIO", 1.30),
    },
}

rows = {}
current = None
with open(raw_path, "r", encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        if line.startswith("transport=") and "_median" not in line:
            current = line.split("=", 1)[1]
            rows[current] = {}
            continue
        if current is None or "_median=" not in line:
            continue
        key, value = line.split("=", 1)
        try:
            rows[current][key] = float(value)
        except ValueError:
            pass

def rsync_label(transport: str) -> str:
    return "rsync_remote" if transport == "daemon" else "rsync_ssh"

failures = []
summary = {"transports": {}}

for transport, data in rows.items():
    label = rsync_label(transport)
    sp1 = data.get("sparsync_first_ms_median")
    sp2 = data.get("sparsync_second_ms_median")
    sp3 = data.get("sparsync_changed_ms_median")
    rs1 = data.get(f"{label}_first_ms_median")
    rs2 = data.get(f"{label}_second_ms_median")
    rs3 = data.get(f"{label}_changed_ms_median")
    if None in (sp1, sp2, sp3, rs1, rs2, rs3):
        failures.append(f"{transport}: missing benchmark keys in median output")
        continue

    ratios = {
        "first": (sp1 / rs1) if rs1 else float("inf"),
        "second": (sp2 / rs2) if rs2 else float("inf"),
        "changed": (sp3 / rs3) if rs3 else float("inf"),
    }
    summary["transports"][transport] = {
        "sparsync_ms": {"first": sp1, "second": sp2, "changed": sp3},
        "rsync_ms": {"first": rs1, "second": rs2, "changed": rs3},
        "ratios": ratios,
        "thresholds": thresholds.get(transport, {}),
    }
    gate = thresholds.get(transport)
    if gate:
        for metric, ratio in ratios.items():
            limit = gate[metric]
            if ratio > limit:
                failures.append(
                    f"{transport}:{metric} ratio {ratio:.3f} exceeds threshold {limit:.3f}"
                )

with open(summary_path, "w", encoding="utf-8") as out:
    json.dump(summary, out, indent=2, sort_keys=True)

print(json.dumps(summary, indent=2, sort_keys=True))
if failures:
    print("benchmark gate failed:")
    for failure in failures:
        print(f"  - {failure}")
    sys.exit(1)
print("benchmark gate passed")
PY
