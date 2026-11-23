#!/usr/bin/env bash
# Reproducible end-to-end gap-recovery example.  Re-runnable: it removes any
# previous ledger, rebuilds the archives, plan and batches, then applies six
# handover rounds and shows the one change slip each round delivers.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
TOOL="$ROOT/narc_tool.py"

cd "$HERE"
rm -rf ledger archive-a.narc archive-b.narc archive-c.narc \
       plan.json batch-*.json

# 1. Build the frozen v1 archives and the batches.
python3 -I build_example.py

# 2. Create the ledger (never overwrites an existing one).
mkdir ledger
python3 -I "$TOOL" gap-init ledger plan.json
echo
echo "== fresh ledger: the whole plan is still to be acquired =="
python3 -I - <<'PY'
import sys
sys.path.insert(0, "../..")
from narc.gap_ledger import pending_intervals
print(pending_intervals("ledger"))
PY

# 3. One handover round per batch; stdout is exactly one change slip.
for batch in 1 2 3 4 5 6; do
  echo
  echo "== batch-$batch =="
  cat "batch-$batch.json"
  echo "-- change slip:"
  python3 -I "$TOOL" gap-apply ledger "batch-$batch.json"
done

# 4. A byte-identical retry of the latest revision returns the original slip
#    without touching any archive: the manual captures are already offline.
echo
echo "== byte-identical retry of the latest revision (archives removed) =="
rm -f archive-a.narc archive-b.narc archive-c.narc
python3 -I "$TOOL" gap-apply ledger batch-6.json

# 5. A different batch at the same revision, a skipped revision and an older
#    revision are all refused without changing the ledger.
echo
echo "== conflicting revisions are refused =="
for attempt in conflict skip older; do
  case "$attempt" in
    conflict) python3 -I - <<'PY'
import json
json.dump({"revision": 6, "changes": [
    {"source_id": "probe-b", "op": "remove"}]},
    open("conflict.json", "w"))
PY
      file=conflict.json ;;
    skip) python3 -I - <<'PY'
import json
json.dump({"revision": 8, "changes": [
    {"source_id": "probe-b", "op": "remove"}]},
    open("skip.json", "w"))
PY
      file=skip.json ;;
    older) python3 -I - <<'PY'
import json
json.dump({"revision": 5, "changes": [
    {"source_id": "probe-b", "op": "remove"}]},
    open("older.json", "w"))
PY
      file=older.json ;;
  esac
  if python3 -I "$TOOL" gap-apply ledger "$file" 2>/dev/null; then
    echo "ERROR: $attempt revision was accepted" >&2
    exit 1
  fi
  echo "OK: $attempt revision refused"
done
rm -f conflict.json skip.json older.json
