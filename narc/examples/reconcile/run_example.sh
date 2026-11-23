#!/usr/bin/env bash
# Reproducible end-to-end reconciliation example.  Re-runnable: it removes
# any previous artifacts, rebuilds the primary/backup archives and the
# alignment sheets, reconciles them, and shows the differences sheet a duty
# officer would review.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
TOOL="$ROOT/narc_tool.py"

cd "$HERE"
rm -rf primary.narc backup.narc alignment.json alignment-shuffled.json \
       differences.jsonl differences-shuffled.jsonl

# 1. Build the two frozen v1 archives and the alignment sheets.
python3 -I build_example.py

# 2. Reconcile: one JSON difference per line, each traceable back to the
#    original archive via (fragment_id, column_id, row).
echo
echo "== reconcile primary against backup =="
python3 -I "$TOOL" reconcile alignment.json differences.jsonl
echo "-- differences.jsonl:"
cat differences.jsonl

# 3. The sheet does not depend on the order the mapping segments are listed.
echo
echo "== segment order invariance =="
python3 -I "$TOOL" reconcile alignment-shuffled.json differences-shuffled.jsonl \
  >/dev/null
if cmp -s differences.jsonl differences-shuffled.jsonl; then
  echo "OK: shuffling the alignment segments changes nothing"
else
  echo "ERROR: sheets differ" >&2
  exit 1
fi

# 4. The output path must not exist: a second run into the same file is
#    refused and the existing sheet is left untouched.
echo
echo "== existing output is refused =="
if python3 -I "$TOOL" reconcile alignment.json differences.jsonl 2>/dev/null; then
  echo "ERROR: existing output was overwritten" >&2
  exit 1
fi
echo "OK: existing differences.jsonl refused and preserved"
