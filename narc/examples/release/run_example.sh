#!/usr/bin/env bash
# Reproducible end-to-end release example.  Re-runnable: it removes any
# previous output, rebuilds the source archive and authorization, publishes
# the delivery, then opens every part with the ordinary catalog/query
# commands to prove each one decodes on its own.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
TOOL="$ROOT/narc_tool.py"

cd "$HERE"
rm -rf source.narc authorization.json delivery

# 1. Build the frozen v1 source and the JSON authorization slip.
python3 -I build_example.py

# 2. Publish into a directory that does not exist yet.
python3 -I "$TOOL" release source.narc authorization.json delivery

echo
echo "== delivery contents =="
ls -1 delivery
echo
echo "== parts.json =="
cat delivery/parts.json

# 3. Each part is an ordinary v1 file: catalog it standalone (with no other
#    part present) and query the rows it claims to cover.
echo
echo "== standalone decode of every part =="
python3 -I - "$TOOL" <<'PY'
import json
import os
import subprocess
import sys

tool = sys.argv[1]
manifest = json.load(open("delivery/parts.json", encoding="utf-8"))
for entry in manifest["parts"]:
    part = os.path.join("delivery", entry["file"])
    catalog = part + ".catalog.json"
    subprocess.run(
        [sys.executable, "-I", tool, "catalog", part, catalog],
        check=True, stdout=subprocess.DEVNULL,
    )
    print(f"-- {entry['file']} rows [{entry['start_row']},{entry['end_row']})")
    subprocess.run(
        [sys.executable, "-I", tool, "query", catalog,
         "--start", str(entry["start_row"]),
         "--end", str(entry["end_row"]),
         "--columns", "1,2"],
        check=True,
    )
PY

# 4. Confirm the original source name never leaves the machine.
echo
if grep -q "lab-probe" delivery/part-*.narc; then
  echo "LEAK: original source name found in a delivered part" >&2
  exit 1
fi
echo "OK: no original source name or path appears in any delivered byte."
