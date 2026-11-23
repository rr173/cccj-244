#!/usr/bin/env python3
"""Build the small, fully reproducible gap-recovery example.

Run with::

    python3 -I build_example.py

It creates, next to this file:

* ``plan.json``        -- sample range [0, 12) with required columns 1 and 2;
* ``archive-a.narc``   -- the first arrival: both columns complete on rows
                          0-3 and 6-9, explicit missings on 4-5 and no
                          coverage for rows 10-11;
* ``archive-b.narc``   -- a corrected replacement of the same source that
                          covers the whole plan (both columns on all rows);
* ``archive-c.narc``   -- a late supplementary capture, filed twice under two
                          source names (a duplicate registration);
* ``batch-1.json`` .. ``batch-5.json`` -- one handover round each: late
                          arrival, replacement of an erroneous copy, a
                          duplicate registration, withdrawal of the duplicate
                          (no work order), then withdrawal of a misreported
                          source (reopens its rows).

Nothing here is required by the library or the tests; the script exists so the
README walk-through can be re-created from scratch.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from narc import format as fmt  # noqa: E402
from narc.writer import ArchiveWriter  # noqa: E402

A = fmt.REC_ABSOLUTE
D = fmt.REC_DELTA
M = fmt.REC_MISSING
IND = fmt.KIND_INDEPENDENT

HERE = os.path.dirname(os.path.abspath(__file__))
PLAN = os.path.join(HERE, "plan.json")
ARCHIVE_A = os.path.join(HERE, "archive-a.narc")
ARCHIVE_B = os.path.join(HERE, "archive-b.narc")
ARCHIVE_C = os.path.join(HERE, "archive-c.narc")


def write_json(name: str, payload: object) -> str:
    path = os.path.join(HERE, name)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    return path


def build_archives() -> None:
    # First arrival.  Rows 4 and 5 are explicit missings in both columns and
    # rows 10-11 are simply not covered by column 2 at all.
    with ArchiveWriter(ARCHIVE_A) as writer:
        writer.fragment(1, "probe-a.raw", 0, 12)
        writer.block(
            IND, 1, 0,
            [(A, 100), (D, 5), (D, 5), (D, 5), (M, 0), (M, 0),
             (A, 70), (D, 1), (D, 1), (D, 1), (D, 1), (D, 1)],
        )
        writer.block(
            IND, 2, 0,
            [(A, 200), (D, 1), (D, 1), (D, 1), (M, 0), (M, 0),
             (A, 90), (D, 1), (D, 1), (D, 1)],
        )

    # Later arrival: a corrected copy of the same source that fills the gaps
    # left by the first copy (rows 4-5 explicit missings, rows 10-11 uncovered).
    with ArchiveWriter(ARCHIVE_B) as writer:
        writer.fragment(1, "probe-b.raw", 0, 12)
        writer.block(
            IND, 1, 0,
            [(A, 100), (D, 5), (D, 5), (D, 5), (A, 500), (D, 1),
             (A, 70), (D, 1), (D, 1), (D, 1), (D, 1), (D, 1)],
        )
        writer.block(
            IND, 2, 0,
            [(A, 200), (D, 1), (D, 1), (D, 1), (A, 600), (D, 1),
             (A, 90), (D, 1), (D, 1), (D, 1), (A, 800), (D, 1)],
        )

    # A supplementary late capture for rows 10-11, filed under its own source
    # name and then again under a second name (same archive, two sources).
    with ArchiveWriter(ARCHIVE_C) as writer:
        writer.fragment(2, "probe-c.raw", 0, 12)
        writer.block(IND, 1, 10, [(A, 700), (D, 1)])
        writer.block(IND, 2, 10, [(A, 800), (D, 1)])


def build_batches() -> None:
    # Round 1: the first archive arrives late.
    write_json("batch-1.json", {
        "revision": 1,
        "changes": [{
            "source_id": "probe-a",
            "op": "upsert",
            "archive": os.path.abspath(ARCHIVE_A),
            "fragment_id": 1,
        }],
    })
    # Round 2: an erroneous copy is replaced by a corrected archive.
    write_json("batch-2.json", {
        "revision": 2,
        "changes": [{
            "source_id": "probe-a",
            "op": "upsert",
            "archive": os.path.abspath(ARCHIVE_B),
            "fragment_id": 1,
        }],
    })
    # Round 3: a late supplementary capture arrives under its own source name.
    write_json("batch-3.json", {
        "revision": 3,
        "changes": [{
            "source_id": "probe-c",
            "op": "upsert",
            "archive": os.path.abspath(ARCHIVE_C),
            "fragment_id": 2,
        }],
    })
    # Round 4: the same capture is filed a second time under a second source
    # name (a duplicate registration).  Its rows are already complete, so no
    # work order is issued.
    write_json("batch-4.json", {
        "revision": 4,
        "changes": [{
            "source_id": "probe-c-duplicate",
            "op": "upsert",
            "archive": os.path.abspath(ARCHIVE_C),
            "fragment_id": 2,
        }],
    })
    # Round 5: the duplicate registration is withdrawn.  The other copy still
    # covers the rows, so again no work order is issued.
    write_json("batch-5.json", {
        "revision": 5,
        "changes": [{"source_id": "probe-c-duplicate", "op": "remove"}],
    })
    # Round 6: the first (misreported) arrival under probe-a is withdrawn
    # because it was filed against the wrong probe.  Since probe-b replaced
    # it, this still produces no work order.
    write_json("batch-6.json", {
        "revision": 6,
        "changes": [{"source_id": "probe-a", "op": "remove"}],
    })


def main() -> None:
    write_json("plan.json", {"range": [0, 12], "columns": [1, 2]})
    build_archives()
    build_batches()
    print(f"wrote {PLAN}")
    print(f"wrote {ARCHIVE_A}")
    print(f"wrote {ARCHIVE_B}")
    print("wrote batch-1.json .. batch-6.json")


if __name__ == "__main__":
    main()
