#!/usr/bin/env python3
"""Build the small, fully reproducible two-archive reconciliation example.

Run with::

    python3 -I build_example.py

It creates, next to this file:

* ``primary.narc``     -- the primary recording: fragment 7, rows 5000-5011,
                          channel ``temp`` in column 1 (one explicit missing,
                          one damaged block) and ``pressure`` in column 2;
* ``backup.narc``      -- the backup recording taken after a device restart:
                          fragment numbering restarted (fragment 2, rows
                          0-11), ``temp`` moved to column 3 with a second
                          capture of it in column 4, ``pressure`` sits in
                          column 1;
* ``alignment.json``   -- the alignment sheet mapping both archives onto the
                          logical channels ``temp`` / ``pressure`` starting at
                          logical sample 9000;
* ``alignment-shuffled.json`` -- the same sheet with the segments listed in
                          a different order (the sheet must not change).

Nothing here is required by the library or the tests; the script exists so
the README walk-through can be re-created from scratch.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from narc import format as fmt  # noqa: E402
from narc.catalog import build_catalog, load_catalog  # noqa: E402
from narc.writer import ArchiveWriter, patch_bytes  # noqa: E402

A = fmt.REC_ABSOLUTE
D = fmt.REC_DELTA
M = fmt.REC_MISSING
IND = fmt.KIND_INDEPENDENT
DEP = fmt.KIND_DEPENDENT

HERE = os.path.dirname(os.path.abspath(__file__))
PRIMARY = os.path.join(HERE, "primary.narc")
BACKUP = os.path.join(HERE, "backup.narc")


def write_json(name: str, payload: object) -> str:
    path = os.path.join(HERE, name)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    return path


def build_primary() -> None:
    with ArchiveWriter(PRIMARY) as writer:
        writer.fragment(7, "primary41.raw", 5000, 5012)
        # temp, column 1: an explicit missing at row 5003, then a block that
        # will be damaged (rows 5006-5008), then a dependent suffix that
        # re-anchors on its own absolute at row 5010.
        writer.block(IND, 1, 5000, [(A, 100), (D, 1), (D, 1), (M, 0),
                                    (D, 1), (D, 1)])
        writer.block(DEP, 1, 5006, [(D, 1), (D, 1), (D, 1)])
        writer.block(DEP, 1, 5009, [(D, 1), (A, 130), (D, 1)])
        # pressure, column 2: uneventful.
        writer.block(IND, 2, 5000, [(A, 900)] + [(D, 1)] * 11)
    # Damage the middle temp block (digest no longer matches).
    probe = os.path.join(HERE, ".probe.json")
    try:
        build_catalog(PRIMARY, probe)
        catalog = load_catalog(probe)
        block = next(b for b in catalog["_blocks"]
                     if b.column_id == 1 and b.start_row == 5006)
        with open(PRIMARY, "rb") as handle:
            handle.seek(block.offset)
            original = handle.read(1)
        patch_bytes(PRIMARY, block.offset, bytes([original[0] ^ 0xFF]))
    finally:
        if os.path.exists(probe):
            os.unlink(probe)


def build_backup() -> None:
    with ArchiveWriter(BACKUP) as writer:
        # Same run, but the device restarted: numbering begins again at 0
        # and the channels moved columns.
        writer.fragment(2, "backup41.raw", 0, 12)
        # pressure is now column 1 and agrees everywhere.
        writer.block(IND, 1, 0, [(A, 900)] + [(D, 1)] * 11)
        # temp is now column 3: row 3 reads 104 (primary is missing there),
        # row 7 reads 109 (primary says 108), rows 9-11 read 130/131/132.
        writer.block(IND, 3, 0, [(A, 100), (D, 1), (D, 1), (D, 1), (D, 1),
                                 (D, 1), (D, 1), (D, 2), (D, 1), (A, 130),
                                 (D, 1), (D, 1)])
        # A second capture of temp in column 4: agrees with column 3 except
        # at row 9, where it reads 131 -- an internal contradiction of the
        # backup copy.
        writer.block(IND, 4, 0, [(A, 100), (D, 1), (D, 1), (D, 1), (D, 1),
                                 (D, 1), (D, 1), (D, 2), (D, 1), (A, 131),
                                 (D, 1), (D, 1)])


def build_alignments() -> None:
    left_segments = [
        {"fragment_id": 7, "column_id": 1, "local": [5000, 5012],
         "channel": "temp", "global_start": 9000},
        {"fragment_id": 7, "column_id": 2, "local": [5000, 5012],
         "channel": "pressure", "global_start": 9000},
    ]
    right_segments = [
        {"fragment_id": 2, "column_id": 3, "local": [0, 12],
         "channel": "temp", "global_start": 9000},
        {"fragment_id": 2, "column_id": 4, "local": [0, 12],
         "channel": "temp", "global_start": 9000},
        {"fragment_id": 2, "column_id": 1, "local": [0, 12],
         "channel": "pressure", "global_start": 9000},
    ]
    write_json("alignment.json", {
        "left": {"archive": PRIMARY, "segments": left_segments},
        "right": {"archive": BACKUP, "segments": right_segments},
    })
    # Same sheet, segments shuffled: the differences sheet must not change.
    write_json("alignment-shuffled.json", {
        "left": {"archive": PRIMARY, "segments": list(reversed(left_segments))},
        "right": {"archive": BACKUP,
                  "segments": [right_segments[2], right_segments[0],
                               right_segments[1]]},
    })


def main() -> None:
    build_primary()
    build_backup()
    build_alignments()
    print(f"wrote {PRIMARY}")
    print(f"wrote {BACKUP}")
    print("wrote alignment.json and alignment-shuffled.json")


if __name__ == "__main__":
    main()
