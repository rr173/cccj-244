#!/usr/bin/env python3
"""Build the small, fully reproducible release example.

Run with::

    python3 -I build_example.py

It creates, next to this file:

* ``source.narc``       -- a frozen v1 archive with two fragments/columns,
                           explicit missings, a dependent delta chain and an
                           uncovered row span;
* ``authorization.json`` -- the JSON authorization slip handed to ``release``;
* ``expected-parts.json`` -- the exact manifest the release must produce
                           (regenerated on every run; bytes are deterministic).

Nothing here is required by the library or the tests; the script exists so
the README walk-through can be re-created from scratch.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))
)

from narc import format as fmt  # noqa: E402
from narc.writer import ArchiveWriter  # noqa: E402

A = fmt.REC_ABSOLUTE
D = fmt.REC_DELTA
M = fmt.REC_MISSING
IND = fmt.KIND_INDEPENDENT
DEP = fmt.KIND_DEPENDENT

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(HERE, "source.narc")
AUTH = os.path.join(HERE, "authorization.json")


def build() -> None:
    with ArchiveWriter(SOURCE) as writer:
        # Fragment 7, rows [0, 16).  Column 1 mixes absolutes, deltas and
        # explicit missings across an independent and a dependent block.
        writer.fragment(7, "lab-probe.raw", 0, 16)
        writer.block(
            IND, 1, 0,
            [(A, 100), (D, 5), (M, 0), (D, -3), (A, 200),
             (D, 1), (D, 1), (D, 1)],
        )
        writer.block(
            DEP, 1, 8,
            [(D, 2), (A, 500), (D, -7), (M, 0),
             (D, 10), (D, -1), (D, 1), (D, 1)],
        )
        # Column 2 covers only rows [0, 6); rows 6-15 stay uncovered and
        # must remain uncovered (never written as missing) in the release.
        writer.block(
            IND, 2, 0,
            [(A, 70), (D, 1), (M, 0), (D, 2), (A, 80), (D, -4)],
        )
        # A second fragment that the authorization never touches; none of
        # its rows or its source name may appear in the delivery.
        writer.fragment(9, "lab-other.raw", 100, 104)
        writer.block(IND, 1, 100, [(A, i) for i in range(4)])

    authorization = {
        "fragment_id": 7,
        "columns": [1, 2],
        # Two ranges; the second starts strictly after the first so the gap
        # rows [12, 14) are not authorized and no part may span across it.
        "ranges": [[0, 12], [14, 16]],
        # Tight enough to force several standalone parts while still above
        # the 180-byte floor of a part carrying both columns on one row
        # (42 fixed + 2 block headers of 60 + 2 records of 9).
        "max_bytes": 200,
    }
    with open(AUTH, "w", encoding="utf-8") as handle:
        json.dump(authorization, handle, indent=2)
        handle.write("\n")
    print(f"wrote {SOURCE}")
    print(f"wrote {AUTH}")


if __name__ == "__main__":
    build()
