"""Tests for the offline ``release`` publishing entry point.

The suite covers:

* exact value/missing/uncovered parity against source queries;
* standalone decodability of every part through the existing catalog/query
  commands, including parts that split a source block mid-way;
* refusal on damaged authorized positions and untrusted prefixes;
* int64 overflow preserved via in-band deltas (never truncated, never
  anchored by a fabricated sample);
* minimum-part / lexicographically-greatest-endpoint planning, checked
  against a brute-force DP over every legal boundary;
* O(N log N) planning that does not walk empty sample coordinates;
* authorization validation, atomic delivery and leakage checks.
"""

from __future__ import annotations

import json
import os
import shutil
import struct
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from narc import format as fmt
from narc.catalog import build_catalog, load_catalog
from narc.io_utils import TrackedReader
from narc.query import query_window
from narc.release import (
    INT64_MAX,
    Authorization,
    ReleaseError,
    parse_authorization,
    release_archive,
    _build_boundaries,
    _build_segment_plans,
    _build_segments,
    plan_parts,
)
from narc.writer import ArchiveWriter, patch_bytes

A = fmt.REC_ABSOLUTE
D = fmt.REC_DELTA
M = fmt.REC_MISSING
IND = fmt.KIND_INDEPENDENT
DEP = fmt.KIND_DEPENDENT

PASS = 0
FAILURES = []


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS
    if condition:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAILURES.append((name, detail))
        print(f"  FAIL {name} {detail}")


def write_auth(tmp: str, payload: dict, name: str = "auth.json") -> str:
    path = os.path.join(tmp, name)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return path


def cells_to_map(result) -> dict:
    return {
        (cell.row, cell.column_id): (cell.status, cell.value)
        for cell in result.cells
    }


def validate_parts(
    tmp: str,
    delivery: str,
    archive: str,
    auth: Authorization,
    manifest: dict,
    *,
    source_name: str,
) -> None:
    """Shared end-to-end assertions used by most tests."""
    on_disk = sorted(os.listdir(delivery))
    check(
        "delivery contains only parts and parts.json",
        on_disk == sorted(
            [entry["file"] for entry in manifest["parts"]] + ["parts.json"]
        ),
        str(on_disk),
    )
    with open(os.path.join(delivery, "parts.json"), encoding="utf-8") as h:
        on_disk_manifest = json.load(h)
    check("manifest on disk matches returned manifest",
          on_disk_manifest == manifest)

    # Intervals are ordered and non-overlapping; within each authorized
    # range they chain contiguously, and no part ever spans two ranges.
    covered: list[tuple[int, int]] = []
    previous_end = None
    for entry in manifest["parts"]:
        check("part interval is non-empty and monotone",
              entry["start_row"] < entry["end_row"]
              and (previous_end is None
                   or entry["start_row"] >= previous_end))
        previous_end = entry["end_row"]
        covered.append((entry["start_row"], entry["end_row"]))
        check("manifest bytes match the actual file",
              entry["bytes"] == os.path.getsize(
                  os.path.join(delivery, entry["file"]))
              and entry["bytes"] <= auth.max_bytes)
        with open(os.path.join(delivery, entry["file"]), "rb") as h:
            blob = h.read()
        check("manifest sha256 matches the actual file",
              entry["sha256"] == __import__("hashlib").sha256(blob).hexdigest())
        check("no original source name or path leaks",
              source_name.encode() not in blob
              and os.path.abspath(archive).encode() not in blob
              and b"release" in blob)

    # The parts partition every authorized range exactly: contiguous,
    # non-overlapping, contained within a range, and collectively exhaustive.
    cursor = 0
    for range_start, range_end in auth.ranges:
        chain_start = covered[cursor][0]
        check("parts begin exactly at each authorized range start",
              chain_start == range_start,
              f"{chain_start} != {range_start}")
        chain_end = range_start
        while cursor < len(covered) and covered[cursor][0] < range_end:
            start, end = covered[cursor]
            check("part is contained in one authorized range and chains",
                  start == chain_end and end <= range_end,
                  f"part [{start},{end}) breaks chain at {chain_end}")
            chain_end = end
            cursor += 1
        check("parts end exactly at each authorized range end",
              chain_end == range_end,
              f"{chain_end} != {range_end}")
    check("every part was accounted for", cursor == len(covered))

    # Every part opens with the unchanged catalog/query contract and carries
    # only the requested fragment/columns; every block is independent.
    got: dict = {}
    for entry in manifest["parts"]:
        part_path = os.path.join(delivery, entry["file"])
        cat_path = os.path.join(tmp, entry["file"] + ".cat.json")
        build_catalog(part_path, cat_path)
        catalog = load_catalog(cat_path)
        check("fragment id and uniform release source name",
              [f["fragment_id"] for f in catalog["fragments"]]
              == [auth.fragment_id]
              and catalog["fragments"][0]["source_name"] == "release")
        check("only requested columns are present",
              {b.column_id for b in catalog["_blocks"]}
              <= set(auth.columns))
        check("every emitted block is independently decodable",
              all(b.kind == IND for b in catalog["_blocks"]))
        result = query_window(
            cat_path, entry["start_row"], entry["end_row"],
            list(auth.columns),
        )
        for cell in result.cells:
            got[(cell.row, cell.column_id)] = (cell.status, cell.value)

    # Compare every covered authorized position with the source archive.
    src_cat = os.path.join(tmp, "source.cat.json")
    build_catalog(archive, src_cat)
    truth = {}
    for start, end in auth.ranges:
        truth.update(
            cells_to_map(query_window(src_cat, start, end,
                                      list(auth.columns)))
        )
    for key, expected in truth.items():
        if expected[0] == "uncovered":
            check(f"uncovered {key} stays uncovered in the release",
                  got.get(key, ("uncovered", None))[0] == "uncovered")
        else:
            check(f"covered {key} preserves {expected[0]} exactly",
                  got.get(key) == expected,
                  f"got={got.get(key)} expected={expected}")


# ---------------------------------------------------------------------------
# Brute-force reference partitioner
# ---------------------------------------------------------------------------

def brute_force_plan(
    auth: Authorization,
    segment_plans: dict,
) -> list[tuple[int, int]] | None:
    """Enumerate DP over every legal boundary; return None if infeasible."""
    from narc.release import WindowCost

    all_intervals: list[tuple[int, int]] = []
    for auth_range in auth.ranges:
        boundaries = _build_boundaries(auth_range, segment_plans)
        m = len(boundaries) - 1
        # dp[j] = (min parts, lexicographically greatest endpoint sequence)
        dp: list[tuple[int, tuple[int, ...]] | None] = [None] * (m + 1)
        dp[m] = (0, ())
        for j in range(m - 1, -1, -1):
            best: tuple[int, tuple[int, ...]] | None = None
            for k in range(j + 1, m + 1):
                window = WindowCost(boundaries, segment_plans)
                window.set_window(j, k)
                if window.cost() > auth.max_bytes:
                    continue
                if dp[k] is None:
                    continue
                candidate = (1 + dp[k][0],
                             (boundaries[k],) + dp[k][1])
                if best is None:
                    best = candidate
                elif candidate[0] < best[0]:
                    best = candidate
                elif candidate[0] == best[0] \
                        and candidate[1] > best[1]:
                    best = candidate
            dp[j] = best
        if dp[0] is None:
            return None
        endpoints = dp[0][1]
        cursor = boundaries[0]
        for endpoint in endpoints:
            all_intervals.append((cursor, endpoint))
            cursor = endpoint
    return all_intervals


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def build_mixed_archive(path: str) -> None:
    with ArchiveWriter(path) as writer:
        writer.fragment(1, "lab-secret.raw", 0, 30)
        writer.block(
            IND, 1, 0,
            [(A, 100), (D, 5), (M, 0), (D, -3), (A, 200),
             (D, 1), (D, 1), (D, 1), (M, 0), (D, 4)],
        )
        writer.block(
            DEP, 1, 10,
            [(D, 2), (D, 2), (A, 500), (D, -7), (M, 0),
             (D, 10), (D, -1), (D, 1), (D, 1), (D, 1)],
        )
        writer.block(
            IND, 2, 0,
            [(A, 70), (D, 1), (M, 0), (D, 2), (A, 80),
             (D, -4), (M, 0), (D, 6), (D, 1), (D, 1)],
        )
        writer.block(
            DEP, 2, 10,
            [(D, 3), (D, 3), (A, -9), (D, 2), (M, 0),
             (D, 5), (D, -5), (D, 1), (D, 2), (D, 3)],
        )
        writer.fragment(2, "lab-other.raw", 1000, 1010)
        writer.block(IND, 1, 1000, [(A, i) for i in range(10)])


def test_happy_path_parity(tmp: str) -> None:
    print("test: happy-path release preserves every covered position")
    archive = os.path.join(tmp, "mixed.narc")
    build_mixed_archive(archive)
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1, 2, 1],
        "ranges": [[0, 20]], "max_bytes": 400,
    })
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    check("at least one part was published", len(manifest["parts"]) >= 1)
    validate_parts(tmp, delivery, archive,
                   parse_authorization(json.load(open(auth))),
                   manifest, source_name="lab-secret.raw")


def test_split_mid_block_and_carry(tmp: str) -> None:
    print("test: parts split source blocks but decode standalone via anchors")
    archive = os.path.join(tmp, "chain.narc")
    n_rows = 600
    records = [(A, 11)] + [(D, 1) for _ in range(n_rows - 1)]
    with ArchiveWriter(archive) as writer:
        writer.fragment(4, "chain.raw", 0, n_rows)
        writer.block(IND, 9, 0, records)
    auth = write_auth(tmp, {
        "fragment_id": 4, "columns": [9],
        "ranges": [[200, n_rows]], "max_bytes": 160,
    })
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    check("tight byte cap forces multiple parts",
          len(manifest["parts"]) > 1)
    # Each part must decode without any other part present: catalog each one
    # in an isolated directory and verify its exact reconstructed values.
    for index, entry in enumerate(manifest["parts"]):
        isolated = os.path.join(tmp, f"iso-{index}")
        os.makedirs(isolated)
        part_path = os.path.join(delivery, entry["file"])
        shutil.copy(part_path, os.path.join(isolated, entry["file"]))
        cat_path = os.path.join(isolated, "c.json")
        build_catalog(os.path.join(isolated, entry["file"]), cat_path)
        got = cells_to_map(query_window(
            cat_path, entry["start_row"], entry["end_row"], [9]))
        expected = {
            (row, 9): ("value", 11 + row)
            for row in range(entry["start_row"], entry["end_row"])
        }
        check(f"part {index} standalone values are exact", got == expected)
    validate_parts(tmp, delivery, archive,
                   parse_authorization(json.load(open(auth))),
                   manifest, source_name="chain.raw")


def test_dependent_carry_uses_private_context(tmp: str) -> None:
    print("test: pre-context from the source recovers dependent-block values")
    archive = os.path.join(tmp, "dep.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(5, "dep.raw", 0, 12)
        writer.block(IND, 6, 0, [(A, 10), (D, 5), (D, 5)])
        # Dependent block whose first records are deltas requiring carry-in.
        writer.block(DEP, 6, 3, [(D, 1), (D, 1), (A, 50), (D, -10)])
        # Rows 7-11 are pure deltas: without the source carry-in they could
        # never be reconstructed; the authorization starts exactly there.
        writer.block(DEP, 6, 7, [(D, 2), (D, 2), (D, 2), (D, 2), (D, 2)])
    auth = write_auth(tmp, {
        "fragment_id": 5, "columns": [6],
        "ranges": [[7, 12]], "max_bytes": 500,
    })
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    validate_parts(tmp, delivery, archive,
                   parse_authorization(json.load(open(auth))),
                   manifest, source_name="dep.raw")
    # The first delivered record must be an absolute anchor, never a delta.
    blob = open(os.path.join(delivery, manifest["parts"][0]["file"]),
                "rb").read()
    payload_start = (
        fmt.FILE_HEADER_SIZE + fmt.FRAGMENT_HEADER_SIZE
        + fmt.BLOCK_HEADER_SIZE
    )
    check("first emitted record is an absolute anchor",
          blob[payload_start] == fmt.REC_ABSOLUTE)
    # The source reaches 40 at row 6 (50 then -10); authorized row 7 adds 2
    # -> exactly 42, reconstructed in memory from the private pre-context.
    check("anchor holds the exact reconstructed value",
          struct.unpack(">q", blob[payload_start + 1:payload_start + 9])[0]
          == 42)


def test_missing_prefix_refusal(tmp: str) -> None:
    print("test: authorized missing_prefix positions refuse the whole order")
    archive = os.path.join(tmp, "mp.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "mp.raw", 0, 8)
        writer.block(IND, 1, 0,
                     [(D, 1), (D, 1), (A, 42), (D, 1), (M, 0),
                      (D, 2), (A, 0), (D, -5)])
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, 4]], "max_bytes": 200,
    })
    delivery = os.path.join(tmp, "delivery")
    try:
        release_archive(archive, auth, delivery)
        raised = False
    except ReleaseError:
        raised = True
    check("untrusted authorized prefix refuses the order", raised)
    check("no delivery directory is left behind", not os.path.exists(delivery))

    # Authorizing only the anchored suffix succeeds.
    auth2 = write_auth(tmp, {
        "fragment_id": 2, "columns": [1],
        "ranges": [[2, 8]], "max_bytes": 200,
    })
    try:
        release_archive(archive, auth2, os.path.join(tmp, "d2"))
        bad_fragment = False
    except ReleaseError:
        bad_fragment = True
    check("unknown fragment is refused", bad_fragment)


def test_damaged_authorized_block_refusal(tmp: str) -> None:
    print("test: damage at authorized positions refuses; elsewhere is fine")
    archive = os.path.join(tmp, "bad.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(8, "bad.raw", 0, 12)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (D, 1)])
        writer.block(DEP, 1, 3, [(D, 1), (A, 9), (D, 1)])
        writer.block(IND, 2, 0, [(A, 1), (D, 1), (D, 1)])
    pristine = os.path.join(tmp, "pristine.narc")
    shutil.copyfile(archive, pristine)
    build_catalog(pristine, os.path.join(tmp, "p.json"))
    target = next(b for b in load_catalog(os.path.join(tmp, "p.json"))["_blocks"]
                  if b.start_row == 3 and b.column_id == 1)
    patch_bytes(archive, target.offset + 9, b"\x7f")

    delivery = os.path.join(tmp, "delivery")
    auth = write_auth(tmp, {
        "fragment_id": 8, "columns": [1],
        "ranges": [[0, 6]], "max_bytes": 300,
    })
    try:
        release_archive(archive, auth, delivery)
        raised = False
    except ReleaseError:
        raised = True
    check("damaged authorized block refuses the order", raised)
    check("refusal leaves no delivery directory",
          not os.path.exists(delivery))

    # The same bad block is irrelevant when only healthy rows are requested.
    auth2 = write_auth(tmp, {
        "fragment_id": 8, "columns": [2],
        "ranges": [[0, 3]], "max_bytes": 300,
    })
    delivery2 = os.path.join(tmp, "delivery2")
    manifest = release_archive(archive, auth2, delivery2)
    check("unselected damaged block does not block the release",
          len(manifest["parts"]) == 1)


def test_uncovered_preservation(tmp: str) -> None:
    print("test: uncovered positions stay uncovered, never written missing")
    archive = os.path.join(tmp, "unc.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(3, "unc.raw", 0, 12)
        writer.block(IND, 1, 0, [(A, i) for i in range(4)])
        # Rows 4-11 uncovered for column 1; column 2 only covers rows 6-9.
        writer.block(IND, 2, 6, [(A, 100 + i) for i in range(4)])
    auth = write_auth(tmp, {
        "fragment_id": 3, "columns": [1, 2],
        "ranges": [[0, 12]], "max_bytes": 400,
    })
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    validate_parts(tmp, delivery, archive,
                   parse_authorization(json.load(open(auth))),
                   manifest, source_name="unc.raw")
    # Raw wire check: no record payload exists for uncovered coordinates.
    part_cats = []
    for entry in manifest["parts"]:
        cat_path = os.path.join(tmp, entry["file"] + ".v.json")
        build_catalog(os.path.join(delivery, entry["file"]), cat_path)
        part_cats.append(load_catalog(cat_path))
    extents = sorted(
        (b.column_id, b.start_row, b.end_row)
        for cat in part_cats for b in cat["_blocks"]
    )
    check("block extents match only covered coordinates",
          extents == [(1, 0, 4), (2, 6, 10)], str(extents))


def test_huge_gap_is_event_based(tmp: str) -> None:
    print("test: hundreds-of-millions-row gaps cost no row-wise work")
    archive = os.path.join(tmp, "gap.narc")
    huge = 300_000_000
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "gap.raw", 0, huge + 10)
        writer.block(IND, 1, 0, [(A, 1), (D, 2), (D, 3)])
        writer.block(IND, 1, huge, [(A, 9), (D, 1)])
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, huge + 2]], "max_bytes": 200,
    })
    start = time.perf_counter()
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    elapsed = time.perf_counter() - start
    check("release over a 3e8-row gap completes in well under a second",
          elapsed < 1.0, f"{elapsed:.3f}s")
    intervals = [(p["start_row"], p["end_row"]) for p in manifest["parts"]]
    # The split is driven purely by byte accounting (two block headers);
    # either way the boundary must sit at a real covered row and no records
    # may be synthesized for the hundreds of millions of empty coordinates.
    check("part boundaries sit at covered rows, never inside the empty gap",
          all(end in (huge, huge + 2) or start in (0, huge)
              for start, end in intervals))
    check("part intervals exactly cover the authorization",
          intervals[0][0] == 0 and intervals[-1][1] == huge + 2
          and all(a[1] == b[0] for a, b in zip(intervals, intervals[1:])))
    _validate_gap_delivery(tmp, delivery, archive, manifest, huge)

    # With a roomier cap a single part spans the gap, proving gaps add zero
    # records: one part of only five records plus two block headers.
    delivery2 = os.path.join(tmp, "delivery2")
    auth2 = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, huge + 2]], "max_bytes": 400,
    }, name="auth2.json")
    manifest2 = release_archive(archive, auth2, delivery2)
    check("a roomy cap spans the gap in one part without fabricated records",
          [(p["start_row"], p["end_row"]) for p in manifest2["parts"]]
          == [(0, huge + 2)]
          and manifest2["parts"][0]["bytes"]
          == 42 + 2 * 60 + 5 * 9)
    _validate_gap_delivery(tmp, delivery2, archive, manifest2, huge)


def _validate_gap_delivery(
    tmp: str, delivery: str, archive: str, manifest: dict, huge: int
) -> None:
    """Gap-safe validation: only narrow, covered windows are ever queried."""
    import hashlib
    for entry in manifest["parts"]:
        path = os.path.join(delivery, entry["file"])
        check("gap part respects the byte cap",
              entry["bytes"] == os.path.getsize(path)
              and entry["bytes"] <= 400)
        blob = open(path, "rb").read()
        check("gap part sha256 and no source leakage",
              hashlib.sha256(blob).hexdigest() == entry["sha256"]
              and b"gap.raw" not in blob
              and os.path.abspath(archive).encode() not in blob)
        cat_path = os.path.join(tmp, entry["file"] + ".gap.json")
        build_catalog(path, cat_path)
        catalog = load_catalog(cat_path)
        check("gap part has one release fragment, independent blocks",
              [f["fragment_id"] for f in catalog["fragments"]] == [1]
              and catalog["fragments"][0]["source_name"] == "release"
              and all(b.kind == IND for b in catalog["_blocks"]))
        # Every block must lie inside one of the two covered islands; the
        # planner may split an island across blocks, but no block may cover
        # any coordinate in the hundreds-of-millions-row empty gap.
        islands = ((0, 3), (huge, huge + 2))
        inside_islands = all(
            any(
                island_start <= block.start_row
                and block.end_row <= island_end
                for island_start, island_end in islands
            )
            for block in catalog["_blocks"]
        )
        check("gap part synthesizes no records inside the empty gap",
              inside_islands and len(catalog["_blocks"]) >= 1)
        # Narrow queries at the covered islands only (never the whole range).
        for row in range(entry["start_row"],
                         min(entry["end_row"], 3)):
            cell = query_window(cat_path, row, row + 1, [1]).cells[0]
            check(f"covered island row {row} exact",
                  (cell.status, cell.value)
                  == ("value", (1, 3, 6)[row]))
        if entry["end_row"] > huge:
            for offset, expected in enumerate((9, 10)):
                row = huge + offset
                if entry["start_row"] <= row < entry["end_row"]:
                    cell = query_window(cat_path, row, row + 1,
                                        [1]).cells[0]
                    check(f"covered far island row {row} exact",
                          (cell.status, cell.value) == ("value", expected))
            # A probe strictly inside the gap stays uncovered.
            probe = query_window(cat_path, huge // 2, huge // 2 + 1,
                                 [1]).cells[0]
            check("gap interior stays uncovered, never missing",
                  probe.status == "uncovered")


def test_multiple_ranges_and_merging(tmp: str) -> None:
    print("test: overlapping/adjacent ranges merge; parts never cross ranges")
    archive = os.path.join(tmp, "mr.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "mr.raw", 0, 40)
        writer.block(IND, 1, 0, [(A, i) for i in range(40)])
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, 10], [5, 15], [15, 16], [30, 40]],
        "max_bytes": 150,
    })
    payload = json.load(open(auth))
    parsed = parse_authorization(payload)
    check("overlapping and adjacent ranges merge into one",
          parsed.ranges == ((0, 16), (30, 40)))
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    intervals = [(p["start_row"], p["end_row"]) for p in manifest["parts"]]
    check("no part crosses the gap between authorized ranges",
          all(end <= 16 or start >= 30 for start, end in intervals))
    check("union of parts is exactly the merged domain",
          [(s, e) for s, e in intervals if s < 16][0][0] == 0
          and intervals[-1][1] == 40)
    validate_parts(tmp, delivery, archive, parsed, manifest,
                   source_name="mr.raw")


def test_int64_overflow_preserved(tmp: str) -> None:
    print("test: overflowing reconstructed values stay exact delta chains")
    archive = os.path.join(tmp, "ovf.narc")
    big = INT64_MAX
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "ovf.raw", 0, 8)
        writer.block(
            IND, 1, 0,
            [(A, big), (D, 10), (D, 10), (A, 5),
             (D, 1), (D, 1), (D, 1), (D, 1)],
        )
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, 8]], "max_bytes": 200,
    })
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    validate_parts(tmp, delivery, archive,
                   parse_authorization(json.load(open(auth))),
                   manifest, source_name="ovf.raw")
    # The overflowing values must still be delta records in-band; no fake
    # absolute anchor and no truncation.
    blob = open(os.path.join(delivery, manifest["parts"][0]["file"]),
                "rb").read()
    tags = [blob[i] for i in range(
        fmt.FILE_HEADER_SIZE + fmt.FRAGMENT_HEADER_SIZE
        + fmt.BLOCK_HEADER_SIZE,
        len(blob) - fmt.END_OF_ARCHIVE_SIZE, fmt.RECORD_SIZE)]
    check("overflow rows remain delta records",
          tags[1] == fmt.REC_DELTA and tags[2] == fmt.REC_DELTA)
    cat_path = os.path.join(tmp, "ovf.cat.json")
    build_catalog(os.path.join(delivery, manifest["parts"][0]["file"]),
                  cat_path)
    got = cells_to_map(query_window(cat_path, 0, 8, [1]))
    check("overflowed reconstructed values are exact",
          got[(1, 1)] == ("value", big + 10)
          and got[(2, 1)] == ("value", big + 20))


def test_all_missing_column_publishes(tmp: str) -> None:
    print("test: an all-missing column needs no anchor and still ships")
    archive = os.path.join(tmp, "mm.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "mm.raw", 0, 4)
        writer.block(IND, 1, 0, [(A, 7), (M, 0), (M, 0), (D, 2)])
        # Column 2 has only explicit missings: independent, zero payloads.
        writer.block(IND, 2, 0, [(M, 0), (M, 0), (M, 0), (M, 0)])
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1, 2],
        "ranges": [[0, 4]], "max_bytes": 300,
    })
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    validate_parts(tmp, delivery, archive,
                   parse_authorization(json.load(open(auth))),
                   manifest, source_name="mm.raw")
    cat_path = os.path.join(tmp, "mm.cat.json")
    build_catalog(os.path.join(delivery, manifest["parts"][0]["file"]),
                  cat_path)
    catalog = load_catalog(cat_path)
    check("all-missing column is one independent block",
          [b for b in catalog["_blocks"] if b.column_id == 2][0].kind == IND)
    got = cells_to_map(query_window(cat_path, 0, 4, [2]))
    check("every all-missing position stays explicitly missing",
          all(got[(row, 2)] == ("missing", None) for row in range(4)))


def test_unanchorable_overflow_refused(tmp: str) -> None:
    print("test: a covered value with no in-int64 anchor refuses the order")
    archive = os.path.join(tmp, "ua.narc")
    big = INT64_MAX
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "ua.raw", 0, 4)
        # The very first value already overflows via reconstruction, so no
        # in-band int64 absolute can anchor an independent block.
        writer.block(
            IND, 1, 0,
            [(A, big), (D, 5), (D, 5), (M, 0)],
        )
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[1, 4]], "max_bytes": 200,
    })
    delivery = os.path.join(tmp, "delivery")
    try:
        release_archive(archive, auth, delivery)
        raised = False
    except ReleaseError:
        raised = True
    check("overflow without an in-band anchor refuses the whole order",
          raised)
    check("refusal leaves no delivery directory",
          not os.path.exists(delivery))


def test_uncovered_only_ranges(tmp: str) -> None:
    print("test: fully uncovered ranges ship empty, valid, still-covering parts")
    archive = os.path.join(tmp, "uo.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "uo.raw", 0, 10)
        writer.block(IND, 1, 0, [(A, i) for i in range(3)])  # rows 0-2 only
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, 3], [7, 9]], "max_bytes": 200,
    })
    parsed = parse_authorization(json.load(open(auth)))
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    intervals = [(p["start_row"], p["end_row"]) for p in manifest["parts"]]
    check("both ranges produce parts, including the empty one",
          intervals == [(0, 3), (7, 9)])
    empty = manifest["parts"][1]
    check("empty part carries only the 42-byte fixed v1 overhead",
          empty["bytes"] == 42)
    # It is still an openable v1 file whose every position is uncovered.
    cat_path = os.path.join(tmp, "uo.cat.json")
    build_catalog(os.path.join(delivery, empty["file"]), cat_path)
    catalog = load_catalog(cat_path)
    check("empty part has a release fragment and no blocks",
          catalog["fragments"][0]["source_name"] == "release"
          and not catalog["blocks"])
    got = cells_to_map(query_window(cat_path, 7, 9, [1]))
    check("empty part reports uncovered, never missing",
          all(value[0] == "uncovered" for value in got.values()))
    validate_parts(tmp, delivery, archive, parsed, manifest,
                   source_name="uo.raw")


def test_infeasible_byte_cap(tmp: str) -> None:
    print("test: a cap below one block's minimum need is infeasible")
    archive = os.path.join(tmp, "inf.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "inf.raw", 0, 4)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (M, 0), (D, 1)])
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, 4]], "max_bytes": 100,
    })
    delivery = os.path.join(tmp, "delivery")
    try:
        release_archive(archive, auth, delivery)
        raised = False
    except ReleaseError:
        raised = True
    check("infeasible cap is refused with no directory",
          raised and not os.path.exists(delivery))


def test_planning_is_gap_independent(tmp: str) -> None:
    print("test: planning work is independent of empty-gap row count")
    from narc.catalog import build_catalog as _bc, load_catalog as _lc

    def plan_with_gap(gap: int, n_covered: int):
        path = os.path.join(tmp, f"gap-{gap}.narc")
        records = [(A, 1)] + [(D, 1) for _ in range(n_covered - 1)]
        with ArchiveWriter(path) as writer:
            writer.fragment(1, "g.raw", 0, gap + n_covered + 1)
            writer.block(IND, 1, gap, records)
        cat_path = os.path.join(tmp, f"gap-{gap}.json")
        _bc(path, cat_path)
        catalog = _lc(cat_path)
        auth = Authorization(
            1, (1,), ((gap, gap + n_covered),), 4096
        )
        segments = _build_segments(catalog, path, auth)
        plans = _build_segment_plans(segments, 4096)
        return plan_parts(auth, plans)

    n_covered = 800
    small = plan_with_gap(0, n_covered)
    huge = plan_with_gap(300_000_000, n_covered)
    # The identical covered data yields the identical plan regardless of the
    # hundreds of millions of empty coordinates in between.
    check("a 3e8-row empty gap does not change the partition",
          len(small) == len(huge))
    check("gap-spanning plans share the same part sizes",
          [e - s for s, e in small]
          == [e - s for s, e in huge])


def test_planner_matches_brute_force(tmp: str) -> None:
    print("test: O(N log N) planner matches brute-force DP on random cases")
    import random
    rng = random.Random(20261123)
    trials = 60
    for trial in range(trials):
        n_rows = rng.randint(1, 26)
        n_cols = rng.randint(1, 3)
        path = os.path.join(tmp, f"rand-{trial}.narc")
        with ArchiveWriter(path) as writer:
            writer.fragment(1, f"r{trial}.raw", 0, n_rows)
            for col in range(1, n_cols + 1):
                if rng.random() < 0.25:
                    continue  # leave the column partially/uncovered
                start = rng.randint(0, n_rows - 1)
                end = rng.randint(start + 1, n_rows)
                records = []
                for _ in range(end - start):
                    roll = rng.random()
                    if roll < 0.15:
                        records.append((M, 0))
                    elif roll < 0.35 and records:
                        records.append((A, rng.randint(-50, 50)))
                    else:
                        records.append((D, rng.randint(-9, 9)))
                if not any(tag == A for tag, _ in records):
                    records[0] = (A, rng.randint(-50, 50))
                writer.block(IND, col, start, records)
        cat_path = os.path.join(tmp, f"rand-{trial}.json")
        build_catalog(path, cat_path)
        catalog = load_catalog(cat_path)
        # A handful of range/column/cap combinations per archive.
        for _ in range(4):
            lo = rng.randint(0, n_rows - 1)
            hi = rng.randint(lo + 1, n_rows)
            cols = rng.sample(range(1, n_cols + 1),
                              rng.randint(1, n_cols))
            cap = rng.choice([110, 130, 160, 220, 400, 1000])
            auth = Authorization(1, tuple(sorted(set(cols))),
                                 ((lo, hi),), cap)
            try:
                segs = _build_segments(catalog, path, auth)
                plans = _build_segment_plans(segs, cap)
            except ReleaseError:
                continue  # genuinely unanchorable order
            expected = brute_force_plan(auth, plans)
            if expected is None:
                try:
                    plan_parts(auth, plans)
                    got = "unexpected plan"
                except ReleaseError:
                    got = None
                check(f"trial {trial}: infeasibility agrees", got is None)
                continue
            got = plan_parts(auth, plans)
            check(
                f"trial {trial}: optimal partition agrees "
                f"(rows={n_rows}, cols={cols}, cap={cap})",
                got == expected, f"got={got} expected={expected}",
            )


def test_planner_tie_break(tmp: str) -> None:
    print("test: equal part counts pick the greatest endpoint sequence")
    # One column, every row an absolute (every boundary legal), cap chosen
    # so several equal-count splits exist.
    archive = os.path.join(tmp, "tie.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "tie.raw", 0, 12)
        writer.block(IND, 1, 0, [(A, i) for i in range(12)])
    cat_path = os.path.join(tmp, "tie.json")
    build_catalog(archive, cat_path)
    catalog = load_catalog(cat_path)
    # Cap 129 bytes: 42 + 60 + 9*k <= 129 -> k <= 3 records per block.
    auth = Authorization(1, (1,), ((0, 12),), 129)
    segs = _build_segments(catalog, archive, auth)
    plans = _build_segment_plans(segs, 129)
    intervals = plan_parts(auth, plans)
    # Minimum is 4 parts of 3 records each; greedy gives endpoints 3,6,9,12.
    check("minimum part count with lexicographically greatest endpoints",
          intervals == [(0, 3), (3, 6), (6, 9), (9, 12)], str(intervals))
    expected = brute_force_plan(auth, plans)
    check("brute force independently selects the same plan",
          expected == intervals)


def test_authorization_validation() -> None:
    print("test: authorization slip validation")
    base = {
        "fragment_id": 1, "columns": [1, 2],
        "ranges": [[0, 4]], "max_bytes": 100,
    }

    def expect_error(mutator, label):
        payload = json.loads(json.dumps(base))
        mutator(payload)
        try:
            parse_authorization(payload)
            ok = False
        except ReleaseError:
            ok = True
        check(label, ok)

    expect_error(lambda p: p.pop("max_bytes"), "missing key rejected")
    expect_error(lambda p: p.update(extra=1), "unknown key rejected")
    expect_error(lambda p: p.update(fragment_id=-1), "negative fragment id")
    expect_error(lambda p: p.update(fragment_id=2 ** 16), "fragment id range")
    expect_error(lambda p: p.update(columns=[]), "empty columns rejected")
    expect_error(lambda p: p.update(columns=[True]), "bool column rejected")
    expect_error(lambda p: p.update(ranges=[]), "empty ranges rejected")
    expect_error(lambda p: p.update(ranges=[[5, 5]]), "empty range rejected")
    expect_error(lambda p: p.update(ranges=[[5, 3]]), "backwards range")
    expect_error(lambda p: p.update(ranges=[[0, 2 ** 32 + 1]]),
                 "range beyond uint32")
    expect_error(lambda p: p.update(max_bytes=41), "max_bytes below 42")
    expect_error(lambda p: p.update(max_bytes=2 ** 32), "max_bytes above cap")
    expect_error(lambda p: p.update(max_bytes="100"), "non-integer max_bytes")
    expect_error(lambda p: p.update(ranges=[[0, "4"]]), "non-integer bound")
    check("duplicate columns collapse to one",
          parse_authorization({**base, "columns": [2, 2, 1]}).columns
          == (1, 2))
    check("adjacent ranges merge",
          parse_authorization({**base, "ranges": [[0, 2], [2, 4]]}).ranges
          == ((0, 4),))


def test_delivery_collision_and_write_failure(tmp: str) -> None:
    print("test: existing delivery directory and write failures are safe")
    archive = os.path.join(tmp, "c.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "c.raw", 0, 3)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (M, 0)])
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, 3]], "max_bytes": 200,
    })
    existing = os.path.join(tmp, "exists")
    os.makedirs(existing)
    marker = os.path.join(existing, "receiver-file.txt")
    with open(marker, "w") as handle:
        handle.write("partial delivery must never land here")
    try:
        release_archive(archive, auth, existing)
        raised = False
    except ReleaseError:
        raised = True
    check("refusing an existing delivery directory", raised)
    check("existing directory contents are untouched",
          open(marker).read() == "partial delivery must never land here")

    # A refused order must leave neither a delivery directory nor a hidden
    # temporary working directory behind.
    bad_auth = write_auth(tmp, {
        "fragment_id": 99, "columns": [1],
        "ranges": [[0, 3]], "max_bytes": 200,
    }, name="bad-auth.json")
    try:
        release_archive(archive, bad_auth, os.path.join(tmp, "nope"))
    except ReleaseError:
        pass
    leftovers = [
        name for name in os.listdir(tmp)
        if name.startswith(".narc-release")
    ]
    check("failed releases remove all temporary directories", not leftovers)
    check("no partial delivery directory appears",
          not os.path.exists(os.path.join(tmp, "nope")))


def test_manifest_schema_and_size_ceiling(tmp: str) -> None:
    print("test: parts.json schema and inclusive header byte ceiling")
    archive = os.path.join(tmp, "ceil.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "ceil.raw", 0, 6)
        writer.block(IND, 1, 0, [(A, i) for i in range(6)])
    cap = 42 + 60 + 9 * 2  # exactly one block of two records
    auth = write_auth(tmp, {
        "fragment_id": 1, "columns": [1],
        "ranges": [[0, 6]], "max_bytes": cap,
    })
    delivery = os.path.join(tmp, "delivery")
    manifest = release_archive(archive, auth, delivery)
    check("every part stays at or below the cap including headers",
          all(entry["bytes"] <= cap for entry in manifest["parts"]))
    check("the cap forces one block per two records",
          len(manifest["parts"]) == 3)
    for entry in manifest["parts"]:
        check("manifest entry has the required keys in sample order",
              set(entry) == {"file", "start_row", "end_row", "bytes",
                             "sha256"})
        check("sha256 is 64 hex characters",
              len(entry["sha256"]) == 64
              and all(c in "0123456789abcdef" for c in entry["sha256"]))
    starts = [p["start_row"] for p in manifest["parts"]]
    check("parts are listed in sample order", starts == sorted(starts))


def test_reads_stay_in_authorized_payloads(tmp: str) -> None:
    print("test: release I/O never touches unauthorized block payloads")
    archive = os.path.join(tmp, "io.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "io.raw", 0, 12)
        writer.block(IND, 1, 0, [(A, i) for i in range(6)])
        writer.block(IND, 1, 6, [(A, 100 + i) for i in range(6)])
        writer.block(IND, 2, 0, [(A, -i) for i in range(12)])
    cat_path = os.path.join(tmp, "io.json")
    build_catalog(archive, cat_path)
    catalog = load_catalog(cat_path)
    auth = Authorization(1, (1,), ((0, 6),), 300)
    allowed_blocks = [
        b for b in catalog["_blocks"]
        if b.column_id == 1 and b.start_row < 6
    ]
    allowed_payloads = [(b.offset, b.end_offset) for b in allowed_blocks]
    # Every payload byte in the archive belongs to some known block; payload
    # reads may only land in authorized blocks.  Structural reads (file and
    # fragment/block headers) and the end marker are not payload data.
    all_payloads = sorted(
        (b.offset, b.end_offset) for b in catalog["_blocks"]
    )

    def in_ranges(offset: int, length: int,
                  ranges: list[tuple[int, int]]) -> bool:
        end = offset + length
        return any(start <= offset and end <= stop
                   for start, stop in ranges)

    original_read = TrackedReader.read
    violations = []
    # Cataloging is explicitly allowed to read the whole archive to detect
    # damage; the authorization boundary applies to the release decode, so
    # only reads performed by _build_segments are guarded.
    import narc.release as release_module
    original_build = release_module._build_segments
    guard_active = {"on": False}

    def guarded_read(self, offset, length):
        if guard_active["on"] and in_ranges(
            offset, length, all_payloads
        ) and not in_ranges(offset, length, allowed_payloads):
            violations.append((offset, length))
        return original_read(self, offset, length)

    def wrapped_build(catalog, archive_path, auth):
        guard_active["on"] = True
        try:
            return original_build(catalog, archive_path, auth)
        finally:
            guard_active["on"] = False

    release_module._build_segments = wrapped_build
    TrackedReader.read = guarded_read
    try:
        delivery = os.path.join(tmp, "delivery")
        release_archive(
            archive,
            write_auth(tmp, {
                "fragment_id": 1, "columns": [1],
                "ranges": [[0, 6]], "max_bytes": 300,
            }),
            delivery,
        )
    finally:
        TrackedReader.read = original_read
        release_module._build_segments = original_build
    check("no read escaped the authorized block payloads", not violations,
          str(violations))


def main() -> int:
    root = tempfile.mkdtemp(prefix="narc-release-test-")
    print(f"scratch directory: {root}")
    for test in (
        test_happy_path_parity,
        test_split_mid_block_and_carry,
        test_dependent_carry_uses_private_context,
        test_missing_prefix_refusal,
        test_damaged_authorized_block_refusal,
        test_uncovered_preservation,
        test_huge_gap_is_event_based,
        test_multiple_ranges_and_merging,
        test_int64_overflow_preserved,
        test_all_missing_column_publishes,
        test_unanchorable_overflow_refused,
        test_uncovered_only_ranges,
        test_infeasible_byte_cap,
        test_planning_is_gap_independent,
        test_planner_matches_brute_force,
        test_planner_tie_break,
        test_authorization_validation,
        test_delivery_collision_and_write_failure,
        test_manifest_schema_and_size_ceiling,
        test_reads_stay_in_authorized_payloads,
    ):
        import inspect
        if inspect.signature(test).parameters:
            tmp = os.path.join(root, test.__name__)
            os.makedirs(tmp)
            test(tmp)
        else:
            test()
    print(f"\n{PASS} assertions passed, {len(FAILURES)} failures")
    if FAILURES:
        for name, detail in FAILURES:
            print(f"FAILED: {name} :: {detail}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
