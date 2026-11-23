"""Tests for the offline two-archive reconciliation command (reconcile).

Coverage: the four difference kinds plus the no-report cases, v1 decode
semantics under alignment (explicit missing / uncovered / damaged /
missing_prefix never count; out-of-interval trusted predecessors do),
damaged-block isolation with dependent-suffix recovery, provenance
tie-breaks, channel/sample ordering independent of segment order, chunked
decoding of oversized blocks, exact big-integer comparison, alignment
validation, atomic delivery, and a randomized differential against an
independently decoded reference.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import struct
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from narc import format as fmt
from narc import reconcile as rec
from narc.catalog import build_catalog, load_catalog
from narc.cli import main as cli_main
from narc.reconcile import ReconcileError, reconcile_archives
from narc.writer import ArchiveWriter, patch_bytes, truncate
from reference import _parse_headers

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


# ---------------------------------------------------------------------------
# Independent reference: fragment-aware truth and a full-materialization
# reconciler.  Decoding re-uses the independently written parser of
# reference.py; nothing here shares code with narc.reconcile.
# ---------------------------------------------------------------------------

def reference_truth(path: str) -> dict:
    """(fragment_id, row, column_id) -> exact value, for trusted values only."""
    _, _, blocks = _parse_headers(path)
    grouped: dict[tuple[int, int], list[dict]] = {}
    for block in blocks:
        grouped.setdefault((block["fragment"], block["column"]), []).append(block)

    values: dict[tuple[int, int, int], int] = {}
    for (fragment_id, column), chain in grouped.items():
        chain.sort(key=lambda item: item["start"])
        carry = None
        for block in chain:
            payload = block["payload"]
            length_bad = block["length"] != block["count"] * fmt.RECORD_SIZE
            digest_bad = hashlib.sha256(payload).hexdigest() != block["digest"]
            illegal = False
            parsed = []
            for index in range(block["count"]):
                base = index * fmt.RECORD_SIZE
                if base + fmt.RECORD_SIZE > len(payload):
                    length_bad = True
                    break
                tag = payload[base]
                value = struct.unpack_from(">q", payload, base + 1)[0]
                if tag == fmt.REC_MISSING:
                    if payload[base + 1:base + 9] != b"\x00" * 8:
                        illegal = True
                    parsed.append((tag, 0))
                elif tag in (fmt.REC_ABSOLUTE, fmt.REC_DELTA):
                    parsed.append((tag, value))
                else:
                    illegal = True
                    parsed.append((tag, value))
            if length_bad or digest_bad or illegal:
                carry = None  # downstream cannot borrow across a bad prefix
                continue
            running = carry if block["kind"] == fmt.KIND_DEPENDENT else None
            for index, (tag, value) in enumerate(parsed):
                row = block["start"] + index
                if tag == fmt.REC_ABSOLUTE:
                    running = value
                    values[(fragment_id, row, column)] = running
                elif tag == fmt.REC_DELTA and running is not None:
                    running += value
                    values[(fragment_id, row, column)] = running
            carry = running
    return values


def _rec_json(rec: tuple) -> dict:
    value, fragment_id, column_id, row = rec
    return {
        "value": value,
        "fragment_id": fragment_id,
        "column_id": column_id,
        "row": row,
    }


def _expected_side(records: list) -> dict | None:
    if not records:
        return None
    min_rec = min(records, key=lambda r: (r[0], r[1], r[2], r[3]))
    max_value = max(r[0] for r in records)
    max_rec = min(
        (r for r in records if r[0] == max_value),
        key=lambda r: (r[1], r[2], r[3]),
    )
    return {"min": _rec_json(min_rec), "max": _rec_json(max_rec)}


def _expected_line(channel: str, sample: int, left: list, right: list):
    left_amb = len({r[0] for r in left}) >= 2
    right_amb = len({r[0] for r in right}) >= 2
    if left_amb or right_amb:
        kind = "ambiguous"
    elif not left and not right:
        return None
    elif not left:
        kind = "right_only"
    elif not right:
        kind = "left_only"
    elif left[0][0] != right[0][0]:
        kind = "different"
    else:
        return None
    return {
        "channel": channel,
        "sample": sample,
        "kind": kind,
        "left": _expected_side(left),
        "right": _expected_side(right),
    }


def expected_sheet(alignment: dict, left_path: str, right_path: str) -> list:
    truths = {
        "left": reference_truth(left_path),
        "right": reference_truth(right_path),
    }
    observations: dict[str, dict] = {"left": {}, "right": {}}
    for side in ("left", "right"):
        for seg in alignment[side]["segments"]:
            fragment_id = seg["fragment_id"]
            column_id = seg["column_id"]
            start, end = seg["local"]
            for row in range(start, end):
                value = truths[side].get((fragment_id, row, column_id))
                if value is None:
                    continue
                key = (seg["channel"], seg["global_start"] + row - start)
                observations[side].setdefault(key, []).append(
                    (value, fragment_id, column_id, row)
                )
    lines = []
    keys = set(observations["left"]) | set(observations["right"])
    for channel, sample in sorted(keys):
        line = _expected_line(
            channel,
            sample,
            observations["left"].get((channel, sample), []),
            observations["right"].get((channel, sample), []),
        )
        if line is not None:
            lines.append(line)
    return lines


def actual_sheet(out_path: str) -> list:
    with open(out_path, "r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_json(path: str, payload: object) -> str:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return path


_RUN_COUNTER = 0


def run_reconcile(tmp: str, alignment: dict, out_name: str | None = None):
    global _RUN_COUNTER
    _RUN_COUNTER += 1
    if out_name is None:
        out_name = f"out-{_RUN_COUNTER}.jsonl"
    alignment_path = write_json(
        os.path.join(tmp, f"alignment-{_RUN_COUNTER}.json"), alignment)
    out_path = os.path.join(tmp, out_name)
    summary = reconcile_archives(alignment_path, out_path)
    return out_path, summary


def side(archive: str, segments: list) -> dict:
    return {"archive": os.path.abspath(archive), "segments": segments}


def seg(fragment_id: int, column_id: int, local: list, channel: str,
        global_start: int) -> dict:
    return {
        "fragment_id": fragment_id,
        "column_id": column_id,
        "local": local,
        "channel": channel,
        "global_start": global_start,
    }


def corrupt_byte(archive: str, catalog_tmp: str, pick) -> None:
    """Flip one payload byte of the block chosen by ``pick`` (digest dies)."""
    build_catalog(archive, catalog_tmp)
    catalog = load_catalog(catalog_tmp)
    block = pick(catalog["_blocks"])
    with open(archive, "rb") as handle:
        handle.seek(block.offset)
        original = handle.read(1)
    patch_bytes(archive, block.offset, bytes([original[0] ^ 0xFF]))


def corrupt_tag_keep_digest(archive: str, catalog_tmp: str, pick) -> None:
    """Turn one record tag illegal while repairing the payload digest."""
    build_catalog(archive, catalog_tmp)
    catalog = load_catalog(catalog_tmp)
    block = pick(catalog["_blocks"])
    patch_bytes(archive, block.offset + fmt.RECORD_SIZE, b"\x7f")
    with open(archive, "rb") as handle:
        handle.seek(block.offset)
        payload = handle.read(block.length)
    digest = hashlib.sha256(payload).digest()
    header_offset = block.offset - fmt.BLOCK_HEADER_SIZE
    patch_bytes(archive, header_offset + 24, digest)  # digest field offset


# ---------------------------------------------------------------------------
# Targeted semantics
# ---------------------------------------------------------------------------

def test_basic_kinds(tmp: str) -> None:
    print("test: difference kinds and the no-report cases")
    left = os.path.join(tmp, "left.narc")
    right = os.path.join(tmp, "right.narc")
    # Logical samples 100..109 come from rows 0..9 of fragment 1 column 1.
    with ArchiveWriter(left) as writer:
        writer.fragment(1, "l.raw", 0, 10)
        writer.block(IND, 1, 0, [(A, 10), (D, 1), (D, 1), (M, 0), (D, 1),
                                 (A, 50), (D, 1), (D, 1), (D, 1), (D, 1)])
        writer.block(IND, 2, 0, [(D, 5), (D, 5), (A, 7), (D, 1), (D, 1),
                                 (D, 1), (D, 1), (D, 1), (D, 1), (D, 1)])
    with ArchiveWriter(right) as writer:
        writer.fragment(1, "r.raw", 0, 10)
        writer.block(IND, 1, 0, [(A, 10), (D, 1), (D, 1), (D, 1), (D, 99),
                                 (A, 50), (D, 1), (D, 1), (D, 1), (D, 1)])
        # Column 2 of the right copy starts one row later and mirrors the
        # left reconstruction from row 2 on (6 at row 1, then 7, 8, ...).
        writer.block(IND, 2, 1, [(A, 6), (D, 1), (D, 1), (D, 1), (D, 1),
                                 (D, 1), (D, 1), (D, 1), (D, 1)])

    alignment = {
        "left": side(left, [seg(1, 1, [0, 10], "temp", 100),
                            seg(1, 2, [0, 10], "hum", 100)]),
        "right": side(right, [seg(1, 1, [0, 10], "temp", 100),
                              seg(1, 2, [1, 10], "hum", 101)]),
    }
    out_path, _ = run_reconcile(tmp, alignment)
    lines = actual_sheet(out_path)
    expected = expected_sheet(alignment, left, right)
    check("sheet matches the independent reference", lines == expected,
          f"got={lines} expected={expected}")

    kinds = {(line["channel"], line["sample"]): line["kind"] for line in lines}
    # temp: rows 0-2 agree; row 3 left missing vs right 13 -> right_only;
    # row 4 left 13 vs right 102 -> different; rows 5-9 agree.
    check("missing on one side is not an observation",
          kinds.get(("temp", 103)) == "right_only", kinds)
    check("reconstructed values are compared, not wire deltas",
          kinds.get(("temp", 104)) == "different", kinds)
    check("agreeing samples produce no lines",
          ("temp", 100) not in kinds and ("temp", 109) not in kinds, kinds)
    # hum: left column 2 rows 0-1 are deltas before the first absolute
    # (missing_prefix, not observations); right column 2 covers rows 1-9,
    # so sample 101 exists only on the right, and from sample 102 on both
    # sides reconstruct to the same values.
    check("missing_prefix is not an observation",
          kinds.get(("hum", 101)) == "right_only", kinds)
    check("missing_prefix plus uncovered produces no line",
          ("hum", 100) not in kinds, kinds)
    check("no line where both sides agree from row 2 on",
          all(not (c == "hum" and s >= 102) for (c, s) in kinds), kinds)


def test_ambiguous_and_duplicates(tmp: str) -> None:
    print("test: ambiguity inside one side; repeated equal values are benign")
    left = os.path.join(tmp, "left.narc")
    right = os.path.join(tmp, "right.narc")
    with ArchiveWriter(left) as writer:
        writer.fragment(1, "l.raw", 0, 4)
        writer.block(IND, 1, 0, [(A, 5), (A, 6), (A, 7), (A, 8)])
        writer.block(IND, 2, 0, [(A, 5), (A, 60), (A, 7), (A, 8)])
    with ArchiveWriter(right) as writer:
        writer.fragment(1, "r.raw", 0, 4)
        writer.block(IND, 1, 0, [(A, 5), (A, 6), (A, 7), (A, 9)])
        writer.block(IND, 2, 0, [(A, 5), (A, 60), (A, 7), (A, 9)])

    # Both sides map the same logical samples from two columns (two captures
    # per side).  At sample 1 each side holds the value set {6, 60}.
    alignment = {
        "left": side(left, [seg(1, 1, [0, 4], "temp", 0),
                            seg(1, 2, [0, 4], "temp", 0)]),
        "right": side(right, [seg(1, 1, [0, 4], "temp", 0),
                              seg(1, 2, [0, 4], "temp", 0)]),
    }
    out_path, _ = run_reconcile(tmp, alignment)
    lines = actual_sheet(out_path)
    check("sheet matches reference", lines == expected_sheet(alignment, left, right),
          lines)
    kinds = {line["sample"]: line["kind"] for line in lines}
    check("two distinct values on one side are ambiguous",
          kinds.get(1) == "ambiguous", kinds)
    both = next(line for line in lines if line["sample"] == 1)
    check("ambiguous even when the other side has the same value set",
          both["left"]["min"]["value"] == 6
          and both["left"]["max"]["value"] == 60
          and both["right"]["min"]["value"] == 6
          and both["right"]["max"]["value"] == 60, both)
    check("same value from two captures is not a contradiction",
          0 not in kinds and 2 not in kinds, kinds)
    check("single-value sides still compare as different",
          kinds.get(3) == "different", kinds)
    amb = next(line for line in lines if line["sample"] == 1)
    check("ambiguous side still reports min and max with provenance",
          amb["left"]["min"]["value"] == 6 and amb["left"]["max"]["value"] == 60
          and amb["left"]["min"]["column_id"] == 1
          and amb["left"]["max"]["column_id"] == 2, amb)


def test_out_of_interval_predecessors(tmp: str) -> None:
    print("test: trusted predecessors outside the aligned interval")
    archive = os.path.join(tmp, "chain.narc")
    other = os.path.join(tmp, "other.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "c.raw", 0, 20)
        # Independent block: absolute at row 2, segment starts at row 5.
        writer.block(IND, 1, 0, [(D, 9), (D, 9), (A, 100), (D, 1), (D, 1),
                                 (D, 1), (D, 1), (D, 1), (D, 1), (D, 1)])
        # Dependent chain: carry-in resolves the second block's head; the
        # segment starts mid-block with no in-block absolute at all.
        writer.block(IND, 2, 0, [(A, 1000), (D, 10), (D, 10), (D, 10), (D, 10)])
        writer.block(DEP, 2, 5, [(D, 1), (D, 1), (D, 1), (D, 1), (D, 1)])
        writer.block(DEP, 2, 10, [(D, 1), (D, 1), (D, 1), (D, 1), (D, 1)])
    with ArchiveWriter(other) as writer:
        writer.fragment(1, "o.raw", 0, 20)
        writer.block(IND, 1, 0, [(A, 0)] * 20)
        writer.block(IND, 2, 0, [(A, 0)] * 20)

    alignment = {
        "left": side(archive, [seg(1, 1, [5, 10], "a", 0),
                               seg(1, 2, [7, 15], "b", 0)]),
        "right": side(other, [seg(1, 1, [5, 10], "a", 0),
                              seg(1, 2, [7, 15], "b", 0)]),
    }
    out_path, _ = run_reconcile(tmp, alignment)
    lines = actual_sheet(out_path)
    check("sheet matches reference",
          lines == expected_sheet(alignment, archive, other), lines)
    values = {(line["channel"], line["sample"]): line["left"]["min"]["value"]
              for line in lines if line["left"] is not None}
    # Channel a: rows 5-9 reconstruct to 103..107 from the row-2 absolute.
    check("in-block absolute before the interval anchors its deltas",
          values.get(("a", 0)) == 103 and values.get(("a", 4)) == 107, values)
    # Channel b: rows 7-14 -> 1043..1050.  The segment starts two records
    # into the second dependent block; v1 applies that block's records from
    # its head on top of the chain carry entering it.
    check("dependent block reconstructs from its head via carry-in",
          values.get(("b", 0)) == 1043 and values.get(("b", 7)) == 1050,
          values)


def test_damaged_isolation_and_suffix(tmp: str) -> None:
    print("test: damaged blocks isolate; dependent suffix recovers per v1")
    archive = os.path.join(tmp, "dmg.narc")
    good = os.path.join(tmp, "good.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "d.raw", 0, 12)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (D, 1)])          # rows 0-2
        writer.block(DEP, 1, 3, [(D, 1), (D, 1), (D, 1)])          # rows 3-5 (damaged)
        writer.block(DEP, 1, 6, [(D, 9), (A, 50), (D, 1)])         # rows 6-8
        writer.block(DEP, 1, 9, [(D, 1), (D, 1), (D, 1)])          # rows 9-11
    with ArchiveWriter(good) as writer:
        writer.fragment(1, "g.raw", 0, 12)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (D, 1), (D, 1),
                                 (D, 1), (D, 1), (D, 1), (D, 1), (D, 1),
                                 (D, 1), (D, 1), (D, 1)])
    # Damage the middle dependent block of the first archive (digest dies).
    corrupt_byte(archive, os.path.join(tmp, "probe.json"),
                 lambda blocks: next(b for b in blocks
                                     if b.column_id == 1 and b.start_row == 3))

    alignment = {
        "left": side(archive, [seg(1, 1, [0, 12], "temp", 0)]),
        "right": side(good, [seg(1, 1, [0, 12], "temp", 0)]),
    }
    out_path, _ = run_reconcile(tmp, alignment)
    lines = actual_sheet(out_path)
    check("sheet matches reference", lines == expected_sheet(alignment, archive, good),
          lines)
    kinds = {line["sample"]: line["kind"] for line in lines}
    check("damaged block contributes no observations",
          kinds.get(3) == "right_only" and kinds.get(5) == "right_only", kinds)
    check("dependent suffix re-anchors on its own absolute",
          kinds.get(6) == "right_only" and kinds.get(7) == "different"
          and kinds.get(8) == "different", kinds)
    check("recovery continues into the next dependent block",
          kinds.get(9) == "different" and kinds.get(11) == "different", kinds)
    line7 = next(line for line in lines if line["sample"] == 7)
    check("recovered suffix value follows v1 absolute recovery",
          line7["left"]["min"]["value"] == 50, line7)


def test_illegal_row_damage(tmp: str) -> None:
    print("test: illegal rows poison their whole block for reconciliation")
    archive = os.path.join(tmp, "ill.narc")
    good = os.path.join(tmp, "good2.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "i.raw", 0, 6)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (D, 1)])
        writer.block(DEP, 1, 3, [(A, 7), (D, 1), (D, 1)])
    with ArchiveWriter(good) as writer:
        writer.fragment(1, "g.raw", 0, 6)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (D, 1),
                                 (A, 7), (D, 1), (D, 1)])
    # An illegal record tag with a repaired digest still poisons the block.
    corrupt_tag_keep_digest(
        archive, os.path.join(tmp, "probe2.json"),
        lambda blocks: next(b for b in blocks if b.start_row == 3))

    alignment = {
        "left": side(archive, [seg(1, 1, [0, 6], "temp", 0)]),
        "right": side(good, [seg(1, 1, [0, 6], "temp", 0)]),
    }
    out_path, _ = run_reconcile(tmp, alignment)
    lines = actual_sheet(out_path)
    check("sheet matches reference",
          lines == expected_sheet(alignment, archive, good), lines)
    kinds = {line["sample"]: line["kind"] for line in lines}
    check("illegal-row block yields no observations",
          kinds.get(3) == "right_only" and kinds.get(5) == "right_only",
          kinds)
    check("rows before the damaged block are unaffected",
          not any(sample < 3 for sample in kinds), kinds)


def test_provenance_and_ordering(tmp: str) -> None:
    print("test: provenance tie-breaks and output ordering")
    left = os.path.join(tmp, "left.narc")
    right = os.path.join(tmp, "right.narc")
    with ArchiveWriter(left) as writer:
        writer.fragment(2, "l2.raw", 100, 104)
        writer.block(IND, 3, 100, [(A, 7), (A, 7), (A, 1), (A, 2)])
        writer.block(IND, 4, 100, [(A, 9), (A, 9), (A, 9), (A, 9)])
        writer.fragment(1, "l1.raw", 0, 4)
        writer.block(IND, 1, 0, [(A, 7), (A, 5), (A, 3), (A, 4)])
        writer.block(IND, 2, 0, [(A, 1), (A, 2), (A, 3), (A, 4)])
    with ArchiveWriter(right) as writer:
        writer.fragment(1, "r.raw", 0, 4)
        writer.block(IND, 9, 0, [(A, 0), (A, 0), (A, 0), (A, 0)])

    # Channel "z" gets two left captures (fragment 1 column 1 and fragment 2
    # column 3); both hold 7 at logical sample 0, so the provenance must pick
    # the smallest (fragment_id, column_id, row).  The other channels come
    # from their own columns -- one (fragment, column) range maps at most
    # once per side.
    segments_left = [
        seg(2, 3, [100, 104], "z", 0),   # written first on purpose
        seg(1, 1, [0, 4], "z", 0),
        seg(1, 2, [0, 4], "温度", 50),
        seg(2, 4, [100, 104], "α", 20),
    ]
    alignment = {
        "left": side(left, segments_left),
        "right": side(right, [seg(1, 9, [0, 4], "z", 0)]),
    }
    out_path, _ = run_reconcile(tmp, alignment)
    raw = open(out_path, "rb").read()
    lines = actual_sheet(out_path)
    check("sheet matches reference", lines == expected_sheet(alignment, left, right),
          lines)

    channels = [line["channel"] for line in lines]
    check("channels ordered by Unicode code point",
          channels == sorted(channels), channels)
    check("code point order puts latin before greek before cjk",
          [c for c in dict.fromkeys(channels)] == ["z", "α", "温度"], channels)
    z0 = next(line for line in lines if line["channel"] == "z"
              and line["sample"] == 0)
    check("min provenance picks smallest (fragment, column, row)",
          z0["left"]["min"] == {"value": 7, "fragment_id": 1, "column_id": 1,
                                "row": 0}
          and z0["left"]["max"] == {"value": 7, "fragment_id": 1,
                                    "column_id": 1, "row": 0}, z0)
    samples_z = [line["sample"] for line in lines if line["channel"] == "z"]
    check("samples ascend within a channel", samples_z == sorted(samples_z),
          samples_z)

    # Re-running with the segments in a different order must not change
    # a single output byte.
    shuffled = {
        "left": side(left, [segments_left[2], segments_left[0],
                            segments_left[3], segments_left[1]]),
        "right": side(right, [seg(1, 9, [0, 4], "z", 0)]),
    }
    out2 = os.path.join(tmp, "out2.jsonl")
    write_json(os.path.join(tmp, "alignment2.json"), shuffled)
    reconcile_archives(os.path.join(tmp, "alignment2.json"), out2)
    check("segment order never changes the sheet",
          open(out2, "rb").read() == raw)


def test_coordinate_bounds(tmp: str) -> None:
    print("test: v1 unsigned ranges, huge logical gaps, fragment containment")
    archive = os.path.join(tmp, "b.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "b.raw", 10, 14)
        writer.block(IND, 1, 10, [(A, 1), (A, 2), (A, 3), (A, 4)])

    base = seg(1, 1, [10, 12], "x", 0)
    ok = {"left": side(archive, [dict(base, global_start=2 ** 32 - 2)]),
          "right": side(archive, [dict(base, global_start=2 ** 32 - 2)])}
    out_path, _ = run_reconcile(tmp, ok)
    check("mapped end exactly 2^32 is accepted and empty",
          actual_sheet(out_path) == [])

    too_far = {"left": side(archive, [dict(base, global_start=2 ** 32 - 1)]),
               "right": side(archive, [seg(1, 1, [10, 12], "x", 0)])}
    try:
        run_reconcile(tmp, too_far, "nope.jsonl")
        raised = False
    except ReconcileError:
        raised = True
    check("mapped end beyond 2^32 is rejected", raised)
    check("rejection leaves no sheet", not os.path.exists(os.path.join(tmp, "nope.jsonl")))

    bad_local = {"left": side(archive, [seg(1, 1, [8, 12], "x", 0)]),
                 "right": side(archive, [seg(1, 1, [10, 12], "x", 0)])}
    try:
        run_reconcile(tmp, bad_local, "nope2.jsonl")
        raised = False
    except ReconcileError:
        raised = True
    check("local range escaping the fragment is rejected", raised)

    no_fragment = {"left": side(archive, [seg(9, 1, [10, 12], "x", 0)]),
                   "right": side(archive, [seg(1, 1, [10, 12], "x", 0)])}
    try:
        run_reconcile(tmp, no_fragment, "nope3.jsonl")
        raised = False
    except ReconcileError:
        raised = True
    check("unknown fragment is rejected", raised)

    overlap = {"left": side(archive, [seg(1, 1, [10, 13], "x", 0),
                                      seg(1, 1, [12, 14], "y", 0)]),
               "right": side(archive, [seg(1, 1, [10, 12], "x", 0)])}
    try:
        run_reconcile(tmp, overlap, "nope4.jsonl")
        raised = False
    except ReconcileError:
        raised = True
    check("intersecting same-column ranges on one side are rejected", raised)

    adjacent = {"left": side(archive, [seg(1, 1, [10, 12], "x", 0),
                                       seg(1, 1, [12, 14], "x", 2)]),
                "right": side(archive, [seg(1, 1, [10, 14], "x", 0)])}
    out_path, _ = run_reconcile(tmp, adjacent, "adj.jsonl")
    check("adjacent (non-intersecting) ranges are fine",
          actual_sheet(out_path) == [])


def test_invalid_input_and_atomicity(tmp: str) -> None:
    print("test: invalid input and uninterpretable archives leave no sheet")
    archive = os.path.join(tmp, "ok.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "ok.raw", 0, 2)
        writer.block(IND, 1, 0, [(A, 1), (A, 2)])
    good = {"left": side(archive, [seg(1, 1, [0, 2], "x", 0)]),
            "right": side(archive, [seg(1, 1, [0, 2], "x", 0)])}

    variants = {
        "not an object": [1, 2],
        "missing side": {"left": good["left"]},
        "extra key": dict(good, extra=1),
        "relative archive": {"left": {"archive": "rel.narc",
                                      "segments": [seg(1, 1, [0, 2], "x", 0)]},
                             "right": good["right"]},
        "empty segments": {"left": {"archive": os.path.abspath(archive),
                                    "segments": []},
                           "right": good["right"]},
        "empty local": {"left": side(archive, [seg(1, 1, [1, 1], "x", 0)]),
                        "right": good["right"]},
        "bool coordinate": {"left": side(archive, [
                                {"fragment_id": 1, "column_id": 1,
                                 "local": [0, 2], "channel": "x",
                                 "global_start": True}]),
                            "right": good["right"]},
        "empty channel": {"left": side(archive, [seg(1, 1, [0, 2], "", 0)]),
                          "right": good["right"]},
        "huge fragment id": {"left": side(archive, [seg(70000, 1, [0, 2], "x", 0)]),
                             "right": good["right"]},
        "extra segment key": {"left": side(archive, [
                                  dict(seg(1, 1, [0, 2], "x", 0), unit="C")]),
                              "right": good["right"]},
    }
    for name, alignment in variants.items():
        out_path = os.path.join(tmp, f"bad-{name}.jsonl")
        try:
            write_json(os.path.join(tmp, "bad.json"), alignment)
            reconcile_archives(os.path.join(tmp, "bad.json"), out_path)
            raised = False
        except ReconcileError:
            raised = True
        check(f"invalid alignment rejected: {name}", raised)
        check(f"no sheet left behind: {name}", not os.path.exists(out_path))

    # Uninterpretable archive (bad magic).
    garbage = os.path.join(tmp, "garbage.narc")
    with open(garbage, "wb") as handle:
        handle.write(b"not a narc archive at all")
    out_path = os.path.join(tmp, "garbage.jsonl")
    try:
        write_json(os.path.join(tmp, "g.json"),
                   {"left": side(garbage, [seg(1, 1, [0, 2], "x", 0)]),
                    "right": good["right"]})
        reconcile_archives(os.path.join(tmp, "g.json"), out_path)
        raised = False
    except ReconcileError:
        raised = True
    check("uninterpretable archive rejected", raised)
    check("no sheet after archive failure", not os.path.exists(out_path))

    # Output path must not exist; an existing file is left untouched.
    existing = os.path.join(tmp, "existing.jsonl")
    with open(existing, "w", encoding="utf-8") as handle:
        handle.write("keep me\n")
    try:
        write_json(os.path.join(tmp, "good.json"), good)
        reconcile_archives(os.path.join(tmp, "good.json"), existing)
        raised = False
    except ReconcileError:
        raised = True
    check("existing output is refused", raised)
    check("existing output is untouched",
          open(existing, "rb").read() == b"keep me\n")

    # No hidden staging directories survive a failed run.
    leftovers = [name for name in os.listdir(tmp) if name.startswith(".narc-")]
    check("no staging directories left behind", leftovers == [], leftovers)


def test_chunked_decode_and_big_values(tmp: str) -> None:
    print("test: chunked block decoding and exact big-integer comparison")
    left = os.path.join(tmp, "big-l.narc")
    right = os.path.join(tmp, "big-r.narc")
    n = 5000
    # The absolute anchor stays inside int64; the delta chain pushes the
    # reconstructed values out of it after a few dozen records.
    base = 2 ** 63 - 100
    dep_tail = [(A, 7)] + [(D, 1)] * 49          # rows 0-49 -> 7..56
    dep_chain = [(D, 1)] * (n - 50)              # rows 50.. -> 57..5006
    with ArchiveWriter(left) as writer:
        writer.fragment(1, "bl.raw", 0, n)
        writer.block(IND, 1, 0, [(A, base)] + [(D, 3)] * (n - 1))
        writer.block(IND, 2, 0, dep_tail)
        writer.block(DEP, 2, 50, dep_chain)      # no in-block absolute
    with ArchiveWriter(right) as writer:
        writer.fragment(1, "br.raw", 0, n)
        # Same chain, but the last delta differs by one.
        writer.block(IND, 1, 0, [(A, base)] + [(D, 3)] * (n - 2) + [(D, 4)])
        writer.block(IND, 2, 0, dep_tail)
        # The dependent chain's last delta also differs by one, so the
        # head-read reconstruction is pinned to an exact value.
        writer.block(DEP, 2, 50, dep_chain[:-1] + [(D, 2)])

    # The "dep" segment starts 50 records into the dependent block, so its
    # values only reconstruct by applying that block's records from its head
    # (across many tiny decode chunks) on top of the chain carry.
    alignment = {
        "left": side(left, [seg(1, 1, [0, n], "big", 0),
                            seg(1, 2, [100, n], "dep", 0)]),
        "right": side(right, [seg(1, 1, [0, n], "big", 0),
                              seg(1, 2, [100, n], "dep", 0)]),
    }

    original_chunk = rec._DECODE_CHUNK_RECORDS
    rec._DECODE_CHUNK_RECORDS = 97  # force many slices, odd boundaries
    try:
        out_path, _ = run_reconcile(tmp, alignment)
    finally:
        rec._DECODE_CHUNK_RECORDS = original_chunk

    lines = actual_sheet(out_path)
    check("sheet matches reference under tiny decode chunks",
          lines == expected_sheet(alignment, left, right), lines[:3])
    check("exactly the two tail samples are reported",
          len(lines) == 2
          and (lines[0]["channel"], lines[0]["sample"]) == ("big", n - 1)
          and (lines[1]["channel"], lines[1]["sample"]) == ("dep", n - 101),
          lines)
    check("dependent head-read across chunks is exact",
          lines[1]["left"]["min"]["value"] == 5006
          and lines[1]["right"]["min"]["value"] == 5007, lines[1])
    expected_value = base + 3 * (n - 1)
    check("big-integer values compare exactly",
          lines[0]["left"]["min"]["value"] == expected_value
          and lines[0]["right"]["min"]["value"] == expected_value + 1,
          lines[0])


def test_random_differential(tmp: str) -> None:
    print("test: randomized differential against the reference reconciler")
    rng = random.Random(20261123)
    cases = 60
    for case in range(cases):
        case_dir = os.path.join(tmp, f"case-{case}")
        os.makedirs(case_dir)
        left = os.path.join(case_dir, "left.narc")
        right = os.path.join(case_dir, "right.narc")
        layout = _random_layout(rng)
        _write_layout(left, layout, mutate_rng=None)
        _write_layout(right, layout, mutate_rng=rng)
        for path, tag in ((left, "l"), (right, "r")):
            if rng.random() < 0.45:
                try:
                    corrupt_byte(path, os.path.join(case_dir, f"probe-{tag}.json"),
                                 lambda blocks: rng.choice(blocks))
                except (IndexError, ValueError):
                    pass
        alignment = _random_alignment(rng, layout, left, right)
        out_path = os.path.join(case_dir, "out.jsonl")
        write_json(os.path.join(case_dir, "alignment.json"), alignment)
        reconcile_archives(os.path.join(case_dir, "alignment.json"), out_path)
        got = actual_sheet(out_path)
        expected = expected_sheet(alignment, left, right)
        check(f"random case {case} matches reference", got == expected,
              f"got={got[:2]} expected={expected[:2]}")
        if case % 7 == 0:
            # Segment order must not matter: shuffle and compare bytes.
            for side_name in ("left", "right"):
                rng.shuffle(alignment[side_name]["segments"])
            out2 = os.path.join(case_dir, "out2.jsonl")
            write_json(os.path.join(case_dir, "alignment2.json"), alignment)
            reconcile_archives(os.path.join(case_dir, "alignment2.json"), out2)
            check(f"random case {case} is segment-order invariant",
                  open(out2, "rb").read() == open(out_path, "rb").read())


def _random_layout(rng: random.Random) -> list:
    """A shared archive layout: fragments -> columns -> block record lists."""
    fragments = []
    row_base = 0
    for fid in (1, 2):
        frag_rows = rng.randint(6, 18)
        columns = {}
        for col in sorted(rng.sample([1, 2, 3], rng.randint(1, 3))):
            blocks = []
            cursor = row_base
            prev_end = None
            while cursor < row_base + frag_rows and rng.random() < 0.8:
                length = min(rng.randint(1, 6), row_base + frag_rows - cursor)
                records = []
                for _ in range(length):
                    roll = rng.random()
                    if roll < 0.3:
                        records.append((A, rng.randint(-20, 20)))
                    elif roll < 0.9:
                        records.append((D, rng.randint(-4, 4)))
                    else:
                        records.append((M, 0))
                kind = IND
                if prev_end == cursor and rng.random() < 0.5:
                    kind = DEP
                blocks.append((kind, col, cursor, records))
                prev_end = cursor + length
                cursor += length + rng.randint(0, 2)
            if blocks:
                columns[col] = blocks
        fragments.append((fid, row_base, row_base + frag_rows, columns))
        row_base += frag_rows + rng.randint(0, 3)
    return fragments


def _write_layout(path: str, layout: list, mutate_rng) -> None:
    with ArchiveWriter(path) as writer:
        for fid, start, end, columns in layout:
            writer.fragment(fid, f"src{fid}.raw", start, end)
            for col in sorted(columns):
                for kind, column_id, block_start, records in columns[col]:
                    records = list(records)
                    if mutate_rng is not None:
                        records = [
                            _mutate_record(mutate_rng, record)
                            for record in records
                        ]
                    writer.block(kind, column_id, block_start, records)


def _mutate_record(rng: random.Random, record: tuple) -> tuple:
    if rng.random() >= 0.12:
        return record
    tag, value = record
    roll = rng.random()
    if tag == M:
        # A missing record must keep its zero payload; mutate the tag.
        return (A, rng.randint(-20, 20)) if roll < 0.5 else (D, rng.randint(-4, 4))
    if roll < 0.4:
        return (tag, value + rng.choice((-3, -1, 1, 3)))
    if roll < 0.6:
        return (M, 0)
    if roll < 0.8:
        return (A, value)
    return (D, rng.randint(-4, 4))


def _random_alignment(rng: random.Random, layout: list,
                      left: str, right: str) -> dict:
    channels = ["alpha", "beta", "温度", "α", "ch9"]
    alignment = {}
    for side_name, archive in (("left", left), ("right", right)):
        segments = []
        used: dict[tuple[int, int], list] = {}
        for _ in range(rng.randint(2, 6)):
            fid, fstart, fend, columns = rng.choice(layout)
            col_choices = sorted(columns) + [8]
            col = rng.choice(col_choices)
            placed = False
            for _attempt in range(25):
                start = rng.randint(fstart, fend - 1)
                end = rng.randint(start + 1, min(fend, start + 8))
                spans = used.setdefault((fid, col), [])
                if all(end <= a or start >= b for a, b in spans):
                    spans.append((start, end))
                    placed = True
                    break
            if not placed:
                continue
            if rng.random() < 0.65:
                global_start = 4000 + (start - fstart)
            else:
                global_start = rng.choice(
                    [0, 7, 2 ** 20, 2 ** 32 - (end - start)]
                )
            segments.append(seg(fid, col, [start, end],
                                rng.choice(channels), global_start))
        # Sometimes duplicate a capture onto another column of the same
        # side so one logical key gets two observations from one side.
        if segments and rng.random() < 0.5:
            donor = rng.choice(segments)
            fid, fstart, fend, columns = layout[0]
            other_cols = [c for c in sorted(columns) if c != donor["column_id"]]
            if other_cols:
                col = other_cols[0]
                start, end = donor["local"]
                spans = used.setdefault((fid, col), [])
                if end <= fend and all(end <= a or start >= b for a, b in spans):
                    spans.append((start, end))
                    segments.append(seg(fid, col, [start, end],
                                        donor["channel"],
                                        donor["global_start"]))
        if not segments:
            segments.append(seg(layout[0][0], 8, [layout[0][1], layout[0][1] + 1],
                                "alpha", 0))
        alignment[side_name] = side(archive, segments)
    return alignment


def test_cli(tmp: str) -> None:
    print("test: command line entry point")
    archive = os.path.join(tmp, "cli.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "c.raw", 0, 2)
        writer.block(IND, 1, 0, [(A, 1), (A, 2)])
    alignment = {"left": side(archive, [seg(1, 1, [0, 2], "x", 0)]),
                 "right": side(archive, [seg(1, 1, [0, 2], "x", 0)])}
    alignment_path = write_json(os.path.join(tmp, "alignment.json"), alignment)
    out_path = os.path.join(tmp, "cli.jsonl")
    code = cli_main(["reconcile", alignment_path, out_path])
    check("cli reconcile exits 0", code == 0)
    check("cli produced an (empty) sheet",
          os.path.exists(out_path) and open(out_path, "rb").read() == b"")

    code = cli_main(["reconcile", alignment_path, out_path])
    check("cli refuses an existing output", code == 2)
    code = cli_main(["reconcile", os.path.join(tmp, "missing.json"),
                     os.path.join(tmp, "other.jsonl")])
    check("cli reports unreadable alignment", code == 2)
    check("cli failure leaves no sheet",
          not os.path.exists(os.path.join(tmp, "other.jsonl")))


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="narc-test-reconcile-")
    print(f"scratch directory: {tmp}")
    for test in (
        test_basic_kinds,
        test_ambiguous_and_duplicates,
        test_out_of_interval_predecessors,
        test_damaged_isolation_and_suffix,
        test_illegal_row_damage,
        test_provenance_and_ordering,
        test_coordinate_bounds,
        test_invalid_input_and_atomicity,
        test_chunked_decode_and_big_values,
        test_random_differential,
        test_cli,
    ):
        test(tmp)
    print(f"\n{PASS} assertions passed, {len(FAILURES)} failures")
    if FAILURES:
        for name, detail in FAILURES:
            print(f"FAILED: {name} :: {detail}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
