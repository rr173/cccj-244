"""End-to-end tests for narc: differential truth, errors and read boundaries."""

from __future__ import annotations

import json
import os
import shutil
import struct
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from narc import format as fmt
from narc.catalog import CatalogError, build_catalog, load_catalog
from narc.io_utils import hash_file
from narc.query import query_window
from narc.writer import ArchiveWriter, patch_bytes, truncate
from reference import (DAMAGED, MISSING, MISSING_PREFIX, UNCOVERED, VALUE,
                       expected_window, reference_map)

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


def build_small_archive(path: str) -> None:
    """Two fragments, three columns, independent + dependent blocks."""
    with ArchiveWriter(path) as writer:
        writer.fragment(1, "source-a.raw", 0, 20)
        # Column 1: independent block with absolute, deltas and a missing.
        writer.block(
            IND, 1, 0,
            [(A, 100), (D, 5), (M, 0), (D, -3), (A, 200),
             (D, 1), (D, 1), (D, 1), (M, 0), (D, 4)],
        )
        # Dependent continuation of column 1, contiguous rows.
        writer.block(
            DEP, 1, 10,
            [(D, 2), (D, 2), (A, 500), (D, -7), (M, 0),
             (D, 10), (D, -1), (D, 1), (D, 1), (D, 1)],
        )
        # Column 2: independent block whose first records are illegal deltas,
        # then an absolute re-anchors the suffix.
        writer.block(
            IND, 2, 0,
            [(D, 9), (D, 8), (M, 0), (A, 70), (D, 1), (D, 2),
             (A, 80), (D, -4), (M, 0), (D, 6)],
        )
        # Column 3: dependent block with no trusted predecessor anywhere.
        writer.block(
            IND, 3, 0,
            [(D, 1), (D, 2), (A, 9), (D, 3), (D, 3)],
        )

        writer.fragment(2, "source-b.raw", 20, 30)
        writer.block(
            IND, 1, 20,
            [(A, -10), (D, -10), (D, -10), (M, 0), (D, 5),
             (A, 2 ** 62), (D, 7), (D, -7), (D, 1), (D, 0)],
        )
        # Column 2 intentionally absent here -> uncovered rows.


def cells_to_map(result) -> dict:
    return {
        (cell.row, cell.column_id): (cell.status, cell.value)
        for cell in result.cells
    }


def test_differential(tmp: str) -> None:
    print("test: exhaustive small-window differential against reference")
    archive = os.path.join(tmp, "small.narc")
    catalog_path = os.path.join(tmp, "small.json")
    build_small_archive(archive)
    build_catalog(archive, catalog_path)

    truth = reference_map(archive)
    # Catalog never stores decoded row data, only scalar summaries.
    raw_catalog = json.load(open(catalog_path, encoding="utf-8"))
    check("catalog records no decoded row stream",
          all("rows" not in block for block in raw_catalog["blocks"]))

    columns = [1, 2, 3]
    for start in range(0, 30):
        for end in range(start + 1, 31):
            result = query_window(catalog_path, start, end, columns)
            got = cells_to_map(result)
            expected = expected_window(archive, start, end, columns)
            check(
                f"window [{start},{end}) matches reference",
                got == expected,
                f"got={got} expected={expected}",
            )
            check(
                f"window [{start},{end}) returns every requested position",
                len(got) == (end - start) * len(columns),
            )

    # Spot-check exact semantics called out in the specification.
    result = query_window(catalog_path, 0, 5, [1])
    got = cells_to_map(result)
    check("missing does not alter the running value",
          got[(2, 1)] == (MISSING, None) and got[(3, 1)] == (VALUE, 102))
    check("absolute re-anchors later deltas",
          got[(4, 1)] == (VALUE, 200))

    result = query_window(catalog_path, 0, 6, [2])
    got = cells_to_map(result)
    check("independent deltas before first absolute are missing_prefix",
          got[(0, 2)] == (MISSING_PREFIX, None)
          and got[(1, 2)] == (MISSING_PREFIX, None)
          and got[(3, 2)] == (VALUE, 70))
    check("suffix after a trusted absolute is readable",
          got[(5, 2)] == (VALUE, 73))


def test_long_delta_chain(tmp: str) -> None:
    print("test: long delta chain tail without head/column scans")
    archive = os.path.join(tmp, "long.narc")
    catalog_path = os.path.join(tmp, "long.json")
    n = 4096
    records = [(A, 3)] + [(D, 1) for _ in range(n - 1)]
    with ArchiveWriter(archive) as writer:
        writer.fragment(7, "long.raw", 0, n)
        writer.block(IND, 4, 0, records)
    build_catalog(archive, catalog_path)

    result = query_window(catalog_path, n - 4, n, [4])
    got = cells_to_map(result)
    expected_tail = 3 + (n - 1)
    check("tail value of a 4096-record delta chain is exact",
          all(got[(n - 4 + i, 4)] == (VALUE, expected_tail - 3 + i)
              for i in range(4)))

    # Bound check: only one payload is touched and the read is a narrow slice,
    # never anything from the file head or the block head.
    catalog = load_catalog(catalog_path)
    block = catalog["_blocks"][0]
    spans = result.spans
    check("tail query issues exactly one narrow read", len(spans) == 1)
    check("read stays inside the target block payload",
          spans[0].offset >= block.offset and spans[0].end <= block.end_offset)
    check("read does not start at file/block head",
          spans[0].offset > block.offset)
    check(
        "narrow read is bounded by the checkpoint stride plus window",
        result.bytes_read
        <= (fmt.CHECKPOINT_STRIDE + 4) * fmt.RECORD_SIZE,
        f"bytes={result.bytes_read}",
    )
    check("narrow read is far smaller than the full block",
          result.bytes_read < block.length // 10)


def test_mid_block_absolute(tmp: str) -> None:
    print("test: mid-block absolute resets dependency on earlier values")
    archive = os.path.join(tmp, "mid.narc")
    catalog_path = os.path.join(tmp, "mid.json")
    with ArchiveWriter(archive) as writer:
        writer.fragment(3, "mid.raw", 0, 8)
        writer.block(IND, 9, 0,
                     [(D, 1), (D, 1), (A, 42), (D, 1), (M, 0),
                      (D, 2), (A, 0), (D, -5)])
    build_catalog(archive, catalog_path)

    # Query only the suffix: the absolute must anchor it without reading rows
    # 0-2 at all.
    result = query_window(catalog_path, 3, 8, [9])
    got = cells_to_map(result)
    check("suffix anchored by in-block absolute",
          got[(3, 9)] == (VALUE, 43)
          and got[(4, 9)] == (MISSING, None)
          and got[(5, 9)] == (VALUE, 45)
          and got[(6, 9)] == (VALUE, 0)
          and got[(7, 9)] == (VALUE, -5))
    block = load_catalog(catalog_path)["_blocks"][0]
    check("suffix read starts at the absolute record, not block head",
          result.spans[0].offset == block.offset + 2 * fmt.RECORD_SIZE)

    # Prefix-only window returns missing_prefix and needs no head scan.
    pre = query_window(catalog_path, 0, 3, [9])
    pre_got = cells_to_map(pre)
    check("prefix deltas of an independent block are missing_prefix",
          pre_got[(0, 9)] == (MISSING_PREFIX, None)
          and pre_got[(1, 9)] == (MISSING_PREFIX, None)
          and pre_got[(2, 9)] == (VALUE, 42))


def test_dependent_carry_and_damage(tmp: str) -> None:
    print("test: dependent blocks, bad prefixes and suffix recovery")
    archive = os.path.join(tmp, "dep.narc")
    catalog_path = os.path.join(tmp, "dep.json")
    with ArchiveWriter(archive) as writer:
        writer.fragment(5, "dep.raw", 0, 12)
        writer.block(IND, 6, 0, [(A, 10), (D, 5), (D, 5)])          # rows 0-3
        writer.block(DEP, 6, 3, [(D, 1), (A, 50), (D, -10)])        # rows 3-6
        writer.block(DEP, 6, 6, [(D, 2), (D, 2), (D, 2)])           # rows 6-9
        writer.block(IND, 7, 0, [(D, 1), (D, 1), (D, 1)])           # rows 0-3, no anchor
        writer.block(DEP, 7, 3, [(D, 9), (D, 9), (A, 1)])           # rows 3-6
    build_catalog(archive, catalog_path)

    result = query_window(catalog_path, 6, 9, [6])
    got = cells_to_map(result)
    check("dependent chain carries values across blocks",
          got[(6, 6)] == (VALUE, 42) and got[(8, 6)] == (VALUE, 46))
    check("tail query never reads the predecessor block bytes",
          all(span.offset >= load_catalog(catalog_path)["_blocks"][2].offset
              for span in result.spans))

    result = query_window(catalog_path, 3, 6, [7])
    got = cells_to_map(result)
    check("dependent block without trusted carry: prefix fails, suffix reads",
          got[(3, 7)] == (MISSING_PREFIX, None)
          and got[(4, 7)] == (MISSING_PREFIX, None)
          and got[(5, 7)] == (VALUE, 1))
    block = load_catalog(catalog_path)["_blocks"][4]
    check("prefix query reads from the absolute record onward",
          result.spans[0].offset == block.offset + 2 * fmt.RECORD_SIZE)


def test_damaged_blocks(tmp: str) -> None:
    print("test: truncation, digest mismatch and illegal rows poison whole block")
    archive = os.path.join(tmp, "bad.narc")
    catalog_path = os.path.join(tmp, "bad.json")
    with ArchiveWriter(archive) as writer:
        writer.fragment(8, "bad.raw", 0, 12)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (D, 1)])
        writer.block(DEP, 1, 3, [(D, 1), (A, 9), (D, 1)])
        writer.block(DEP, 1, 6, [(A, 5), (D, 1), (D, 1)])
        writer.block(IND, 2, 0, [(A, 1), (D, 1), (D, 1)])

    # Corrupt a payload byte inside block 2 (the dependent middle block).
    block2_payload = None
    # Recompute offsets by cataloging a pristine copy first.
    pristine = os.path.join(tmp, "pristine.narc")
    shutil.copyfile(archive, pristine)
    build_catalog(pristine, os.path.join(tmp, "pristine.json"))
    pristine_catalog = load_catalog(os.path.join(tmp, "pristine.json"))
    target = next(b for b in pristine_catalog["_blocks"]
                  if b.start_row == 3 and b.column_id == 1)
    patch_bytes(archive, target.offset + 9, b"\x7f")

    build_catalog(archive, catalog_path)
    catalog = load_catalog(catalog_path)
    damaged = [b for b in catalog["_blocks"] if b.status == "damaged"]
    check("digest-corrupt block is cataloged as damaged",
          len(damaged) == 1 and damaged[0].block_id == target.block_id)

    result = query_window(catalog_path, 0, 9, [1])
    got = cells_to_map(result)
    check("damaged block returns damaged for every row",
          all(got[(row, 1)] == (DAMAGED, None) for row in range(3, 6)))
    check("downstream block cannot borrow across the bad prefix",
          all(got[(row, 1)] == (VALUE, row - 6 + 5) for row in range(6, 9)))
    check("damaged block costs zero payload bytes at query time",
          all(span.offset != target.offset or span.length == 0
              for span in result.spans))
    check("isolated healthy column remains fully readable",
          cells_to_map(query_window(catalog_path, 0, 3, [2])) ==
          expected_window(archive, 0, 3, [2]))

    # Truncation: cut the file inside another block's payload.
    truncated = os.path.join(tmp, "trunc.narc")
    shutil.copyfile(pristine, truncated)
    last = pristine_catalog["_blocks"][-1]
    truncate(truncated, last.offset + last.length - 4)
    catalog2 = os.path.join(tmp, "trunc.json")
    build_catalog(truncated, catalog2)
    cat2 = load_catalog(catalog2)
    check("truncated block is marked damaged with a structural reason",
          any(b.status == "damaged" and "truncated" in (b.damage_reason or "")
              for b in cat2["_blocks"]))


def test_illegal_row_and_foreign_endian(tmp: str) -> None:
    print("test: illegal rows and foreign-endian rejection")
    archive = os.path.join(tmp, "illegal.narc")
    catalog_path = os.path.join(tmp, "illegal.json")
    with ArchiveWriter(archive) as writer:
        writer.fragment(9, "illegal.raw", 0, 4)
        writer.block(IND, 1, 0, [(A, 1), (D, 1), (M, 0), (D, 1)])
    block_offset = build_catalog(archive, os.path.join(tmp, "ok.json"))
    ok_catalog = load_catalog(os.path.join(tmp, "ok.json"))
    block = ok_catalog["_blocks"][0]
    # Flip the last record's tag to an unused value.
    patch_bytes(archive, block.offset + 3 * fmt.RECORD_SIZE, b"\x55")
    build_catalog(archive, catalog_path)
    damaged = load_catalog(catalog_path)["_blocks"]
    check("illegal record tag damages the entire block",
          len(damaged) == 1 and damaged[0].status == "damaged"
          and "unknown record tag" in damaged[0].damage_reason)

    foreign = os.path.join(tmp, "foreign.narc")
    with open(foreign, "wb") as handle:
        handle.write(fmt.MAGIC)
        handle.write(struct.pack(">H", 0xFFFE))
        handle.write(struct.pack(">H", fmt.FORMAT_VERSION))
    try:
        build_catalog(foreign, os.path.join(tmp, "foreign.json"))
        raised = False
    except CatalogError:
        raised = True
    check("foreign byte order is rejected at catalog time", raised)


def test_identity_binding(tmp: str) -> None:
    print("test: catalog binds immutable source identity")
    archive = os.path.join(tmp, "id.narc")
    catalog_path = os.path.join(tmp, "id.json")
    with ArchiveWriter(archive) as writer:
        writer.fragment(11, "id.raw", 0, 2)
        writer.block(IND, 1, 0, [(A, 1), (D, 1)])
    build_catalog(archive, catalog_path)

    # Same-size content mutation must be caught by full verification.
    mutated = os.path.join(tmp, "id-mut.narc")
    shutil.copyfile(archive, mutated)
    catalog = load_catalog(catalog_path)
    block = catalog["_blocks"][0]
    patch_bytes(mutated, block.offset + 1, struct.pack(">q", 2))
    try:
        from narc.catalog import verify_identity
        verify_identity(load_catalog(catalog_path), mutated, full=True)
        raised = False
    except CatalogError:
        raised = True
    check("same-size mutation fails full identity verification", raised)

    # Size change is caught even by the cheap per-query check.
    grown = os.path.join(tmp, "id-grow.narc")
    shutil.copyfile(archive, grown)
    with open(grown, "ab") as handle:
        handle.write(b"\x00")
    try:
        query_window(catalog_path, 0, 2, [1], archive_path=grown)
        raised = False
    except CatalogError:
        raised = True
    check("size change blocks queries and demands re-cataloging", raised)

    # Re-cataloging the mutated version restores service and records new hash.
    recatalog = os.path.join(tmp, "id-recat.json")
    build_catalog(mutated, recatalog)
    check("re-cataloged source carries the new identity",
          load_catalog(recatalog)["source"]["sha256"]
          == hash_file(mutated)[0])


def test_projection_and_read_boundaries(tmp: str) -> None:
    print("test: column projection and strict read-range enforcement")
    archive = os.path.join(tmp, "proj.narc")
    catalog_path = os.path.join(tmp, "proj.json")
    with ArchiveWriter(archive) as writer:
        writer.fragment(12, "proj.raw", 0, 6)
        writer.block(IND, 1, 0, [(A, i) for i in range(6)])
        writer.block(IND, 2, 0, [(A, 100 + i) for i in range(6)])
        writer.block(IND, 3, 0, [(A, 1000 + i) for i in range(6)])
    build_catalog(archive, catalog_path)
    catalog = load_catalog(catalog_path)
    ranges = {
        block.column_id: (block.offset, block.end_offset)
        for block in catalog["_blocks"]
    }

    result = query_window(catalog_path, 2, 5, [2])
    check("projection returns only requested columns",
          {cell.column_id for cell in result.cells} == {2})
    check("every physical read stays inside the selected column payload",
          all(ranges[2][0] <= span.offset and span.end <= ranges[2][1]
              for span in result.spans))
    other_columns = [ranges[col] for col in ranges if col != 2]
    check("no byte from unrelated columns is read",
          all(
              span.end <= start or span.offset >= end
              for span in result.spans
              for start, end in other_columns
          ))
    check("narrow read covers only three records",
          result.bytes_read == 3 * fmt.RECORD_SIZE)

    # Repeated small queries must not grow into head scans: run twice and
    # confirm identical, bounded byte counts.
    again = query_window(catalog_path, 2, 5, [2])
    check("repeated narrow queries have identical bounded read cost",
          again.bytes_read == result.bytes_read
          and again.bytes_read < os.path.getsize(archive) // 2)

    # Half-open boundary semantics.
    edge = query_window(catalog_path, 5, 6, [1, 3])
    got = cells_to_map(edge)
    check("half-open window includes start row and excludes end row",
          set(got) == {(5, 1), (5, 3)})


def test_unbounded_int64_arithmetic(tmp: str) -> None:
    print("test: signed 64-bit reconstruction is exact and unbounded after decode")
    archive = os.path.join(tmp, "int64.narc")
    catalog_path = os.path.join(tmp, "int64.json")
    big = 2 ** 63 - 1
    with ArchiveWriter(archive) as writer:
        writer.fragment(13, "int64.raw", 0, 5)
        writer.block(
            IND, 1, 0,
            [(A, big), (D, 1), (D, -2), (A, -(2 ** 63)), (D, 2 ** 62)],
        )
    build_catalog(archive, catalog_path)
    got = cells_to_map(query_window(catalog_path, 0, 5, [1]))
    check("post-decode arithmetic keeps Python-precise values",
          got[(1, 1)] == (VALUE, big + 1)
          and got[(2, 1)] == (VALUE, big - 1)
          and got[(4, 1)] == (VALUE, -(2 ** 63) + 2 ** 62))


def test_reader_range_enforcement(tmp: str) -> None:
    print("test: tracked reader enforces allowed ranges at the OS boundary")
    from narc.io_utils import TrackedReader

    path = os.path.join(tmp, "bounded.bin")
    with open(path, "wb") as handle:
        handle.write(b"x" * 100)
    with TrackedReader(path, allowed=[(20, 30)]) as reader:
        try:
            reader.read(0, 5)
            raised = False
        except PermissionError:
            raised = True
        check("reads from the file head are rejected", raised)
        try:
            reader.read(29, 2)
            raised = False
        except PermissionError:
            raised = True
        check("reads crossing an allowed-range boundary are rejected", raised)
        data = reader.read(20, 10)
        check("allowed narrow read succeeds and is accounted",
              len(data) == 10 and reader.ledger.bytes_read == 10)
        reader.ledger.assert_within([(20, 30)])
        check("ledger boundary assertion accepts the recorded spans", True)


def test_checkpoint_stride_boundary(tmp: str) -> None:
    print("test: windows around checkpoint boundaries stay bounded")
    archive = os.path.join(tmp, "stride.narc")
    catalog_path = os.path.join(tmp, "stride.json")
    n = fmt.CHECKPOINT_STRIDE * 3 + 7
    records = [(A, 11)] + [(D, 1) for _ in range(n - 1)]
    with ArchiveWriter(archive) as writer:
        writer.fragment(21, "stride.raw", 0, n)
        writer.block(IND, 5, 0, records)
    build_catalog(archive, catalog_path)
    catalog = load_catalog(catalog_path)
    block = catalog["_blocks"][0]
    probe_rows = [
        fmt.CHECKPOINT_STRIDE - 1,
        fmt.CHECKPOINT_STRIDE,
        fmt.CHECKPOINT_STRIDE + 1,
        fmt.CHECKPOINT_STRIDE * 2,
        n - 1,
    ]
    for row in probe_rows:
        result = query_window(catalog_path, row, row + 1, [5])
        got = cells_to_map(result)
        check(f"row {row} reconstructs exact value via checkpoints",
              got[(row, 5)] == (VALUE, 11 + row))
        check(f"row {row} read stays inside the single target block",
              len(result.spans) == 1
              and result.spans[0].offset >= block.offset
              and result.spans[0].end <= block.end_offset)
        check(
            f"row {row} read is bounded by one stride plus one record",
            result.bytes_read
            <= (fmt.CHECKPOINT_STRIDE + 1) * fmt.RECORD_SIZE,
            f"bytes={result.bytes_read}",
        )


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="narc-test-")
    print(f"scratch directory: {tmp}")
    for test in (
        test_differential,
        test_long_delta_chain,
        test_mid_block_absolute,
        test_dependent_carry_and_damage,
        test_damaged_blocks,
        test_illegal_row_and_foreign_endian,
        test_identity_binding,
        test_projection_and_read_boundaries,
        test_unbounded_int64_arithmetic,
        test_reader_range_enforcement,
        test_checkpoint_stride_boundary,
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
