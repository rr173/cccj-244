"""Narrow-window point queries over a catalogued archive.

The reader only ever opens the source file with ranges restricted to the
payloads of the blocks the query actually intersects.  Anchoring uses block
entry values and sparse checkpoints, so no query ever walks from the file
head or the column head.
"""

from __future__ import annotations

import bisect
import struct
from dataclasses import dataclass

from . import format as fmt
from .catalog import Block, blocks_for, load_catalog, verify_identity
from .io_utils import ReadLedger, TrackedReader


@dataclass
class Cell:
    row: int
    column_id: int
    status: str
    value: int | None
    block_id: str | None
    detail: str | None = None

    def to_json(self) -> dict:
        return {
            "row": self.row,
            "column_id": self.column_id,
            "status": self.status,
            "value": self.value,
            "block_id": self.block_id,
            "detail": self.detail,
        }


@dataclass
class QueryResult:
    start_row: int
    end_row: int
    columns: list[int]
    cells: list[Cell]
    bytes_read: int
    spans: list
    blocks_touched: list[str]

    def to_json(self) -> dict:
        return {
            "start_row": self.start_row,
            "end_row": self.end_row,
            "columns": self.columns,
            "cells": [cell.to_json() for cell in self.cells],
            "io": {
                "bytes_read": self.bytes_read,
                "spans": [[span.offset, span.end] for span in self.spans],
                "blocks_touched": self.blocks_touched,
            },
        }


def _intersecting_blocks(
    chain: list[Block], start_row: int, end_row: int
) -> list[Block]:
    starts = [block.start_row for block in chain]
    index = bisect.bisect_right(starts, start_row) - 1
    selected: list[Block] = []
    cursor = max(index, 0)
    while cursor < len(chain):
        block = chain[cursor]
        if block.start_row >= end_row:
            break
        if block.end_row > start_row:
            selected.append(block)
        cursor += 1
    return selected


def _read_records(
    reader: TrackedReader,
    block: Block,
    begin_local: int,
    end_local: int,
) -> list[tuple[int, int]]:
    """Read exactly the record slice [begin_local, end_local)."""
    if end_local <= begin_local:
        return []
    offset = block.offset + begin_local * fmt.RECORD_SIZE
    length = (end_local - begin_local) * fmt.RECORD_SIZE
    raw = reader.read(offset, length)
    records: list[tuple[int, int]] = []
    for index in range(0, length, fmt.RECORD_SIZE):
        records.append(
            (raw[index], struct.unpack(">q", raw[index + 1:index + 9])[0])
        )
    return records


def _anchor_from_checkpoint(
    block: Block, local_begin: int
) -> tuple[int, int, int]:
    """Return (start_local, running_value, anchor_local).

    A checkpoint stores the reconstructed value *at* that local position, so
    the anchor record itself must be emitted from the stored value without
    applying its delta again.
    """
    candidates = [index for index in block.checkpoints if index <= local_begin]
    if candidates:
        anchor = max(candidates)
        return anchor, block.checkpoints[anchor], anchor
    if (
        block.first_absolute_local is not None
        and block.first_absolute_local <= local_begin
    ):
        anchor = block.first_absolute_local
        return anchor, block.checkpoints[anchor], anchor
    raise RuntimeError(f"block {block.block_id} has no usable anchor")


def _resolve_block_cells(
    reader: TrackedReader,
    block: Block,
    window_start: int,
    window_end: int,
) -> list[Cell]:
    """Decode the rows of one readable block inside the query window."""
    local_begin = max(window_start, block.start_row) - block.start_row
    local_end = min(window_end, block.end_row) - block.start_row
    first_abs = block.first_absolute_local

    # Phase 1: choose the earliest record to physically read and the trusted
    # value in effect just before it.  Checkpoints and carry-in values replace
    # any scan from the block or column head.
    start_local = local_begin
    running: int | None = None
    anchor_local: int | None = None  # position whose value is stored directly

    if block.kind == fmt.KIND_INDEPENDENT:
        if first_abs is None:
            pass  # no anchor exists; deltas are unresolvable, missings stay
        elif first_abs <= local_begin:
            start_local, running, anchor_local = _anchor_from_checkpoint(
                block, local_begin
            )
        else:
            # Read from the absolute onward; prefix rows are synthesized from
            # catalogued prefix indices so no block-head scan is needed.
            start_local = first_abs
    else:  # dependent block
        if first_abs is None:
            running = block.carry_in  # may be None: deltas are unresolvable
        elif first_abs <= local_begin:
            start_local, running, anchor_local = _anchor_from_checkpoint(
                block, local_begin
            )
        elif block.carry_in is not None:
            # The carry value is in effect *before* local 0.  Read from one
            # record before the window (or local 0) so the first in-window
            # delta is applied on top of the correct reconstructed value.
            start_local = max(local_begin - 1, 0)
            running = block.carry_in
        else:
            # No trusted carry and the window precedes the in-block absolute.
            # Read from the absolute onward; prefix rows are synthesized from
            # the catalogued anchor position without touching block-head bytes.
            start_local = first_abs

    raw_records = _read_records(reader, block, start_local, local_end)

    cells: list[Cell] = []
    # Prefix rows skipped physically: explicit missings stay missing, every
    # other record (a delta without a trusted prior value) is missing_prefix.
    if start_local > local_begin:
        missing_set = set(block.prefix_missing_locals)
        for local in range(local_begin, start_local):
            row = block.start_row + local
            if local in missing_set:
                cells.append(
                    Cell(row, block.column_id, fmt.STATUS_MISSING, None,
                         block.block_id)
                )
            else:
                cells.append(
                    Cell(
                        row, block.column_id, fmt.STATUS_MISSING_PREFIX, None,
                        block.block_id,
                        "delta has no trusted prior value in scope",
                    )
                )

    # Phase 3: classify every record that belongs to the window.  When the
    # first read record is a delta reconstructed from a checkpoint, emit the
    # stored anchor value directly instead of applying its delta again.  An
    # absolute record at the anchor position is handled by its normal branch.
    first_slot_is_delta_anchor = (
        anchor_local is not None
        and raw_records
        and raw_records[0][0] == fmt.REC_DELTA
    )

    for slot, (tag, value) in enumerate(raw_records):
        local = start_local + slot
        row = block.start_row + local
        in_window = local >= local_begin

        if (
            first_slot_is_delta_anchor
            and slot == 0
            and anchor_local == local
        ):
            if in_window:
                cells.append(
                    Cell(row, block.column_id, fmt.STATUS_VALUE, running,
                         block.block_id)
                )
            continue

        if tag == fmt.REC_MISSING:
            # An explicit missing observation is reported as missing even
            # before any trusted absolute exists; it never changes running.
            if in_window:
                cells.append(
                    Cell(row, block.column_id, fmt.STATUS_MISSING, None,
                         block.block_id)
                )
        elif tag == fmt.REC_ABSOLUTE:
            running = value
            if in_window:
                cells.append(
                    Cell(row, block.column_id, fmt.STATUS_VALUE, value,
                         block.block_id)
                )
        elif tag == fmt.REC_DELTA:
            if running is None:
                if in_window:
                    cells.append(
                        Cell(
                            row, block.column_id, fmt.STATUS_MISSING_PREFIX,
                            None, block.block_id,
                            "delta has no trusted prior value in scope",
                        )
                    )
            else:
                running += value
                if in_window:
                    cells.append(
                        Cell(row, block.column_id, fmt.STATUS_VALUE, running,
                             block.block_id)
                    )
        else:
            if in_window:
                cells.append(
                    Cell(row, block.column_id, fmt.STATUS_DAMAGED, None,
                         block.block_id, "illegal record tag at query time")
                )
    return cells


def query_window(
    catalog_path: str,
    start_row: int,
    end_row: int,
    columns: list[int],
    *,
    full_verify: bool = False,
    archive_path: str | None = None,
) -> QueryResult:
    """Return exact per-position results for rows [start_row, end_row)."""
    if end_row <= start_row:
        raise ValueError("query window must be a non-empty half-open range")
    columns = sorted(dict.fromkeys(columns))
    if not columns:
        raise ValueError("at least one column is required")

    catalog = load_catalog(catalog_path)
    source_path = archive_path or catalog["source"]["path"]
    verify_identity(catalog, source_path, full=full_verify)

    ledger = ReadLedger()
    cells_by_column: dict[int, list[Cell]] = {column: [] for column in columns}
    touched: list[str] = []
    allowed: list[tuple[int, int]] = []
    plans: list[tuple[Block, int, int]] = []

    for fragment in catalog["fragments"]:
        fragment_id = fragment["fragment_id"]
        if fragment["end_row"] <= start_row or fragment["start_row"] >= end_row:
            continue
        for column in columns:
            chain = blocks_for(catalog, fragment_id, column)
            for block in _intersecting_blocks(chain, start_row, end_row):
                touched.append(block.block_id)
                if block.status == "readable":
                    allowed.append((block.offset, block.end_offset))
                    plans.append((block, start_row, end_row))
                else:
                    rows = range(
                        max(start_row, block.start_row),
                        min(end_row, block.end_row),
                    )
                    cells_by_column[column].extend(
                        Cell(
                            row, column, fmt.STATUS_DAMAGED, None,
                            block.block_id, block.damage_reason,
                        )
                        for row in rows
                    )

    if allowed:
        with TrackedReader(source_path, allowed=allowed, ledger=ledger) as reader:
            for block, window_start, window_end in plans:
                cells_by_column[block.column_id].extend(
                    _resolve_block_cells(reader, block, window_start, window_end)
                )

    all_cells: list[Cell] = []
    for column in columns:
        produced = {cell.row: cell for cell in cells_by_column[column]}
        for row in range(start_row, end_row):
            all_cells.append(
                produced.get(
                    row,
                    Cell(
                        row, column, fmt.STATUS_UNCOVERED, None, None,
                        "no block covers this row for the requested column",
                    ),
                )
            )

    ledger.assert_within(allowed)
    return QueryResult(
        start_row=start_row,
        end_row=end_row,
        columns=columns,
        cells=all_cells,
        bytes_read=ledger.bytes_read,
        spans=ledger.spans,
        blocks_touched=touched,
    )
