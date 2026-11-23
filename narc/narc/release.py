"""Offline release publishing for a frozen v1 instrument archive.

Given an immutable v1 archive, a JSON authorization slip and a delivery
directory that does not exist yet, ``release_archive`` publishes a batch of
standalone v1 files plus ``parts.json``.  The original archive, the v1 wire
format and the existing catalog/query contracts are never modified.

Security properties of every published part:

* it carries only the authorized fragment id and the requested columns;
* the fragment source name is uniformly ``release`` -- the original source
  name and path never leave the machine;
* records exist only for authorized *covered* positions.  Explicit missings
  stay missings and uncovered positions stay uncovered (no block owns them,
  so they can never be mistaken for missing observations);
* every block is independent and its first in-band value is an absolute
  anchor taken from an authorized sample, so no private predecessor value is
  embedded.  Pre-context from the original archive is used only to compute
  reconstructed values in memory.

Planning is event based: work is proportional to the authorized ranges, the
requested columns and the number of covered records, never to the (possibly
hundreds of millions of) empty sample coordinates.  The partition runs in
O(N log N): per-segment jump tables plus monotone two-pointer sweeps over
the global boundary list.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import os
import shutil
import struct
import tempfile
from dataclasses import dataclass, field

from . import format as fmt
from .catalog import Block, build_catalog, blocks_for, load_catalog
from .io_utils import TrackedReader

INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
UINT16_MAX = (1 << 16) - 1
UINT32_MAX = (1 << 32) - 1

RELEASE_SOURCE_NAME = "release"
PARTS_MANIFEST = "parts.json"
_PART_NAME_WIDTH = 5

# Fixed wire cost of a part regardless of its records:
# file header (12) + fragment header (28) + end-of-archive marker (2).
_PART_FIXED_COST = (
    fmt.FILE_HEADER_SIZE + fmt.FRAGMENT_HEADER_SIZE + fmt.END_OF_ARCHIVE_SIZE
)


class ReleaseError(ValueError):
    """Raised for invalid input, an infeasible plan or a refused order."""


# ---------------------------------------------------------------------------
# Authorization slip
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Authorization:
    fragment_id: int
    columns: tuple[int, ...]            # de-duplicated, sorted, v1 column ids
    ranges: tuple[tuple[int, int], ...]  # merged, sorted, half-open
    max_bytes: int


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int; an authorization flag is not a coordinate.
    return isinstance(value, int) and not isinstance(value, bool)


def parse_authorization(data: object) -> Authorization:
    """Validate the JSON authorization slip and normalize its contents."""
    if not isinstance(data, dict):
        raise ReleaseError("authorization must be a JSON object")
    expected = {"fragment_id", "columns", "ranges", "max_bytes"}
    if set(data) != expected:
        raise ReleaseError(
            f"authorization must contain exactly {sorted(expected)}"
        )

    fragment_id = data["fragment_id"]
    if not _is_plain_int(fragment_id) or not (0 <= fragment_id <= UINT16_MAX):
        raise ReleaseError("fragment_id must be a uint16 integer")

    columns_raw = data["columns"]
    if not isinstance(columns_raw, list) or not columns_raw:
        raise ReleaseError("columns must be a non-empty list")
    columns: set[int] = set()
    for column in columns_raw:
        if not _is_plain_int(column) or not (0 <= column <= UINT16_MAX):
            raise ReleaseError("column ids must be uint16 integers")
        columns.add(column)

    ranges_raw = data["ranges"]
    if not isinstance(ranges_raw, list) or not ranges_raw:
        raise ReleaseError("ranges must be a non-empty list")
    parsed_ranges: list[tuple[int, int]] = []
    for item in ranges_raw:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not all(_is_plain_int(part) for part in item)
        ):
            raise ReleaseError("each range must be a [start, end] pair")
        start, end = item
        if not (0 <= start < end <= UINT32_MAX + 1):
            raise ReleaseError(
                "range bounds must satisfy 0 <= start < end <= 2^32"
            )
        parsed_ranges.append((start, end))
    parsed_ranges.sort()

    # Overlapping or adjacent ranges are one and the same authorization.
    merged: list[tuple[int, int]] = []
    for start, end in parsed_ranges:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    max_bytes = data["max_bytes"]
    if not _is_plain_int(max_bytes) or not (42 <= max_bytes <= UINT32_MAX):
        raise ReleaseError("max_bytes must be an integer in [42, 2^32-1]")

    return Authorization(
        fragment_id=fragment_id,
        columns=tuple(sorted(columns)),
        ranges=tuple(merged),
        max_bytes=max_bytes,
    )


def load_authorization(path: str) -> Authorization:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseError(f"cannot read authorization {path}: {exc}") from exc
    return parse_authorization(data)


# ---------------------------------------------------------------------------
# Decoded covered segments
# ---------------------------------------------------------------------------

@dataclass
class Segment:
    """One maximal run of consecutive covered rows for one column.

    The parallel lists hold one entry per covered row, in sample order, so
    their length equals ``end_row - start_row`` (there are no gaps inside a
    segment).
    """

    column_id: int
    start_row: int
    end_row: int
    tags: list[int] = field(default_factory=list)
    wire_values: list[int] = field(default_factory=list)
    recon: list[int | None] = field(default_factory=list)
    is_value: list[bool] = field(default_factory=list)
    # Rows at which a fresh independent block may start: value positions
    # whose exact reconstructed value still fits a signed int64 payload.
    anchor_rows: list[int] = field(default_factory=list)

    def __len__(self) -> int:
        return self.end_row - self.start_row

    def local(self, row: int) -> int:
        return row - self.start_row


def _intersect_ranges(
    span: tuple[int, int], ranges: tuple[tuple[int, int], ...]
) -> list[tuple[int, int]]:
    """Clip one block span against the authorized domain."""
    out: list[tuple[int, int]] = []
    lo, hi = span
    for start, end in ranges:
        if start >= hi:
            break
        if end <= lo:
            continue
        out.append((max(lo, start), min(hi, end)))
    return out


def _build_segments(
    catalog: dict,
    archive_path: str,
    auth: Authorization,
) -> dict[int, list[Segment]]:
    """Decode covered positions and group them into per-column segments.

    Reads are restricted to the payloads of the readable blocks intersecting
    the authorization; damaged blocks cost zero payload bytes and refuse the
    whole order.  Records outside the authorization are decoded in memory
    only (to maintain the running value) and never retained.
    """
    blocks_by_column: dict[int, list[Block]] = {
        column: blocks_for(catalog, auth.fragment_id, column)
        for column in auth.columns
    }

    allowed: list[tuple[int, int]] = []
    plans: list[tuple[Block, int]] = []
    for column, chain in blocks_by_column.items():
        for block in chain:
            pieces = _intersect_ranges(
                (block.start_row, block.end_row), auth.ranges
            )
            if not pieces:
                continue
            if block.status == "damaged":
                raise ReleaseError(
                    f"authorized position in damaged block {block.block_id} "
                    f"(column {column}); the whole order is refused"
                )
            allowed.append((block.offset, block.end_offset))
            plans.append((block, column))

    # rows_out[column] = (row, tag, wire_payload, recon, is_value)
    rows_out: dict[int, list[tuple[int, int, int, int | None, bool]]] = {
        column: [] for column in auth.columns
    }

    if plans:
        with TrackedReader(archive_path, allowed=allowed) as reader:
            for block, column in plans:
                payload = reader.read(block.offset, block.length)
                running = (
                    block.carry_in
                    if block.kind == fmt.KIND_DEPENDENT
                    else None
                )
                count = block.end_row - block.start_row
                wanted: set[int] = set()
                for start, end in _intersect_ranges(
                    (block.start_row, block.end_row), auth.ranges
                ):
                    wanted.update(range(start, end))
                for local in range(count):
                    row = block.start_row + local
                    base = local * fmt.RECORD_SIZE
                    tag = payload[base]
                    wire = struct.unpack(
                        ">q", payload[base + 1:base + 9]
                    )[0]
                    retain = row in wanted
                    is_value = False
                    if tag == fmt.REC_MISSING:
                        pass  # explicit missing never changes the running value
                    elif tag == fmt.REC_ABSOLUTE:
                        running = wire
                        is_value = True
                    elif tag == fmt.REC_DELTA:
                        if running is None:
                            if retain:
                                raise ReleaseError(
                                    f"authorized row {row} column {column} "
                                    "has no trusted predecessor; the whole "
                                    "order is refused"
                                )
                        else:
                            running += wire
                            is_value = True
                    else:  # pragma: no cover - cataloging poisons such blocks
                        raise ReleaseError(
                            f"illegal record tag in block {block.block_id}"
                        )
                    if retain:
                        rows_out[column].append(
                            (row, tag, wire,
                             running if is_value else None, is_value)
                        )

    segments_by_column: dict[int, list[Segment]] = {}
    for column in auth.columns:
        segments: list[Segment] = []
        current: Segment | None = None
        for row, tag, wire, recon_value, is_value in rows_out[column]:
            if current is None or row != current.end_row:
                current = Segment(
                    column_id=column, start_row=row, end_row=row
                )
                segments.append(current)
            current.tags.append(tag)
            current.wire_values.append(wire)
            current.recon.append(recon_value if is_value else None)
            current.is_value.append(is_value)
            if is_value and INT64_MIN <= recon_value <= INT64_MAX:
                current.anchor_rows.append(row)
            current.end_row = row + 1
        segments_by_column[column] = segments
    return segments_by_column


# ---------------------------------------------------------------------------
# Per-segment block feasibility and jump tables
# ---------------------------------------------------------------------------

@dataclass
class SegmentPlan:
    segment: Segment
    max_records: int
    # cut[i] = index of the furthest anchor row that may start the block
    # following a block whose own start is anchor index i.
    cut: list[int]
    # Binary-lifting levels over ``cut``; jumps[k][i] = cut applied 2**k times.
    jumps: list[list[int]]

    def _furthest_anchor(self, start_row: int) -> int:
        """Anchor index of the next greedy cut from a block start row."""
        index = bisect.bisect_right(
            self.segment.anchor_rows, start_row + self.max_records
        ) - 1
        if index < 0:  # pragma: no cover - feasibility prechecked
            raise ReleaseError("internal error: no reachable anchor")
        return index

    def block_count(self, start_row: int, end_row: int) -> int:
        """Minimum independent blocks covering [start_row, end_row).

        ``start_row`` must be the segment start or an anchor row and the
        slice must be feasible (validated up front for the whole segment).
        Runs in O(log N) via the jump table.
        """
        if end_row <= start_row:
            return 0
        if end_row - start_row <= self.max_records:
            return 1

        anchors = self.segment.anchor_rows
        # First greedy cut (works uniformly for the segment start, which
        # need not itself be an anchor row).
        index = self._furthest_anchor(start_row)
        cuts = 1
        # Take the largest power-of-two jumps while the landing point still
        # requires another block afterwards.  Fixed points (the last anchor
        # cuts to itself) never advance and must not be counted.
        for level in range(len(self.jumps) - 1, -1, -1):
            target = self.jumps[level][index]
            if (
                target > index
                and end_row - anchors[target] > self.max_records
            ):
                index = target
                cuts += 1 << level
        # One final single cut if the finishing block does not fit yet.
        if end_row - anchors[index] > self.max_records:
            target = self.cut[index]
            if target <= index:  # pragma: no cover - feasibility prechecked
                raise ReleaseError("internal error: segment cannot be split")
            index = target
            cuts += 1
        return cuts + 1  # the finishing block


def _build_segment_plans(
    segments_by_column: dict[int, list[Segment]], max_bytes: int
) -> dict[int, list[SegmentPlan]]:
    max_records = (max_bytes - fmt.BLOCK_HEADER_SIZE) // fmt.RECORD_SIZE
    plans: dict[int, list[SegmentPlan]] = {}
    for column, segments in segments_by_column.items():
        column_plans: list[SegmentPlan] = []
        for seg in segments:
            # A segment made solely of explicit missings needs no anchor at
            # all: every record is a zero-payload missing, the block stays
            # independent and decodes on its own.
            if not seg.anchor_rows:
                if any(seg.is_value):
                    # Value records exist but none is int64-representable, so
                    # no in-band anchor can start an independent block.
                    raise ReleaseError(
                        f"column {seg.column_id} rows "
                        f"[{seg.start_row},{seg.end_row}) contain value "
                        "records but none can anchor an independent block; "
                        "the whole order is refused"
                    )
                if len(seg) > max_records:
                    raise ReleaseError(
                        "no compliant split: the all-missing column "
                        f"{seg.column_id} segment exceeds the "
                        f"{max_bytes}-byte cap"
                    )
                column_plans.append(
                    SegmentPlan(
                        segment=seg, max_records=max_records,
                        cut=[], jumps=[[]],
                    )
                )
                continue
            # Every value record before the first eligible anchor would need
            # a private predecessor outside the authorized data: such an
            # order has no compliant encoding and is refused rather than
            # turning those positions into missing/missing_prefix outputs.
            first_value_local = seg.is_value.index(True)
            first_value_row = seg.start_row + first_value_local
            if seg.anchor_rows[0] != first_value_row:
                raise ReleaseError(
                    f"column {seg.column_id} row {first_value_row} cannot be "
                    "anchored inside the authorization (its exact value "
                    "overflows int64 and no earlier in-band anchor exists); "
                    "the whole order is refused"
                )
            # The first block must reach the first anchor ...
            if seg.anchor_rows[0] - seg.start_row + 1 > max_records:
                raise ReleaseError(
                    "no compliant split: the first anchor of column "
                    f"{seg.column_id} is too far from row {seg.start_row} "
                    f"for the {max_bytes}-byte cap"
                )
            # ... every later anchor must be reachable from its predecessor ...
            for prev, nxt in zip(seg.anchor_rows, seg.anchor_rows[1:]):
                if nxt - prev > max_records:
                    raise ReleaseError(
                        "no compliant split: anchor gap of column "
                        f"{seg.column_id} around row {nxt} exceeds the "
                        f"{max_bytes}-byte cap"
                    )
            # ... and the tail after the last anchor must fit one block.
            if seg.end_row - seg.anchor_rows[-1] > max_records:
                raise ReleaseError(
                    "no compliant split: the tail of column "
                    f"{seg.column_id} after its last anchor exceeds the "
                    f"{max_bytes}-byte cap"
                )

            n = len(seg.anchor_rows)
            cut = [
                bisect.bisect_right(
                    seg.anchor_rows, seg.anchor_rows[i] + max_records
                ) - 1
                for i in range(n)
            ]
            jumps = [cut]
            while True:
                previous = jumps[-1]
                level = [previous[previous[i]] for i in range(n)]
                jumps.append(level)
                if level == previous:
                    break
            column_plans.append(
                SegmentPlan(
                    segment=seg, max_records=max_records,
                    cut=cut, jumps=jumps,
                )
            )
        plans[column] = column_plans
    return plans


# ---------------------------------------------------------------------------
# Global boundary list and incremental window costing
# ---------------------------------------------------------------------------

def _build_boundaries(
    auth_range: tuple[int, int],
    segment_plans: dict[int, list[SegmentPlan]],
) -> list[int]:
    """All rows at which a part boundary may legally be placed.

    A covered row is a legal boundary when every column covering it can
    restart there (segment start or an in-int64 anchor).  The authorized
    range edges are always legal; boundaries inside pure uncovered gaps are
    dominated by the gap's far edge and therefore omitted.
    """
    range_start, range_end = auth_range
    cover_events: dict[int, int] = {}
    legal_counts: dict[int, int] = {}
    for plans in segment_plans.values():
        for plan in plans:
            seg = plan.segment
            if seg.end_row <= range_start or seg.start_row >= range_end:
                continue
            cover_events[seg.start_row] = (
                cover_events.get(seg.start_row, 0) + 1
            )
            cover_events[seg.end_row] = cover_events.get(seg.end_row, 0) - 1
            # Every segment start and every eligible anchor row is a row at
            # which this column can restart an independent block.
            legal_rows = set(seg.anchor_rows)
            legal_rows.add(seg.start_row)
            for row in legal_rows:
                legal_counts[row] = legal_counts.get(row, 0) + 1

    event_rows = sorted(
        set(cover_events)
        | set(legal_counts)
        | {range_start, range_end}
    )
    covering = 0
    boundaries: list[int] = []
    for row in event_rows:
        covering += cover_events.get(row, 0)
        is_edge = row == range_start or row == range_end
        if covering == 0:
            if is_edge:
                boundaries.append(row)
        elif is_edge or legal_counts.get(row, 0) == covering:
            boundaries.append(row)
    if not boundaries or boundaries[0] != range_start \
            or boundaries[-1] != range_end:
        raise ReleaseError("internal error: boundary list misses range edges")
    return boundaries


class WindowCost:
    """Incremental byte cost of a sliding window over the boundary list.

    A window [B[left], B[right]) costs::

        42 + 9 * covered_records + 60 * minimum_total_blocks

    Pointers move one step at a time (in either direction) during the
    monotone two-pointer sweeps; each segment slice is recomputed only when
    a boundary actually crosses one of its edges.  The total number of
    recomputations is O(N), each O(log N) via the jump tables.
    """

    def __init__(
        self,
        boundaries: list[int],
        segment_plans: dict[int, list[SegmentPlan]],
    ) -> None:
        self.boundaries = boundaries
        self.plans = [
            plan
            for col_plans in segment_plans.values()
            for plan in col_plans
        ]
        self.plans.sort(key=lambda plan: plan.segment.start_row)
        self.left = 0
        self.right = 0
        self.records = 0
        self.blocks = 0
        # (start, end) of each plan's current slice; equal rows mean empty.
        self._slices: list[tuple[int, int]] = [
            (plan.segment.start_row, plan.segment.start_row)
            for plan in self.plans
        ]
        # For each boundary row, the plans whose span contains that row as
        # an internal edge, so a pointer step touches only relevant slices.
        self._affected: dict[int, list[int]] = {}
        for index, plan in enumerate(self.plans):
            seg = plan.segment
            begin = bisect.bisect_left(boundaries, seg.start_row)
            # Moving onto seg.end_row empties the slice, so the end row is
            # itself an internal edge that requires recomputation.
            finish = bisect.bisect_left(boundaries, seg.end_row)
            for boundary_index in range(begin + 1, finish + 1):
                self._affected.setdefault(
                    boundaries[boundary_index], []
                ).append(index)

    def _recompute(self, index: int) -> None:
        plan = self.plans[index]
        old_start, old_end = self._slices[index]
        seg = plan.segment
        start = max(self.boundaries[self.left], seg.start_row)
        end = min(self.boundaries[self.right], seg.end_row)
        if start >= end:
            start = end = seg.start_row
        self._slices[index] = (start, end)
        self.records -= max(0, old_end - old_start)
        self.records += max(0, end - start)
        self.blocks -= (
            plan.block_count(old_start, old_end)
            if old_end > old_start else 0
        )
        self.blocks += plan.block_count(start, end) if end > start else 0

    def set_window(self, left: int, right: int) -> None:
        """Move to [boundaries[left], boundaries[right]), one step at a time.

        The sweeps never need to advance the left edge while simultaneously
        rolling the right edge back, so only two interleavings occur:
        right-edge rollback followed by left-edge advance, and left-edge
        rollback followed by right-edge extension.
        """
        if left > self.left and right < self.right:
            raise ReleaseError(  # pragma: no cover - internal invariant
                "internal error: window moved in an unsupported direction"
            )
        # 1. Roll the right edge back (window shrinks on the right).
        while self.right > right:
            self.right -= 1
            for index in self._affected.get(
                self.boundaries[self.right + 1], ()
            ):
                self._recompute(index)
        # 2. Advance the left edge (window shrinks on the left).
        while self.left < left:
            self.left += 1
            for index in self._affected.get(self.boundaries[self.left], ()):
                self._recompute(index)
        # 3. Roll the left edge back (window grows on the left).
        while self.left > left:
            self.left -= 1
            for index in self._affected.get(
                self.boundaries[self.left + 1], ()
            ):
                self._recompute(index)
        # 4. Extend the right edge (window grows on the right).
        while self.right < right:
            self.right += 1
            for index in self._affected.get(self.boundaries[self.right], ()):
                self._recompute(index)

    def cost(self) -> int:
        return (
            _PART_FIXED_COST
            + fmt.RECORD_SIZE * self.records
            + fmt.BLOCK_HEADER_SIZE * self.blocks
        )


def _plan_range(
    auth_range: tuple[int, int],
    segment_plans: dict[int, list[SegmentPlan]],
    max_bytes: int,
) -> list[tuple[int, int]]:
    """Optimal part intervals for one merged authorization range."""
    boundaries = _build_boundaries(auth_range, segment_plans)
    m = len(boundaries) - 1

    # Forward sweep: R[j] = furthest boundary reachable as one part from j.
    # Reachability is monotone in both edges: shrinking a window never grows
    # its cost, and both pointers only move forward over the whole sweep.
    reach = [m] * m
    window = WindowCost(boundaries, segment_plans)
    right = 1
    for j in range(m):
        if right < j + 1:
            right = j + 1
        window.set_window(j, right)
        while right < m:
            window.set_window(j, right + 1)
            if window.cost() <= max_bytes:
                right += 1
            else:
                # Roll back the exploratory step; the window is again at
                # (j, right) for the feasibility check below.
                window.set_window(j, right)
                break
        if window.cost() > max_bytes or right <= j:
            raise ReleaseError(
                "no compliant partition: even the smallest legal part "
                "exceeds max_bytes"
            )
        reach[j] = right

    # Minimum part count per suffix (computed from the stored reach array,
    # without any further window work).
    need = [0] * (m + 1)
    for j in range(m - 1, -1, -1):
        need[j] = 1 + need[reach[j]]

    # Greedy reconstruction always takes the furthest reachable boundary,
    # which keeps the minimum count and maximizes every endpoint in sample
    # order (the lexicographically greatest endpoint sequence).
    intervals: list[tuple[int, int]] = []
    left = 0
    while left < m:
        right = reach[left]
        intervals.append((boundaries[left], boundaries[right]))
        left = right
    return intervals


def plan_parts(
    auth: Authorization,
    segment_plans: dict[int, list[SegmentPlan]],
) -> list[tuple[int, int]]:
    """Return the optimal ordered list of part half-open intervals.

    Parts never cross between merged authorization ranges, so the union of
    their intervals is exactly the authorized domain.
    """
    intervals: list[tuple[int, int]] = []
    for auth_range in auth.ranges:
        intervals.extend(
            _plan_range(auth_range, segment_plans, auth.max_bytes)
        )
    return intervals


# ---------------------------------------------------------------------------
# Part writing
# ---------------------------------------------------------------------------

def _encode_part_record(tag: int, wire_value: int) -> bytes:
    if tag == fmt.REC_MISSING:
        return bytes([fmt.REC_MISSING]) + b"\x00" * 8
    return bytes([tag]) + struct.pack(">q", wire_value)


def _partition_segment(
    plan: SegmentPlan, start_row: int, end_row: int
) -> list[tuple[int, int]]:
    """Greedy minimum block slices covering [start_row, end_row)."""
    seg = plan.segment
    if start_row >= end_row:
        return []
    slices: list[tuple[int, int]] = []
    cur = start_row
    cap = plan.max_records
    while cur < end_row:
        if end_row - cur <= cap:
            slices.append((cur, end_row))
            break
        # An anchorless segment contains only missings; feasibility already
        # proved the whole segment fits one block, so the branch above must
        # have consumed it.  Reaching here with no anchors is an internal bug.
        if not seg.anchor_rows:  # pragma: no cover - feasibility prechecked
            raise ReleaseError("internal error: anchorless segment too long")
        cut_index = plan._furthest_anchor(cur)
        cut_row = seg.anchor_rows[cut_index]
        if cut_row <= cur:  # pragma: no cover - feasibility prechecked
            raise ReleaseError("internal error: segment slice cannot split")
        slices.append((cur, cut_row))
        cur = cut_row
    return slices


def _slice_records(
    plan: SegmentPlan, start_row: int, end_row: int
) -> list[bytes]:
    """Wire records for one block slice, with an absolute anchor up front."""
    seg = plan.segment
    lo = seg.local(start_row)
    hi = seg.local(end_row)
    anchor_done = not any(seg.is_value[lo:hi])  # all-missing needs no anchor
    out: list[bytes] = []
    for local in range(lo, hi):
        tag = seg.tags[local]
        wire = seg.wire_values[local]
        if not anchor_done and seg.is_value[local]:
            # The first in-band value becomes the absolute anchor carrying
            # the exact reconstructed value.  Block starts are segment
            # starts or anchor rows, so this value is guaranteed to fit a
            # signed int64 payload.
            value = seg.recon[local]
            out.append(
                bytes([fmt.REC_ABSOLUTE]) + struct.pack(">q", value)
            )
            anchor_done = True
        else:
            out.append(_encode_part_record(tag, wire))
    if not anchor_done:  # pragma: no cover - feasibility prechecked
        raise ReleaseError("internal error: block slice without anchor")
    return out


def _write_part(
    path: str,
    auth: Authorization,
    interval: tuple[int, int],
    segment_plans: dict[int, list[SegmentPlan]],
) -> dict:
    start_row, end_row = interval
    block_count = 0
    record_count = 0
    with open(path, "wb") as handle:
        handle.write(
            struct.pack(
                fmt.F_FILE_HEADER, fmt.MAGIC, fmt.BYTE_ORDER_MARK,
                fmt.FORMAT_VERSION,
            )
        )
        name = RELEASE_SOURCE_NAME.encode("utf-8")
        handle.write(
            struct.pack(
                fmt.F_FRAGMENT_HEADER,
                fmt.FRAGMENT_TAG, 0, auth.fragment_id,
                name.ljust(16, b"\x00"), start_row, end_row,
            )
        )
        for column in auth.columns:
            for plan in segment_plans.get(column, []):
                seg = plan.segment
                if seg.end_row <= start_row or seg.start_row >= end_row:
                    continue
                slice_start = max(start_row, seg.start_row)
                slice_end = min(end_row, seg.end_row)
                for block_start, block_end in _partition_segment(
                    plan, slice_start, slice_end
                ):
                    records = _slice_records(plan, block_start, block_end)
                    payload = b"".join(records)
                    digest = hashlib.sha256(payload).digest()
                    payload_offset = handle.tell() + fmt.BLOCK_HEADER_SIZE
                    handle.write(
                        struct.pack(
                            fmt.F_BLOCK_HEADER,
                            fmt.BLOCK_TAG, fmt.KIND_INDEPENDENT, column,
                            len(records), block_start, block_end,
                            payload_offset, len(payload), digest, 0,
                        )
                    )
                    handle.write(payload)
                    block_count += 1
                    record_count += len(records)
        handle.write(
            struct.pack(fmt.F_END_OF_ARCHIVE, fmt.END_OF_ARCHIVE, 0)
        )

    actual_size = os.path.getsize(path)
    expected_size = (
        _PART_FIXED_COST
        + fmt.BLOCK_HEADER_SIZE * block_count
        + fmt.RECORD_SIZE * record_count
    )
    if actual_size != expected_size:  # pragma: no cover - defensive
        raise ReleaseError(
            f"part size mismatch: wrote {actual_size}, planned {expected_size}"
        )
    if actual_size > auth.max_bytes:
        raise ReleaseError(
            f"part {path} is {actual_size} bytes, exceeding {auth.max_bytes}"
        )
    with open(path, "rb") as handle:
        digest = hashlib.sha256()
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return {
        "file": os.path.basename(path),
        "start_row": start_row,
        "end_row": end_row,
        "bytes": actual_size,
        "sha256": digest.hexdigest(),
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def release_archive(
    archive_path: str,
    authorization_path: str,
    delivery_dir: str,
) -> dict:
    """Publish an authorized offline release.

    Returns the ``parts.json`` manifest as a dict.  On any failure the
    delivery directory is never created: all work happens in a hidden
    temporary directory next to the target, which is renamed into place
    atomically only after every part and the manifest exist.
    """
    auth = load_authorization(authorization_path)
    if os.path.exists(delivery_dir):
        raise ReleaseError(
            f"delivery directory {delivery_dir} already exists; refusing to "
            "overwrite"
        )
    parent = os.path.dirname(os.path.abspath(delivery_dir)) or "."
    if not os.path.isdir(parent):
        raise ReleaseError(f"parent directory {parent} does not exist")

    tmp_dir = tempfile.mkdtemp(prefix=".narc-release-", dir=parent)
    # The working catalog stays in a separate directory so that the renamed
    # delivery directory contains exactly the published parts plus the
    # parts.json manifest -- no internal sidecars ever leave the machine.
    work_dir = tempfile.mkdtemp(prefix=".narc-release-work-", dir=parent)
    try:
        catalog_path = os.path.join(work_dir, "source-catalog.json")
        build_catalog(archive_path, catalog_path)
        catalog = load_catalog(catalog_path)

        fragment = next(
            (item for item in catalog["fragments"]
             if item["fragment_id"] == auth.fragment_id),
            None,
        )
        if fragment is None:
            raise ReleaseError(
                f"fragment {auth.fragment_id} is not present in the archive"
            )
        for start, end in auth.ranges:
            if start < fragment["start_row"] or end > fragment["end_row"]:
                raise ReleaseError(
                    f"authorized range [{start},{end}) escapes fragment "
                    f"{auth.fragment_id} bounds "
                    f"[{fragment['start_row']},{fragment['end_row']})"
                )

        segments_by_column = _build_segments(catalog, archive_path, auth)
        segment_plans = _build_segment_plans(
            segments_by_column, auth.max_bytes
        )
        intervals = plan_parts(auth, segment_plans)

        manifest_entries: list[dict] = []
        for index, interval in enumerate(intervals):
            name = f"part-{index + 1:0{_PART_NAME_WIDTH}d}.narc"
            entry = _write_part(
                os.path.join(tmp_dir, name), auth, interval, segment_plans
            )
            manifest_entries.append(entry)

        manifest = {"parts": manifest_entries}
        with open(
            os.path.join(tmp_dir, PARTS_MANIFEST), "w", encoding="utf-8"
        ) as handle:
            json.dump(manifest, handle, indent=2, ensure_ascii=False)
            handle.write("\n")

        os.rename(tmp_dir, delivery_dir)
        return manifest
    except BaseException:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
