"""Offline reconciliation of two frozen v1 archives against an alignment sheet.

One observation run is archived twice (a primary and a backup copy).  After a
device restart the fragment numbering may restart and a channel may move to a
different column, so diffing by original row number pairs the wrong samples,
while trusting the first copy blindly hides contradictions *inside* one copy.
``reconcile`` aligns both archives onto logical ``(channel, sample)`` keys
through an explicit alignment sheet and emits the differences a duty officer
must review, with coordinates that trace back into either original archive.

Semantics (all decoding follows the v1 rules and the reference decoder):

* the original sample ``row`` of a segment maps to the logical key
  ``(channel, global_start + row - start)``;
* only trusted values are observations: explicit missings, uncovered
  positions, damaged blocks and deltas without a trusted predecessor
  (``missing_prefix``) never count.  Records outside the aligned intervals
  never appear in the sheet, but they may still be the trusted predecessors
  that resolve an in-interval delta (an earlier absolute in the same block,
  or the value a dependent chain carries in from its decisive predecessor).
  An isolated damaged block contributes nothing, and a dependent suffix
  after it is recovered only from its own absolute records, exactly as v1
  prescribes;
* values are compared as the exact reconstructed integers, never as raw
  deltas or floats;
* one logical key may carry several observations per side (overlapping
  logical ranges are multiple captures).  Repeating the same value on one
  side is not a contradiction; two or more distinct trusted values on one
  side make that side ``ambiguous`` even when the other side shows the same
  value set.

Resource contract: the alignment sheet and the per-archive block-header
index may live in memory (or in the temporary work directory).  Observation
decoding streams records in bounded chunks, the per-channel merge holds one
entry per mapping segment, the merge join holds one logical key per side,
and the sheet is written as a stream -- so working memory never grows with
a single block payload, the total observation count or the number of
difference lines, and no work is ever proportional to empty coordinate
spans.  The output path must not exist; the sheet is staged in a hidden
temporary directory next to the target and committed with a no-clobber
link, so invalid input, an uninterpretable archive or an I/O failure never
leaves a formal differences sheet behind.
"""

from __future__ import annotations

import bisect
import heapq
import json
import os
import shutil
import struct
import tempfile
from dataclasses import dataclass

from . import format as fmt
from .catalog import (
    Block,
    CatalogError,
    build_catalog,
    load_catalog,
)
from .io_utils import TrackedReader

UINT16_MAX = (1 << 16) - 1
UINT32_MAX = (1 << 32) - 1

# Records are decoded in bounded slices so one huge block payload never
# materializes in memory.  16384 records = 144 KiB per slice.
_DECODE_CHUNK_RECORDS = 16384


class ReconcileError(ValueError):
    """Invalid alignment sheet, unusable archive, or unsafe output."""


# ---------------------------------------------------------------------------
# Alignment sheet
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SegmentMap:
    """One mapping: archive rows [local_start, local_end) of one
    fragment/column onto logical samples starting at ``global_start`` of one
    channel."""

    fragment_id: int
    column_id: int
    local_start: int
    local_end: int
    channel: str
    global_start: int

    def sample_of(self, row: int) -> int:
        return self.global_start + row - self.local_start


@dataclass(frozen=True)
class Side:
    archive: str
    segments: tuple[SegmentMap, ...]


@dataclass(frozen=True)
class Alignment:
    left: Side
    right: Side


def _is_plain_int(value: object) -> bool:
    # bool is an int subclass but never a coordinate or an identifier.
    return isinstance(value, int) and not isinstance(value, bool)


_SEGMENT_KEYS = {"fragment_id", "column_id", "local", "channel", "global_start"}
_SIDE_KEYS = {"archive", "segments"}
_ALIGNMENT_KEYS = {"left", "right"}


def _parse_segment(data: object, label: str, index: int) -> SegmentMap:
    where = f"{label} segment #{index}"
    if not isinstance(data, dict):
        raise ReconcileError(f"{where} must be a JSON object")
    if set(data) != _SEGMENT_KEYS:
        raise ReconcileError(
            f"{where} must contain exactly {sorted(_SEGMENT_KEYS)}"
        )

    fragment_id = data["fragment_id"]
    if not _is_plain_int(fragment_id) or not (0 <= fragment_id <= UINT16_MAX):
        raise ReconcileError(f"{where}: fragment_id must be a uint16 integer")
    column_id = data["column_id"]
    if not _is_plain_int(column_id) or not (0 <= column_id <= UINT16_MAX):
        raise ReconcileError(f"{where}: column_id must be a uint16 integer")

    local = data["local"]
    if (
        not isinstance(local, list)
        or len(local) != 2
        or not all(_is_plain_int(part) for part in local)
    ):
        raise ReconcileError(f"{where}: local must be a [start, end] pair")
    local_start, local_end = local
    if not (0 <= local_start < local_end <= UINT32_MAX + 1):
        raise ReconcileError(
            f"{where}: local range must satisfy 0 <= start < end <= 2^32"
        )

    channel = data["channel"]
    if not isinstance(channel, str) or not channel:
        raise ReconcileError(f"{where}: channel must be a non-empty string")

    global_start = data["global_start"]
    if not _is_plain_int(global_start):
        raise ReconcileError(f"{where}: global_start must be an integer")
    # The mapped half-open interval [global_start, global_start + length)
    # must stay inside the v1 unsigned sample-coordinate range.
    if not (0 <= global_start
            and global_start + (local_end - local_start) <= UINT32_MAX + 1):
        raise ReconcileError(
            f"{where}: mapped logical range escapes the v1 unsigned "
            "sample-coordinate range"
        )

    return SegmentMap(
        fragment_id=fragment_id,
        column_id=column_id,
        local_start=local_start,
        local_end=local_end,
        channel=channel,
        global_start=global_start,
    )


def _parse_side(data: object, label: str) -> Side:
    if not isinstance(data, dict):
        raise ReconcileError(f"alignment {label} must be a JSON object")
    if set(data) != _SIDE_KEYS:
        raise ReconcileError(
            f"alignment {label} must contain exactly {sorted(_SIDE_KEYS)}"
        )
    archive = data["archive"]
    if not isinstance(archive, str) or not os.path.isabs(archive):
        raise ReconcileError(
            f"alignment {label}: archive must be an absolute path"
        )
    raw_segments = data["segments"]
    if not isinstance(raw_segments, list) or not raw_segments:
        raise ReconcileError(
            f"alignment {label}: segments must be a non-empty list"
        )
    segments = tuple(
        _parse_segment(item, label, index)
        for index, item in enumerate(raw_segments)
    )
    _check_local_disjoint(segments, label)
    return Side(archive=archive, segments=segments)


def _check_local_disjoint(segments: tuple[SegmentMap, ...], label: str) -> None:
    """Same-side ranges of one (fragment, column) must never intersect.

    Two segments mapping the same original sample would make one physical
    observation count twice on one side.  Adjacent ranges are fine; logical
    ranges may overlap freely (that is what multiple captures means).
    """
    by_column: dict[tuple[int, int], list[tuple[int, int]]] = {}
    for seg in segments:
        by_column.setdefault((seg.fragment_id, seg.column_id), []).append(
            (seg.local_start, seg.local_end)
        )
    for (fragment_id, column_id), spans in by_column.items():
        spans.sort()
        for (_, prev_end), (next_start, _) in zip(spans, spans[1:]):
            if next_start < prev_end:
                raise ReconcileError(
                    f"alignment {label}: local ranges on fragment "
                    f"{fragment_id} column {column_id} intersect"
                )


def parse_alignment(data: object) -> Alignment:
    """Validate the alignment sheet structurally (archive checks come later)."""
    if not isinstance(data, dict):
        raise ReconcileError("alignment must be a JSON object")
    if set(data) != _ALIGNMENT_KEYS:
        raise ReconcileError(
            f"alignment must contain exactly {sorted(_ALIGNMENT_KEYS)}"
        )
    return Alignment(
        left=_parse_side(data["left"], "left"),
        right=_parse_side(data["right"], "right"),
    )


def load_alignment(path: str) -> Alignment:
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as exc:
        raise ReconcileError(f"cannot read alignment {path}: {exc}") from exc
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReconcileError(f"cannot parse alignment {path}: {exc}") from exc
    return parse_alignment(data)


def _validate_segments_against_catalog(
    side: Side, catalog: dict, label: str
) -> None:
    fragments = {item["fragment_id"]: item for item in catalog["fragments"]}
    for seg in side.segments:
        fragment = fragments.get(seg.fragment_id)
        if fragment is None:
            raise ReconcileError(
                f"alignment {label}: fragment {seg.fragment_id} is not "
                f"present in archive {side.archive}"
            )
        if not (
            fragment["start_row"] <= seg.local_start
            and seg.local_end <= fragment["end_row"]
        ):
            raise ReconcileError(
                f"alignment {label}: local range "
                f"[{seg.local_start},{seg.local_end}) of fragment "
                f"{seg.fragment_id} escapes its declared range "
                f"[{fragment['start_row']},{fragment['end_row']})"
            )


# ---------------------------------------------------------------------------
# Per-side runtime: block index plus a range-restricted reader
# ---------------------------------------------------------------------------

class _SideRuntime:
    """Catalog block index for one side, plus its canonical channel groups."""

    def __init__(self, side: Side, catalog: dict) -> None:
        self.side = side
        self.catalog = catalog
        self.chains: dict[tuple[int, int], list[Block]] = {}
        for block in catalog["_blocks"]:
            self.chains.setdefault(
                (block.fragment_id, block.column_id), []
            ).append(block)
        self._index_in_chain: dict[int, int] = {}
        for chain in self.chains.values():
            chain.sort(key=lambda block: block.start_row)
            for index, block in enumerate(chain):
                self._index_in_chain[id(block)] = index
        # Sorted checkpoint positions per block, for O(log n) anchor lookup.
        self._checkpoint_keys: dict[int, list[int]] = {
            id(block): sorted(block.checkpoints)
            for chain in self.chains.values()
            for block in chain
        }
        # Lazily resolved entry values for dependent blocks (see
        # entry_resolution); the resolution of a block never changes.
        self._entry_cache: dict[int, tuple[int | None, tuple[Block, ...]]] = {}
        # Channel -> segments, canonically ordered so the merge (and hence
        # the sheet) never depends on the order segments were listed in.
        self.channels: dict[str, list[SegmentMap]] = {}
        for seg in side.segments:
            self.channels.setdefault(seg.channel, []).append(seg)
        for segments in self.channels.values():
            segments.sort(
                key=lambda seg: (
                    seg.global_start,
                    seg.fragment_id,
                    seg.column_id,
                    seg.local_start,
                )
            )

    def checkpoint_anchor(self, block: Block, local_begin: int) -> tuple[int, int]:
        """Latest checkpoint at or before ``local_begin`` -> (local, value)."""
        keys = self._checkpoint_keys[id(block)]
        index = bisect.bisect_right(keys, local_begin) - 1
        if index < 0:  # pragma: no cover - callers guarantee an anchor exists
            raise ReconcileError(
                f"internal error: block {block.block_id} has no anchor"
            )
        local = keys[index]
        return local, block.checkpoints[local]

    def entry_resolution(self, block: Block) -> tuple[int | None, tuple[Block, ...]]:
        """Trusted value entering ``block`` and the blocks needed to prove it.

        A dependent chain may run through several blocks that carry no
        absolute of their own; the catalogued one-hop ``carry_in`` is then
        None even though v1 reconstruction is fully defined.  Walking back
        to the decisive predecessor (an independent block, or a dependent
        block with its own absolute) gives the base value, and the
        absolute-less dependent blocks between it and ``block`` are the
        out-of-interval trusted predecessors whose deltas must still be
        applied.  A damaged block anywhere on the walk breaks the chain.
        Returns ``(base, prefix)``: the value entering the first prefix
        block (None when no trusted value exists) and the prefix blocks in
        chain order.
        """
        cached = self._entry_cache.get(id(block))
        if cached is not None:
            return cached
        chain = self.chains[(block.fragment_id, block.column_id)]
        cursor = self._index_in_chain[id(block)] - 1
        prefix: list[Block] = []
        base: int | None = None
        while cursor >= 0:
            previous = chain[cursor]
            if previous.status != "readable":
                prefix = []  # a damaged prefix breaks the chain outright
                base = None
                break
            if previous.kind == fmt.KIND_INDEPENDENT:
                base = previous.last_value  # None when it never anchored
                break
            if previous.first_absolute_local is not None:
                base = previous.last_value  # decisive: anchored in-block
                break
            prefix.insert(0, previous)  # absolute-less dependent: walk on
            cursor -= 1
        resolution = (base, tuple(prefix))
        self._entry_cache[id(block)] = resolution
        return resolution

    def allowed_ranges(self) -> list[tuple[int, int]]:
        """Payload ranges this side may read: blocks intersecting segments,
        plus the absolute-less dependent prefixes that resolve their entry
        values (trusted predecessors outside the aligned intervals)."""
        allowed: set[tuple[int, int]] = set()
        for seg in self.side.segments:
            chain = self.chains.get((seg.fragment_id, seg.column_id), ())
            for block in _intersecting_blocks(
                chain, seg.local_start, seg.local_end
            ):
                if block.status != "readable":
                    continue
                allowed.add((block.offset, block.end_offset))
                if block.kind != fmt.KIND_DEPENDENT:
                    continue
                local_begin = max(seg.local_start, block.start_row) \
                    - block.start_row
                first_abs = block.first_absolute_local
                if first_abs is None or first_abs > local_begin:
                    _, prefix = self.entry_resolution(block)
                    for previous in prefix:
                        allowed.add((previous.offset, previous.end_offset))
        return sorted(allowed)


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


# ---------------------------------------------------------------------------
# Streaming decode of trusted values
# ---------------------------------------------------------------------------

def _read_record_slices(reader: TrackedReader, block: Block,
                        begin_local: int, end_local: int):
    """Yield ``(tag, wire_value)`` for locals [begin_local, end_local).

    Reads happen in bounded chunks; a block payload far larger than memory
    is decoded slice by slice, never materialized at once.
    """
    local = begin_local
    while local < end_local:
        stop = min(local + _DECODE_CHUNK_RECORDS, end_local)
        offset = block.offset + local * fmt.RECORD_SIZE
        raw = reader.read(offset, (stop - local) * fmt.RECORD_SIZE)
        for base in range(0, len(raw), fmt.RECORD_SIZE):
            yield raw[base], struct.unpack(">q", raw[base + 1:base + 9])[0]
        local = stop


def _block_values(runtime: _SideRuntime, reader: TrackedReader, block: Block,
                  window_start: int, window_end: int):
    """Yield ``(row, value)`` for trusted values of one block in the window.

    Anchoring follows the v1 rules: a catalogued checkpoint replaces any
    scan from the column head, and a dependent block with no in-block
    anchor at or before the window reconstructs its entry value through
    ``entry_resolution`` -- the decisive predecessor's catalogued value
    plus the deltas of the absolute-less dependent blocks in between,
    applied from their payloads in bounded chunks.  Those out-of-window
    records are exactly the trusted predecessors the reconciliation
    contract allows (they are decoded, never emitted).
    """
    local_begin = max(window_start, block.start_row) - block.start_row
    local_end = min(window_end, block.end_row) - block.start_row
    first_abs = block.first_absolute_local

    start_local = local_begin
    running: int | None = None
    anchor_local: int | None = None  # checkpoint position, value stored there

    if block.kind == fmt.KIND_INDEPENDENT:
        if first_abs is None:
            return  # no anchor anywhere: deltas unresolvable, missings stay
        if first_abs <= local_begin:
            anchor_local, running = runtime.checkpoint_anchor(
                block, local_begin
            )
            start_local = anchor_local
        else:
            start_local = first_abs  # prefix is missing/missing_prefix only
    else:  # dependent block
        if first_abs is not None and first_abs <= local_begin:
            anchor_local, running = runtime.checkpoint_anchor(
                block, local_begin
            )
            start_local = anchor_local
        else:
            base, prefix = runtime.entry_resolution(block)
            if base is None:
                # No trusted entry value: only an in-block absolute
                # re-anchors, and it sits beyond the window start (or does
                # not exist).
                if first_abs is None:
                    return
                start_local = first_abs
            else:
                # Trusted entry value: apply the absolute-less dependent
                # prefix (trusted predecessors outside the interval), then
                # this block's records from its head -- all in bounded
                # chunks, emitting nothing before local_begin.
                running = base
                for previous in prefix:
                    span = previous.end_row - previous.start_row
                    for tag, wire in _read_record_slices(
                        reader, previous, 0, span
                    ):
                        if tag == fmt.REC_ABSOLUTE:
                            running = wire
                        elif tag == fmt.REC_DELTA and running is not None:
                            running += wire
                start_local = 0

    local = start_local
    for tag, wire in _read_record_slices(reader, block, start_local, local_end):
        value: int | None = None
        if anchor_local is not None and local == anchor_local \
                and tag == fmt.REC_DELTA:
            # The checkpoint stores the reconstructed value *at* the anchor,
            # so the anchor delta must not be applied a second time.
            value = running
        elif tag == fmt.REC_ABSOLUTE:
            running = wire
            value = running
        elif tag == fmt.REC_DELTA:
            if running is not None:
                running += wire
                value = running
        # Explicit missings and unresolvable deltas are not observations.
        if value is not None and local >= local_begin:
            yield block.start_row + local, value
        local += 1


def _segment_observations(runtime: _SideRuntime, reader: TrackedReader | None,
                          seg: SegmentMap):
    """Yield ``(sample, value, fragment_id, column_id, row)`` for one segment,
    in strictly increasing sample order."""
    if reader is None:
        return
    chain = runtime.chains.get((seg.fragment_id, seg.column_id), ())
    for block in _intersecting_blocks(chain, seg.local_start, seg.local_end):
        if block.status != "readable":
            continue  # an isolated damaged block yields no observations
        for row, value in _block_values(
            runtime, reader, block, seg.local_start, seg.local_end
        ):
            yield (seg.sample_of(row), value,
                   seg.fragment_id, seg.column_id, row)


def _merged_observations(runtime: _SideRuntime,
                         reader: TrackedReader | None,
                         segments: list[SegmentMap]):
    """k-way merge of one channel's segment streams into sample order.

    The heap holds exactly one entry per mapping segment, so merge memory is
    bounded by the alignment sheet, never by the observation count.
    """
    heap: list[tuple[int, int, tuple[int, int, int, int]]] = []
    streams = [
        _segment_observations(runtime, reader, seg) for seg in segments
    ]
    for seq, stream in enumerate(streams):
        first = next(stream, None)
        if first is not None:
            heapq.heappush(heap, (first[0], seq, first[1:]))
    while heap:
        sample, seq, rest = heapq.heappop(heap)
        value, fragment_id, column_id, row = rest
        yield sample, value, fragment_id, column_id, row
        nxt = next(streams[seq], None)
        if nxt is not None:
            heapq.heappush(heap, (nxt[0], seq, nxt[1:]))


# ---------------------------------------------------------------------------
# Per-key aggregation and the difference sheet
# ---------------------------------------------------------------------------

class _SideAccum:
    """One side's trusted observations for one logical key.

    Tracks only what the sheet needs: whether two or more distinct values
    were seen (``ambiguous``), and the minimum/maximum observed values with
    their provenance.  Ties on value resolve to the smallest
    ``(fragment_id, column_id, row)`` origin tuple.  Memory is O(1) per key.
    """

    __slots__ = ("distinct", "min_rec", "max_rec")

    def __init__(self) -> None:
        self.distinct: set[int] = set()  # capped at two entries
        self.min_rec: tuple[int, int, int, int] | None = None
        self.max_rec: tuple[int, int, int, int] | None = None

    def add(self, value: int, fragment_id: int, column_id: int, row: int) -> None:
        if len(self.distinct) < 2:
            self.distinct.add(value)
        origin = (fragment_id, column_id, row)
        if self.min_rec is None or value < self.min_rec[0] or (
            value == self.min_rec[0] and origin < self.min_rec[1:]
        ):
            self.min_rec = (value, fragment_id, column_id, row)
        if self.max_rec is None or value > self.max_rec[0] or (
            value == self.max_rec[0] and origin < self.max_rec[1:]
        ):
            self.max_rec = (value, fragment_id, column_id, row)

    @property
    def ambiguous(self) -> bool:
        return len(self.distinct) >= 2

    def to_json(self) -> dict:
        assert self.min_rec is not None and self.max_rec is not None
        return {
            "min": _record_json(self.min_rec),
            "max": _record_json(self.max_rec),
        }


def _record_json(rec: tuple[int, int, int, int]) -> dict:
    value, fragment_id, column_id, row = rec
    return {
        "value": value,
        "fragment_id": fragment_id,
        "column_id": column_id,
        "row": row,
    }


def _grouped_by_sample(observations):
    """Group a sample-ordered observation stream into (sample, accum)."""
    current: int | None = None
    accum: _SideAccum | None = None
    for sample, value, fragment_id, column_id, row in observations:
        if current is None or sample != current:
            if accum is not None:
                yield current, accum
            current = sample
            accum = _SideAccum()
        accum.add(value, fragment_id, column_id, row)
    if accum is not None:
        yield current, accum


def _difference_line(channel: str, sample: int,
                     left: _SideAccum | None,
                     right: _SideAccum | None) -> dict | None:
    """One sheet line, or None when the key needs no review."""
    if (left is not None and left.ambiguous) \
            or (right is not None and right.ambiguous):
        kind = "ambiguous"
    elif left is None and right is None:
        return None  # no trusted value on either side: nothing to report
    elif left is None:
        kind = "right_only"
    elif right is None:
        kind = "left_only"
    elif next(iter(left.distinct)) != next(iter(right.distinct)):
        kind = "different"
    else:
        return None  # one trusted value each, and they agree
    return {
        "channel": channel,
        "sample": sample,
        "kind": kind,
        "left": left.to_json() if left is not None else None,
        "right": right.to_json() if right is not None else None,
    }


def _channel_lines(channel: str,
                   left_rt: _SideRuntime, left_reader: TrackedReader | None,
                   right_rt: _SideRuntime, right_reader: TrackedReader | None):
    """Merge-join both sides of one channel, one logical key at a time."""
    left_groups = _grouped_by_sample(_merged_observations(
        left_rt, left_reader, left_rt.channels.get(channel, [])))
    right_groups = _grouped_by_sample(_merged_observations(
        right_rt, right_reader, right_rt.channels.get(channel, [])))
    left_item = next(left_groups, None)
    right_item = next(right_groups, None)
    while left_item is not None or right_item is not None:
        if right_item is None or (
            left_item is not None and left_item[0] < right_item[0]
        ):
            sample, left_accum = left_item
            right_accum = None
            left_item = next(left_groups, None)
        elif left_item is None or right_item[0] < left_item[0]:
            sample, right_accum = right_item
            left_accum = None
            right_item = next(right_groups, None)
        else:
            sample, left_accum = left_item
            right_accum = right_item[1]
            left_item = next(left_groups, None)
            right_item = next(right_groups, None)
        line = _difference_line(channel, sample, left_accum, right_accum)
        if line is not None:
            yield line


def _serialize_line(line: dict) -> str:
    return json.dumps(line, ensure_ascii=False, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Atomic delivery
# ---------------------------------------------------------------------------

def _commit(tmp_output: str, output_path: str) -> None:
    """Publish the staged sheet without ever clobbering an existing path."""
    try:
        os.link(tmp_output, output_path)
    except FileExistsError as exc:
        raise ReconcileError(
            f"output {output_path} already exists; refusing to overwrite"
        ) from exc
    except OSError:
        # Filesystems without hard links: fall back to a rename.  The
        # existence check above already refused an existing target.
        os.rename(tmp_output, output_path)


class _maybe_reader:
    """Open a range-restricted reader, or yield None when nothing is readable."""

    def __init__(self, archive: str, allowed: list[tuple[int, int]]) -> None:
        self.archive = archive
        self.allowed = allowed
        self.reader: TrackedReader | None = None

    def __enter__(self) -> TrackedReader | None:
        if self.allowed:
            self.reader = TrackedReader(self.archive, allowed=self.allowed)
        return self.reader

    def __exit__(self, *exc: object) -> None:
        if self.reader is not None:
            self.reader.close()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def reconcile_archives(alignment_path: str, output_path: str) -> dict:
    """Reconcile the two archives named by the alignment sheet.

    Writes one JSON object per line to ``output_path`` (which must not exist
    yet) and returns a small summary.  Any invalid input, uninterpretable
    archive or I/O failure raises before the commit and leaves no formal
    differences sheet behind.
    """
    alignment = load_alignment(alignment_path)
    if os.path.exists(output_path):
        raise ReconcileError(
            f"output {output_path} already exists; refusing to overwrite"
        )
    parent = os.path.dirname(os.path.abspath(output_path)) or "."
    if not os.path.isdir(parent):
        raise ReconcileError(f"parent directory {parent} does not exist")

    work_dir = tempfile.mkdtemp(prefix=".narc-reconcile-", dir=parent)
    try:
        runtimes: dict[str, _SideRuntime] = {}
        for label, side in (("left", alignment.left),
                            ("right", alignment.right)):
            catalog_path = os.path.join(work_dir, f"{label}.catalog.json")
            try:
                build_catalog(side.archive, catalog_path)
            except (CatalogError, OSError) as exc:
                raise ReconcileError(
                    f"cannot catalog {label} archive {side.archive}: {exc}"
                ) from exc
            catalog = load_catalog(catalog_path)
            _validate_segments_against_catalog(side, catalog, label)
            runtimes[label] = _SideRuntime(side, catalog)

        left_rt = runtimes["left"]
        right_rt = runtimes["right"]
        # Channel order is Unicode code point order; sample order ascending.
        # Neither depends on the order the mapping segments were listed in.
        channels = sorted(set(left_rt.channels) | set(right_rt.channels))

        tmp_output = os.path.join(work_dir, "differences.jsonl.tmp")
        differences = 0
        left_allowed = left_rt.allowed_ranges()
        right_allowed = right_rt.allowed_ranges()
        # A side whose segments touch no readable block performs no reads at
        # all; its reader stays closed and every stream is empty.
        with _maybe_reader(left_rt.side.archive, left_allowed) as left_reader, \
                _maybe_reader(right_rt.side.archive, right_allowed) as right_reader:
            with open(tmp_output, "w", encoding="utf-8") as out:
                for channel in channels:
                    for line in _channel_lines(
                        channel, left_rt, left_reader, right_rt, right_reader
                    ):
                        out.write(_serialize_line(line))
                        out.write("\n")
                        differences += 1
                out.flush()
                os.fsync(out.fileno())
        _commit(tmp_output, output_path)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    return {
        "output": os.path.abspath(output_path),
        "channels": len(channels),
        "differences": differences,
    }
