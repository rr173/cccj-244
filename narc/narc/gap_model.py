"""Plan/batch parsing and per-fragment completeness scanning for ``gap``.

A *plan* fixes one global sample range and a non-empty, duplicate-free set of
required columns.  A position is **complete** for one source when the chosen
fragment of that source yields a trusted value for *every* required column at
that position; columns from different sources are never mixed.

Completeness is computed from the existing v1 catalog summary plus a narrow
read of the fragment's own payload tags, and stored as event-based half-open
intervals, so plans spanning hundreds of millions of empty coordinates never
expand per row.

Decoding follows the v1 semantics used by the reference decoder:

* an explicit missing record is never a value and never changes the running
  value;
* a delta with no trusted predecessor (``missing_prefix``) is not a value,
  but a trusted absolute inside a block re-anchors its suffix;
* a damaged block yields no values at all, so a bad copy never cancels a
  complete copy under another source;
* a dependent block continues its predecessor's running value across the
  whole dependency chain; a chain is not judged broken merely because it is
  long.
"""

from __future__ import annotations

import json
import os
import tempfile

from . import format as fmt
from .catalog import Block, CatalogError, blocks_for, build_catalog, load_catalog
from .io_utils import TrackedReader

UINT16_MAX = (1 << 16) - 1
UINT32_MAX = (1 << 32) - 1


class GapError(ValueError):
    """Invalid plan/batch, unusable archive, or unsafe ledger update."""


# ---------------------------------------------------------------------------
# Scalar validation helpers
# ---------------------------------------------------------------------------

def is_plain_int(value: object) -> bool:
    # bool is an int subclass but never a coordinate or revision.
    return isinstance(value, int) and not isinstance(value, bool)


# ---------------------------------------------------------------------------
# Interval algebra (half-open [start, end))
# ---------------------------------------------------------------------------

def merge_intervals(items) -> list[tuple[int, int]]:
    ordered = sorted(
        (int(start), int(end))
        for start, end in items
        if int(end) > int(start)
    )
    merged: list[list[int]] = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1][1] = end
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def intersect_intervals(
    left: list[tuple[int, int]], right: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    i = j = 0
    while i < len(left) and j < len(right):
        start = max(left[i][0], right[j][0])
        end = min(left[i][1], right[j][1])
        if start < end:
            out.append((start, end))
        if left[i][1] <= right[j][1]:
            i += 1
        else:
            j += 1
    return out


def intersect_all(groups: list[list[tuple[int, int]]]) -> list[tuple[int, int]]:
    if not groups:
        return []
    current = merge_intervals(groups[0])
    for other in groups[1:]:
        current = intersect_intervals(current, merge_intervals(other))
        if not current:
            return []
    return current


def union_intervals(
    left: list[tuple[int, int]], right: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    return merge_intervals(list(left) + list(right))


def subtract_intervals(
    left: list[tuple[int, int]], right: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """Return ``left \\ right`` for merged half-open interval lists."""
    out: list[tuple[int, int]] = []
    j = 0
    for start, end in left:
        cursor = start
        while j < len(right) and right[j][1] <= cursor:
            j += 1
        k = j
        while k < len(right) and right[k][0] < end:
            cut_start, cut_end = right[k]
            if cut_start > cursor:
                out.append((cursor, min(cut_start, end)))
            if cut_end > cursor:
                cursor = cut_end
            if cursor >= end:
                break
            k += 1
        if cursor < end:
            out.append((cursor, end))
    return out


def clip_intervals(
    intervals: list[tuple[int, int]], start: int, end: int
) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for item_start, item_end in intervals:
        low = max(item_start, start)
        high = min(item_end, end)
        if low < high:
            out.append((low, high))
    return merge_intervals(out)


def _rows_to_intervals(rows: list[int]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for row in rows:
        if out and row == out[-1][1]:
            out[-1] = (out[-1][0], row + 1)
        elif out and row < out[-1][1]:
            continue
        else:
            out.append((row, row + 1))
    return out


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------

class Plan:
    """A validated sampling plan."""

    __slots__ = ("start", "end", "columns")

    def __init__(self, start: int, end: int, columns: tuple[int, ...]) -> None:
        self.start = start
        self.end = end
        self.columns = columns

    def to_json(self) -> dict:
        return {"range": [self.start, self.end], "columns": list(self.columns)}


def parse_plan(data: object) -> Plan:
    if not isinstance(data, dict):
        raise GapError("plan must be a JSON object")
    if set(data) != {"range", "columns"}:
        raise GapError("plan must contain exactly ['columns', 'range']")

    span = data["range"]
    if (
        not isinstance(span, list)
        or len(span) != 2
        or not all(is_plain_int(part) for part in span)
    ):
        raise GapError("plan range must be a [start, end] pair")
    start, end = span
    if not (0 <= start < end <= UINT32_MAX + 1):
        raise GapError("plan range must satisfy 0 <= start < end <= 2^32")

    raw_columns = data["columns"]
    if not isinstance(raw_columns, list) or not raw_columns:
        raise GapError("plan columns must be a non-empty list")
    columns: list[int] = []
    seen: set[int] = set()
    for column in raw_columns:
        if not is_plain_int(column) or not (0 <= column <= UINT16_MAX):
            raise GapError("plan column ids must be uint16 integers")
        if column in seen:
            raise GapError(f"plan column {column} is duplicated")
        seen.add(column)
        columns.append(column)
    return Plan(start, end, tuple(sorted(columns)))


def load_json_file(path: str, what: str) -> tuple[object, bytes]:
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as exc:
        raise GapError(f"cannot read {what} {path}: {exc}") from exc
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GapError(f"cannot parse {what} {path}: {exc}") from exc
    return data, raw


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

class Change:
    __slots__ = ("source_id", "op", "archive", "fragment_id")

    def __init__(
        self,
        source_id: str,
        op: str,
        archive: str | None = None,
        fragment_id: int | None = None,
    ) -> None:
        self.source_id = source_id
        self.op = op
        self.archive = archive
        self.fragment_id = fragment_id

    @property
    def is_upsert(self) -> bool:
        return self.op == "upsert"


class Batch:
    __slots__ = ("revision", "changes", "digest")

    def __init__(self, revision: int, changes: list[Change], digest: str) -> None:
        self.revision = revision
        self.changes = changes
        self.digest = digest


_UPSERT_KEYS = {"source_id", "op", "archive", "fragment_id"}
_REMOVE_KEYS = {"source_id", "op"}


def parse_batch(data: object, digest: str) -> Batch:
    if not isinstance(data, dict):
        raise GapError("batch must be a JSON object")
    if set(data) != {"revision", "changes"}:
        raise GapError("batch must contain exactly ['changes', 'revision']")

    revision = data["revision"]
    if not is_plain_int(revision) or revision < 1:
        raise GapError("batch revision must be an integer >= 1")

    raw_changes = data["changes"]
    if not isinstance(raw_changes, list) or not raw_changes:
        raise GapError("batch changes must be a non-empty list")

    changes: list[Change] = []
    seen: set[str] = set()
    for item in raw_changes:
        if not isinstance(item, dict):
            raise GapError("each batch change must be a JSON object")
        op = item.get("op")
        if op not in ("upsert", "remove"):
            raise GapError("each batch change op must be 'upsert' or 'remove'")
        expected = _UPSERT_KEYS if op == "upsert" else _REMOVE_KEYS
        if set(item) != expected:
            raise GapError(f"{op} change must contain exactly {sorted(expected)}")

        source_id = item["source_id"]
        if not isinstance(source_id, str) or not source_id:
            raise GapError("source_id must be a non-empty string")
        if source_id in seen:
            raise GapError(
                f"source {source_id!r} is operated on twice in one batch"
            )
        seen.add(source_id)

        if op == "remove":
            changes.append(Change(source_id, op))
            continue

        archive = item["archive"]
        if not isinstance(archive, str) or not os.path.isabs(archive):
            raise GapError(
                f"archive for source {source_id!r} must be an absolute path"
            )
        fragment_id = item["fragment_id"]
        if not is_plain_int(fragment_id) or not (
            0 <= fragment_id <= UINT16_MAX
        ):
            raise GapError(
                f"fragment_id for source {source_id!r} must be a uint16 integer"
            )
        # The archive is opened only when the batch is actually applied; a
        # byte-identical retry of a committed batch must not touch any file.
        changes.append(Change(source_id, op, archive, fragment_id))

    return Batch(revision, changes, digest)


# ---------------------------------------------------------------------------
# Fragment scanning
# ---------------------------------------------------------------------------

def _catalog_archive(archive: str) -> dict:
    """Catalog one upserted archive through a private temporary sidecar."""
    handle, catalog_path = tempfile.mkstemp(prefix="narc-gap-", suffix=".json")
    os.close(handle)
    try:
        try:
            build_catalog(archive, catalog_path)
        except CatalogError as exc:
            raise GapError(f"cannot catalog archive {archive}: {exc}") from exc
        except OSError as exc:
            raise GapError(f"cannot read archive {archive}: {exc}") from exc
        return load_catalog(catalog_path)
    finally:
        try:
            os.unlink(catalog_path)
        except OSError:
            pass


def _locate_first(chain: list[Block], plan_start: int) -> int:
    """First block whose end_row exceeds ``plan_start``."""
    lo, hi = 0, len(chain)
    while lo < hi:
        mid = (lo + hi) // 2
        if chain[mid].end_row <= plan_start:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _entry_has_value(chain: list[Block], index: int) -> bool:
    """Whether a trusted value enters ``chain[index]`` from its predecessor.

    Only dependent blocks continue the chain.  A readable predecessor that
    saw an absolute (in-block or inherited) ends with a trusted value; an
    independent block restarts, so it supplies nothing; a damaged block
    breaks the chain.  This walks catalog metadata backwards and stops at the
    first decisive block, never touching payload bytes.
    """
    cursor = index - 1
    while cursor >= 0:
        block = chain[cursor]
        if block.status != "readable":
            return False
        if block.kind == fmt.KIND_INDEPENDENT:
            return block.first_absolute_local is not None
        if block.first_absolute_local is not None:
            return True
        # Dependent block without an absolute: ask its own predecessor.
        cursor -= 1
    return False


def _end_has_value(block: Block, entry: bool) -> bool:
    """Whether a trusted value is in effect after ``block``."""
    if block.status != "readable":
        return False
    if block.kind == fmt.KIND_INDEPENDENT:
        entry = False
    return block.first_absolute_local is not None or entry


def scan_fragment(
    archive: str, fragment_id: int, plan: Plan
) -> tuple[list[tuple[int, int]], dict]:
    """Return ``(complete, identity)`` for one fragment of one archive.

    Only blocks of the chosen fragment are considered.  Physical reads are
    tag slices of the block payloads that intersect the plan; damaged blocks
    and unrelated columns cost zero payload bytes.
    """
    catalog = _catalog_archive(archive)
    fragment = next(
        (
            item
            for item in catalog["fragments"]
            if item["fragment_id"] == fragment_id
        ),
        None,
    )
    if fragment is None:
        raise GapError(
            f"fragment {fragment_id} does not exist in archive {archive}"
        )

    # plans[column] = [(block, begin_local, end_local, entry_defined)]
    plans: dict[int, list[tuple[Block, int, int, bool]]] = {}
    allowed: list[tuple[int, int]] = []
    seen_allowed: set[tuple[int, int]] = set()

    for column in plan.columns:
        chain = blocks_for(catalog, fragment_id, column)
        column_plans: list[tuple[Block, int, int, bool]] = []
        cursor = _locate_first(chain, plan.start)
        # Entry state for the first in-plan block comes from catalog metadata
        # only; it is then advanced block by block without any payload read.
        entry: bool | None = (
            _entry_has_value(chain, cursor) if cursor < len(chain) else None
        )
        while cursor < len(chain) and chain[cursor].start_row < plan.end:
            block = chain[cursor]
            if entry is None:
                entry = _entry_has_value(chain, cursor)
            # An independent block always restarts; a predecessor value can
            # only be inherited by a dependent block.
            block_entry = entry and block.kind == fmt.KIND_DEPENDENT
            slice_start = max(block.start_row, plan.start)
            slice_end = min(block.end_row, plan.end)
            if slice_start < slice_end and block.status == "readable":
                column_plans.append(
                    (
                        block,
                        slice_start - block.start_row,
                        slice_end - block.start_row,
                        block_entry,
                    )
                )
                payload_range = (block.offset, block.end_offset)
                if payload_range not in seen_allowed:
                    seen_allowed.add(payload_range)
                    allowed.append(payload_range)
            entry = _end_has_value(block, block_entry)
            cursor += 1
        plans[column] = column_plans

    tags: dict[tuple[int, int], bytes] = {}
    if allowed:
        with TrackedReader(archive, allowed=allowed) as reader:
            for column, column_plans in plans.items():
                for block, begin_local, end_local, _ in column_plans:
                    offset = block.offset + begin_local * fmt.RECORD_SIZE
                    length = (end_local - begin_local) * fmt.RECORD_SIZE
                    tags[(id(block), column)] = reader.read(offset, length)

    value_by_column: list[list[tuple[int, int]]] = []
    for column in plan.columns:
        rows: list[int] = []
        for block, begin_local, end_local, entry in plans[column]:
            raw = tags[(id(block), column)]
            first_abs = block.first_absolute_local
            for slot in range(end_local - begin_local):
                local = begin_local + slot
                if first_abs is None:
                    # No in-block anchor: every delta needs the entry value.
                    defined = entry
                elif local < first_abs:
                    # Before the first in-block absolute only a trusted entry
                    # value makes deltas resolvable.
                    defined = entry
                else:
                    defined = True
                if not defined:
                    continue
                if raw[slot * fmt.RECORD_SIZE] == fmt.REC_MISSING:
                    continue
                rows.append(block.start_row + local)
        value_by_column.append(_rows_to_intervals(rows))

    # Every required column must have a value at the position, from this one
    # source.  Columns are never combined across sources.
    complete = intersect_all(value_by_column)

    identity = {
        "size": catalog["source"]["size"],
        "sha256": catalog["source"]["sha256"],
    }
    return complete, identity
