"""Atomic local gap-recovery ledger backed by SQLite.

The ledger stores:

* the fixed plan and the current revision;
* the exact bytes digest and JSON of the last committed change slip;
* every registered source together with its precomputed "complete" intervals
  (positions where that one source supplies every required column);
* a partition of the plan interval into maximal segments carrying a
  *source-complete count*.

That count partition is what keeps each batch incremental.  A source update
touches only the intervals that actually changed for that source; adding or
removing a source is a range ``+1`` / ``-1`` over those intervals.  Positions
that are complete are exactly the segments with a positive count, so the
command never has to walk other sources or disjoint historical ranges.

Ledger state, the count partition and the change slip live in one SQLite
transaction.  A crash before the commit leaves the previous ledger intact; a
crash after it leaves the whole batch visible.  A byte-identical retry of the
latest revision returns the stored slip without opening any archive.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile

from .gap_model import (
    GapError,
    Plan,
    clip_intervals,
    load_json_file,
    merge_intervals,
    parse_batch,
    parse_plan,
    scan_fragment,
    subtract_intervals,
    union_intervals,
)

DB_FILE = "narc-gap-ledger.sqlite3"
LEDGER_FORMAT = 1

_SCHEMA = """
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE segments (
    lo  INTEGER PRIMARY KEY,
    hi  INTEGER NOT NULL,
    cnt INTEGER NOT NULL
);
CREATE TABLE sources (
    source_id   TEXT PRIMARY KEY,
    archive     TEXT NOT NULL,
    fragment_id INTEGER NOT NULL,
    revision    INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL
);
CREATE TABLE source_intervals (
    source_id TEXT NOT NULL,
    lo        INTEGER NOT NULL,
    hi        INTEGER NOT NULL,
    PRIMARY KEY (source_id, lo)
);
"""


def db_path(workspace: str) -> str:
    return os.path.join(workspace, DB_FILE)


def serialize_slip(slip: dict) -> bytes:
    return (
        json.dumps(slip, sort_keys=True, separators=(",", ":")).encode("utf-8")
        + b"\n"
    )


def _connect(path: str, *, create: bool) -> sqlite3.Connection:
    if not create and not os.path.exists(path):
        raise GapError(f"no gap ledger at {path}; run gap-init first")
    try:
        conn = sqlite3.connect(path, isolation_level=None, timeout=30)
    except sqlite3.Error as exc:
        raise GapError(f"cannot open gap ledger {path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    return conn


def _require_existing_schema(conn: sqlite3.Connection) -> None:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='segments'"
        ).fetchone()
    except sqlite3.DatabaseError as exc:
        raise GapError(f"gap ledger is not a readable database: {exc}") from exc
    if row is None:
        raise GapError("gap ledger is missing its schema; run gap-init first")


def _read_meta(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT key, value FROM meta").fetchall()
    meta = {row["key"]: row["value"] for row in rows}
    if meta.get("ledger_format") != str(LEDGER_FORMAT):
        raise GapError("gap ledger has an unsupported format")
    return meta


def _write_meta(conn: sqlite3.Connection, values: dict[str, str]) -> None:
    for key, value in values.items():
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


# ---------------------------------------------------------------------------
# Plan / revision helpers
# ---------------------------------------------------------------------------

def _plan_from_meta(meta: dict) -> Plan:
    try:
        data = json.loads(meta["plan"])
    except (KeyError, json.JSONDecodeError) as exc:
        raise GapError("gap ledger plan is corrupt") from exc
    return parse_plan(data)


def _current_revision(meta: dict) -> int:
    try:
        return int(meta["revision"])
    except (KeyError, ValueError) as exc:
        raise GapError("gap ledger revision is corrupt") from exc


# ---------------------------------------------------------------------------
# Count partition maintenance
# ---------------------------------------------------------------------------

def _split_at(conn: sqlite3.Connection, row: int) -> bool:
    """Ensure a segment boundary exists at ``row``.  True if one was made."""
    found = conn.execute(
        "SELECT lo, hi, cnt FROM segments WHERE lo <= ? "
        "ORDER BY lo DESC LIMIT 1",
        (row,),
    ).fetchone()
    if found is None:
        return False
    lo, hi, cnt = found["lo"], found["hi"], found["cnt"]
    if lo < row < hi:
        conn.execute("DELETE FROM segments WHERE lo = ?", (lo,))
        conn.execute(
            "INSERT INTO segments(lo, hi, cnt) VALUES(?, ?, ?)",
            (lo, row, cnt),
        )
        conn.execute(
            "INSERT INTO segments(lo, hi, cnt) VALUES(?, ?, ?)",
            (row, hi, cnt),
        )
        return True
    return False


def _normalize(conn: sqlite3.Connection, start: int, end: int) -> None:
    """Merge adjacent equally-counted segments around [start, end)."""
    if start >= end:
        return
    rows = conn.execute(
        "SELECT lo, hi, cnt FROM segments WHERE lo >= ? AND lo < ? "
        "ORDER BY lo",
        (start, end),
    ).fetchall()
    if not rows:
        return
    # ``start`` and ``end`` are boundaries by construction, so only the
    # immediate neighbours can merge with this span; no wider scan is needed.
    left = conn.execute(
        "SELECT lo, hi, cnt FROM segments WHERE hi = ?", (start,)
    ).fetchone()
    right = conn.execute(
        "SELECT lo, hi, cnt FROM segments WHERE lo = ?", (end,)
    ).fetchone()

    combined: list[list[int]] = []
    if left is not None:
        combined.append([left["lo"], left["hi"], left["cnt"]])
    combined.extend([row["lo"], row["hi"], row["cnt"]] for row in rows)
    if right is not None:
        combined.append([right["lo"], right["hi"], right["cnt"]])

    merged: list[list[int]] = []
    for lo, hi, cnt in combined:
        if merged and merged[-1][2] == cnt and merged[-1][1] == lo:
            merged[-1][1] = hi
        else:
            merged.append([lo, hi, cnt])

    conn.execute(
        "DELETE FROM segments WHERE lo >= ? AND lo < ?",
        (combined[0][0], combined[-1][1]),
    )
    conn.executemany(
        "INSERT INTO segments(lo, hi, cnt) VALUES(?, ?, ?)",
        [(lo, hi, cnt) for lo, hi, cnt in merged],
    )


def _adjust(conn: sqlite3.Connection, start: int, end: int, delta: int) -> None:
    """Add ``delta`` to the complete-count over [start, end)."""
    if start >= end:
        return
    _split_at(conn, start)
    _split_at(conn, end)
    conn.execute(
        "UPDATE segments SET cnt = cnt + ? WHERE lo >= ? AND lo < ?",
        (delta, start, end),
    )
    negative = conn.execute(
        "SELECT lo FROM segments WHERE cnt < 0 AND lo >= ? AND lo < ? LIMIT 1",
        (start, end),
    ).fetchone()
    if negative is not None:
        raise GapError("internal error: negative completeness count")
    _normalize(conn, start, end)


def _complete_within(
    conn: sqlite3.Connection, region: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """Positions with a positive count, restricted to ``region``.

    Segments partition the plan contiguously, so the walk starts at the
    segment containing each region start and stops at the region end; it never
    scans segments of disjoint historical ranges.
    """
    complete: list[tuple[int, int]] = []
    for start, end in region:
        row = conn.execute(
            "SELECT lo, hi, cnt FROM segments WHERE lo <= ? "
            "ORDER BY lo DESC LIMIT 1",
            (start,),
        ).fetchone()
        if row is None:
            continue
        cursor = row["lo"]
        while cursor < end:
            rows = conn.execute(
                "SELECT lo, hi, cnt FROM segments WHERE lo = ?", (cursor,)
            ).fetchall()
            if not rows:
                break
            segment = rows[0]
            if segment["cnt"] > 0:
                low = max(segment["lo"], start)
                high = min(segment["hi"], end)
                if low < high:
                    complete.append((low, high))
            cursor = segment["hi"]
    return merge_intervals(complete)


def _pending(conn: sqlite3.Connection, plan: Plan) -> list[tuple[int, int]]:
    pending: list[tuple[int, int]] = []
    cursor = plan.start
    while cursor < plan.end:
        row = conn.execute(
            "SELECT lo, hi, cnt FROM segments WHERE lo = ?", (cursor,)
        ).fetchone()
        if row is None:
            break
        if row["cnt"] <= 0:
            pending.append((max(row["lo"], plan.start),
                            min(row["hi"], plan.end)))
        cursor = row["hi"]
    return merge_intervals(pending)


def _source_intervals(
    conn: sqlite3.Connection, source_id: str
) -> list[tuple[int, int]]:
    rows = conn.execute(
        "SELECT lo, hi FROM source_intervals WHERE source_id = ? ORDER BY lo",
        (source_id,),
    ).fetchall()
    return [(row["lo"], row["hi"]) for row in rows]


def _replace_source_intervals(
    conn: sqlite3.Connection,
    source_id: str,
    old: list[tuple[int, int]],
    new: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    """Rewrite one source's intervals and return the changed region."""
    added = subtract_intervals(new, old)
    removed = subtract_intervals(old, new)

    changed: list[tuple[int, int]] = []
    for start, end in removed:
        _adjust(conn, start, end, -1)
        changed.append((start, end))
    for start, end in added:
        _adjust(conn, start, end, +1)
        changed.append((start, end))

    if added or removed:
        # Rewrite the whole interval list for this source.  The list is the
        # source's own coverage, not historical coverage elsewhere; an upsert
        # may legitimately change it at many points.
        conn.execute(
            "DELETE FROM source_intervals WHERE source_id = ?", (source_id,)
        )
        conn.executemany(
            "INSERT INTO source_intervals(source_id, lo, hi) VALUES(?, ?, ?)",
            [(source_id, start, end) for start, end in new],
        )
    return merge_intervals(changed)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_ledger(workspace: str, plan_path: str) -> None:
    """Create a fresh ledger; never overwrites an existing one.

    The database is built in a sibling temporary file and only linked into
    place once the schema and the empty plan segment are durably committed,
    so an interrupted init cannot leave a half-built ledger behind.
    """
    plan_data, _ = load_json_file(plan_path, "plan")
    plan = parse_plan(plan_data)
    if not os.path.isdir(workspace):
        raise GapError(f"workspace {workspace} is not an existing directory")

    path = db_path(workspace)
    if os.path.exists(path):
        raise GapError(
            f"a gap ledger already exists at {path}; refusing to overwrite it"
        )
    handle, tmp_path = tempfile.mkstemp(
        prefix=".narc-gap-init-", suffix=".sqlite3", dir=workspace
    )
    os.close(handle)
    conn = None
    try:
        conn = sqlite3.connect(tmp_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO segments(lo, hi, cnt) VALUES(?, ?, 0)",
            (plan.start, plan.end),
        )
        _write_meta(
            conn,
            {
                "ledger_format": str(LEDGER_FORMAT),
                "plan": json.dumps(plan.to_json(), sort_keys=True),
                "revision": "0",
                "last_digest": "",
                "last_slip": "",
            },
        )
        conn.commit()
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.close()
        conn = None
        try:
            os.link(tmp_path, path)  # atomic, fails if the target exists
        except FileExistsError:
            raise GapError(
                f"a gap ledger already exists at {path}; refusing to "
                "overwrite it"
            )
        _fsync_directory(workspace)
    except sqlite3.Error as exc:
        raise GapError(f"cannot create gap ledger {path}: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _fsync_directory(directory: str) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def apply_batch(workspace: str, batch_path: str) -> bytes:
    """Apply one batch and return the change-slip bytes."""
    data, raw = load_json_file(batch_path, "batch")
    digest = hashlib.sha256(raw).hexdigest()
    batch = parse_batch(data, digest)

    path = db_path(workspace)
    if not os.path.exists(path):
        raise GapError(f"no gap ledger at {path}; run gap-init first")
    try:
        conn = sqlite3.connect(path, isolation_level=None, timeout=30)
    except sqlite3.Error as exc:
        raise GapError(f"cannot open gap ledger {path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN IMMEDIATE")
        _require_existing_schema(conn)
        meta = _read_meta(conn)
        plan = _plan_from_meta(meta)
        current = _current_revision(meta)

        # ---- retry / ordering contract ----------------------------------
        if batch.revision == current:
            if meta.get("last_digest") == digest and meta.get("last_slip"):
                slip_bytes = meta["last_slip"].encode("utf-8")
                conn.rollback()
                return slip_bytes
            raise GapError(
                f"revision {batch.revision} is already committed with different "
                "content; refusing to replay it"
            )
        if batch.revision < current + 1:
            raise GapError(
                f"revision {batch.revision} is older than the next revision "
                f"{current + 1}; refusing an out-of-order batch"
            )
        if batch.revision > current + 1:
            raise GapError(
                f"revision {batch.revision} skips the next revision "
                f"{current + 1}; revisions must be consecutive"
            )

        # Removals are validated with one point lookup per changed source, so
        # a batch never has to walk the whole historical source list.
        for change in batch.changes:
            if change.is_upsert:
                continue
            row = conn.execute(
                "SELECT 1 FROM sources WHERE source_id = ?",
                (change.source_id,),
            ).fetchone()
            if row is None:
                raise GapError(
                    f"cannot remove source {change.source_id!r}: it is not "
                    "registered in the ledger"
                )
        # ---- scan only this batch's upserted archives -------------------
        scanned: dict[str, tuple[list[tuple[int, int]], dict]] = {}
        for change in batch.changes:
            if not change.is_upsert:
                continue
            complete, identity = scan_fragment(
                change.archive, change.fragment_id, plan
            )
            scanned[change.source_id] = (
                clip_intervals(complete, plan.start, plan.end),
                identity,
            )

        # ---- compute the changed region per touched source --------------
        changes_and_region: list[tuple] = []
        region: list[tuple[int, int]] = []
        for change in batch.changes:
            source_id = change.source_id
            old = _source_intervals(conn, source_id)
            new = scanned[source_id][0] if change.is_upsert else []
            changes_and_region.append((change, old, new))
            region = union_intervals(region, subtract_intervals(new, old))
            region = union_intervals(region, subtract_intervals(old, new))
        region = clip_intervals(merge_intervals(region), plan.start, plan.end)

        # ---- completeness before, then apply, then completeness after ---
        complete_before = _complete_within(conn, region)

        for change, old, new in changes_and_region:
            source_id = change.source_id
            _replace_source_intervals(conn, source_id, old, new)
            if change.is_upsert:
                identity = scanned[source_id][1]
                conn.execute(
                    "INSERT INTO sources"
                    "(source_id, archive, fragment_id, revision, size, sha256) "
                    "VALUES(?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(source_id) DO UPDATE SET "
                    "archive=excluded.archive, fragment_id=excluded.fragment_id, "
                    "revision=excluded.revision, size=excluded.size, "
                    "sha256=excluded.sha256",
                    (
                        source_id,
                        change.archive,
                        change.fragment_id,
                        batch.revision,
                        identity["size"],
                        identity["sha256"],
                    ),
                )
            else:
                conn.execute(
                    "DELETE FROM sources WHERE source_id = ?", (source_id,)
                )

        complete_after = _complete_within(conn, region)

        opened = clip_intervals(
            subtract_intervals(complete_before, complete_after),
            plan.start,
            plan.end,
        )
        resolved = clip_intervals(
            subtract_intervals(complete_after, complete_before),
            plan.start,
            plan.end,
        )

        slip = {
            "revision": batch.revision,
            "opened": [[start, end] for start, end in opened],
            "resolved": [[start, end] for start, end in resolved],
        }
        slip_bytes = serialize_slip(slip)

        _write_meta(
            conn,
            {
                "revision": str(batch.revision),
                "last_digest": digest,
                "last_slip": slip_bytes.decode("utf-8"),
            },
        )
        conn.commit()
        return slip_bytes
    except BaseException:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def pending_intervals(workspace: str) -> list[tuple[int, int]]:
    """Whole-plan positions still to be acquired (never re-reads archives)."""
    conn = _connect(db_path(workspace), create=False)
    try:
        _require_existing_schema(conn)
        meta = _read_meta(conn)
        plan = _plan_from_meta(meta)
        return _pending(conn, plan)
    finally:
        conn.close()


def read_plan(workspace: str) -> Plan:
    conn = _connect(db_path(workspace), create=False)
    try:
        _require_existing_schema(conn)
        return _plan_from_meta(_read_meta(conn))
    finally:
        conn.close()
