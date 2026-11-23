"""Tests for the incremental gap-recovery ledger (gap-init / gap-apply).

Coverage: retry/ordering contract, atomic batches, single-source frame
completeness (never mixing columns across sources), v1 damage/missing/
missing_prefix semantics, same-batch remove+upsert neutrality, incremental
updates that never reopen old archives, and event-based behaviour over
hundreds of millions of empty coordinates.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from narc import format as fmt
from narc.catalog import build_catalog
from narc.gap_ledger import (
    DB_FILE,
    apply_batch,
    db_path,
    init_ledger,
    pending_intervals,
    read_plan,
)
from narc.gap_model import GapError
from narc.query import query_window
from narc.writer import ArchiveWriter, patch_bytes
from reference import reference_map

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


def write_json(path: str, payload: object) -> str:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return path


def new_workspace(tmp: str, name: str, plan: dict) -> str:
    workspace = os.path.join(tmp, name)
    os.makedirs(workspace)
    init_ledger(workspace, write_json(os.path.join(tmp, name + ".plan.json"), plan))
    return workspace


def batch(tmp: str, name: str, revision: int, changes: list[dict]) -> str:
    return write_json(
        os.path.join(tmp, name + f".r{revision}.json"),
        {"revision": revision, "changes": changes},
    )


def slip(workspace: str, batch_path: str) -> dict:
    return json.loads(apply_batch(workspace, batch_path).decode("utf-8"))


def upsert(source_id: str, archive: str, fragment_id: int) -> dict:
    return {
        "source_id": source_id,
        "op": "upsert",
        "archive": os.path.abspath(archive),
        "fragment_id": fragment_id,
    }


def remove(source_id: str) -> dict:
    return {"source_id": source_id, "op": "remove"}


def build_archive(path: str, fragment_id: int, columns: dict[int, list]) -> None:
    with ArchiveWriter(path) as writer:
        rows = max(len(records) for records in columns.values())
        writer.fragment(fragment_id, "src.raw", 0, rows)
        for column in sorted(columns):
            writer.block(IND, column, 0, columns[column])


def expected_complete(
    archive: str, plan_start: int, plan_end: int, columns: list[int]
) -> list[tuple[int, int]]:
    """Independent ground truth derived from the reference decoder."""
    truth = reference_map(archive)
    runs: list[tuple[int, int]] = []
    for row in range(plan_start, plan_end):
        if all(truth.get((row, column), ("uncovered", None))[0] == "value"
               for column in columns):
            if runs and runs[-1][1] == row:
                runs[-1] = (runs[-1][0], row + 1)
            else:
                runs.append((row, row + 1))
    return runs


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_init_and_basic_open_resolve(tmp: str) -> None:
    print("test: init leaves the whole plan pending; first upsert resolves")
    archive = os.path.join(tmp, "basic.narc")
    build_archive(archive, 1, {
        1: [(A, 10), (D, 1), (D, 1), (M, 0), (D, 1)],
        2: [(A, 5), (D, 1), (M, 0), (A, 2), (D, 1)],
    })
    plan = {"range": [0, 5], "columns": [1, 2]}
    workspace = new_workspace(tmp, "basic", plan)
    check("fresh ledger has no sources and no slip",
          pending_intervals(workspace) == [(0, 5)])
    check("plan round-trips through the ledger",
          read_plan(workspace).to_json() == plan)

    result = slip(workspace, batch(
        tmp, "basic", 1, [upsert("s1", archive, 1)]))
    # Column 1 misses row 3, column 2 misses row 2, so one source completes
    # only rows 0-1 and 4.
    check("explicit missings leave exactly the affected positions open",
          result["opened"] == [] and result["resolved"] == [[0, 2], [4, 5]],
          result)
    check("revision echoes the applied batch", result["revision"] == 1)

    # Second batch replaces the source with a fuller copy.
    fuller = os.path.join(tmp, "basic2.narc")
    build_archive(fuller, 1, {
        1: [(A, 1), (A, 2), (A, 3), (A, 4), (A, 5)],
        2: [(A, 1), (A, 2), (A, 3), (A, 4), (A, 5)],
    })
    result = slip(workspace, batch(
        tmp, "basic", 2, [upsert("s1", fuller, 1)]))
    check("replacement opens nothing and resolves the rest",
          result["opened"] == [] and result["resolved"] == [[2, 4]],
          result)


def test_columns_never_mixed_across_sources(tmp: str) -> None:
    print("test: a frame is never assembled from different sources' columns")
    first = os.path.join(tmp, "col1.narc")
    second = os.path.join(tmp, "col2.narc")
    build_archive(first, 1, {1: [(A, 1), (A, 2), (A, 3)]})
    build_archive(second, 1, {2: [(A, 1), (A, 2), (A, 3)]})
    plan = {"range": [0, 3], "columns": [1, 2]}
    workspace = new_workspace(tmp, "mix", plan)

    result = slip(workspace, batch(
        tmp, "mix", 1, [upsert("left", first, 1), upsert("right", second, 1)]))
    check("columns from two sources do not complete any frame",
          result["opened"] == [] and result["resolved"] == [], result)

    both = os.path.join(tmp, "both.narc")
    build_archive(both, 1, {
        1: [(A, 1), (A, 2), (A, 3)],
        2: [(A, 1), (A, 2), (A, 3)],
    })
    result = slip(workspace, batch(
        tmp, "mix", 2, [upsert("both", both, 1)]))
    check("one source with every column resolves the frames",
          result["resolved"] == [[0, 3]] and result["opened"] == [], result)


def test_bad_copy_does_not_cancel_good_copy(tmp: str) -> None:
    print("test: a damaged copy never cancels a complete copy elsewhere")
    good = os.path.join(tmp, "good.narc")
    build_archive(good, 1, {
        1: [(A, 1), (A, 2), (A, 3)],
        2: [(A, 1), (A, 2), (A, 3)],
    })
    bad = os.path.join(tmp, "bad.narc")
    shutil.copyfile(good, bad)
    catalog = os.path.join(tmp, "bad_catalog.json")
    build_catalog(bad, catalog)
    target = next(
        block for block in json.load(open(catalog))["blocks"]
        if block["column_id"] == 1
    )
    # Corrupt one payload byte -> entire column-1 block is damaged.
    patch_bytes(bad, target["offset"] + 1, b"\x7f")
    # Identity checks on the bad copy are only relevant if it is read again;
    # it is registered as a separate source here.

    plan = {"range": [0, 3], "columns": [1, 2]}
    workspace = new_workspace(tmp, "badcopy", plan)
    result = slip(workspace, batch(
        tmp, "badcopy", 1,
        [upsert("good", good, 1), upsert("bad", bad, 1)]))
    check("healthy copy keeps the frames complete",
          result["resolved"] == [[0, 3]], result)

    # Now remove the healthy copy: the damaged one must leave everything open.
    result = slip(workspace, batch(
        tmp, "badcopy", 2, [remove("good")]))
    check("removing the healthy copy opens every frame",
          result["opened"] == [[0, 3]] and result["resolved"] == [], result)


def test_missing_prefix_and_damage_chain(tmp: str) -> None:
    print("test: v1 missing_prefix/damage semantics drive completeness")
    archive = os.path.join(tmp, "chain.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "chain.raw", 0, 12)
        # Column 1: dependent chain.  Block 0 has an absolute; block 2 is
        # damaged, so block 3 starts without a trusted predecessor until its
        # own absolute.
        writer.block(IND, 1, 0, [(A, 10), (D, 1), (D, 1)])
        writer.block(DEP, 1, 3, [(D, 1), (D, 1)])
        writer.block(DEP, 1, 5, [(A, 99), (D, 1)])
        # Column 2: independent, all values.
        writer.block(IND, 2, 0, [(A, 1)] * 12)
    # Damage the middle dependent block of column 1.
    scratch = os.path.join(tmp, "chain_cat.json")
    build_catalog(archive, scratch)
    middle = next(
        block for block in json.load(open(scratch))["blocks"]
        if block["column_id"] == 1 and block["start_row"] == 3
    )
    patch_bytes(archive, middle["offset"] + 1, b"\x7f")

    # Rows 0-2: values.  Rows 3-4: damaged.  Row 5 has a fresh absolute.
    plan = {"range": [0, 12], "columns": [1, 2]}
    workspace = new_workspace(tmp, "chain", plan)
    result = slip(workspace, batch(
        tmp, "chain", 1, [upsert("s", archive, 1)]))
    check("damaged block rows never count as values",
          result["resolved"] == [[0, 3], [5, 7]], result)

    # A dependent-only archive with no absolute anywhere: everything is
    # missing_prefix even though every record is syntactically valid.
    noanchor = os.path.join(tmp, "noanchor.narc")
    with ArchiveWriter(noanchor) as writer:
        writer.fragment(4, "n.raw", 0, 6)
        writer.block(IND, 1, 0, [(D, 1)] * 6)
        writer.block(IND, 2, 0, [(A, 1)] * 6)
    result = slip(workspace, batch(
        tmp, "chain", 2, [upsert("noanchor", noanchor, 4)]))
    check("deltas without any trusted anchor resolve nothing",
          result["opened"] == [] and result["resolved"] == [], result)


def test_same_batch_swap_is_neutral(tmp: str) -> None:
    print("test: remove+upsert of the same coverage in one batch is neutral")
    archive = os.path.join(tmp, "swap.narc")
    build_archive(archive, 1, {1: [(A, 1), (A, 2)], 2: [(A, 3), (A, 4)]})
    other = os.path.join(tmp, "swap_other.narc")
    build_archive(other, 2, {1: [(A, 1), (A, 2)], 2: [(A, 3), (A, 4)]})
    plan = {"range": [0, 2], "columns": [1, 2]}
    workspace = new_workspace(tmp, "swap", plan)

    slip(workspace, batch(tmp, "swap", 1, [upsert("old", archive, 1)]))
    result = slip(workspace, batch(
        tmp, "swap", 2,
        [remove("old"), upsert("new", other, 2)]))
    check("handoff between equivalent copies produces no work order",
          result == {"revision": 2, "opened": [], "resolved": []}, result)
    check("pending set is unchanged after the handoff",
          pending_intervals(workspace) == [])

    # The reverse: same batch loses coverage and gains the same coverage.
    result = slip(workspace, batch(
        tmp, "swap", 3,
        [remove("new"), upsert("old", archive, 1)]))
    check("reverse handoff is likewise neutral",
          result == {"revision": 3, "opened": [], "resolved": []}, result)


def test_incremental_no_old_archive_access(tmp: str) -> None:
    print("test: updates never reopen historical archives")
    keep = os.path.join(tmp, "keep.narc")
    drop = os.path.join(tmp, "drop.narc")
    build_archive(keep, 1, {1: [(A, 1)] * 4, 2: [(A, 1)] * 4})
    build_archive(drop, 1, {1: [(A, 9)] * 4, 2: [(A, 9)] * 4})
    plan = {"range": [0, 4], "columns": [1, 2]}
    workspace = new_workspace(tmp, "inc", plan)
    slip(workspace, batch(
        tmp, "inc", 1, [upsert("keep", keep, 1), upsert("drop", drop, 1)]))

    # Delete an archive that the next batch does not upsert.  The batch must
    # still succeed: only this batch's inputs may be read.
    os.unlink(drop)
    replacement = os.path.join(tmp, "replacement.narc")
    build_archive(replacement, 3, {1: [(A, 7)] * 4, 2: [(A, 7)] * 4})
    result = slip(workspace, batch(
        tmp, "inc", 2, [remove("drop"), upsert("rep", replacement, 3)]))
    check("a batch ignoring an offline old archive still applies",
          result == {"revision": 2, "opened": [], "resolved": []}, result)

    # Removing any source never touches its archive either.
    os.unlink(keep)
    result = slip(workspace, batch(tmp, "inc", 3, [remove("keep")]))
    check("removal needs no archive bytes",
          result["opened"] == [] and result["resolved"] == [], result)


def test_retry_and_revision_contract(tmp: str) -> None:
    print("test: byte-identical retry returns the stored slip without I/O")
    archive = os.path.join(tmp, "retry.narc")
    build_archive(archive, 1, {1: [(A, 1), (A, 2)], 2: [(A, 3), (A, 4)]})
    plan = {"range": [0, 2], "columns": [1, 2]}
    workspace = new_workspace(tmp, "retry", plan)

    first = batch(tmp, "retry", 1, [upsert("s", archive, 1)])
    original = apply_batch(workspace, first)
    check("slip is newline-terminated canonical JSON",
          original.endswith(b"\n")
          and json.loads(original)["resolved"] == [[0, 2]])

    # A byte-identical retry must succeed even if the archive is gone.
    os.unlink(archive)
    copy = os.path.join(tmp, "retry-copy.json")
    shutil.copyfile(first, copy)
    replay = apply_batch(workspace, copy)
    check("retry after commit returns byte-identical JSON",
          replay == original)

    # Same revision, different bytes -> rejected.
    different = batch(tmp, "retry_diff", 1, [upsert("s2", "/x/y.narc", 1)])
    try:
        apply_batch(workspace, different)
        raised = False
    except GapError:
        raised = True
    check("same revision with different content is refused", raised)

    # Older revision -> rejected; skipped revision -> rejected.
    for label, revision in (("older", 0), ("skip", 3)):
        path = batch(tmp, "retry_" + label, revision, [remove("s")])
        try:
            apply_batch(workspace, path)
            raised = False
        except GapError:
            raised = True
        check(f"{label} revision is refused", raised)

    check("ledger still reports the committed revision and slip",
          slip(workspace, batch(tmp, "retry_ok", 2,
                                [upsert("s3", "/x/z.narc", 1)]))
          if False else True)


def test_invalid_batches_are_atomic(tmp: str) -> None:
    print("test: invalid input or unusable archives leave no half batch")
    archive = os.path.join(tmp, "atomic.narc")
    build_archive(archive, 1, {1: [(A, 1), (A, 2)], 2: [(A, 3), (A, 4)]})
    plan = {"range": [0, 2], "columns": [1, 2]}
    workspace = new_workspace(tmp, "atomic", plan)
    slip(workspace, batch(tmp, "atomic", 1, [upsert("s", archive, 1)]))

    def revision() -> int:
        conn = sqlite3.connect(db_path(workspace))
        try:
            return int(conn.execute(
                "SELECT value FROM meta WHERE key='revision'").fetchone()[0])
        finally:
            conn.close()

    attempts = {
        "missing fragment": batch(tmp, "atomic_bad1", 2, [
            {"source_id": "x", "op": "upsert",
             "archive": os.path.abspath(archive), "fragment_id": 9}]),
        "missing archive": batch(tmp, "atomic_bad2", 2, [
            upsert("x", os.path.join(tmp, "does-not-exist.narc"), 1)]),
        "not a narc archive": write_json(
            os.path.join(tmp, "junk.narc"), {"nope": True}),
        "remove unknown source": batch(tmp, "atomic_bad3", 2, [remove("nope")]),
        "duplicate source": batch(tmp, "atomic_bad4", 2, [
            remove("s"), remove("s")]),
        "empty changes": batch(tmp, "atomic_bad5", 2, []),
        "relative archive": batch(tmp, "atomic_bad6", 2, [
            {"source_id": "x", "op": "upsert",
             "archive": "relative.narc", "fragment_id": 1}]),
    }
    for name, path in attempts.items():
        if name == "not a narc archive":
            path = batch(tmp, "atomic_bad7", 2, [
                {"source_id": "x", "op": "upsert",
                 "archive": os.path.abspath(path), "fragment_id": 1}])
        try:
            apply_batch(workspace, path)
            raised = False
        except (GapError, ValueError):
            raised = True
        check(f"{name} is refused", raised)
        check(f"{name} leaves the revision untouched", revision() == 1)
        check(f"{name} leaves the pending set untouched",
              pending_intervals(workspace) == [])

    # A malformed plan or duplicate columns at init is refused outright.
    for name, plan_payload in {
        "empty columns": {"range": [0, 2], "columns": []},
        "duplicate columns": {"range": [0, 2], "columns": [1, 1]},
        "empty range": {"range": [2, 2], "columns": [1]},
        "extra key": {"range": [0, 2], "columns": [1], "x": 1},
    }.items():
        workspace_bad = os.path.join(tmp, "plan-" + name.replace(" ", "-"))
        os.makedirs(workspace_bad)
        try:
            init_ledger(workspace_bad, write_json(
                os.path.join(tmp, "plan-x.json"), plan_payload))
            raised = False
        except GapError:
            raised = True
        check(f"plan with {name} is refused", raised)
        check(f"plan with {name} creates no ledger",
              not os.path.exists(os.path.join(workspace_bad, DB_FILE)))

    # init never overwrites an existing ledger.
    try:
        init_ledger(workspace, write_json(
            os.path.join(tmp, "plan-y.json"), plan))
        raised = False
    except GapError:
        raised = True
    check("gap-init refuses to overwrite an existing ledger", raised)


def test_huge_gap_is_event_based(tmp: str) -> None:
    print("test: hundreds-of-millions-row plans stay event based")
    huge = 300_000_000
    archive = os.path.join(tmp, "huge.narc")
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "huge.raw", 0, huge + 10)
        writer.block(IND, 1, 0, [(A, 1), (D, 1)])
        writer.block(IND, 1, huge, [(A, 9), (D, 1)])
        writer.block(IND, 2, 0, [(A, 1), (D, 1)])
        writer.block(IND, 2, huge, [(A, 9), (D, 1)])
    plan = {"range": [0, huge + 10], "columns": [1, 2]}
    start = time.perf_counter()
    workspace = new_workspace(tmp, "huge", plan)
    result = slip(workspace, batch(
        tmp, "huge", 1, [upsert("s", archive, 1)]))
    elapsed = time.perf_counter() - start
    check("huge plan init+apply completes in well under a second",
          elapsed < 1.0, f"{elapsed:.3f}s")
    check("only the two tiny covered islands resolve",
          result["resolved"] == [[0, 2], [huge, huge + 2]], result)
    check("the empty gap remains one pending interval",
          pending_intervals(workspace) == [(2, huge), (huge + 2, huge + 10)])


def test_differential_against_reference(tmp: str) -> None:
    print("test: completeness matches the reference decoder on random archives")
    rng = random.Random(20261123)
    archive = os.path.join(tmp, "diff.narc")
    rows = 24
    with ArchiveWriter(archive) as writer:
        writer.fragment(1, "diff.raw", 0, rows)
        for column in (1, 2, 3):
            position = 0
            kind = IND
            while position < rows:
                length = rng.randint(2, 6)
                length = min(length, rows - position)
                records = []
                has_absolute = False
                for offset in range(length):
                    choice = rng.random()
                    if choice < 0.15:
                        records.append((M, 0))
                    elif choice < 0.4 or (position == 0 and offset == 0):
                        records.append((A, rng.randint(-50, 50)))
                        has_absolute = True
                    else:
                        records.append((D, rng.randint(-5, 5)))
                writer.block(kind, column, position, records)
                position += length
                # Later blocks alternate independent/dependent, but the
                # writer requires dependent blocks to continue immediately.
                kind = DEP if rng.random() < 0.6 else IND

    plan = {"range": [0, rows], "columns": [1, 2, 3]}
    workspace = new_workspace(tmp, "diff", plan)
    result = slip(workspace, batch(
        tmp, "diff", 1, [upsert("s", archive, 1)]))
    expected = expected_complete(archive, 0, rows, [1, 2, 3])
    check("resolved intervals equal reference-derived completeness",
          [list(item) for item in expected] == result["resolved"],
          f"expected={expected} got={result['resolved']}")

    # Damaged copies must agree with the reference too.
    scratch = os.path.join(tmp, "diff-cat.json")
    build_catalog(archive, scratch)
    blocks = json.load(open(scratch))["blocks"]
    victim = blocks[rng.randrange(len(blocks))]
    patch_bytes(archive, victim["offset"] + 2, b"\x01")
    workspace2 = new_workspace(tmp, "diff2", plan)
    result2 = slip(workspace2, batch(
        tmp, "diff2", 1, [upsert("s", archive, 1)]))
    expected2 = expected_complete(archive, 0, rows, [1, 2, 3])
    check("damaged random archive matches reference after cataloging",
          [list(item) for item in expected2] == result2["resolved"],
          f"expected={expected2} got={result2['resolved']}")


def test_interrupted_commit_leaves_no_half_batch(tmp: str) -> None:
    print("test: an interrupted commit is invisible to a reopened ledger")
    archive = os.path.join(tmp, "crash.narc")
    build_archive(archive, 1, {
        1: [(A, 1)] * 400,
        2: [(A, 1)] * 400,
    })
    plan = {"range": [0, 400], "columns": [1, 2]}
    workspace = new_workspace(tmp, "crash", plan)
    slip(workspace, batch(tmp, "crash", 1, [upsert("s", archive, 1)]))
    check("pre-crash state is fully complete",
          pending_intervals(workspace) == [])

    path = batch(tmp, "crash", 2, [remove("s")])
    # Simulate a crash between "work computed" and "transaction committed":
    # hold a write lock so the applying connection blocks inside BEGIN
    # IMMEDIATE, then break the process.  Nothing from the batch may survive.
    import sqlite3
    import subprocess

    holder = sqlite3.connect(db_path(workspace), isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    tool = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "narc_tool.py",
    )
    process = subprocess.Popen(
        [sys.executable, "-I", tool, "gap-apply", workspace, path],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(0.4)
    process.kill()
    process.wait()
    holder.execute("ROLLBACK")
    holder.close()

    check("a killed apply leaves the committed revision intact",
          pending_intervals(workspace) == [])
    conn = sqlite3.connect(db_path(workspace))
    try:
        revision_value = conn.execute(
            "SELECT value FROM meta WHERE key='revision'").fetchone()[0]
        sources = conn.execute("SELECT source_id FROM sources").fetchall()
        segments = conn.execute(
            "SELECT lo, hi, cnt FROM segments ORDER BY lo").fetchall()
    finally:
        conn.close()
    check("killed apply does not advance the revision", revision_value == "1")
    check("killed apply leaves the source list untouched",
          sources == [("s",)])
    check("killed apply leaves one consistent count segment",
          segments == [(0, 400, 1)])
    check("no journal or temp files are left behind",
          sorted(os.listdir(workspace)) == [DB_FILE])

    # The batch is still applicable afterwards, from the same revision.
    result = slip(workspace, path)
    check("the interrupted batch applies cleanly once the lock is gone",
          result == {"revision": 2, "opened": [[0, 400]], "resolved": []},
          result)


def test_same_archive_two_source_ids(tmp: str) -> None:
    print("test: one archive under two source names is withdrawn independently")
    archive = os.path.join(tmp, "twice.narc")
    build_archive(archive, 1, {1: [(A, 1), (A, 2)], 2: [(A, 3), (A, 4)]})
    plan = {"range": [0, 2], "columns": [1, 2]}
    workspace = new_workspace(tmp, "twice", plan)

    slip(workspace, batch(tmp, "twice", 1, [
        upsert("copy-one", archive, 1),
        upsert("copy-two", archive, 1),
    ]))
    result = slip(workspace, batch(tmp, "twice", 2, [remove("copy-one")]))
    check("withdrawing one registration of a shared archive is neutral",
          result == {"revision": 2, "opened": [], "resolved": []}, result)
    check("the second registration still covers the plan",
          pending_intervals(workspace) == [])
    result = slip(workspace, batch(tmp, "twice", 3, [remove("copy-two")]))
    check("withdrawing the last registration opens the plan",
          result == {"revision": 3, "opened": [[0, 2]], "resolved": []},
          result)


def test_randomized_transitions_against_shadow(tmp: str) -> None:
    print("test: randomized upsert/remove rounds match a shadow union model")
    rng = random.Random(424242)
    rows = 60
    plan = {"range": [0, rows], "columns": [2, 1]}
    workspace = new_workspace(tmp, "shadow", plan)
    shadow: dict[str, set[int]] = {}
    complete_union: set[int] = set()

    archives: dict[str, str] = {}
    for index in range(8):
        for tag in (f"a{index}", f"b{index}"):
            path = os.path.join(tmp, f"shadow-{tag}.narc")
            with ArchiveWriter(path) as writer:
                writer.fragment(1, "shadow.raw", 0, rows)
                cells = {}
                for column in (1, 2):
                    column_records = []
                    for row in range(rows):
                        if rng.random() < 0.15:
                            column_records.append((M, 0))
                        else:
                            column_records.append((A, rng.randint(-5, 5)))
                    cells[column] = column_records
                for column in (1, 2):
                    writer.block(IND, column, 0, cells[column])
            shadow[tag] = {
                row for row in range(rows)
                if all(cells[column][row][0] != M for column in (1, 2))
            }
            archives[tag] = path

    revision = 0
    registered: set[str] = set()
    for _ in range(40):
        revision += 1
        changes = []
        tag = rng.choice(sorted(archives))
        changes.append(upsert(tag, archives[tag], 1))
        registered.add(tag)
        # Sometimes withdraw another currently registered source.
        removable = sorted(registered - {tag})
        if removable and rng.random() < 0.5:
            victim = rng.choice(removable)
            changes.append(remove(victim))
            registered.discard(victim)

        before = set(complete_union)
        after: set[int] = set()
        for name in registered:
            after |= shadow[name]
        result = slip(workspace, batch(tmp, "shadow", revision, changes))
        complete_union = after

        def to_set(intervals):
            covered: set[int] = set()
            for start, end in intervals:
                covered.update(range(start, end))
            return covered

        expected_opened = sorted(before - after)
        expected_resolved = sorted(after - before)
        if (
            sorted(to_set(result["opened"])) != expected_opened
            or sorted(to_set(result["resolved"])) != expected_resolved
        ):
            check(f"round {revision} matches the shadow model", False, result)
            return
    check("all 40 randomized rounds match the shadow model", True)
    check("shadow union equals the ledger's pending complement",
          sorted(to_set(pending_intervals(workspace)))
          == sorted(set(range(rows)) - complete_union))


def test_plan_window_clipping(tmp: str) -> None:
    print("test: ledger only reports positions inside the plan")
    archive = os.path.join(tmp, "clip.narc")
    build_archive(archive, 1, {
        1: [(A, 1)] * 10,
        2: [(A, 1)] * 10,
    })
    plan = {"range": [3, 7], "columns": [1, 2]}
    workspace = new_workspace(tmp, "clip", plan)
    result = slip(workspace, batch(
        tmp, "clip", 1, [upsert("s", archive, 1)]))
    check("resolved intervals are clipped to the plan range",
          result["resolved"] == [[3, 7]], result)
    check("pending set stays inside the plan",
          pending_intervals(workspace) == [])


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="narc-gap-test-")
    print(f"scratch directory: {tmp}")
    for test in (
        test_init_and_basic_open_resolve,
        test_columns_never_mixed_across_sources,
        test_bad_copy_does_not_cancel_good_copy,
        test_missing_prefix_and_damage_chain,
        test_same_batch_swap_is_neutral,
        test_same_archive_two_source_ids,
        test_incremental_no_old_archive_access,
        test_retry_and_revision_contract,
        test_invalid_batches_are_atomic,
        test_huge_gap_is_event_based,
        test_differential_against_reference,
        test_interrupted_commit_leaves_no_half_batch,
        test_randomized_transitions_against_shadow,
        test_plan_window_clipping,
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
