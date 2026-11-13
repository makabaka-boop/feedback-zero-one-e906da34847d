"""Acceptance tests for the transaction service.

Run from the project directory with:

    python3 -I tests/test_acceptance.py

The suite covers: snapshot/version mechanics, the spec's X/Y example, a long
ring, empty-read phantom detection, tombstones and real nulls, read-only
transactions (including a read-only participant in a cycle), write-conflict
priority, persistence across restarts, hard-kill recovery (os._exit and real
SIGKILL), concurrency serial equivalence, and validation/phase rules.
"""

import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from transaction_service import (  # noqa: E402
    ServiceError,
    TransactionService,
    find_cycle,
)

PASS = 0
FAILURES = []


def check(name, condition, detail=""):
    global PASS
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAILURES.append((name, detail))
        print(f"  FAIL  {name}  {detail}")


def section(title):
    print(f"\n=== {title} ===")


def fresh_db():
    d = tempfile.mkdtemp(prefix="txntest_")
    return d, os.path.join(d, "svc.db")


def reopen(path):
    return TransactionService(path)


# ---------------------------------------------------------------------------
def test_unit_cycle_finder():
    section("unit: cycle finder")
    check("acyclic graph -> None", find_cycle({"a": {"b"}, "b": set(), "c": {"a"}}) is None)
    cyc = find_cycle({"a": {"b"}, "b": {"c"}, "c": {"a"}})
    check("simple 3-ring found and closed", cyc is not None and cyc[0] == cyc[-1] and len(cyc) == 4, str(cyc))
    check("self loop", find_cycle({"a": {"a"}}) == ["a", "a"])
    long_ring = {f"n{i}": {f"n{(i + 1) % 200}"} for i in range(200)}
    cyc = find_cycle(long_ring)
    check("200-node ring found", cyc is not None and len(cyc) == 201, str(cyc and len(cyc)))


# ---------------------------------------------------------------------------
def test_basic_snapshot_and_version():
    section("basic snapshot, range reads and empty-commit version bumps")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        check("initial version 0", svc.snapshot()["version"] == 0)
        check("initial snapshot empty", svc.snapshot()["rows"] == [])

        svc.begin("t1")
        check("begin fixes snapshot 0", svc.begin("t1")["snapshot"] == 0 and
              svc.begin("t1")["status"] == "open")
        r = svc.read_range("t1", None, None)
        check("full range read on empty table", r["rows"] == [] and r["snapshot"] == 0)

        res = svc.stage("t1", {"a": "1", "b": "2"})
        check("stage reports two writes", res["write_count"] == 2)
        try:
            svc.read_range("t1", None, None)
            check("read after stage rejected", False)
        except ServiceError:
            check("read after stage rejected", True)

        c = svc.commit("t1")
        check("first commit version 1", c == {"tid": "t1", "status": "committed", "version": 1}, str(c))
        check("snapshot now v1 with rows",
              svc.snapshot() == {"version": 1, "rows": [["a", "1"], ["b", "2"]]},
              str(svc.snapshot()))

        # Empty write set commit still increments the version.
        svc.begin("t2")
        svc.read_range("t2", "a", "b")
        svc.stage("t2", {})
        c = svc.commit("t2")
        check("empty commit increments version", c["version"] == 2)
        check("data unchanged after empty commit",
              svc.snapshot()["rows"] == [["a", "1"], ["b", "2"]])

        # Half-open ranges and infinity endpoints.
        svc.begin("t3")
        check("[a,b) contains only a",
              svc.read_range("t3", "a", "b")["rows"] == [["a", "1"]])
        check("[b,inf) contains only b",
              svc.read_range("t3", "b", None)["rows"] == [["b", "2"]])
        check("[a,a) empty range still succeeds",
              svc.read_range("t3", "a", "a")["rows"] == [])
        check("[inf,inf) full scan",
              svc.read_range("t3", None, None)["rows"] == [["a", "1"], ["b", "2"]])
        svc.abort("t3")

        try:
            svc.read_range("nope", None, None)
            check("unknown tid rejected", False)
        except ServiceError:
            check("unknown tid rejected", True)
        try:
            svc.read_range("z1", "z", "a")
            check("lo > hi rejected", False)
        except ServiceError:
            check("lo > hi rejected", True)

        svc2 = reopen(path)
        check("restart keeps version", svc2.snapshot()["version"] == 2)
        check("restart keeps data", svc2.snapshot()["rows"] == [["a", "1"], ["b", "2"]])
        svc.close()
        svc2.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_spec_xy_example():
    section("spec example: X/Y write-skew ring")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        svc.begin("X")
        svc.begin("Y")
        rx = svc.read_range("X", "b", "c")
        ry = svc.read_range("Y", "a", "b")
        check("X sees empty [b,c) on empty snapshot", rx["rows"] == [])
        check("Y sees empty [a,b) on empty snapshot", ry["rows"] == [])
        svc.stage("X", {"a": "1"})
        svc.stage("Y", {"b": "2"})
        cx = svc.commit("X")
        check("X commits at v1", cx["status"] == "committed" and cx["version"] == 1, str(cx))
        cy = svc.commit("Y")
        check("Y rejected with a cycle", cy["status"] == "cycle", str(cy))
        check("cycle is a closed id sequence containing X and Y",
              cy["status"] == "cycle" and cy["cycle"][0] == cy["cycle"][-1]
              and set(cy["cycle"]) == {"X", "Y"}, str(cy))
        check("version stays 1 after rejection", svc.snapshot()["version"] == 1)
        check("only X's data visible", svc.snapshot()["rows"] == [["a", "1"]])
        # Repeated commit replays the original outcome; terminal is immutable.
        again = svc.commit("Y")
        check("repeated commit replays cycle", again == cy, str(again))
        try:
            svc.abort("Y")
            check("terminal txn cannot be aborted", False)
        except ServiceError:
            check("terminal txn cannot be aborted", True)
        check("repeated begin returns original Y",
              svc.begin("Y")["status"] == "cycle")
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_long_ring():
    section("long ring: N transactions close only at the last commit")
    d, path = fresh_db()
    n = 8
    letters = [chr(ord("a") + i) for i in range(n)]
    try:
        svc = TransactionService(path)
        for i in range(n):
            tid = f"t{i}"
            svc.begin(tid)
            if i < n - 1:
                # Read the range that will contain the next txn's key.
                lo = letters[i + 1]
                hi = letters[i + 2] if i + 2 < n else None
            else:
                # Last reader covers t0's key, closing the ring back to t0.
                lo, hi = letters[0], letters[1]
            r = svc.read_range(tid, lo, hi)
            check(f"t{i} empty read registered", r["rows"] == [])
            svc.stage(tid, {letters[i]: str(i)})
        for i in range(n - 1):
            c = svc.commit(f"t{i}")
            check(f"t{i} commits before ring closes", c["status"] == "committed", str(c))
        last = svc.commit(f"t{n - 1}")
        check("last txn rejected with cycle", last["status"] == "cycle", str(last))
        check("cycle walks every transaction and closes",
              last["status"] == "cycle"
              and last["cycle"][0] == last["cycle"][-1]
              and len(last["cycle"]) == n + 1
              and set(last["cycle"][:-1]) == {f"t{i}" for i in range(n)},
              str(last["cycle"]))
        check("version counts only the n-1 successful commits",
              svc.snapshot()["version"] == n - 1, str(svc.snapshot()["version"]))
        check("only t0..t6 data visible and sorted",
              svc.snapshot()["rows"] == [[letters[i], str(i)] for i in range(n - 1)],
              str(svc.snapshot()["rows"]))
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_phantom_empty_read():
    section("phantom protection: empty reads register range dependencies")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        # Two empty-read transactions on snapshot 0; a key inserted later into
        # their ranges creates anti-dependency edges.
        svc.begin("p1")
        svc.begin("p2")
        check("p1 empty read", svc.read_range("p1", "a", "c")["rows"] == [])
        check("p2 empty read", svc.read_range("p2", "b", "d")["rows"] == [])
        # Filler transaction inserts a (in p1's range only) and commits.
        svc.begin("fill")
        svc.stage("fill", {"a": "x"})
        check("fill commits v1", svc.commit("fill")["version"] == 1)
        # p1 writes b (inside p2's [b,d)); p2 writes b9 (inside p1's [a,c)).
        # Each empty-read reader then anti-depends on the other's write, so
        # the second commit must close a phantom ring.
        svc.stage("p1", {"b": "1"})
        svc.stage("p2", {"b9": "2"})
        c1 = svc.commit("p1")
        check("p1 commits (no ring yet)", c1["status"] == "committed", str(c1))
        c2 = svc.commit("p2")
        check("p2 rejected: empty-read phantom ring", c2["status"] == "cycle", str(c2))
        check("phantom ring includes both readers",
              c2["status"] == "cycle" and {"p1", "p2"}.issubset(set(c2["cycle"])),
              str(c2))
        check("p2's b9 was never published",
              svc.snapshot()["rows"] == [["a", "x"], ["b", "1"]],
              str(svc.snapshot()["rows"]))
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_tombstones_and_nulls():
    section("tombstones, real null values, deleting missing keys")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)

        def put(tid, mapping):
            svc.begin(tid)
            svc.read_range(tid, None, None)
            svc.stage(tid, mapping)
            return svc.commit(tid)

        check("commit v1", put("w1", {"k1": "v1", "k2": "v2"})["version"] == 1)
        check("real null staged and published",
              put("w2", {"k1": None}) == {
                  "tid": "w2", "status": "committed", "version": 2})
        snap = svc.snapshot()
        check("null value is a live row distinct from deletion",
              snap["rows"] == [["k1", None], ["k2", "v2"]], str(snap))

        check("delete existing + delete missing + keep null all legal",
              put("w3", {"k2": {"delete": True}, "k3": {"delete": True}})[
                  "status"] == "committed")
        snap = svc.snapshot()
        check("tombstone hides k2, k1 null survives",
              snap["rows"] == [["k1", None]], str(snap))

        # Recreate a previously deleted key.
        check("recreate deleted key", put("w4", {"k2": "back"})["status"] == "committed")
        check("k2 visible again",
              svc.snapshot()["rows"] == [["k1", None], ["k2", "back"]],
              str(svc.snapshot()["rows"]))

        # Range reads must not return tombstoned keys.
        svc.begin("rd")
        check("range read skips tombstones and returns nulls",
              svc.read_range("rd", "k", "z")["rows"] == [["k1", None], ["k2", "back"]])
        svc.abort("rd")

        # Old snapshots: k2 must appear deleted at v3 but live at v4.
        check("snapshot v3 reflects the delete",
              svc.snapshot(3)["rows"] == [["k1", None]],
              str(svc.snapshot(3)))
        check("snapshot v2 had k2",
              svc.snapshot(2)["rows"] == [["k1", None], ["k2", "v2"]],
              str(svc.snapshot(2)))

        # A tombstone still counts as a writer for dependency edges.  Build a
        # ring in which the tombstone writer itself is a ring node:
        #   * r1 starts on an old snapshot and reads [k,z);
        #   * d1 later tombstones k2 inside that range (anti-dependency
        #     r1 -> d1) and reads the range where r1 will write k8
        #     (d1 -> r1 once r1 commits), closing d1 -> r1 -> d1;
        #   * r2 additionally exercises a read of the tombstoned key itself.
        svc.begin("r1")
        check("r1 reads live rows before the tombstone era",
              svc.read_range("r1", "k", "z")["rows"] == [["k1", None], ["k2", "back"]])

        svc.begin("d1")
        # d1 reads the range where r1 will later write k8.
        check("d1 empty range read", svc.read_range("d1", "k7", None)["rows"] == [])
        svc.stage("d1", {"k2": {"delete": True}})
        cd = svc.commit("d1")
        check("deleting k2 commits (tombstone is a successful write)",
              cd["status"] == "committed", str(cd))
        check("snapshot after tombstone",
              svc.snapshot()["rows"] == [["k1", None]], str(svc.snapshot()["rows"]))

        svc.begin("r2")
        # r2's range covers k2: tombstone writer d1 (last writer, version
        # <= r2's snapshot) therefore points to r2.
        check("r2 reads k2 as absent (tombstone respected)",
              svc.read_range("r2", "k2", "k3")["rows"] == [])
        svc.stage("r2", {"k9": "z"})
        check("r2 commits", svc.commit("r2")["status"] == "committed")

        svc.stage("r1", {"k8": "rewrite"})
        cr = svc.commit("r1")
        # Any complete ring is acceptable; here the detector returns the
        # shorter d1 -> r1 -> d1, and the tombstone writer d1 is on it.
        check("r1 closes a ring that contains the tombstone writer d1",
              cr["status"] == "cycle"
              and cr["cycle"][0] == cr["cycle"][-1]
              and "d1" in cr["cycle"] and "r1" in cr["cycle"],
              str(cr))
        check("r1's k8 was never published",
              svc.snapshot()["rows"] == [["k1", None], ["k9", "z"]],
              str(svc.snapshot()["rows"]))

        # Re-writing a tombstoned key at a fresh snapshot is legal.
        svc.begin("revive")
        check("revive reads k2 as absent",
              svc.read_range("revive", "k2", "k3")["rows"] == [])
        svc.stage("revive", {"k2": "alive"})
        check("re-writing the tombstoned key commits",
              svc.commit("revive")["status"] == "committed")
        check("k2 live again with null k1 preserved",
              svc.snapshot()["rows"] == [["k1", None], ["k2", "alive"], ["k9", "z"]],
              str(svc.snapshot()["rows"]))
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_read_only_transactions():
    section("read-only transactions: commits, version bumps, ring membership")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        svc.begin("ro1")
        svc.read_range("ro1", None, None)
        c = svc.commit("ro1")
        check("read-only commit succeeds and bumps version",
              c == {"tid": "ro1", "status": "committed", "version": 1}, str(c))
        check("data still empty", svc.snapshot()["rows"] == [])

        # Ring with a read-only participant.  Classic three-txn SSI ring:
        #   base writes a; Z (old snapshot) and R (newer snapshot) both read
        #   ranges; W writes between their starts.  The ring is
        #   base -> R -> W -> Z -> base, where R never writes.
        svc.begin("base")
        svc.stage("base", {"a": "1"})
        check("base commits v2", svc.commit("base")["version"] == 2)

        svc.begin("Z")                      # snapshot 2, before W exists
        check("Z empty read of W's future key",
              svc.read_range("Z", "b", "c")["rows"] == [])

        svc.begin("W")                      # snapshot 2
        svc.stage("W", {"b": "2"})
        check("W commits v3", svc.commit("W")["version"] == 3)

        svc.begin("R")                      # snapshot 3, strictly read-only
        check("R reads both a and b",
              svc.read_range("R", "a", "c")["rows"] == [["a", "1"], ["b", "2"]])
        check("read-only R commits v4", svc.commit("R")["version"] == 4)

        svc.stage("Z", {"a": "9"})         # a already written by base
        cz = svc.commit("Z")
        # The detector may return any complete ring; here it returns
        # R -> Z -> W -> R, which includes the strictly read-only R.
        check("Z is rejected by a cycle that includes read-only R",
              cz["status"] == "cycle"
              and cz["cycle"][0] == cz["cycle"][-1]
              and {"R", "Z", "W"}.issubset(set(cz["cycle"])),
              str(cz))
        check("version remains 4", svc.snapshot()["version"] == 4)
        check("rejected write unpublished",
              svc.snapshot()["rows"] == [["a", "1"], ["b", "2"]])
        check("retry returns same cycle", svc.commit("Z") == cz)
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_write_conflict_priority():
    section("write_conflict is checked before cycle detection")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        # Build a ring-shaped dependency arrangement while also forcing a
        # stale write; the conflict verdict must win.
        svc.begin("X")
        svc.begin("Y")
        svc.read_range("X", "b", "c")
        svc.read_range("Y", "a", "b")
        svc.stage("X", {"a": "1"})
        svc.stage("Y", {"b": "2", "a": "stale"})  # a overlaps X's key
        check("X commits v1", svc.commit("X")["version"] == 1)
        cy = svc.commit("Y")
        check("Y ends as write_conflict, not cycle",
              cy["status"] == "write_conflict", str(cy))
        check("conflict verdict replays", svc.commit("Y") == cy)
        check("version stays 1", svc.snapshot()["version"] == 1)

        # Pure conflict without any ring: later writer of the same key.
        svc.begin("p")
        svc.stage("p", {"a": "p"})
        check("p commits v2", svc.commit("p")["version"] == 2)
        svc.begin("q")            # snapshot 2
        svc.begin("upd")
        svc.stage("upd", {"a": "u"})
        check("upd commits v3", svc.commit("upd")["version"] == 3)
        svc.stage("q", {"a": "q"})
        cq = svc.commit("q")
        check("stale same-key write conflicts",
              cq["status"] == "write_conflict", str(cq))
        check("a keeps upd's value", svc.snapshot()["rows"] == [["a", "u"]])
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_abort_and_interleaving():
    section("abort semantics and interleaved transactions")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        svc.begin("a")
        svc.begin("b")
        svc.read_range("a", "a", "m")
        svc.stage("a", {"a": "1"})
        svc.read_range("b", "m", "z")
        svc.stage("b", {"m": "2"})
        check("abort b", svc.abort("b")["status"] == "aborted")
        check("abort is idempotent", svc.abort("b")["status"] == "aborted")
        check("a still commits", svc.commit("a")["status"] == "committed")
        # b's write never landed.
        check("aborted write absent", svc.snapshot()["rows"] == [["a", "1"]])
        try:
            svc.stage("b", {"x": "1"})
            check("staging aborted txn rejected", False)
        except ServiceError:
            check("staging aborted txn rejected", True)

        # A long-interleaved chain on disjoint keys all commits serially.
        for i in range(5):
            svc.begin(f"i{i}")
        for i in range(5):
            svc.read_range(f"i{i}", f"k{i}", f"k{i}z")
        for i in range(5):
            svc.stage(f"i{i}", {f"k{i}": str(i)})
        for i in range(5):
            r = svc.commit(f"i{i}")
            check(f"i{i} commits", r["status"] == "committed", str(r))
        check("version advanced by 6 total", svc.snapshot()["version"] == 6)
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_persistence_restart():
    section("persistence: outcomes and open transactions survive restart")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        svc.begin("keep")                      # stays open across restart
        svc.read_range("keep", "keep", "keeq")
        svc.stage("keep", {"keep": "1"})

        svc.begin("done")
        svc.read_range("done", None, None)
        svc.stage("done", {"done": "1"})
        svc.commit("done")

        svc.begin("gone")
        svc.stage("gone", {"gone": "1"})
        svc.abort("gone")

        svc.begin("cyc")
        svc.begin("cyc2")
        svc.read_range("cyc", "a", "b")
        svc.read_range("cyc2", "b", "c")
        svc.stage("cyc", {"b": "1"})
        svc.stage("cyc2", {"a": "1"})
        svc.commit("cyc")
        cyc_result = svc.commit("cyc2")
        check("pre-restart cycle recorded", cyc_result["status"] == "cycle")
        svc.close()

        svc = reopen(path)
        check("version preserved", svc.snapshot()["version"] == 2,
              str(svc.snapshot()))
        check("committed rows preserved",
              svc.snapshot()["rows"] == [["b", "1"], ["done", "1"]],
              str(svc.snapshot()["rows"]))
        check("open txn preserved and usable",
              svc.begin("keep")["status"] == "open")
        ck = svc.commit("keep")
        check("staged write set survived restart and commits",
              ck["status"] == "committed" and ck["version"] == 3, str(ck))
        check("cycle outcome replays after restart",
              svc.commit("cyc2")["status"] == "cycle")
        check("aborted stays aborted after restart",
              svc.begin("gone")["status"] == "aborted")
        check("committed stays committed after restart",
              svc.commit("done")["status"] == "committed"
              and svc.commit("done")["version"] == 1)
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def _run_crash_driver(db_path, tid, crash_at):
    return subprocess.run(
        [sys.executable, "-I", os.path.join(ROOT, "crash_driver.py"),
         db_path, tid, crash_at],
        cwd=ROOT, capture_output=True, text=True, timeout=60,
    )


def test_crash_recovery():
    section("hard-kill recovery: data, version and retry outcomes")
    d, path = fresh_db()
    try:
        # 1. Crash before the durable commit: nothing may be visible.
        p = _run_crash_driver(path, "pre1", "pre_commit")
        check("pre-commit os._exit process died", p.returncode == 42, str(p.returncode))
        svc = reopen(path)
        check("pre-commit crash: version still 0", svc.snapshot()["version"] == 0)
        check("pre-commit crash: no rows", svc.snapshot()["rows"] == [])
        check("pre-commit crash: txn still open for retry",
              svc.begin("pre1")["status"] == "open")
        retry = svc.commit("pre1")
        check("retry after pre-commit crash commits",
              retry["status"] == "committed" and retry["version"] == 1, str(retry))
        check("retry data visible", svc.snapshot()["rows"] == [["k1", "v1"]])
        svc.close()

        # 2. Real SIGKILL before the durable commit.
        p = _run_crash_driver(path, "kill1", "pre_commit_sigkill")
        check("SIGKILL delivered", p.returncode == -signal.SIGKILL, str(p.returncode))
        svc = reopen(path)
        check("SIGKILL: version unchanged at 1", svc.snapshot()["version"] == 1)
        check("SIGKILL: rows unchanged", svc.snapshot()["rows"] == [["k1", "v1"]])
        check("SIGKILL: killed txn open and retryable",
              svc.begin("kill1")["status"] == "open")
        r = svc.commit("kill1")
        check("SIGKILL retry commits at v2",
              r["status"] == "committed" and r["version"] == 2, str(r))
        svc.close()

        # 3. Crash right after the durable commit returned: it must stick.
        p = _run_crash_driver(path, "post1", "post_commit")
        check("post-commit os._exit process died", p.returncode == 43)
        svc = reopen(path)
        check("post-commit crash: version advanced to 3",
              svc.snapshot()["version"] == 3, str(svc.snapshot()))
        check("post-commit crash: write durable",
              svc.snapshot()["rows"] == [["k1", "v1"]])
        again = svc.commit("post1")
        check("post-commit crash: replay shows committed",
              again["status"] == "committed" and again["version"] == 3, str(again))
        svc.close()

        # 4. Crash after staging, before commit: open txn with write set kept.
        p = _run_crash_driver(path, "stage1", "stage_done")
        check("stage-time os._exit process died", p.returncode == 44)
        svc = reopen(path)
        check("stage crash: version unchanged", svc.snapshot()["version"] == 3)
        check("stage crash: open txn recovered",
              svc.begin("stage1")["status"] == "open")
        r = svc.commit("stage1")
        check("stage crash: recovered staged set commits",
              r["status"] == "committed" and r["version"] == 4, str(r))
        svc.close()

        # 5. Crash INSIDE the durable transaction, before COMMIT is issued:
        #    SQLite must roll back the whole multi-statement transaction.
        p = _run_crash_driver(path, "mid1", "mid_commit")
        check("mid-commit os._exit process died", p.returncode == 45, p.stderr)
        svc = reopen(path)
        check("mid-commit crash: version rolled back to 4",
              svc.snapshot()["version"] == 4, str(svc.snapshot()["version"]))
        check("mid-commit crash: no partial rows",
              svc.snapshot()["rows"] == [["k1", "v1"]],
              str(svc.snapshot()["rows"]))
        check("mid-commit crash: txn still open and retryable",
              svc.begin("mid1")["status"] == "open")
        r = svc.commit("mid1")
        check("mid-commit crash: retry commits at v5",
              r["status"] == "committed" and r["version"] == 5, str(r))
        svc.close()

        # 6. Same point but a real SIGKILL.
        p = _run_crash_driver(path, "mid2", "mid_commit_sigkill")
        check("mid-commit SIGKILL delivered",
              p.returncode == -signal.SIGKILL, str(p.returncode))
        svc = reopen(path)
        check("mid-commit SIGKILL: version unchanged at 5",
              svc.snapshot()["version"] == 5)
        check("mid-commit SIGKILL: no partial rows",
              svc.snapshot()["rows"] == [["k1", "v1"]])
        check("mid-commit SIGKILL: txn retryable",
              svc.begin("mid2")["status"] == "open")
        r = svc.commit("mid2")
        check("mid-commit SIGKILL: retry commits at v6",
              r["status"] == "committed" and r["version"] == 6, str(r))
        svc.close()

        # 7. A normal control run against the same database file.
        p = _run_crash_driver(path, "clean1", "none")
        check("control run exits 0", p.returncode == 0, p.stderr)
        svc = reopen(path)
        check("control run advanced version to 7", svc.snapshot()["version"] == 7)
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_concurrency():
    section("concurrency: parallel clients are serial-equivalent")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        errors = []

        def worker(i):
            try:
                for j in range(30):
                    tid = f"w{i:02d}{j:02d}"
                    svc.begin(tid)
                    # Narrow, disjoint read ranges: no cross-transaction
                    # anti-dependencies, so every commit must succeed.
                    svc.read_range(tid, f"k{i:02d}{j:02d}", f"k{i:02d}{j:02d}z")
                    svc.stage(tid, {f"k{i:02d}{j:02d}": f"{i}-{j}"})
                    r = svc.commit(tid)
                    if r["status"] != "committed":
                        errors.append((tid, r))
            except ServiceError as exc:  # pragma: no cover - failure path
                errors.append((f"worker{i}", str(exc)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        check("all 240 concurrent txns committed", not errors, str(errors[:5]))
        snap = svc.snapshot()
        check("version equals commit count 240", snap["version"] == 240,
              str(snap["version"]))
        check("all 240 distinct rows present and sorted",
              len(snap["rows"]) == 240 and
              snap["rows"] == sorted(snap["rows"], key=lambda kv: kv[0]),
              str(len(snap["rows"])))

        # Concurrent snapshots while writers run must always be internally
        # consistent (sorted, unique keys, version within range).
        snap_errors = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                s = svc.snapshot()
                keys = [r[0] for r in s["rows"]]
                if keys != sorted(keys) or len(keys) != len(set(keys)):
                    snap_errors.append(s)

        rt = threading.Thread(target=reader)
        rt.start()

        def writer2():
            for j in range(100):
                tid = f"c{j:03d}"
                svc.begin(tid)
                svc.stage(tid, {f"c{j:03d}": "x"})
                svc.commit(tid)

        wt = threading.Thread(target=writer2)
        wt.start()
        wt.join()
        stop.set()
        rt.join()
        check("concurrent snapshots always consistent", not snap_errors)
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_validation_rules():
    section("validation and phase rules")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        for bad in ["", "bad-id", "中文", "a b", "x!"]:
            try:
                svc.begin(bad)
                check(f"reject tid {bad!r}", False)
            except ServiceError:
                check(f"reject tid {bad!r}", True)
        svc.begin("ok1")
        for bad_key in [{"bad-key": "v"}, {"": "v"}, {1: "v"}]:
            try:
                svc.stage("ok1", bad_key)
                check(f"reject key {bad_key!r}", False)
            except ServiceError:
                check(f"reject key {bad_key!r}", True)
        try:
            svc.stage("ok1", {"k": 123})
            check("reject non-string scalar value", False)
        except ServiceError:
            check("reject non-string scalar value", True)
        try:
            svc.stage("ok1", {"k": {"delete": False}})
            check("reject malformed delete marker", False)
        except ServiceError:
            check("reject malformed delete marker", True)
        svc.stage("ok1", {"k": "v"})
        try:
            svc.stage("ok1", {"k": "v2"})
            check("double stage rejected", False)
        except ServiceError:
            check("double stage rejected", True)
        svc.commit("ok1")
        check("re-commit replays committed outcome without raising",
              svc.commit("ok1")["status"] == "committed")
        try:
            svc.read_range("ok1", None, None)
            check("read on committed txn rejected", False)
        except ServiceError:
            check("read on committed txn rejected", True)
        try:
            svc.snapshot(999)
            check("future snapshot rejected", False)
        except ServiceError:
            check("future snapshot rejected", True)
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
def test_no_change_and_tombstone_conflicts():
    section("no-value-change writes count; tombstone writes cause conflicts")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)

        def put(tid, mapping):
            svc.begin(tid)
            svc.stage(tid, mapping)
            return svc.commit(tid)

        # Two commits writing the identical value to the same key: both are
        # real writes, both create history and bump the version.
        c1 = put("s1", {"a": "same"})
        c2 = put("s2", {"a": "same"})
        check("identical-value rewrite still commits and versions",
              c1["version"] == 1 and c2["version"] == 2,
              f"{c1} {c2}")
        rows = svc._conn.execute(
            "SELECT tid, version, key, value FROM committed_writes ORDER BY id"
        ).fetchall()
        check("both no-change writes are recorded in history",
              len(rows) == 2 and [r["tid"] for r in rows] == ["s1", "s2"])

        # A stale transaction writing the same key (even the same value) loses
        # the write_conflict check against both later successful writes.
        svc.begin("stale")          # snapshot 2
        put("fresh", {"a": "same"})  # version 3
        r = svc.stage("stale", {"a": "same"})
        check("staged before the conflicting commit", r["status"] == "staged")
        cs = svc.commit("stale")
        check("no-value-change stale write still conflicts",
              cs["status"] == "write_conflict", str(cs))

        # A tombstone is a successful write for the conflict rule too.
        svc.begin("victim")        # snapshot 3
        check("tombstone commit v4",
              put("eraser", {"a": {"delete": True}})["version"] == 4)
        svc.stage("victim", {"a": "reborn"})
        cv = svc.commit("victim")
        check("write against a newer tombstone conflicts",
              cv["status"] == "write_conflict", str(cv))
        check("key stays deleted", svc.snapshot()["rows"] == [])

        # A transaction with no reads at all commits normally (ww edges only).
        r = put("blind", {"z": "1"})
        check("blind write commits", r["status"] == "committed", str(r))
        svc.close()
    finally:
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
def test_contention_serial_equivalence():
    section("contention: same-key commits are serial-equivalent")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        outcomes = []
        lock = threading.Lock()

        def hammer(i):
            local = []
            for j in range(40):
                tid = f"h{i:02d}{j:03d}"
                svc.begin(tid)
                svc.read_range(tid, "hot", "hou")
                svc.stage(tid, {"hot": f"{i}-{j}"})
                local.append(svc.commit(tid)["status"])
            with lock:
                outcomes.extend(local)

        threads = [threading.Thread(target=hammer, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        committed = outcomes.count("committed")
        conflicts = outcomes.count("write_conflict")
        cycles = outcomes.count("cycle")
        check("every verdict is a known terminal state",
              len(outcomes) == 240 and committed + conflicts + cycles == 240,
              str((committed, conflicts, cycles)))
        check("version equals exactly the number of commits",
              svc.snapshot()["version"] == committed,
              f"{svc.snapshot()['version']} vs {committed}")
        snap = svc.snapshot()
        check("one live row, sorted, value from the last committer",
              len(snap["rows"]) == 1 and snap["rows"][0][0] == "hot",
              str(snap))
        # The stored history must be a strictly increasing per-key chain.
        hist = svc._conn.execute(
            "SELECT version FROM committed_writes WHERE key='hot' ORDER BY id"
        ).fetchall()
        versions = [r["version"] for r in hist]
        check("committed history versions strictly increasing and unique",
              len(versions) == committed and versions == sorted(set(versions)),
              str(versions))

        # Concurrent begin of the same id: exactly one creator, all callers
        # get the same original transaction.
        results = []
        barrier = threading.Barrier(8)

        def same_id():
            barrier.wait()
            results.append(svc.begin("dup"))

        ts = [threading.Thread(target=same_id) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        check("all concurrent begins return the identical txn",
              all(r == results[0] and r["status"] == "open"
                  and r["snapshot"] == snap["version"] for r in results),
              str(results))
        svc.close()
    finally:
        shutil.rmtree(d)


def test_write_set_durable_after_terminal():
    section("write sets stay durable after every terminal outcome")
    d, path = fresh_db()
    try:
        svc = TransactionService(path)
        svc.begin("ok")
        svc.read_range("ok", "a", "z")
        svc.stage("ok", {"a": "1"})
        svc.commit("ok")

        svc.begin("bad")
        svc.read_range("bad", "b", "c")
        svc.stage("bad", {"b": "2", "c": {"delete": True}, "n": None})
        # Make bad conflict: newer successful write of b.
        svc.begin("newer")
        svc.stage("newer", {"b": "9"})
        svc.commit("newer")
        check("bad ends in write_conflict",
              svc.commit("bad")["status"] == "write_conflict")

        svc.begin("gone")
        svc.stage("gone", {"g": "x"})
        svc.abort("gone")

        def write_sets(conn):
            out = {}
            for wr in conn.execute(
                "SELECT tid, key, value FROM staged_writes"
            ).fetchall():
                out.setdefault(wr["tid"], []).append(
                    (wr["key"], wr["value"])
                )
            return {tid: sorted(items) for tid, items in out.items()}

        before = write_sets(svc._conn)
        check("committed write set retained",
              before.get("ok") == [("a", "1")], str(before))
        check("failed write set retained incl. tombstone/null encodings",
              before.get("bad") == [("b", "2"), ("c", "\x00__TOMBSTONE__\x00"),
                                    ("n", None)], str(before))
        check("aborted write set retained",
              before.get("gone") == [("g", "x")], str(before))
        svc.close()

        svc = reopen(path)
        check("write sets survive restart", write_sets(svc._conn) == before)
        svc.close()
    finally:
        shutil.rmtree(d)


def main():
    tests = [
        test_unit_cycle_finder,
        test_basic_snapshot_and_version,
        test_spec_xy_example,
        test_long_ring,
        test_phantom_empty_read,
        test_tombstones_and_nulls,
        test_read_only_transactions,
        test_write_conflict_priority,
        test_abort_and_interleaving,
        test_persistence_restart,
        test_crash_recovery,
        test_concurrency,
        test_validation_rules,
        test_no_change_and_tombstone_conflicts,
        test_write_set_durable_after_terminal,
        test_contention_serial_equivalence,
    ]
    for t in tests:
        t()
    print(f"\n{'=' * 60}")
    if FAILURES:
        print(f"{PASS} checks passed, {len(FAILURES)} FAILED:")
        for name, detail in FAILURES:
            print(f"  - {name}: {detail}")
        return 1
    print(f"ALL {PASS} CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
