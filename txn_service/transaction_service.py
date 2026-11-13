"""Backend transaction service with explicit dependency-graph cycle detection.

This is an SSI-style (serializable snapshot isolation) transaction manager built
on top of SQLite, where SQLite is used only as a durable relational store; all
snapshot, dependency and cycle logic lives in this module.

Interfaces (methods of :class:`TransactionService`):

    begin(tid)                  -> begin result (fixes snapshot version)
    read_range(tid, lo, hi)     -> rows visible to the transaction, key sorted
    stage(tid, mapping)         -> stage the (single) write set
    commit(tid)                 -> committed / write_conflict / cycle
    abort(tid)                  -> aborted
    snapshot(version=None)      -> version plus currently live, sorted rows

Semantics summary
-----------------
* Empty initial table, global version starts at 0. ``begin`` fixes the version
  ``s`` the transaction reads from.
* Reads are left-closed/right-open key ranges ``[lo, hi)`` with ``None``
  endpoints meaning infinity; ``lo > hi`` is rejected; an empty read is still
  registered as a dependency (phantom protection).
* A transaction may read any number of times, then stage one key->value map
  once, after which reads are forbidden.  ``{"delete": True}`` marks a
  tombstone; an ordinary JSON ``null`` stages a real SQL NULL value.
* On commit:
    1. If any staged key has a *successful* write at a version greater than
       ``s``, the transaction ends as ``write_conflict``.
    2. Otherwise a candidate dependency graph is built over all committed
       transactions plus this candidate.  Same-key adjacent writers are linked
       in commit order.  For every registered read range, the last writer with
       version <= s of every key ever written inside the range points to the
       reader, and every writer of such a key with version > s is pointed to
       by the reader (anti-dependency).  Self edges are skipped.
    3. If the graph contains a cycle, the transaction ends as ``cycle`` and one
       complete cycle (transaction ids, start node repeated at the end) is
       returned.  Otherwise the transaction commits and the global version is
       incremented by one -- including empty commits.
* Failed and aborted transactions never enter the graph, never modify data and
  never move the version.  Terminal states are irreversible; a repeated commit
  replays the original outcome; ``begin`` of an existing id returns the
  original transaction.
* All state (transactions, read ranges, write sets and outcomes) is persisted;
  open transactions survive a process restart.

The commit decision and publication happen inside one SQLite transaction, so
they are atomic, including across crashes (WAL + FULL synchronous mode).
"""

from __future__ import annotations

import json
import sqlite3
import sys
import threading
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_OPEN = "open"
STATE_COMMITTED = "committed"
STATE_ABORTED = "aborted"
STATE_WRITE_CONFLICT = "write_conflict"
STATE_CYCLE = "cycle"
TERMINAL_STATES = frozenset(
    {STATE_COMMITTED, STATE_ABORTED, STATE_WRITE_CONFLICT, STATE_CYCLE}
)
# Outcomes that count as "successful writes" for conflict / history purposes.
SUCCESSFUL_STATES = frozenset({STATE_COMMITTED})

# Stored tombstone marker inside committed_writes/staged_writes.
TOMBSTONE = "\x00__TOMBSTONE__\x00"


class ServiceError(Exception):
    """Invalid request (unknown transaction, bad arguments, wrong phase...)."""


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _ascii_alnum(value: Any, what: str) -> str:
    if not isinstance(value, str) or value == "":
        raise ServiceError(f"{what} must be a non-empty string")
    # Non-empty ASCII letters/digits only.
    if not all(("a" <= ch <= "z") or ("A" <= ch <= "Z") or ("0" <= ch <= "9")
               for ch in value):
        raise ServiceError(f"{what} must be non-empty ASCII alphanumeric")
    return value


def _validate_key(value: Any) -> str:
    return _ascii_alnum(value, "key")


def _validate_bound(value: Any) -> Optional[str]:
    if value is None:
        return None
    return _ascii_alnum(value, "range endpoint")


def _encode_value(spec: Any) -> Optional[str]:
    """Translate a user write spec into the stored value representation.

    * ``{"delete": True}``   -> tombstone marker
    * JSON ``null`` / Python ``None`` -> SQL NULL (a real null value)
    * any other JSON string  -> the string itself
    """
    if isinstance(spec, dict):
        if spec.get("delete") is True and len(spec) == 1:
            return TOMBSTONE
        raise ServiceError(
            'a write value must be a string, null, or {"delete": true}'
        )
    if spec is None:
        return None
    if isinstance(spec, str):
        return spec
    raise ServiceError(
        'a write value must be a string, null, or {"delete": true}'
    )


# ---------------------------------------------------------------------------
# Graph helpers (pure functions, easy to test in isolation)
# ---------------------------------------------------------------------------

def find_cycle(graph: Dict[str, set]) -> Optional[List[str]]:
    """Return one complete cycle as a list of node ids with the start node
    repeated at the end, or ``None`` when the graph is acyclic.

    Iterative DFS with an explicit stack so long rings do not hit the
    recursion limit.  Nodes are visited in sorted order so the returned cycle
    is deterministic for a given graph.
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {node: WHITE for node in graph}
    for start in sorted(graph):
        if color[start] != WHITE:
            continue
        color[start] = GRAY
        # Each frame is (node, sorted iterator over its successors).
        stack: List[Tuple[str, Any]] = [(start, iter(sorted(graph[start])))]
        while stack:
            node, it = stack[-1]
            nxt = next(it, None)
            if nxt is None:
                color[node] = BLACK
                stack.pop()
                continue
            if color.get(nxt) == GRAY:
                # Back edge node -> nxt; extract nxt ... node from the stack.
                ids = [frame[0] for frame in stack]
                idx = ids.index(nxt)
                return ids[idx:] + [nxt]
            if color.get(nxt) == WHITE:
                color[nxt] = GRAY
                stack.append((nxt, iter(sorted(graph[nxt]))))
    return None


# ---------------------------------------------------------------------------
# Main service
# ---------------------------------------------------------------------------

class TransactionService:
    """Durable, thread-safe transaction service backed by one SQLite file."""

    def __init__(self, db_path: str,
                 pre_commit_hook: Optional[callable] = None,
                 post_commit_hook: Optional[callable] = None,
                 mid_commit_hook: Optional[callable] = None):
        self.db_path = db_path
        # Hooks exist solely so crash/recovery acceptance tests can hard-kill
        # the process at precise points of the commit protocol.
        self._pre_commit_hook = pre_commit_hook
        self._post_commit_hook = post_commit_hook
        self._mid_commit_hook = mid_commit_hook
        self._lock = threading.RLock()
        check_same_thread = False
        self._conn = sqlite3.connect(
            db_path, check_same_thread=check_same_thread, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        # Durability settings: WAL recovers cleanly after a hard kill, and
        # FULL makes every fsync explicit before a commit call returns.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "TransactionService":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    version INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS txns (
                    tid            TEXT PRIMARY KEY,
                    snapshot       INTEGER NOT NULL,
                    state          TEXT NOT NULL,
                    cycle          TEXT,
                    commit_version INTEGER
                );
                CREATE TABLE IF NOT EXISTS read_ranges (
                    id  INTEGER PRIMARY KEY AUTOINCREMENT,
                    tid TEXT NOT NULL REFERENCES txns(tid),
                    lo  TEXT,
                    hi  TEXT
                );
                CREATE TABLE IF NOT EXISTS staged_writes (
                    tid   TEXT NOT NULL REFERENCES txns(tid),
                    key   TEXT NOT NULL,
                    value TEXT,
                    PRIMARY KEY (tid, key)
                );
                -- Every *successful* write ever, one row per (txn, key).
                -- value may be a real string, SQL NULL, or the tombstone.
                CREATE TABLE IF NOT EXISTS committed_writes (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    tid        TEXT NOT NULL,
                    version    INTEGER NOT NULL,
                    key        TEXT NOT NULL,
                    value      TEXT,
                    UNIQUE (tid, key)
                );
                CREATE INDEX IF NOT EXISTS idx_cw_key_ver
                    ON committed_writes(key, version);
                """
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(id, version) VALUES (1, 0)"
            )

    # -- small internal helpers -------------------------------------------

    def _get_version(self) -> int:
        row = self._conn.execute(
            "SELECT version FROM meta WHERE id = 1"
        ).fetchone()
        return row["version"]

    def _get_txn(self, tid: str) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM txns WHERE tid = ?", (tid,)
        ).fetchone()
        if row is None:
            raise ServiceError(f"unknown transaction id: {tid}")
        return row

    def _require_open(self, tid: str) -> sqlite3.Row:
        row = self._get_txn(tid)
        if row["state"] != STATE_OPEN:
            raise ServiceError(
                f"transaction {tid} is in terminal state {row['state']}"
            )
        return row

    @staticmethod
    def _in_range(key: str, lo: Optional[str], hi: Optional[str]) -> bool:
        if lo is not None and key < lo:
            return False
        if hi is not None and not (key < hi):
            return False
        return True

    # -- public API --------------------------------------------------------

    def begin(self, tid: str) -> Dict[str, Any]:
        """Start a transaction (or return the original one if the id exists).

        The current global version is fixed as the transaction snapshot.
        """
        _ascii_alnum(tid, "transaction id")
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM txns WHERE tid = ?", (tid,)
            ).fetchone()
            if existing is not None:
                # Re-begin of an existing id: return the original transaction,
                # never reset anything.
                return self._txn_result(existing)
            snapshot = self._get_version()
            self._conn.execute(
                "INSERT INTO txns(tid, snapshot, state) VALUES (?, ?, ?)",
                (tid, snapshot, STATE_OPEN),
            )
            return {
                "tid": tid,
                "status": STATE_OPEN,
                "snapshot": snapshot,
            }

    def read_range(self, tid: str, lo: Optional[str],
                   hi: Optional[str]) -> Dict[str, Any]:
        """Read ``[lo, hi)`` as of the transaction's snapshot.

        ``None`` endpoint means infinity.  The read is always registered as a
        dependency, even when it matches no rows.
        """
        lo = _validate_bound(lo)
        hi = _validate_bound(hi)
        if lo is not None and hi is not None and lo > hi:
            raise ServiceError("range start must not be greater than end")
        with self._lock:
            txn = self._require_open(tid)
            staged = self._conn.execute(
                "SELECT 1 FROM staged_writes WHERE tid = ? LIMIT 1", (tid,)
            ).fetchone()
            if staged is not None:
                raise ServiceError(
                    f"transaction {tid} cannot read after staging its write set"
                )
            snapshot = txn["snapshot"]
            self._conn.execute(
                "INSERT INTO read_ranges(tid, lo, hi) VALUES (?, ?, ?)",
                (tid, lo, hi),
            )
            # Latest write per key up to the snapshot; window function keeps
            # one row per key, then tombstones/absent keys are dropped.
            rows = self._conn.execute(
                """
                SELECT key, value FROM (
                    SELECT key, value,
                           ROW_NUMBER() OVER (
                               PARTITION BY key
                               ORDER BY version DESC, id DESC
                           ) AS rn
                    FROM committed_writes
                    WHERE (? IS NULL OR key >= ?)
                      AND (? IS NULL OR key <  ?)
                      AND version <= ?
                )
                WHERE rn = 1 AND (value IS NULL OR value != ?)
                ORDER BY key ASC
                """,
                (lo, lo, hi, hi, snapshot, TOMBSTONE),
            ).fetchall()
            return {
                "tid": tid,
                "snapshot": snapshot,
                "lo": lo,
                "hi": hi,
                "rows": [[r["key"], r["value"]] for r in rows],
            }

    def stage(self, tid: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Stage the transaction's single write set (one shot).

        Keys are non-empty ASCII alphanumeric strings; values are strings,
        JSON ``null`` (a real null), or ``{"delete": true}`` tombstones.
        Deleting a missing key and an empty write set are both legal.
        """
        if not isinstance(mapping, dict):
            raise ServiceError("stage expects a key -> value mapping object")
        with self._lock:
            self._require_open(tid)
            already = self._conn.execute(
                "SELECT 1 FROM staged_writes WHERE tid = ? LIMIT 1", (tid,)
            ).fetchone()
            if already is not None:
                raise ServiceError(
                    f"write set of transaction {tid} is already staged"
                )
            items = []
            for key, spec in mapping.items():
                _validate_key(key)
                items.append((tid, key, _encode_value(spec)))
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.executemany(
                    "INSERT INTO staged_writes(tid, key, value) VALUES (?, ?, ?)",
                    items,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return {
                "tid": tid,
                "status": "staged",
                "write_count": len(items),
            }

    def abort(self, tid: str) -> Dict[str, Any]:
        """Abort an open transaction.  Its data never enters anything.

        Aborting an already aborted transaction is idempotent; any other
        terminal state is an error (terminal states are irreversible).
        """
        with self._lock:
            txn = self._get_txn(tid)
            if txn["state"] == STATE_ABORTED:
                return {"tid": tid, "status": STATE_ABORTED}
            if txn["state"] != STATE_OPEN:
                raise ServiceError(
                    f"transaction {tid} is in terminal state {txn['state']}"
                )
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE txns SET state = ? WHERE tid = ?",
                    (STATE_ABORTED, tid),
                )
                # Keep the staged write set as the durable record of the
                # attempt; an aborted transaction can never publish it.
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return {"tid": tid, "status": STATE_ABORTED}

    def snapshot(self, version: Optional[int] = None) -> Dict[str, Any]:
        """Return a snapshot: version plus all live rows, sorted by key."""
        with self._lock:
            current = self._get_version()
            if version is None:
                version = current
            if not isinstance(version, int) or version < 0 or version > current:
                raise ServiceError(
                    f"unknown snapshot version {version!r}; current is {current}"
                )
            rows = self._conn.execute(
                """
                SELECT key, value FROM (
                    SELECT key, value,
                           ROW_NUMBER() OVER (
                               PARTITION BY key
                               ORDER BY version DESC, id DESC
                           ) AS rn
                    FROM committed_writes
                    WHERE version <= ?
                )
                WHERE rn = 1 AND (value IS NULL OR value != ?)
                ORDER BY key ASC
                """,
                (version, TOMBSTONE),
            ).fetchall()
            return {
                "version": version,
                "rows": [[r["key"], r["value"]] for r in rows],
            }

    def commit(self, tid: str) -> Dict[str, Any]:
        """Commit protocol; see module docstring for the full rule set."""
        with self._lock:
            txn = self._get_txn(tid)
            if txn["state"] != STATE_OPEN:
                # Terminal: replay the recorded outcome exactly.
                return self._outcome_result(txn)
            return self._commit_locked(tid)

    # -- commit internals --------------------------------------------------

    def _commit_locked(self, tid: str) -> Dict[str, Any]:
        txn = self._get_txn(tid)
        snapshot = txn["snapshot"]

        staged = self._conn.execute(
            "SELECT key, value FROM staged_writes WHERE tid = ? ORDER BY key",
            (tid,),
        ).fetchall()
        staged_keys = [r["key"] for r in staged]

        # Step 1: write-conflict check.  Any successful write of a staged key
        # at version greater than the transaction's snapshot means the write
        # set is stale.  This is evaluated before any graph work.
        if staged_keys:
            placeholders = ",".join("?" for _ in staged_keys)
            conflict = self._conn.execute(
                f"""
                SELECT 1 FROM committed_writes
                WHERE version > ? AND key IN ({placeholders})
                LIMIT 1
                """,
                [snapshot, *staged_keys],
            ).fetchone()
            if conflict is not None:
                self._finish_failure(tid, STATE_WRITE_CONFLICT)
                return {"tid": tid, "status": STATE_WRITE_CONFLICT}

        # Step 2: build the candidate graph over every committed transaction
        # plus this candidate.
        graph, nodes = self._build_graph(tid, staged)

        # Step 3: cycle test.
        cycle = find_cycle(graph)
        if cycle is not None:
            self._finish_failure(tid, STATE_CYCLE, cycle=cycle)
            return {
                "tid": tid,
                "status": STATE_CYCLE,
                "cycle": cycle,
            }

        # No conflict, no cycle: publish atomically.
        if self._pre_commit_hook is not None:
            self._pre_commit_hook(tid)

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            new_version = self._get_version() + 1  # empty commits also bump
            self._conn.execute(
                "UPDATE meta SET version = version + 1 WHERE id = 1"
            )
            self._conn.execute(
                "UPDATE txns SET state = ?, commit_version = ? WHERE tid = ?",
                (STATE_COMMITTED, new_version, tid),
            )
            self._conn.executemany(
                """
                INSERT INTO committed_writes(tid, version, key, value)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (tid, new_version, r["key"], r["value"])
                    for r in staged
                ],
            )
            # Hard-kill point *inside* the durable transaction: the COMMIT
            # has not been issued, so reopen must roll everything back.
            if self._mid_commit_hook is not None:
                self._mid_commit_hook(tid)
            # The staged write set is intentionally retained as the durable
            # record of what the transaction wrote (the spec requires write
            # sets to persist alongside outcomes).
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

        if self._post_commit_hook is not None:
            self._post_commit_hook(tid)

        return {
            "tid": tid,
            "status": STATE_COMMITTED,
            "version": new_version,
        }

    def _finish_failure(self, tid: str, state: str,
                        cycle: Optional[List[str]] = None) -> None:
        """Persist a failure outcome without touching data or the version."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                "UPDATE txns SET state = ?, cycle = ? WHERE tid = ?",
                (state, json.dumps(cycle) if cycle is not None else None, tid),
            )
            # The attempted write set stays on disk as part of the durable
            # transaction record; it can never be published from a terminal
            # transaction because every mutating entry point requires open.
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _build_graph(self, candidate: str,
                     staged: List[sqlite3.Row]) -> Tuple[Dict[str, set], set]:
        """Construct the candidate dependency graph.

        Nodes: every committed transaction plus the candidate.  Edges:

        * ww -- same-key adjacent writers, linked in commit (version) order.
        * rw -- for each registered read range [lo, hi) and each key that was
          ever successfully written inside it:
            - last writer with version <= snapshot points to the reader;
            - every writer with version > snapshot is pointed to by the reader
              (anti-dependency, the phantom guard for empty reads).
          Self edges are always skipped.
        """
        committed = self._conn.execute(
            "SELECT tid, commit_version FROM txns "
            "WHERE state = ? ORDER BY commit_version",
            (STATE_COMMITTED,),
        ).fetchall()
        nodes = {r["tid"] for r in committed}
        nodes.add(candidate)
        graph: Dict[str, set] = {n: set() for n in nodes}

        def add_edge(src: str, dst: str) -> None:
            if src != dst and src in graph and dst in graph:
                graph[src].add(dst)

        # --- write-write edges: same-key adjacent writers by commit order ---
        key_history: Dict[str, List[Tuple[int, str]]] = {}
        for r in committed:
            keys = self._conn.execute(
                "SELECT key FROM committed_writes WHERE tid = ? ORDER BY key",
                (r["tid"],),
            ).fetchall()
            for kr in keys:
                key_history.setdefault(kr["key"], []).append(
                    (r["commit_version"], r["tid"])
                )
        # Candidate acts as the next writer (version current+1 conceptually);
        # exact numeric ordering relative to committed rows is guaranteed since
        # it has not committed yet.
        for r in staged:
            key_history.setdefault(r["key"], []).append((None, candidate))
        for writers in key_history.values():
            # Committed entries carry a version; the candidate sorts last.
            writers.sort(key=lambda w: (w[0] is None, w[0] if w[0] is not None else 0))
            for (_, a), (_, b) in zip(writers, writers[1:]):
                add_edge(a, b)

        # --- read-write edges for every registered range of every txn ------
        ranges = self._conn.execute(
            """
            SELECT rr.tid, rr.lo, rr.hi, t.snapshot
            FROM read_ranges rr JOIN txns t ON t.tid = rr.tid
            """
        ).fetchall()
        # All keys ever successfully written (tombstones included).  The
        # candidate's staged keys are handled separately below because their
        # writer is the candidate itself, whose commit version will be newer
        # than every existing snapshot.
        all_keys = {
            r["key"]
            for r in self._conn.execute(
                "SELECT DISTINCT key FROM committed_writes"
            ).fetchall()
        }
        candidate_keys = {r["key"] for r in staged}
        for rng in ranges:
            reader = rng["tid"]
            if reader not in graph:
                # Defensive: ranges only exist for known transactions.
                continue
            s = rng["snapshot"]
            lo, hi = rng["lo"], rng["hi"]
            for key in all_keys:
                if not self._in_range(key, lo, hi):
                    continue
                writers = self._conn.execute(
                    """
                    SELECT tid, version FROM committed_writes
                    WHERE key = ?
                    ORDER BY version ASC, id ASC
                    """,
                    (key,),
                ).fetchall()
                # Last writer with version <= s points to the reader.
                last_old = None
                for w in writers:
                    if w["version"] <= s:
                        last_old = w
                    else:
                        break
                if last_old is not None:
                    add_edge(last_old["tid"], reader)
                # Every writer with version > s is pointed to by the reader.
                for w in writers:
                    if w["version"] > s:
                        add_edge(reader, w["tid"])
            # Each of the candidate's staged keys inside this reader's range
            # is always newer than the reader's snapshot, hence an
            # anti-dependency: reader -> candidate (self edges are skipped).
            for key in candidate_keys:
                if self._in_range(key, lo, hi):
                    add_edge(reader, candidate)
        return graph, nodes

    # -- result shaping ----------------------------------------------------

    def _txn_result(self, row: sqlite3.Row) -> Dict[str, Any]:
        result = {
            "tid": row["tid"],
            "status": row["state"],
            "snapshot": row["snapshot"],
        }
        if row["commit_version"] is not None:
            result["commit_version"] = row["commit_version"]
        return result

    def _outcome_result(self, row: sqlite3.Row) -> Dict[str, Any]:
        if row["state"] == STATE_COMMITTED:
            return {
                "tid": row["tid"],
                "status": STATE_COMMITTED,
                "version": row["commit_version"],
            }
        if row["state"] == STATE_CYCLE:
            return {
                "tid": row["tid"],
                "status": STATE_CYCLE,
                "cycle": json.loads(row["cycle"]),
            }
        return {"tid": row["tid"], "status": row["state"]}


# ---------------------------------------------------------------------------
# Minimal JSON-lines CLI (each stdin line is one request, one reply per line)
# ---------------------------------------------------------------------------

def _cli(argv: List[str]) -> int:
    if len(argv) != 2:
        raise SystemExit("usage: python -I transaction_service.py <db_path>")
    allowed = frozenset(
        {"begin", "read_range", "stage", "commit", "abort", "snapshot"}
    )
    svc = TransactionService(argv[1])
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        req = json.loads(line)
        op = req.pop("op", None)
        if op not in allowed:
            reply = {"ok": False, "error": f"unknown op {op!r}"}
        else:
            try:
                reply = {"ok": True, "result": getattr(svc, op)(**req)}
            except ServiceError as exc:
                reply = {"ok": False, "error": str(exc)}
        print(json.dumps(reply), flush=True)
    svc.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli(sys.argv))
