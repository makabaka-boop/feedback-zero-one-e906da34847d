"""Hard-kill driver used by the crash/recovery acceptance tests.

Runs a fixed transaction against a database and terminates the process
*without* any Python cleanup at a precisely chosen point of the commit
protocol, so the acceptance suite can reopen the database and verify state.

Usage:
    python3 -I crash_driver.py <db_path> <tid> <crash_at>

crash_at:
    stage_done         hard exit right after the write set has been staged
    pre_commit         os._exit inside the pre-commit hook (before durable txn)
    pre_commit_sigkill SIGKILL inside the pre-commit hook
    mid_commit         os._exit inside the durable txn, before its COMMIT
    mid_commit_sigkill SIGKILL inside the durable txn, before its COMMIT
    post_commit        os._exit inside the post-commit hook (after durable txn)
    none               normal full commit, no crash
"""

import os
import signal
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from transaction_service import TransactionService  # noqa: E402

CRASH_KEY = "k1"
CRASH_VALUE = "v1"


def main() -> int:
    db_path, tid, crash_at = sys.argv[1], sys.argv[2], sys.argv[3]

    def pre_commit_hook(t: str) -> None:
        if t != tid:
            return
        if crash_at == "pre_commit":
            os._exit(42)
        if crash_at == "pre_commit_sigkill":
            os.kill(os.getpid(), signal.SIGKILL)

    def mid_commit_hook(t: str) -> None:
        if t != tid:
            return
        if crash_at == "mid_commit":
            os._exit(45)
        if crash_at == "mid_commit_sigkill":
            os.kill(os.getpid(), signal.SIGKILL)

    def post_commit_hook(t: str) -> None:
        if t == tid and crash_at == "post_commit":
            os._exit(43)

    svc = TransactionService(
        db_path,
        pre_commit_hook=pre_commit_hook,
        mid_commit_hook=mid_commit_hook,
        post_commit_hook=post_commit_hook,
    )
    svc.begin(tid)
    svc.read_range(tid, "a", "z")
    svc.stage(tid, {CRASH_KEY: CRASH_VALUE})
    if crash_at == "stage_done":
        os._exit(44)
    result = svc.commit(tid)
    # Only reached for crash_at == "none".
    print(result)
    svc.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
