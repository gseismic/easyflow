from __future__ import annotations

import sys
import threading

from easyflow._internal.state_store import SQLiteStateStore


def main() -> int:
    database_url, owner, seconds = sys.argv[1:]
    lease_seconds = int(seconds)
    stopped = threading.Event()

    def watch_parent() -> None:
        try:
            sys.stdin.buffer.read(1)
        finally:
            stopped.set()

    threading.Thread(target=watch_parent, daemon=True).start()
    store = SQLiteStateStore(database_url)
    try:
        if not store.renew_scheduler_lease(owner=owner, lease_seconds=lease_seconds):
            return 1
        print("ready", flush=True)
        while not stopped.wait(min(1.0, lease_seconds / 3)):
            if not store.renew_scheduler_lease(
                owner=owner, lease_seconds=lease_seconds
            ):
                return 1
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
