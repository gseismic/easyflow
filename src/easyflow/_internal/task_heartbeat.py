from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

from easyflow._internal.state_store import SQLiteStateStore


class TaskHeartbeat:
    """在独立解释器续租；调用方必须在 finally 中 stop，直到终态提交后才停止。"""

    def __init__(
        self,
        *,
        store: SQLiteStateStore,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int,
        owner: str,
        lease_seconds: int,
    ) -> None:
        self.arguments = [
            store.database_url,
            dag_id,
            run_id,
            task_id,
            str(try_number),
            owner,
            str(os.getpid()),
            str(lease_seconds),
        ]
        self.lease_seconds = lease_seconds
        self._process: subprocess.Popen[bytes] | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._started = False

    def start(self) -> None:
        environment = os.environ.copy()
        source_root = str(Path(__file__).resolve().parents[2])
        existing_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            source_root + os.pathsep + existing_pythonpath
            if existing_pythonpath
            else source_root
        )
        self._process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "easyflow._internal.task_heartbeat",
                *self.arguments,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
        )
        self._thread = threading.Thread(target=self._await_ready, daemon=True)
        self._thread.start()
        if (
            not self._ready.wait(max(2, min(10, self.lease_seconds)))
            or not self._started
        ):
            raise RuntimeError("任务续租进程启动失败。")

    def _await_ready(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        try:
            self._started = self._process.stdout.readline().strip() == b"ready"
        finally:
            self._ready.set()

    def stop(self) -> None:
        process = self._process
        if process is None:
            return
        if process.stdin is not None:
            try:
                process.stdin.close()
            except BrokenPipeError:
                pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if process.stdout is not None:
            process.stdout.close()


def main() -> int:
    database_url, dag_id, run_id, task_id, attempt, owner, pid, seconds = sys.argv[1:]
    worker_pid = int(pid)
    lease_seconds = int(seconds)
    stopped = threading.Event()

    def watch_worker() -> None:
        try:
            sys.stdin.buffer.read(1)
        finally:
            stopped.set()

    threading.Thread(target=watch_worker, daemon=True).start()
    store = SQLiteStateStore(database_url)
    first = True
    try:
        while not stopped.is_set():
            # worker 派生的其他进程即使继承管道，也不能在 worker 退出后继续续租。
            if os.getppid() != worker_pid:
                return 0
            if not store.renew_task_lease(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                try_number=int(attempt),
                owner=owner,
                worker_pid=worker_pid,
                lease_seconds=lease_seconds,
            ):
                return 1
            if first:
                print("ready", flush=True)
                first = False
            stopped.wait(max(0.2, min(1.0, lease_seconds / 3)))
        return 0
    except (OSError, sqlite3.Error, ValueError):
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
