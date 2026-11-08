from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from collections.abc import Container
from contextlib import nullcontext
from typing import Any

from easyflow.sdk.exceptions import AirflowSkipException
from easyflow.sdk.operators.base import BaseOperator


class BashOperator(BaseOperator):
    template_fields = ("bash_command", "env", "cwd")

    def __init__(
        self,
        *,
        bash_command: str,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        cwd: str | None = None,
        skip_on_exit_code: int | Container[int] | None = 99,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.env = None if env is None else dict(env)
        self.append_env = append_env
        self.cwd = cwd
        self.skip_on_exit_code = (
            ()
            if skip_on_exit_code is None
            else (skip_on_exit_code,)
            if isinstance(skip_on_exit_code, int)
            else skip_on_exit_code
        )
        self.set_xcom_dependencies()

    def execute(self, context: dict[str, Any]) -> str:
        env = (
            os.environ.copy() if self.append_env else (None if self.env is None else {})
        )
        if self.env is not None:
            assert env is not None
            env.update(self.env)
        bash = (
            "/bin/bash"
            if os.path.isfile("/bin/bash")
            else shutil.which("bash") or "bash"
        )
        directory = (
            tempfile.TemporaryDirectory(prefix="easyflow-")
            if self.cwd is None
            else nullcontext(self.cwd)
        )
        with directory as cwd:
            with subprocess.Popen(
                [bash, "-c", self.bash_command],
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            ) as process:
                assert process.stdout is not None
                last_line = ""
                for line in process.stdout:
                    print(line, end="", flush=True)
                    last_line = line.rstrip("\r\n")
                returncode = process.wait()
        if returncode in self.skip_on_exit_code:
            raise AirflowSkipException(
                f"Bash command skipped with exit code {returncode}."
            )
        if returncode != 0:
            raise RuntimeError(f"Bash command failed with exit code {returncode}.")
        return last_line
