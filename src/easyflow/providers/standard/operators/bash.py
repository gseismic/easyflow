from __future__ import annotations

import os
import shutil
import subprocess
from typing import Any

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
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.env = None if env is None else dict(env)
        self.append_env = append_env
        self.cwd = cwd

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
        completed = subprocess.run(
            [bash, "-c", self.bash_command],
            cwd=self.cwd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="")
        if completed.returncode != 0:
            raise RuntimeError(
                f"Bash command failed with exit code {completed.returncode}: {self.bash_command}"
            )
        return completed.stdout
