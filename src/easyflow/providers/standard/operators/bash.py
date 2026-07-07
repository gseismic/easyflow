from __future__ import annotations

import os
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
        self.env = dict(env or {})
        self.append_env = append_env
        self.cwd = cwd

    def execute(self, context: dict[str, Any]) -> str:
        env = os.environ.copy() if self.append_env else {}
        env.update(self.env)
        completed = subprocess.run(
            self.bash_command,
            shell=True,
            cwd=self.cwd,
            env=env or None,
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
