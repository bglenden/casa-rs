"""Shared bounded subprocess execution and provenance capture."""

from __future__ import annotations

import hashlib
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    return_code: int
    elapsed_seconds: float
    stdout: str
    stderr: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "argv": list(self.argv),
            "return_code": self.return_code,
            "elapsed_seconds": self.elapsed_seconds,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }


def run_command(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout_seconds: float,
) -> CommandResult:
    started = time.perf_counter()
    completed = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds,
        check=False,
    )
    return CommandResult(
        argv=tuple(argv),
        return_code=completed.returncode,
        elapsed_seconds=time.perf_counter() - started,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def environment_with_pythonpath(repo_root: Path, base: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(base or os.environ)
    python_path = str(repo_root / "crates" / "casars-python" / "python")
    previous = env.get("PYTHONPATH")
    env["PYTHONPATH"] = python_path if not previous else python_path + os.pathsep + previous
    return env


def path_record(path: Path, root: Path | None = None) -> dict[str, Any]:
    absolute = path.absolute()
    display = str(absolute.relative_to(root)) if root and absolute.is_relative_to(root) else str(absolute)
    record: dict[str, Any] = {
        "path": display,
        "exists": absolute.exists(),
        "kind": "directory" if absolute.is_dir() else "file" if absolute.is_file() else "missing",
    }
    if absolute.is_file():
        digest = hashlib.sha256()
        with absolute.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        record["sha256"] = digest.hexdigest()
        record["size_bytes"] = absolute.stat().st_size
    return record
