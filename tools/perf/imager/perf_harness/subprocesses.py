# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical subprocess boundary for imaging evidence tools."""

from __future__ import annotations

import pathlib
import subprocess


def run_command(
    argv: list[str],
    *,
    cwd: pathlib.Path | None = None,
    environment: dict[str, str] | None = None,
    input_text: str | None = None,
    timeout_seconds: float | None = None,
    merge_stderr: bool = True,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        env=environment,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT if merge_stderr else subprocess.PIPE,
        timeout=timeout_seconds,
        check=check,
    )
