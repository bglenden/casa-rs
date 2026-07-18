# SPDX-License-Identifier: LGPL-3.0-or-later
"""JSON file protocol for checked-in CASA-side programs."""

from __future__ import annotations

import pathlib
import subprocess
from dataclasses import dataclass
from typing import Any

from .artifacts import ArtifactError, atomic_write_json, load_json_object
from .subprocesses import run_command


@dataclass(frozen=True)
class CasaProtocolResult:
    status: str
    return_code: int | None
    output: dict[str, Any] | None
    reason: str | None
    request_path: pathlib.Path
    output_path: pathlib.Path
    log_path: pathlib.Path


def run_json_file_protocol(
    *,
    casa_python: str | None,
    script: pathlib.Path,
    request: dict[str, Any],
    request_path: pathlib.Path,
    output_path: pathlib.Path,
    log_path: pathlib.Path,
    cwd: pathlib.Path,
    timeout_seconds: float | None = None,
    environment: dict[str, str] | None = None,
) -> CasaProtocolResult:
    atomic_write_json(request_path, request)
    output_path.unlink(missing_ok=True)
    if not casa_python:
        return _result(
            "unavailable",
            None,
            None,
            "CASA Python is not configured",
            request_path,
            output_path,
            log_path,
        )
    if not script.is_file():
        return _result(
            "failed_execution",
            None,
            None,
            f"checked-in CASA program is missing: {script}",
            request_path,
            output_path,
            log_path,
        )
    try:
        completed = run_command(
            [casa_python, str(script), str(request_path), str(output_path)],
            cwd=cwd,
            environment=environment,
            timeout_seconds=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        log_path.write_text(str(error) + "\n", encoding="utf-8")
        return _result(
            "failed_execution",
            None,
            None,
            str(error),
            request_path,
            output_path,
            log_path,
        )
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        return _result(
            "failed_execution",
            completed.returncode,
            None,
            f"CASA program exited {completed.returncode}",
            request_path,
            output_path,
            log_path,
        )
    try:
        output = load_json_object(output_path, description="CASA protocol output")
    except ArtifactError as error:
        return _result(
            "failed_execution",
            completed.returncode,
            None,
            str(error),
            request_path,
            output_path,
            log_path,
        )
    return _result(
        "completed",
        completed.returncode,
        output,
        None,
        request_path,
        output_path,
        log_path,
    )


def _result(
    status: str,
    return_code: int | None,
    output: dict[str, Any] | None,
    reason: str | None,
    request_path: pathlib.Path,
    output_path: pathlib.Path,
    log_path: pathlib.Path,
) -> CasaProtocolResult:
    return CasaProtocolResult(
        status=status,
        return_code=return_code,
        output=output,
        reason=reason,
        request_path=request_path,
        output_path=output_path,
        log_path=log_path,
    )
