"""Internal runtime helpers for task subprocess orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import os
from pathlib import Path
import shutil
import subprocess
from typing import Any

from .data import StrPath

CALIBRATION_PROTOCOL_NAME = "casa_calibration_task"
CALIBRATION_PROTOCOL_VERSION = 1
CALIBRATE_BINARY_ENVVAR = "CASARS_CALIBRATE_BIN"

_configured_calibrate_binary: str | None = None


class CalibrationBinaryNotFoundError(FileNotFoundError):
    """Raised when the ``calibrate`` binary cannot be resolved."""


class CalibrationProtocolMismatchError(RuntimeError):
    """Raised when the Python wrapper and binary protocol versions diverge."""


class CalibrationInvocationError(RuntimeError):
    """Raised when the ``calibrate`` subprocess returns a non-zero status."""


@dataclass(frozen=True, slots=True)
class ProtocolInfo:
    """Compatibility descriptor advertised by ``calibrate --protocol-info``."""

    protocol_name: str
    protocol_version: int
    binary_version: str


def configure_calibrate_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default calibrate binary override."""

    global _configured_calibrate_binary
    _configured_calibrate_binary = None if binary is None else os.fspath(binary)


def resolve_calibrate_binary(binary: StrPath | None = None) -> str:
    """Resolve the calibrate binary using the documented precedence order."""

    if binary is not None:
        return _require_binary(os.fspath(binary), source="explicit function override")
    if _configured_calibrate_binary is not None:
        return _require_binary(_configured_calibrate_binary, source="module configuration")

    env_binary = os.environ.get(CALIBRATE_BINARY_ENVVAR)
    if env_binary:
        return _require_binary(env_binary, source=f"${CALIBRATE_BINARY_ENVVAR}")

    repo_binary = _find_repo_local_binary()
    if repo_binary is not None:
        return repo_binary

    path_binary = shutil.which("calibrate")
    if path_binary is not None:
        return path_binary

    raise CalibrationBinaryNotFoundError(
        "could not resolve the calibrate binary; pass binary=..., call "
        "configure(binary=...), set CASARS_CALIBRATE_BIN, or ensure calibrate is on PATH"
    )


def get_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected binary."""

    resolved = resolve_calibrate_binary(binary)
    return _validated_protocol_info(resolved)


def fetch_calibration_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the binary."""

    resolved = resolve_calibrate_binary(binary)
    stdout = _run_process([resolved, "--json-schema"])
    return json.loads(stdout)


def invoke_calibration_task(
    *,
    kind: str,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one calibration task request through ``calibrate --json-run -``."""

    resolved = resolve_calibrate_binary(binary)
    _validated_protocol_info(resolved)
    payload = json.dumps({"kind": kind, "request": request}, sort_keys=True)
    stdout = _run_process([resolved, "--json-run", "-"], stdin=payload)
    return json.loads(stdout)


def _run_process(argv: list[str], stdin: str | None = None) -> str:
    process = subprocess.run(
        argv,
        input=stdin,
        capture_output=True,
        check=False,
        text=True,
    )
    if process.returncode != 0:
        stderr = process.stderr.strip()
        raise CalibrationInvocationError(
            f"{Path(argv[0]).name} exited with status {process.returncode}: {stderr or 'no stderr'}"
        )
    return process.stdout


def _require_binary(candidate: str, *, source: str) -> str:
    resolved = shutil.which(candidate) if os.path.sep not in candidate else candidate
    if resolved is None or not Path(resolved).exists():
        raise CalibrationBinaryNotFoundError(
            f"{source} did not resolve to an existing calibrate binary: {candidate}"
        )
    return resolved


def _find_repo_local_binary() -> str | None:
    here = Path(__file__).resolve()
    suffix = ".exe" if os.name == "nt" else ""
    for parent in here.parents:
        for profile in ("debug", "release"):
            candidate = parent / "target" / profile / f"calibrate{suffix}"
            if candidate.exists():
                return str(candidate)
    return None


@lru_cache(maxsize=None)
def _validated_protocol_info(binary: str) -> ProtocolInfo:
    stdout = _run_process([binary, "--protocol-info"])
    payload = json.loads(stdout)
    info = ProtocolInfo(
        protocol_name=str(payload["protocol_name"]),
        protocol_version=int(payload["protocol_version"]),
        binary_version=str(payload["binary_version"]),
    )
    if info.protocol_name != CALIBRATION_PROTOCOL_NAME:
        raise CalibrationProtocolMismatchError(
            f"expected protocol {CALIBRATION_PROTOCOL_NAME!r}, got {info.protocol_name!r}"
        )
    if info.protocol_version != CALIBRATION_PROTOCOL_VERSION:
        raise CalibrationProtocolMismatchError(
            "expected protocol version "
            f"{CALIBRATION_PROTOCOL_VERSION}, got {info.protocol_version}"
        )
    return info
