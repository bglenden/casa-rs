"""Internal runtime helpers for task subprocess orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import os
from os import PathLike
from pathlib import Path
import shutil
import subprocess
from typing import Any, TypeAlias

StrPath: TypeAlias = str | PathLike[str]

CALIBRATION_PROTOCOL_NAME = "casa_calibration_task"
CALIBRATION_PROTOCOL_VERSION = 1
CALIBRATE_BINARY_NAME = "calibrate"
CALIBRATE_BINARY_ENVVAR = "CASARS_CALIBRATE_BIN"

IMPORTVLA_PROTOCOL_NAME = "casa_importvla_task"
IMPORTVLA_PROTOCOL_VERSION = 1
IMPORTVLA_BINARY_NAME = "casars-importvla"
IMPORTVLA_BINARY_ENVVAR = "CASARS_IMPORTVLA_BIN"

CASARS_SUITE_ROOT_ENVVAR = "CASARS_SUITE_ROOT"

_configured_calibrate_binary: str | None = None
_configured_importvla_binary: str | None = None


class CalibrationBinaryNotFoundError(FileNotFoundError):
    """Raised when the ``calibrate`` binary cannot be resolved."""


class CalibrationProtocolMismatchError(RuntimeError):
    """Raised when the Python wrapper and ``calibrate`` protocol versions diverge."""


class CalibrationInvocationError(RuntimeError):
    """Raised when the ``calibrate`` subprocess returns a non-zero status."""


class ImportVlaBinaryNotFoundError(FileNotFoundError):
    """Raised when the ``casars-importvla`` binary cannot be resolved."""


class ImportVlaProtocolMismatchError(RuntimeError):
    """Raised when the Python wrapper and ``importvla`` protocol versions diverge."""


class ImportVlaInvocationError(RuntimeError):
    """Raised when the ``casars-importvla`` subprocess returns a non-zero status."""


@dataclass(frozen=True, slots=True)
class ProtocolInfo:
    """Compatibility descriptor advertised by a task provider binary."""

    protocol_name: str
    protocol_version: int
    binary_version: str
    surface_kind: str | None = None


def configure_calibrate_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default calibrate binary override."""

    global _configured_calibrate_binary
    _configured_calibrate_binary = None if binary is None else os.fspath(binary)


def configure_importvla_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default importvla binary override."""

    global _configured_importvla_binary
    _configured_importvla_binary = None if binary is None else os.fspath(binary)


def resolve_calibrate_binary(binary: StrPath | None = None) -> str:
    """Resolve the calibrate binary using the documented precedence order."""

    return _resolve_task_binary(
        binary=binary,
        configured_binary=_configured_calibrate_binary,
        envvar=CALIBRATE_BINARY_ENVVAR,
        binary_name=CALIBRATE_BINARY_NAME,
        missing_error_cls=CalibrationBinaryNotFoundError,
        description="calibrate",
    )


def resolve_importvla_binary(binary: StrPath | None = None) -> str:
    """Resolve the importvla binary using the documented precedence order."""

    return _resolve_task_binary(
        binary=binary,
        configured_binary=_configured_importvla_binary,
        envvar=IMPORTVLA_BINARY_ENVVAR,
        binary_name=IMPORTVLA_BINARY_NAME,
        missing_error_cls=ImportVlaBinaryNotFoundError,
        description="casars-importvla",
    )


def get_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected calibrate binary."""

    resolved = resolve_calibrate_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=CALIBRATION_PROTOCOL_NAME,
        protocol_version=CALIBRATION_PROTOCOL_VERSION,
        mismatch_error_cls=CalibrationProtocolMismatchError,
    )


def get_importvla_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected importvla binary."""

    resolved = resolve_importvla_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMPORTVLA_PROTOCOL_NAME,
        protocol_version=IMPORTVLA_PROTOCOL_VERSION,
        mismatch_error_cls=ImportVlaProtocolMismatchError,
    )


def fetch_calibration_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the calibrate binary."""

    resolved = resolve_calibrate_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=CalibrationInvocationError)
    return json.loads(stdout)


def fetch_importvla_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the importvla binary."""

    resolved = resolve_importvla_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImportVlaInvocationError)
    return json.loads(stdout)


def invoke_calibration_task(
    *,
    kind: str,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one calibration task request through ``calibrate --json-run -``."""

    resolved = resolve_calibrate_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=CALIBRATION_PROTOCOL_NAME,
        protocol_version=CALIBRATION_PROTOCOL_VERSION,
        mismatch_error_cls=CalibrationProtocolMismatchError,
    )
    payload = json.dumps({"kind": kind, "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=CalibrationInvocationError,
    )
    return json.loads(stdout)


def invoke_importvla_task(
    *,
    kind: str,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one importvla task request through ``casars-importvla --json-run -``."""

    resolved = resolve_importvla_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMPORTVLA_PROTOCOL_NAME,
        protocol_version=IMPORTVLA_PROTOCOL_VERSION,
        mismatch_error_cls=ImportVlaProtocolMismatchError,
    )
    payload = json.dumps({"kind": kind, "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImportVlaInvocationError,
    )
    return json.loads(stdout)


def _run_process(
    argv: list[str],
    *,
    error_cls: type[RuntimeError],
    stdin: str | None = None,
) -> str:
    process = subprocess.run(
        argv,
        input=stdin,
        capture_output=True,
        check=False,
        text=True,
    )
    if process.returncode != 0:
        stderr = process.stderr.strip()
        raise error_cls(
            f"{Path(argv[0]).name} exited with status {process.returncode}: {stderr or 'no stderr'}"
        )
    return process.stdout


def _resolve_task_binary(
    *,
    binary: StrPath | None,
    configured_binary: str | None,
    envvar: str,
    binary_name: str,
    missing_error_cls: type[FileNotFoundError],
    description: str,
) -> str:
    if binary is not None:
        return _require_binary(
            os.fspath(binary),
            source="explicit function override",
            missing_error_cls=missing_error_cls,
            description=description,
        )
    if configured_binary is not None:
        return _require_binary(
            configured_binary,
            source="module configuration",
            missing_error_cls=missing_error_cls,
            description=description,
        )

    env_binary = os.environ.get(envvar)
    if env_binary:
        return _require_binary(
            env_binary,
            source=f"${envvar}",
            missing_error_cls=missing_error_cls,
            description=description,
        )

    env_suite_root = os.environ.get(CASARS_SUITE_ROOT_ENVVAR)
    if env_suite_root:
        return _require_suite_binary(
            Path(env_suite_root),
            source=f"${CASARS_SUITE_ROOT_ENVVAR}",
            binary_name=binary_name,
            missing_error_cls=missing_error_cls,
            description=description,
        )

    suite_binary = _find_installed_suite_binary(binary_name)
    if suite_binary is not None:
        return suite_binary

    repo_binary = _find_repo_local_binary(binary_name)
    if repo_binary is not None:
        return repo_binary

    path_binary = shutil.which(binary_name)
    if path_binary is not None:
        return path_binary

    raise missing_error_cls(
        f"could not resolve the {description} binary; pass binary=..., call "
        f"configure(binary=...), set {envvar}, set {CASARS_SUITE_ROOT_ENVVAR}, "
        f"install the casa-rs suite, or ensure {binary_name} is on PATH"
    )


def _require_binary(
    candidate: str,
    *,
    source: str,
    missing_error_cls: type[FileNotFoundError],
    description: str,
) -> str:
    resolved = shutil.which(candidate) if os.path.sep not in candidate else candidate
    if resolved is None or not Path(resolved).exists():
        raise missing_error_cls(
            f"{source} did not resolve to an existing {description} binary: {candidate}"
        )
    return resolved


def _require_suite_binary(
    root: Path,
    *,
    source: str,
    binary_name: str,
    missing_error_cls: type[FileNotFoundError],
    description: str,
) -> str:
    candidate = _suite_binary_path(root, binary_name)
    if not candidate.exists():
        raise missing_error_cls(
            f"{source} did not contain a suite {description} binary at {candidate}"
        )
    return str(candidate)


def _binary_name(binary_name: str) -> str:
    suffix = ".exe" if os.name == "nt" else ""
    return f"{binary_name}{suffix}"


def _suite_binary_path(root: Path, binary_name: str) -> Path:
    return root / "bin" / _binary_name(binary_name)


def _find_installed_suite_binary(
    binary_name: str = CALIBRATE_BINARY_NAME,
    *,
    module_file: StrPath | None = None,
    home: Path | None = None,
) -> str | None:
    here = Path(module_file).resolve() if module_file is not None else Path(__file__).resolve()
    seen: set[Path] = set()

    for ancestor in here.parents:
        candidate = _suite_binary_path(ancestor, binary_name)
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            return str(candidate)

    standard_root = (home or Path.home()) / ".local" / "opt" / "casa-rs" / "current"
    standard_candidate = _suite_binary_path(standard_root, binary_name)
    if standard_candidate not in seen and standard_candidate.exists():
        return str(standard_candidate)

    return None


def _find_repo_local_binary(binary_name: str = CALIBRATE_BINARY_NAME) -> str | None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        for profile in ("debug", "release"):
            candidate = parent / "target" / profile / _binary_name(binary_name)
            if candidate.exists():
                return str(candidate)
    return None


@lru_cache(maxsize=None)
def _validated_protocol_info(
    binary: str,
    *,
    protocol_name: str,
    protocol_version: int,
    mismatch_error_cls: type[RuntimeError],
) -> ProtocolInfo:
    stdout = _run_process([binary, "--protocol-info"], error_cls=RuntimeError)
    payload = json.loads(stdout)
    info = ProtocolInfo(
        protocol_name=str(payload["protocol_name"]),
        protocol_version=int(payload["protocol_version"]),
        binary_version=str(payload["binary_version"]),
        surface_kind=str(payload["surface_kind"]) if "surface_kind" in payload else None,
    )
    if info.protocol_name != protocol_name:
        raise mismatch_error_cls(
            f"expected protocol {protocol_name!r}, got {info.protocol_name!r}"
        )
    if info.protocol_version != protocol_version:
        raise mismatch_error_cls(
            f"expected protocol version {protocol_version}, got {info.protocol_version}"
        )
    return info
