"""Internal runtime helpers for task subprocess orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import os
from os import PathLike
from pathlib import Path
import subprocess
from typing import Any, TypeAlias

from . import _core

StrPath: TypeAlias = str | PathLike[str]

CALIBRATION_PROTOCOL_NAME = "casa_calibration_task"
CALIBRATION_PROTOCOL_VERSION = 1

IMPORTVLA_PROTOCOL_NAME = "casa_importvla_task"
IMPORTVLA_PROTOCOL_VERSION = 1

SIMOBSERVE_PROTOCOL_NAME = "casa_simobserve_task"
SIMOBSERVE_PROTOCOL_VERSION = 2

MSEXPLORE_PROTOCOL_NAME = "casa_msexplore_task"
MSEXPLORE_PROTOCOL_VERSION = 1

IMAGER_PROTOCOL_NAME = "casa_imager_task"
IMAGER_PROTOCOL_VERSION = 3

IMAGE_ANALYSIS_PROTOCOL_NAME = "casa_image_analysis_task"
IMAGE_ANALYSIS_PROTOCOL_VERSION = 1
CASARS_SUITE_ROOT_ENVVAR = "CASARS_SUITE_ROOT"
CASARS_LAUNCH_MODE_ENVVAR = "CASARS_LAUNCH_MODE"
CASARS_DEVELOPMENT_WORKSPACE_ENVVAR = "CASARS_DEVELOPMENT_WORKSPACE"

_configured_calibrate_binary: str | None = None
_configured_importvla_binary: str | None = None
_configured_simobserve_binary: str | None = None
_configured_msexplore_binary: str | None = None
_configured_mstransform_binary: str | None = None
_configured_imager_binary: str | None = None
_configured_imexplore_binary: str | None = None
_configured_immoments_binary: str | None = None
_configured_impv_binary: str | None = None
_configured_imsubimage_binary: str | None = None
_configured_immath_binary: str | None = None
_configured_impbcor_binary: str | None = None
_configured_exportfits_binary: str | None = None
_configured_importfits_binary: str | None = None


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


class SimobserveBinaryNotFoundError(FileNotFoundError):
    """Raised when the ``simobserve`` binary cannot be resolved."""


class SimobserveProtocolMismatchError(RuntimeError):
    """Raised when the Python wrapper and ``simobserve`` protocol versions diverge."""


class SimobserveInvocationError(RuntimeError):
    """Raised when the ``simobserve`` subprocess returns a non-zero status."""


class MsExploreBinaryNotFoundError(FileNotFoundError):
    """Raised when the ``msexplore`` binary cannot be resolved."""


class MsExploreProtocolMismatchError(RuntimeError):
    """Raised when the Python wrapper and ``msexplore`` protocol versions diverge."""


class MsExploreInvocationError(RuntimeError):
    """Raised when the ``msexplore`` subprocess returns a non-zero status."""


class MsTransformBinaryNotFoundError(FileNotFoundError):
    """Raised when the ``mstransform`` binary cannot be resolved."""


class MsTransformInvocationError(RuntimeError):
    """Raised when the ``mstransform`` subprocess returns a non-zero status."""


class ImagerBinaryNotFoundError(FileNotFoundError):
    """Raised when the ``casars-imager`` binary cannot be resolved."""


class ImagerProtocolMismatchError(RuntimeError):
    """Raised when the Python wrapper and ``casars-imager`` protocol versions diverge."""


class ImagerInvocationError(RuntimeError):
    """Raised when the ``casars-imager`` subprocess returns a non-zero status."""


class ImageAnalysisBinaryNotFoundError(FileNotFoundError):
    """Raised when an image-analysis binary cannot be resolved."""


class ImageAnalysisProtocolMismatchError(RuntimeError):
    """Raised when an image-analysis binary protocol version diverges."""


class ImageAnalysisInvocationError(RuntimeError):
    """Raised when an image-analysis subprocess returns a non-zero status."""


@dataclass(frozen=True, slots=True)
class ProtocolInfo:
    """Compatibility descriptor advertised by a task provider binary."""

    protocol_name: str
    protocol_version: int
    binary_version: str
    surface_kind: str | None = None


@dataclass(frozen=True, slots=True)
class ApplicationLaunch:
    """Launch metadata projected from the canonical provider-owned catalog."""

    executable: str
    cargo_package: str
    override_env: str


def configure_calibrate_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default calibrate binary override."""

    global _configured_calibrate_binary
    _configured_calibrate_binary = None if binary is None else os.fspath(binary)


def configure_importvla_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default importvla binary override."""

    global _configured_importvla_binary
    _configured_importvla_binary = None if binary is None else os.fspath(binary)


def configure_simobserve_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default simobserve binary override."""

    global _configured_simobserve_binary
    _configured_simobserve_binary = None if binary is None else os.fspath(binary)


def configure_msexplore_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default msexplore binary override."""

    global _configured_msexplore_binary
    _configured_msexplore_binary = None if binary is None else os.fspath(binary)


def configure_mstransform_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default mstransform binary override."""

    global _configured_mstransform_binary
    _configured_mstransform_binary = None if binary is None else os.fspath(binary)


def configure_imager_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default imager binary override."""

    global _configured_imager_binary
    _configured_imager_binary = None if binary is None else os.fspath(binary)


def configure_imexplore_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default imexplore binary override."""

    global _configured_imexplore_binary
    _configured_imexplore_binary = None if binary is None else os.fspath(binary)


def configure_immoments_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default immoments binary override."""

    global _configured_immoments_binary
    _configured_immoments_binary = None if binary is None else os.fspath(binary)


def configure_impv_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default impv binary override."""

    global _configured_impv_binary
    _configured_impv_binary = None if binary is None else os.fspath(binary)


def configure_imsubimage_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default imsubimage binary override."""

    global _configured_imsubimage_binary
    _configured_imsubimage_binary = None if binary is None else os.fspath(binary)


def configure_immath_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default immath binary override."""

    global _configured_immath_binary
    _configured_immath_binary = None if binary is None else os.fspath(binary)


def configure_impbcor_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default impbcor binary override."""

    global _configured_impbcor_binary
    _configured_impbcor_binary = None if binary is None else os.fspath(binary)


def configure_exportfits_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default exportfits binary override."""

    global _configured_exportfits_binary
    _configured_exportfits_binary = None if binary is None else os.fspath(binary)


def configure_importfits_binary(binary: StrPath | None) -> None:
    """Set or clear the module-wide default importfits binary override."""

    global _configured_importfits_binary
    _configured_importfits_binary = None if binary is None else os.fspath(binary)


def resolve_calibrate_binary(binary: StrPath | None = None) -> str:
    """Resolve the calibrate binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="calibrate",
        binary=binary,
        configured_binary=_configured_calibrate_binary,
        missing_error_cls=CalibrationBinaryNotFoundError,
        description="calibrate",
    )


def resolve_importvla_binary(binary: StrPath | None = None) -> str:
    """Resolve the importvla binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="importvla",
        binary=binary,
        configured_binary=_configured_importvla_binary,
        missing_error_cls=ImportVlaBinaryNotFoundError,
        description="casars-importvla",
    )


def resolve_simobserve_binary(binary: StrPath | None = None) -> str:
    """Resolve the simobserve binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="simobserve",
        binary=binary,
        configured_binary=_configured_simobserve_binary,
        missing_error_cls=SimobserveBinaryNotFoundError,
        description="simobserve",
    )


def resolve_msexplore_binary(binary: StrPath | None = None) -> str:
    """Resolve the msexplore binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="msexplore",
        binary=binary,
        configured_binary=_configured_msexplore_binary,
        missing_error_cls=MsExploreBinaryNotFoundError,
        description="msexplore",
    )


def resolve_mstransform_binary(binary: StrPath | None = None) -> str:
    """Resolve the mstransform binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="mstransform",
        binary=binary,
        configured_binary=_configured_mstransform_binary,
        missing_error_cls=MsTransformBinaryNotFoundError,
        description="mstransform",
    )


def resolve_imager_binary(binary: StrPath | None = None) -> str:
    """Resolve the imager binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="imager",
        binary=binary,
        configured_binary=_configured_imager_binary,
        missing_error_cls=ImagerBinaryNotFoundError,
        description="casars-imager",
    )


def resolve_imexplore_binary(binary: StrPath | None = None) -> str:
    """Resolve the imexplore binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="imexplore",
        binary=binary,
        configured_binary=_configured_imexplore_binary,
        missing_error_cls=ImageAnalysisBinaryNotFoundError,
        description="imexplore",
    )


def resolve_immoments_binary(binary: StrPath | None = None) -> str:
    """Resolve the immoments binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="immoments",
        binary=binary,
        configured_binary=_configured_immoments_binary,
        missing_error_cls=ImageAnalysisBinaryNotFoundError,
        description="immoments",
    )


def resolve_impv_binary(binary: StrPath | None = None) -> str:
    """Resolve the impv binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="impv",
        binary=binary,
        configured_binary=_configured_impv_binary,
        missing_error_cls=ImageAnalysisBinaryNotFoundError,
        description="impv",
    )


def resolve_imsubimage_binary(binary: StrPath | None = None) -> str:
    """Resolve the imsubimage binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="imsubimage",
        binary=binary,
        configured_binary=_configured_imsubimage_binary,
        missing_error_cls=ImageAnalysisBinaryNotFoundError,
        description="imsubimage",
    )


def resolve_immath_binary(binary: StrPath | None = None) -> str:
    """Resolve the immath binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="immath",
        binary=binary,
        configured_binary=_configured_immath_binary,
        missing_error_cls=ImageAnalysisBinaryNotFoundError,
        description="immath",
    )


def resolve_impbcor_binary(binary: StrPath | None = None) -> str:
    """Resolve the impbcor binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="impbcor",
        binary=binary,
        configured_binary=_configured_impbcor_binary,
        missing_error_cls=ImageAnalysisBinaryNotFoundError,
        description="impbcor",
    )


def resolve_exportfits_binary(binary: StrPath | None = None) -> str:
    """Resolve the exportfits binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="exportfits",
        binary=binary,
        configured_binary=_configured_exportfits_binary,
        missing_error_cls=ImageAnalysisBinaryNotFoundError,
        description="exportfits",
    )


def resolve_importfits_binary(binary: StrPath | None = None) -> str:
    """Resolve the importfits binary using the documented precedence order."""

    return _resolve_task_binary(
        application_id="importfits",
        binary=binary,
        configured_binary=_configured_importfits_binary,
        missing_error_cls=ImageAnalysisBinaryNotFoundError,
        description="importfits",
    )


def get_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected calibrate binary."""

    resolved = resolve_calibrate_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=CALIBRATION_PROTOCOL_NAME,
        protocol_version=CALIBRATION_PROTOCOL_VERSION,
        mismatch_error_cls=CalibrationProtocolMismatchError,
        invocation_error_cls=CalibrationInvocationError,
    )


def get_importvla_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected importvla binary."""

    resolved = resolve_importvla_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMPORTVLA_PROTOCOL_NAME,
        protocol_version=IMPORTVLA_PROTOCOL_VERSION,
        mismatch_error_cls=ImportVlaProtocolMismatchError,
        invocation_error_cls=ImportVlaInvocationError,
    )


def get_simobserve_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected simobserve binary."""

    resolved = resolve_simobserve_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=SIMOBSERVE_PROTOCOL_NAME,
        protocol_version=SIMOBSERVE_PROTOCOL_VERSION,
        mismatch_error_cls=SimobserveProtocolMismatchError,
        invocation_error_cls=SimobserveInvocationError,
    )


def get_msexplore_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected msexplore binary."""

    resolved = resolve_msexplore_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=MSEXPLORE_PROTOCOL_NAME,
        protocol_version=MSEXPLORE_PROTOCOL_VERSION,
        mismatch_error_cls=MsExploreProtocolMismatchError,
        invocation_error_cls=MsExploreInvocationError,
    )


def get_imager_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected imager binary."""

    resolved = resolve_imager_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMAGER_PROTOCOL_NAME,
        protocol_version=IMAGER_PROTOCOL_VERSION,
        mismatch_error_cls=ImagerProtocolMismatchError,
        invocation_error_cls=ImagerInvocationError,
    )


def get_immoments_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected immoments binary."""

    resolved = resolve_immoments_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )


def get_impv_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected impv binary."""

    resolved = resolve_impv_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )


def get_imsubimage_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected imsubimage binary."""

    resolved = resolve_imsubimage_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )


def get_immath_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected immath binary."""

    resolved = resolve_immath_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )


def get_impbcor_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected impbcor binary."""

    resolved = resolve_impbcor_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )


def get_exportfits_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected exportfits binary."""

    resolved = resolve_exportfits_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )


def get_importfits_protocol_info(binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol info for the selected importfits binary."""

    resolved = resolve_importfits_binary(binary)
    return _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
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


def fetch_simobserve_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the simobserve binary."""

    resolved = resolve_simobserve_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=SimobserveInvocationError)
    return json.loads(stdout)


def fetch_msexplore_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the msexplore binary."""

    resolved = resolve_msexplore_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=MsExploreInvocationError)
    return json.loads(stdout)


def fetch_imager_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the imager binary."""

    resolved = resolve_imager_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImagerInvocationError)
    return json.loads(stdout)


def fetch_immoments_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the immoments binary."""

    resolved = resolve_immoments_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImageAnalysisInvocationError)
    return json.loads(stdout)


def fetch_impv_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the impv binary."""

    resolved = resolve_impv_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImageAnalysisInvocationError)
    return json.loads(stdout)


def fetch_imsubimage_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the imsubimage binary."""

    resolved = resolve_imsubimage_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImageAnalysisInvocationError)
    return json.loads(stdout)


def fetch_immath_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the immath binary."""

    resolved = resolve_immath_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImageAnalysisInvocationError)
    return json.loads(stdout)


def fetch_impbcor_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the impbcor binary."""

    resolved = resolve_impbcor_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImageAnalysisInvocationError)
    return json.loads(stdout)


def fetch_exportfits_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the exportfits binary."""

    resolved = resolve_exportfits_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImageAnalysisInvocationError)
    return json.loads(stdout)


def fetch_importfits_schema(binary: StrPath | None = None) -> dict[str, Any]:
    """Fetch the JSON schema bundle advertised by the importfits binary."""

    resolved = resolve_importfits_binary(binary)
    stdout = _run_process([resolved, "--json-schema"], error_cls=ImageAnalysisInvocationError)
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
        invocation_error_cls=CalibrationInvocationError,
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
        invocation_error_cls=ImportVlaInvocationError,
    )
    payload = json.dumps({"kind": kind, "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImportVlaInvocationError,
    )
    return json.loads(stdout)


def invoke_simobserve_task(
    *,
    kind: str,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one simobserve task request through ``simobserve --json-run -``."""

    resolved = resolve_simobserve_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=SIMOBSERVE_PROTOCOL_NAME,
        protocol_version=SIMOBSERVE_PROTOCOL_VERSION,
        mismatch_error_cls=SimobserveProtocolMismatchError,
        invocation_error_cls=SimobserveInvocationError,
    )
    payload = json.dumps({"kind": kind, "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=SimobserveInvocationError,
    )
    return json.loads(stdout)


def invoke_msexplore_task(
    *,
    kind: str,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one msexplore task request through ``msexplore --json-run -``."""

    resolved = resolve_msexplore_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=MSEXPLORE_PROTOCOL_NAME,
        protocol_version=MSEXPLORE_PROTOCOL_VERSION,
        mismatch_error_cls=MsExploreProtocolMismatchError,
        invocation_error_cls=MsExploreInvocationError,
    )
    payload = json.dumps({"kind": kind, "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=MsExploreInvocationError,
    )
    return json.loads(stdout)


def invoke_imager_task(
    *,
    kind: str,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one imager task request through ``casars-imager --json-run -``."""

    resolved = resolve_imager_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMAGER_PROTOCOL_NAME,
        protocol_version=IMAGER_PROTOCOL_VERSION,
        mismatch_error_cls=ImagerProtocolMismatchError,
        invocation_error_cls=ImagerInvocationError,
    )
    payload = json.dumps({"kind": kind, "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImagerInvocationError,
    )
    return json.loads(stdout)


def invoke_immoments_task(
    *,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one immoments task request through ``immoments --json-run -``."""

    resolved = resolve_immoments_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )
    payload = json.dumps({"kind": "immoments", "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImageAnalysisInvocationError,
    )
    return json.loads(stdout)


def invoke_impv_task(
    *,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one impv task request through ``impv --json-run -``."""

    resolved = resolve_impv_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )
    payload = json.dumps({"kind": "impv", "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImageAnalysisInvocationError,
    )
    return json.loads(stdout)


def invoke_imsubimage_task(
    *,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one imsubimage task request through ``imsubimage --json-run -``."""

    resolved = resolve_imsubimage_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )
    payload = json.dumps({"kind": "imsubimage", "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImageAnalysisInvocationError,
    )
    return json.loads(stdout)


def invoke_immath_task(
    *,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one immath task request through ``immath --json-run -``."""

    resolved = resolve_immath_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )
    payload = json.dumps({"kind": "immath", "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImageAnalysisInvocationError,
    )
    return json.loads(stdout)


def invoke_impbcor_task(
    *,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one impbcor task request through ``impbcor --json-run -``."""

    resolved = resolve_impbcor_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )
    payload = json.dumps({"kind": "impbcor", "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImageAnalysisInvocationError,
    )
    return json.loads(stdout)


def invoke_exportfits_task(
    *,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one exportfits task request through ``exportfits --json-run -``."""

    resolved = resolve_exportfits_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )
    payload = json.dumps({"kind": "exportfits", "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImageAnalysisInvocationError,
    )
    return json.loads(stdout)


def invoke_importfits_task(
    *,
    request: dict[str, Any],
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run one importfits task request through ``importfits --json-run -``."""

    resolved = resolve_importfits_binary(binary)
    _validated_protocol_info(
        resolved,
        protocol_name=IMAGE_ANALYSIS_PROTOCOL_NAME,
        protocol_version=IMAGE_ANALYSIS_PROTOCOL_VERSION,
        mismatch_error_cls=ImageAnalysisProtocolMismatchError,
        invocation_error_cls=ImageAnalysisInvocationError,
    )
    payload = json.dumps({"kind": "importfits", "request": request}, sort_keys=True)
    stdout = _run_process(
        [resolved, "--json-run", "-"],
        stdin=payload,
        error_cls=ImageAnalysisInvocationError,
    )
    return json.loads(stdout)


def invoke_imexplore_json_subcommand(
    subcommand: str,
    argv: list[str],
    *,
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run an imexplore JSON subcommand such as ``imhead`` or ``imstat``."""

    resolved = resolve_imexplore_binary(binary)
    stdout = _run_process(
        [resolved, subcommand, *argv, "--json"],
        error_cls=ImageAnalysisInvocationError,
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
    application_id: str,
    binary: StrPath | None,
    configured_binary: str | None,
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

    launch = _application_launch(application_id)
    mode = os.environ.get(CASARS_LAUNCH_MODE_ENVVAR, "installed_suite")
    if mode == "installed_suite":
        env_binary = os.environ.get(launch.override_env)
        if env_binary:
            return _require_binary(
                env_binary,
                source=f"${launch.override_env}",
                missing_error_cls=missing_error_cls,
                description=description,
            )
        suite_root = Path(
            os.environ.get(
                CASARS_SUITE_ROOT_ENVVAR,
                Path.home() / ".local" / "opt" / "casa-rs" / "current",
            )
        )
        return _require_suite_binary(
            suite_root,
            source="installed-suite launch mode",
            binary_name=launch.executable,
            missing_error_cls=missing_error_cls,
            description=description,
        )
    if mode == "development_workspace":
        workspace = os.environ.get(CASARS_DEVELOPMENT_WORKSPACE_ENVVAR)
        if not workspace:
            raise missing_error_cls(
                f"development-workspace launch mode requires "
                f"{CASARS_DEVELOPMENT_WORKSPACE_ENVVAR} for {description}"
            )
        candidate = Path(workspace) / "target" / "debug" / _binary_name(launch.executable)
        return _require_binary(
            str(candidate),
            source="development-workspace launch mode",
            missing_error_cls=missing_error_cls,
            description=description,
        )
    raise ValueError(
        f"invalid {CASARS_LAUNCH_MODE_ENVVAR} {mode!r}; expected "
        "'installed_suite' or 'development_workspace'"
    )


@lru_cache(maxsize=1)
def _application_launches() -> dict[str, ApplicationLaunch]:
    payload = json.loads(_core.application_catalog_json())
    applications = payload.get("applications")
    if not isinstance(applications, list):
        raise RuntimeError("canonical application catalog has no applications array")
    launches: dict[str, ApplicationLaunch] = {}
    for application in applications:
        if not isinstance(application, dict) or not isinstance(application.get("id"), str):
            raise RuntimeError("canonical application catalog contains an invalid entry")
        launch = application.get("launch")
        if not isinstance(launch, dict):
            raise RuntimeError(f"application {application['id']!r} has no launch descriptor")
        launches[application["id"]] = ApplicationLaunch(
            executable=str(launch["executable"]),
            cargo_package=str(launch["cargo_package"]),
            override_env=str(launch["override_env"]),
        )
    return launches


def _application_launch(application_id: str) -> ApplicationLaunch:
    try:
        return _application_launches()[application_id]
    except KeyError as error:
        raise RuntimeError(
            f"canonical application catalog has no entry for {application_id!r}"
        ) from error


def _require_binary(
    candidate: str,
    *,
    source: str,
    missing_error_cls: type[FileNotFoundError],
    description: str,
) -> str:
    resolved = Path(candidate).expanduser()
    if not resolved.is_file():
        raise missing_error_cls(
            f"{source} did not resolve to an existing {description} binary: {candidate}"
        )
    return str(resolved)


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


@lru_cache(maxsize=None)
def _validated_protocol_info(
    binary: str,
    *,
    protocol_name: str,
    protocol_version: int,
    mismatch_error_cls: type[RuntimeError],
    invocation_error_cls: type[RuntimeError],
) -> ProtocolInfo:
    stdout = _run_process([binary, "--protocol-info"], error_cls=invocation_error_cls)
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
