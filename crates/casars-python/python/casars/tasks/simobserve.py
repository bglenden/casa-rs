"""Synthetic-observation task wrapper backed by the canonical Rust JSON contract."""

from __future__ import annotations

import os
import json
from os import PathLike
from pathlib import Path
from typing import Any, TypeAlias

from .._task_runtime import (
    ProtocolInfo,
    configure_simobserve_binary,
    fetch_simobserve_schema,
    get_simobserve_protocol_info,
    invoke_simobserve_task,
    invoke_simobserve_task_file,
)

StrPath: TypeAlias = str | PathLike[str]
TaskResult: TypeAlias = dict[str, Any]


def configure(*, binary: StrPath | None) -> None:
    """Configure the default ``simobserve`` binary override for this module."""

    configure_simobserve_binary(binary)


def protocol_info(*, binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol information for the selected binary."""

    return get_simobserve_protocol_info(binary=binary)


def schema(*, binary: StrPath | None = None) -> dict[str, Any]:
    """Return the Rust-emitted request/result schema bundle."""

    return fetch_simobserve_schema(binary=binary)


def run(request: dict[str, Any], *, binary: StrPath | None = None) -> TaskResult:
    """Execute one canonical ``simobserve`` run request."""

    return invoke_simobserve_task(kind="run", request=request, binary=binary)


def family(request: dict[str, Any], *, binary: StrPath | None = None) -> TaskResult:
    """Execute one canonical synthetic-MS family request."""

    return invoke_simobserve_task(kind="family", request=request, binary=binary)


def run_file(source: StrPath, *, binary: StrPath | None = None) -> TaskResult:
    """Execute a saved canonical ``simobserve`` JSON request file."""

    return invoke_simobserve_task_file(source, binary=binary)


def save_request(path: StrPath, *, kind: str, request: dict[str, Any]) -> None:
    """Save a canonical ``simobserve`` request envelope."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps({"kind": kind, "request": request}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_request(path: StrPath) -> dict[str, Any]:
    """Load a canonical ``simobserve`` request envelope."""

    return json.loads(Path(path).read_text(encoding="utf-8"))


def vla_ppdisk(
    model_image: StrPath,
    output_ms: StrPath,
    *,
    overwrite: bool = False,
    telescope_name: str | None = None,
    field_name: str | None = None,
    antennas: list[dict[str, Any]] | None = None,
    fields: list[dict[str, Any]] | None = None,
    model_peak_jy_per_pixel: float | None = 3.0e-5,
    phase_center_rad: tuple[float, float] | None = None,
    start_time_mjd_seconds: float | None = None,
    duration_seconds: float | None = 3600.0,
    integration_seconds: float | None = 2.0,
    start_frequency_hz: float = 44.0e9,
    channel_width_hz: float = 128.0e6,
    channel_count: int = 1,
    polarization_setup: dict[str, Any] | None = None,
    polarizations: int = 2,
    polarization_basis: str = "circular",
    predict_model: bool = True,
    corruption: dict[str, Any] | None = None,
    worker_policy: str = "auto",
    row_workers: int | None = None,
    channel_workers: int | None = None,
    binary: StrPath | None = None,
) -> TaskResult:
    """Generate a VLA protoplanetary-disk synthetic MeasurementSet."""

    request = {
        "model_image": os.fspath(model_image),
        "model_peak_jy_per_pixel": model_peak_jy_per_pixel,
        "output_ms": os.fspath(output_ms),
        "overwrite": overwrite,
        "telescope_name": telescope_name,
        "field_name": field_name,
        "antennas": antennas or [],
        "fields": fields or [],
        "phase_center_rad": None
        if phase_center_rad is None
        else [phase_center_rad[0], phase_center_rad[1]],
        "start_time_mjd_seconds": start_time_mjd_seconds,
        "duration_seconds": duration_seconds,
        "integration_seconds": integration_seconds,
        "spectral_setup": {
            "name": "band1",
            "start_frequency_hz": start_frequency_hz,
            "channel_width_hz": channel_width_hz,
            "channel_count": channel_count,
        },
        "polarization_setup": polarization_setup
        or {"basis": polarization_basis, "correlation_count": polarizations},
        "predict_model": predict_model,
        "corruption": corruption,
        "worker_policy": worker_policy,
        "row_workers": row_workers,
        "channel_workers": channel_workers,
    }
    return run(request, binary=binary)
