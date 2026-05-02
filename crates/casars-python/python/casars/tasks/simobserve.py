"""Synthetic-observation task wrapper backed by the canonical Rust JSON contract."""

from __future__ import annotations

import os
from os import PathLike
from typing import Any, TypeAlias

from .._task_runtime import (
    ProtocolInfo,
    configure_simobserve_binary,
    fetch_simobserve_schema,
    get_simobserve_protocol_info,
    invoke_simobserve_task,
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


def vla_ppdisk(
    model_image: StrPath,
    output_ms: StrPath,
    *,
    overwrite: bool = False,
    antennas: list[dict[str, Any]] | None = None,
    model_peak_jy_per_pixel: float | None = 3.0e-5,
    phase_center_rad: tuple[float, float] | None = None,
    start_time_mjd_seconds: float | None = None,
    duration_seconds: float | None = 3600.0,
    integration_seconds: float | None = 2.0,
    start_frequency_hz: float = 44.0e9,
    channel_width_hz: float = 128.0e6,
    channel_count: int = 1,
    predict_model: bool = True,
    corruption: dict[str, Any] | None = None,
    binary: StrPath | None = None,
) -> TaskResult:
    """Generate a VLA protoplanetary-disk synthetic MeasurementSet."""

    request = {
        "model_image": os.fspath(model_image),
        "model_peak_jy_per_pixel": model_peak_jy_per_pixel,
        "output_ms": os.fspath(output_ms),
        "overwrite": overwrite,
        "antennas": antennas or [],
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
        "predict_model": predict_model,
        "corruption": corruption,
    }
    return run(request, binary=binary)
