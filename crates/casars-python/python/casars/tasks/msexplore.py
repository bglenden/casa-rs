"""MeasurementSet exploration wrapper backed by the canonical ``msexplore`` executable."""

from __future__ import annotations

import os
from os import PathLike
from typing import Any, Literal, TypeAlias

from .._task_runtime import (
    ProtocolInfo,
    configure_msexplore_binary,
    fetch_msexplore_schema,
    get_msexplore_protocol_info,
    invoke_msexplore_task,
)

StrPath: TypeAlias = str | PathLike[str]
TaskResult: TypeAlias = dict[str, Any]
SummaryFormat: TypeAlias = Literal["text", "json"]


def configure(*, binary: StrPath | None) -> None:
    """Configure the default ``msexplore`` binary override for this module."""

    configure_msexplore_binary(binary)


def protocol_info(*, binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol information for the selected binary."""

    return get_msexplore_protocol_info(binary=binary)


def schema(*, binary: StrPath | None = None) -> dict[str, Any]:
    """Return the Rust-emitted request/result schema bundle."""

    return fetch_msexplore_schema(binary=binary)


def run(request: dict[str, Any], *, binary: StrPath | None = None) -> TaskResult:
    """Execute one canonical ``msexplore`` run request."""

    return invoke_msexplore_task(kind="run", request=request, binary=binary)


def summary(
    measurement_set: StrPath,
    *,
    format: SummaryFormat = "json",
    output_path: StrPath | None = None,
    overwrite: bool = False,
    selection: dict[str, Any] | None = None,
    binary: StrPath | None = None,
) -> TaskResult:
    """Summarize a MeasurementSet through ``msexplore``."""

    request = {
        "spec": {
            "ms_path": os.fspath(measurement_set),
            "summary_format": _summary_format(format),
            "selection": _selection_request(selection),
            "header_items": [],
            "page_title": None,
            "exprange": "current",
            "plots": [],
        },
        "summary_output_path": None if output_path is None else os.fspath(output_path),
        "overwrite_outputs": overwrite,
        "flag_edit": None,
        "plot_export": None,
    }
    return run(request, binary=binary)


def _summary_format(format: SummaryFormat) -> str:
    if format == "json":
        return "Json"
    if format == "text":
        return "Text"
    raise ValueError("format must be 'json' or 'text'")


def _selection_request(selection: dict[str, Any] | None) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "selectdata": True,
        "field": None,
        "spw": None,
        "timerange": None,
        "uvrange": None,
        "antenna": None,
        "scan": None,
        "correlation": None,
        "array": None,
        "observation": None,
        "intent": None,
        "feed": None,
        "msselect": None,
    }
    if selection is not None:
        defaults.update(selection)
    return defaults
