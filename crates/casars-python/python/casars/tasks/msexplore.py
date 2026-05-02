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
PlotFormat: TypeAlias = Literal["png", "pdf", "txt"]


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


def plot(
    measurement_set: StrPath,
    output_path: StrPath,
    *,
    x_axis: str = "Time",
    y_axis: str = "Amplitude",
    data_column: str = "data",
    color_by: str = "Field",
    format: PlotFormat = "png",
    width: int = 1200,
    height: int = 800,
    selection: dict[str, Any] | None = None,
    title: str | None = None,
    binary: StrPath | None = None,
) -> TaskResult:
    """Render one ``plotms``-style visibility plot through ``msexplore``."""

    request = {
        "spec": {
            "ms_path": os.fspath(measurement_set),
            "summary_format": "Json",
            "selection": _selection_request(selection),
            "header_items": [],
            "page_title": None,
            "exprange": "current",
            "max_plot_points": 100000,
            "plots": [
                {
                    "preset": None,
                    "x_axis": x_axis,
                    "y_axes": [y_axis],
                    "data_column": data_column,
                    "color_by": color_by,
                    "averaging": _averaging_request(),
                    "transforms": _transform_request(),
                    "layout": _layout_request(),
                    "iteration": _iteration_request(),
                    "style": _style_request(title=title),
                    "flag_edit": None,
                }
            ],
        },
        "summary_output_path": None,
        "overwrite_outputs": True,
        "flag_edit": None,
        "plot_export": {
            "output_path": os.fspath(output_path),
            "format": _plot_format(format),
            "width": width,
            "height": height,
        },
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


def _plot_format(format: PlotFormat) -> str:
    if format == "png":
        return "Png"
    if format == "pdf":
        return "Pdf"
    if format == "txt":
        return "Txt"
    raise ValueError("format must be 'png', 'pdf', or 'txt'")


def _averaging_request() -> dict[str, Any]:
    return {
        "avgchannel": None,
        "avgtime": None,
        "avgscan": False,
        "avgfield": False,
        "avgbaseline": False,
        "avgantenna": False,
        "avgspw": False,
        "scalar": False,
    }


def _transform_request() -> dict[str, Any]:
    return {
        "transform": True,
        "freqframe": None,
        "restfreq": None,
        "veldef": "RADIO",
        "phasecenter": None,
        "xframe": None,
        "xinterp": None,
        "yframe": None,
        "yinterp": None,
    }


def _layout_request() -> dict[str, int]:
    return {
        "gridrows": 1,
        "gridcols": 1,
        "rowindex": 0,
        "colindex": 0,
        "plotindex": 0,
    }


def _iteration_request() -> dict[str, Any]:
    return {
        "iteraxis": None,
        "xselfscale": False,
        "yselfscale": False,
        "xsharedaxis": False,
        "ysharedaxis": False,
    }


def _style_request(*, title: str | None) -> dict[str, Any]:
    return {
        "title": title,
        "xlabel": None,
        "ylabel": None,
        "symbol_size": None,
        "showlegend": False,
        "legendposition": "upperRight",
        "showmajorgrid": False,
        "showminorgrid": False,
    }
