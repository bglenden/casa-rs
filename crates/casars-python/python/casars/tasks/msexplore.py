"""Raw msexplore-provider request/result object API.

This module intentionally does not load profiles or update managed Last state.
Use :func:`casars.tasks.profiles.msexplore` for the unified CASA-named
parameter lifecycle.
"""

from __future__ import annotations

import os
import re
import json
from os import PathLike
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

import numpy as np
import numpy.typing as npt

from .. import _core

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


@dataclass(frozen=True)
class PlotAxisData:
    """One numeric axis supplied by the shared Rust plot-data contract."""

    id: str
    label: str
    unit: str
    lower: float
    upper: float


@dataclass(frozen=True)
class PlotSeriesData:
    """One editable numeric series and its MeasurementSet provenance."""

    label: str
    color_group: str
    y_axis_id: str
    x: npt.NDArray[np.float64]
    y: npt.NDArray[np.float64]
    provenance: tuple[dict[str, int], ...]


@dataclass(frozen=True)
class PlotPanelData:
    """One panel of Python-native numeric MeasurementSet plot data."""

    id: str
    title: str
    axes: tuple[PlotAxisData, ...]
    series: tuple[PlotSeriesData, ...]


@dataclass(frozen=True)
class PlotData:
    """Renderer-neutral plot data returned directly by casa-rs."""

    schema_version: int
    title: str
    summary: str
    header_lines: tuple[str, ...]
    show_legend: bool
    panels: tuple[PlotPanelData, ...]
    measurement_set: str
    request: dict[str, Any]


def data(
    measurement_set: StrPath,
    *,
    preset: str = "amplitude_vs_time",
    x_axis: str | None = None,
    y_axis: str | None = None,
    data_column: str = "data",
    color_by: str = "Field",
    selection: dict[str, Any] | None = None,
) -> PlotData:
    """Return the same numeric visibility data used by casa-rs renderers.

    No PNG or screenshot is read. Arrays are ordinary NumPy arrays and may be
    filtered, fitted, or rendered with any Python plotting library.
    """

    resolved_x_axis, resolved_y_axes = _plot_axes_request(
        preset=preset,
        x_axis=x_axis,
        y_axis=y_axis,
        y_axes=None,
    )
    selection_request = _selection_request(selection)
    plot_request = {
        "preset": _protocol_token(preset),
        "x_axis": resolved_x_axis,
        "y_axes": resolved_y_axes,
        "data_column": _protocol_token(data_column),
        "color_by": _protocol_token(color_by),
        "averaging": _averaging_request(),
        "transforms": _transform_request(),
        "layout": _layout_request(),
        "iteration": _iteration_request(),
        "style": _style_request(showlegend=True),
        "flag_edit": None,
    }
    payload = json.loads(_core.msexplore_plot_data_json(
        os.fspath(measurement_set),
        json.dumps(selection_request),
        json.dumps(plot_request),
    ))
    request = {
        "selection": selection_request,
        "plot": plot_request,
    }
    return _plot_data(payload, os.fspath(measurement_set), request)


def plot_matplotlib(plot_data: PlotData, *, figure: Any = None) -> tuple[Any, Any]:
    """Render Python-native data and return editable Matplotlib objects."""

    try:
        from matplotlib import pyplot as plt
    except ImportError as error:  # pragma: no cover - environment dependent
        raise ImportError(
            "Matplotlib plotting requires `pip install casa-rs-python[plot]`"
        ) from error
    panel_count = max(1, len(plot_data.panels))
    if figure is None:
        figure, axes = plt.subplots(panel_count, 1, squeeze=False)
        resolved_axes = list(axes[:, 0])
    else:
        resolved_axes = [figure.add_subplot(panel_count, 1, index + 1) for index in range(panel_count)]
    for panel, axis in zip(plot_data.panels, resolved_axes, strict=True):
        for series in panel.series:
            axis.scatter(series.x, series.y, label=series.label, s=9)
        axis.set_title(panel.title)
        if panel.axes:
            axis.set_xlabel(panel.axes[0].label)
        if len(panel.axes) > 1:
            axis.set_ylabel(panel.axes[1].label)
        if plot_data.show_legend and len(panel.series) > 1:
            axis.legend()
    figure.suptitle(plot_data.title)
    return figure, resolved_axes[0] if len(resolved_axes) == 1 else resolved_axes


def _plot_data(payload: dict[str, Any], measurement_set: str, request: dict[str, Any]) -> PlotData:
    return PlotData(
        schema_version=int(payload["schema_version"]),
        title=str(payload["title"]),
        summary=str(payload["summary"]),
        header_lines=tuple(str(value) for value in payload["header_lines"]),
        show_legend=bool(payload["show_legend"]),
        panels=tuple(
            PlotPanelData(
                id=str(panel["id"]),
                title=str(panel["title"]),
                axes=tuple(PlotAxisData(**axis) for axis in panel["axes"]),
                series=tuple(
                    PlotSeriesData(
                        label=str(series["label"]),
                        color_group=str(series["color_group"]),
                        y_axis_id=str(series["y_axis_id"]),
                        x=np.asarray(series["x"], dtype=np.float64),
                        y=np.asarray(series["y"], dtype=np.float64),
                        provenance=tuple(series["provenance"]),
                    )
                    for series in panel["series"]
                ),
            )
            for panel in payload["panels"]
        ),
        measurement_set=measurement_set,
        request=request,
    )


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
    preset: str | None = None,
    x_axis: str | None = None,
    y_axis: str | None = None,
    y_axes: Sequence[str] | None = None,
    data_column: str = "data",
    color_by: str = "Field",
    format: PlotFormat = "png",
    width: int = 1200,
    height: int = 800,
    max_plot_points: int = 100000,
    avgchannel: int | None = None,
    avgtime: float | None = None,
    avgscan: bool = False,
    avgfield: bool = False,
    avgbaseline: bool = False,
    avgantenna: bool = False,
    avgspw: bool = False,
    scalar_average: bool = False,
    transform: bool = True,
    freqframe: str | None = None,
    restfreq: str | None = None,
    veldef: str = "RADIO",
    phasecenter: str | None = None,
    xframe: str | None = None,
    xinterp: str | None = None,
    yframe: str | None = None,
    yinterp: str | None = None,
    gridrows: int = 1,
    gridcols: int = 1,
    rowindex: int = 0,
    colindex: int = 0,
    plotindex: int = 0,
    iteraxis: str | None = None,
    xselfscale: bool = False,
    yselfscale: bool = False,
    xsharedaxis: bool = False,
    ysharedaxis: bool = False,
    selection: dict[str, Any] | None = None,
    title: str | None = None,
    xlabel: str | None = None,
    ylabel: str | None = None,
    symbol_size: float | None = None,
    showlegend: bool = False,
    legendposition: str = "upperRight",
    showmajorgrid: bool = False,
    showminorgrid: bool = False,
    flag_edit: dict[str, Any] | None = None,
    binary: StrPath | None = None,
) -> TaskResult:
    """Render one ``plotms``-style visibility plot through ``msexplore``."""

    resolved_x_axis, resolved_y_axes = _plot_axes_request(
        preset=preset,
        x_axis=x_axis,
        y_axis=y_axis,
        y_axes=y_axes,
    )
    request = {
        "spec": {
            "ms_path": os.fspath(measurement_set),
            "summary_format": "Json",
            "selection": _selection_request(selection),
            "header_items": [],
            "page_title": None,
            "exprange": "current",
            "max_plot_points": max_plot_points,
            "plots": [
                {
                    "preset": preset,
                    "x_axis": resolved_x_axis,
                    "y_axes": resolved_y_axes,
                    "data_column": _protocol_token(data_column),
                    "color_by": _protocol_token(color_by),
                    "averaging": _averaging_request(
                        avgchannel=avgchannel,
                        avgtime=avgtime,
                        avgscan=avgscan,
                        avgfield=avgfield,
                        avgbaseline=avgbaseline,
                        avgantenna=avgantenna,
                        avgspw=avgspw,
                        scalar_average=scalar_average,
                    ),
                    "transforms": _transform_request(
                        transform=transform,
                        freqframe=freqframe,
                        restfreq=restfreq,
                        veldef=veldef,
                        phasecenter=phasecenter,
                        xframe=xframe,
                        xinterp=xinterp,
                        yframe=yframe,
                        yinterp=yinterp,
                    ),
                    "layout": _layout_request(
                        gridrows=gridrows,
                        gridcols=gridcols,
                        rowindex=rowindex,
                        colindex=colindex,
                        plotindex=plotindex,
                    ),
                    "iteration": _iteration_request(
                        iteraxis=iteraxis,
                        xselfscale=xselfscale,
                        yselfscale=yselfscale,
                        xsharedaxis=xsharedaxis,
                        ysharedaxis=ysharedaxis,
                    ),
                    "style": _style_request(
                        title=title,
                        xlabel=xlabel,
                        ylabel=ylabel,
                        symbol_size=symbol_size,
                        showlegend=showlegend,
                        legendposition=legendposition,
                        showmajorgrid=showmajorgrid,
                        showminorgrid=showminorgrid,
                    ),
                    "flag_edit": flag_edit,
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
        return "png"
    if format == "pdf":
        return "pdf"
    if format == "txt":
        return "txt"
    raise ValueError("format must be 'png', 'pdf', or 'txt'")


def _protocol_token(value: str) -> str:
    """Normalize common display/Python spellings to Rust protocol enum tokens."""

    if "_" in value or value.islower():
        return value
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def _plot_axes_request(
    *,
    preset: str | None,
    x_axis: str | None,
    y_axis: str | None,
    y_axes: Sequence[str] | None,
) -> tuple[str, list[str]]:
    if x_axis is None or (y_axis is None and y_axes is None):
        preset_axes = _PRESET_AXES.get(_protocol_token(preset) if preset is not None else None)
    else:
        preset_axes = None
    resolved_x_axis = x_axis if x_axis is not None else (preset_axes[0] if preset_axes else "Time")
    if y_axes is not None:
        resolved_y_axes = list(y_axes)
    elif y_axis is not None:
        resolved_y_axes = [y_axis]
    elif preset_axes:
        resolved_y_axes = list(preset_axes[1])
    else:
        resolved_y_axes = ["Amplitude"]
    return _protocol_token(resolved_x_axis), [_protocol_token(axis) for axis in resolved_y_axes]


_PRESET_AXES: dict[str | None, tuple[str, tuple[str, ...]]] = {
    "uv_coverage": ("u", ("v",)),
    "amplitude_vs_time": ("time", ("amplitude",)),
    "phase_vs_time": ("time", ("phase",)),
    "amplitude_phase_vs_time": ("time", ("amplitude", "phase")),
    "amplitude_phase_vs_time_stacked": ("time", ("amplitude", "phase")),
    "amplitude_vs_uv_distance": ("uv_distance", ("amplitude",)),
    "weight_vs_time": ("time", ("weight",)),
    "sigma_vs_time": ("time", ("sigma",)),
    "flag_vs_time": ("time", ("flag",)),
    "weight_spectrum_vs_time": ("time", ("weight_spectrum",)),
    "sigma_spectrum_vs_time": ("time", ("sigma_spectrum",)),
    "flagrow_vs_time": ("time", ("flag_row",)),
    "elevation_vs_time": ("time", ("elevation",)),
    "azimuth_vs_time": ("time", ("azimuth",)),
    "hour_angle_vs_time": ("time", ("hour_angle",)),
    "parallactic_angle_vs_time": ("time", ("parallactic_angle",)),
    "azimuth_vs_elevation": ("elevation", ("azimuth",)),
    "amplitude_vs_channel": ("channel", ("amplitude",)),
    "phase_vs_channel": ("channel", ("phase",)),
    "phase_vs_frequency": ("frequency", ("phase",)),
    "amplitude_vs_frequency": ("frequency", ("amplitude",)),
    "amplitude_vs_velocity": ("velocity", ("amplitude",)),
    "phase_vs_velocity": ("velocity", ("phase",)),
    "u_v": ("u", ("v",)),
    "amplitude_vs_u": ("u", ("amplitude",)),
    "amplitude_vs_v": ("v", ("amplitude",)),
    "amplitude_vs_w": ("w", ("amplitude",)),
    "real_vs_imaginary": ("real", ("imaginary",)),
}


def _averaging_request(
    *,
    avgchannel: int | None = None,
    avgtime: float | None = None,
    avgscan: bool = False,
    avgfield: bool = False,
    avgbaseline: bool = False,
    avgantenna: bool = False,
    avgspw: bool = False,
    scalar_average: bool = False,
) -> dict[str, Any]:
    return {
        "avgchannel": avgchannel,
        "avgtime": avgtime,
        "avgscan": avgscan,
        "avgfield": avgfield,
        "avgbaseline": avgbaseline,
        "avgantenna": avgantenna,
        "avgspw": avgspw,
        "scalar": scalar_average,
    }


def _transform_request(
    *,
    transform: bool = True,
    freqframe: str | None = None,
    restfreq: str | None = None,
    veldef: str = "RADIO",
    phasecenter: str | None = None,
    xframe: str | None = None,
    xinterp: str | None = None,
    yframe: str | None = None,
    yinterp: str | None = None,
) -> dict[str, Any]:
    return {
        "transform": transform,
        "freqframe": freqframe,
        "restfreq": restfreq,
        "veldef": veldef,
        "phasecenter": phasecenter,
        "xframe": xframe,
        "xinterp": xinterp,
        "yframe": yframe,
        "yinterp": yinterp,
    }


def _layout_request(
    *,
    gridrows: int = 1,
    gridcols: int = 1,
    rowindex: int = 0,
    colindex: int = 0,
    plotindex: int = 0,
) -> dict[str, int]:
    return {
        "gridrows": gridrows,
        "gridcols": gridcols,
        "rowindex": rowindex,
        "colindex": colindex,
        "plotindex": plotindex,
    }


def _iteration_request(
    *,
    iteraxis: str | None = None,
    xselfscale: bool = False,
    yselfscale: bool = False,
    xsharedaxis: bool = False,
    ysharedaxis: bool = False,
) -> dict[str, Any]:
    return {
        "iteraxis": iteraxis,
        "xselfscale": xselfscale,
        "yselfscale": yselfscale,
        "xsharedaxis": xsharedaxis,
        "ysharedaxis": ysharedaxis,
    }


def _style_request(
    *,
    title: str | None = None,
    xlabel: str | None = None,
    ylabel: str | None = None,
    symbol_size: float | None = None,
    showlegend: bool = False,
    legendposition: str = "upperRight",
    showmajorgrid: bool = False,
    showminorgrid: bool = False,
) -> dict[str, Any]:
    return {
        "title": title,
        "xlabel": xlabel,
        "ylabel": ylabel,
        "symbol_size": symbol_size,
        "showlegend": showlegend,
        "legendposition": legendposition,
        "showmajorgrid": showmajorgrid,
        "showminorgrid": showminorgrid,
    }
