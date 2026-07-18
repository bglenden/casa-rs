"""Persistent image and table access for casa-rs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
import json
import os
from os import PathLike
from typing import Any, TypeAlias

import numpy as np
import numpy.typing as npt

from . import _core
from .parameters import _frontend

StrPath: TypeAlias = str | PathLike[str]
ArrayLike: TypeAlias = npt.NDArray[Any]
RecordLike: TypeAlias = dict[str, Any]


@dataclass(frozen=True)
class ProtocolInfo:
    """Compatibility descriptor for the canonical `casars.data` object surface."""

    protocol_name: str
    protocol_version: int
    surface_kind: str
    binding_version: str


@dataclass(frozen=True)
class PlotAxisData:
    """One numeric axis supplied by the generated Rust plot contract."""

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
class MeasurementSetPlotData:
    """Renderer-neutral MeasurementSet data returned through generated UniFFI."""

    title: str
    summary: str
    header_lines: tuple[str, ...]
    show_legend: bool
    panels: tuple[PlotPanelData, ...]
    measurement_set: str
    preset: str
    selection_summary: str


def measurement_set_plot(
    measurement_set: StrPath,
    *,
    preset: str = "amplitude_vs_time",
    data_column: str = "data",
    color_by: str | None = "field",
    selection: Mapping[str, str] | None = None,
    avgchannel: int | None = None,
    avgtime: float | None = None,
    avgscan: bool = False,
    avgfield: bool = False,
    avgbaseline: bool = False,
    avgantenna: bool = False,
    avgspw: bool = False,
    scalar: bool = False,
    iteraxis: str | None = None,
    width: int = 1200,
    height: int = 800,
    max_plot_points: int = 100_000,
) -> MeasurementSetPlotData:
    """Return editable numeric plot data from the canonical Rust MS planner.

    This is an in-process data-object operation, not a task invocation. It uses
    the same typed Rust plot document projected into Swift and other frontends;
    no rendered image or provider JSON protocol is read by Python.
    """

    api = _frontend()
    preset_name = preset.strip().upper().replace("-", "_")
    if preset_name == "FLAGROW_VS_TIME":
        preset_name = "FLAG_ROW_VS_TIME"
    try:
        resolved_preset = api.MeasurementSetPlotPreset[preset_name]
    except KeyError as error:
        allowed = ", ".join(member.name.lower() for member in api.MeasurementSetPlotPreset)
        raise ValueError(f"unknown MeasurementSet plot preset {preset!r}; expected one of: {allowed}") from error

    selectors = dict(selection or {})
    allowed_selectors = {
        "field",
        "spw",
        "timerange",
        "uvrange",
        "antenna",
        "scan",
        "correlation",
        "array",
        "observation",
        "intent",
        "feed",
        "msselect",
    }
    unexpected = sorted(set(selectors) - allowed_selectors)
    if unexpected:
        raise ValueError(f"unknown MeasurementSet selectors: {', '.join(unexpected)}")

    request = api.MeasurementSetPlotRequest(
        dataset_path=os.fspath(measurement_set),
        preset=resolved_preset,
        field=selectors.get("field"),
        spectral_window=selectors.get("spw"),
        timerange=selectors.get("timerange"),
        uvrange=selectors.get("uvrange"),
        antenna=selectors.get("antenna"),
        scan=selectors.get("scan"),
        correlation=selectors.get("correlation"),
        array=selectors.get("array"),
        observation=selectors.get("observation"),
        intent=selectors.get("intent"),
        feed=selectors.get("feed"),
        msselect=selectors.get("msselect"),
        data_column=data_column,
        color_by=color_by,
        avgchannel=avgchannel,
        avgtime=avgtime,
        avgscan=avgscan,
        avgfield=avgfield,
        avgbaseline=avgbaseline,
        avgantenna=avgantenna,
        avgspw=avgspw,
        scalar=scalar,
        iteraxis=iteraxis,
        width=width,
        height=height,
        max_plot_points=max_plot_points,
    )
    try:
        result = api.build_measurement_set_plot(request)
    except api.FrontendServiceError as error:
        raise ValueError(str(error)) from error
    return _measurement_set_plot_data(result)


def plot_matplotlib(
    plot_data: MeasurementSetPlotData,
    *,
    figure: Any = None,
) -> tuple[Any, Any]:
    """Render MeasurementSet plot data into editable Matplotlib objects."""

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
        resolved_axes = [
            figure.add_subplot(panel_count, 1, index + 1)
            for index in range(panel_count)
        ]
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


def _measurement_set_plot_data(result: Any) -> MeasurementSetPlotData:
    document = result.document
    if document.panels:
        panels = tuple(
            _plot_panel(panel.id, panel.title, panel.axes, panel.layers)
            for panel in document.panels
        )
    else:
        panels = (
            _plot_panel(document.id, document.title, document.axes, document.layers),
        )
    return MeasurementSetPlotData(
        title=result.title,
        summary=result.summary,
        header_lines=tuple(document.header_lines),
        show_legend=document.show_legend,
        panels=panels,
        measurement_set=result.dataset_path,
        preset=result.preset.name.lower(),
        selection_summary=result.selection_summary,
    )


def _plot_panel(id: str, title: str, axes: Any, layers: Any) -> PlotPanelData:
    return PlotPanelData(
        id=id,
        title=title,
        axes=tuple(
            PlotAxisData(
                id=axis.id,
                label=axis.label,
                unit=axis.unit,
                lower=axis.lower,
                upper=axis.upper,
            )
            for axis in axes
        ),
        series=tuple(
            PlotSeriesData(
                label=layer.title,
                color_group=layer.color_group,
                y_axis_id=layer.y_axis_id,
                x=np.asarray(layer.x_values, dtype=np.float64),
                y=np.asarray(layer.y_values, dtype=np.float64),
                provenance=tuple(
                    {
                        "row": point.row,
                        "corr": point.corr,
                        "chan_start": point.chan_start,
                        "chan_end": point.chan_end,
                    }
                    for point in layer.provenance
                ),
            )
            for layer in layers
        ),
    )


@lru_cache(maxsize=1)
def protocol_info() -> ProtocolInfo:
    """Return the canonical object-surface protocol descriptor emitted by Rust."""

    payload = json.loads(_core.data_protocol_info_json())
    return ProtocolInfo(
        protocol_name=str(payload["protocol_name"]),
        protocol_version=int(payload["protocol_version"]),
        surface_kind=str(payload["surface_kind"]),
        binding_version=str(payload["binding_version"]),
    )


@lru_cache(maxsize=1)
def schema_bundle() -> dict[str, Any]:
    """Return the canonical object-surface schema bundle emitted by Rust."""

    payload = json.loads(_core.data_schema_bundle_json())
    assert isinstance(payload, dict)
    return payload


class Image:
    """Persistent CASA-compatible image access.

    This wrapper exposes rectangular slice and plane I/O using native NumPy
    arrays while keeping the storage model file-backed and stateful. V1 write
    support is limited to pixel-slice updates on existing persistent images.
    Image creation and coordinate/metadata authoring are intentionally out of
    scope.
    """

    def __init__(self, inner: _core.Image) -> None:
        self._inner = inner

    @classmethod
    def open(cls, path: StrPath, *, writable: bool = False) -> "Image":
        """Open an existing persistent image.

        Set ``writable=True`` to enable pixel-slice updates with
        :meth:`put_slice`.
        """

        return cls(_core.Image.open(path, writable=writable))

    @property
    def shape(self) -> tuple[int, ...]:
        """Image shape in CASA/Rust axis order."""

        return tuple(self._inner.shape)

    @property
    def pixel_type(self) -> str:
        """Pixel dtype name such as ``float32`` or ``complex128``."""

        return self._inner.pixel_type

    @property
    def units(self) -> str:
        """Image unit string."""

        return self._inner.units

    @property
    def image_info(self) -> RecordLike:
        """Structured image-info record."""

        return self._inner.image_info

    @property
    def misc_info(self) -> RecordLike:
        """Structured miscellaneous-info record."""

        return self._inner.misc_info

    @property
    def coordinate_system(self) -> RecordLike:
        """Renderer-neutral coordinate metadata, including celestial WCS."""

        return self._inner.coordinate_system

    @property
    def mask_names(self) -> list[str]:
        """Names of persisted masks attached to the image."""

        return list(self._inner.mask_names)

    @property
    def default_mask_name(self) -> str | None:
        """Name of the default mask, if any."""

        return self._inner.default_mask_name

    def get_slice(
        self,
        start: Sequence[int],
        shape: Sequence[int],
        *,
        stride: Sequence[int] | None = None,
    ) -> ArrayLike:
        """Read a rectangular image chunk as a NumPy array."""

        return self._inner.get_slice(list(start), list(shape), stride=None if stride is None else list(stride))

    def put_slice(self, data: ArrayLike, start: Sequence[int]) -> None:
        """Write a rectangular image chunk into an existing persistent image.

        This is the supported v1 write path for images. It does not imply
        support for Python-side image creation or coordinate/metadata editing.
        """

        self._inner.put_slice(np.asarray(data), list(start))

    def get_plane(self, axis: int, index: int) -> ArrayLike:
        """Read one plane orthogonal to ``axis``."""

        return np.squeeze(self._inner.get_plane(axis, index), axis=axis)

    def get_mask_slice(
        self,
        start: Sequence[int],
        shape: Sequence[int],
        *,
        stride: Sequence[int] | None = None,
    ) -> ArrayLike | None:
        """Read a rectangular chunk from the default mask."""

        return self._inner.get_mask_slice(
            list(start),
            list(shape),
            stride=None if stride is None else list(stride),
        )


class Table:
    """Persistent CASA-compatible table access."""

    def __init__(self, inner: _core.Table) -> None:
        self._inner = inner

    @classmethod
    def open(cls, path: StrPath, *, writable: bool = False) -> "Table":
        """Open an existing persistent table."""

        return cls(_core.Table.open(path, writable=writable))

    @property
    def row_count(self) -> int:
        """Number of rows in the table."""

        return self._inner.row_count

    @property
    def column_names(self) -> list[str]:
        """Known column names."""

        return list(self._inner.column_names)

    @property
    def keywords(self) -> RecordLike:
        """Table-level keyword record."""

        return self._inner.keywords

    def column_keywords(self, name: str) -> RecordLike | None:
        """Column-level keyword record for ``name``."""

        return self._inner.column_keywords(name)

    def get_cell(self, row: int, column: str) -> Any:
        """Read one cell."""

        return self._inner.get_cell(row, column)

    def set_cell(self, row: int, column: str, value: Any) -> None:
        """Write one cell."""

        self._inner.set_cell(row, column, value)

    def get_column(
        self,
        column: str,
        *,
        start: int = 0,
        count: int | None = None,
        step: int = 1,
    ) -> Any:
        """Read a column range as NumPy arrays or native Python structures."""

        return self._inner.get_column(column, start=start, count=count, step=step)

    def put_column(
        self,
        column: str,
        values: Sequence[Any],
        *,
        start: int = 0,
        step: int = 1,
    ) -> int:
        """Write a column range from Python-native values."""

        return self._inner.put_column(column, values, start=start, step=step)

    def set_column_keywords(self, column: str, keywords: Mapping[str, Any]) -> None:
        """Replace the keyword record for ``column``."""

        self._inner.set_column_keywords(column, dict(keywords))


__all__ = [
    "ArrayLike",
    "Image",
    "MeasurementSetPlotData",
    "PlotAxisData",
    "PlotPanelData",
    "PlotSeriesData",
    "ProtocolInfo",
    "RecordLike",
    "StrPath",
    "Table",
    "measurement_set_plot",
    "plot_matplotlib",
    "protocol_info",
    "schema_bundle",
]
