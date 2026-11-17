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

StrPath: TypeAlias = str | PathLike[str]
ArrayLike: TypeAlias = npt.NDArray[Any]
RecordLike: TypeAlias = dict[str, Any]


def linearmosaic(
    pointings: Sequence[tuple[StrPath, StrPath]],
    *,
    template: StrPath,
    output: StrPath,
    output_weight: StrPath,
) -> None:
    """Stitch pairs of restored ``.image`` and primary-beam ``.pb`` products.

    Use uncorrected images restored with a common beam. A CASA image supplies
    the output grid through ``template``. Pointings may have different sky
    centres but must share the template's spectral and Stokes axes and frame.
    Both outputs must be new. The weight image records beam-squared coverage.
    """
    _core.linearmosaic(pointings, template, output, output_weight)


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
    """Renderer-neutral MeasurementSet plot data built by Rust."""

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
    max_plot_points: int = 100_000,
) -> MeasurementSetPlotData:
    """Return editable numeric plot data from the canonical Rust MS planner.

    This is an in-process data-object operation, not a task invocation. Rust
    builds a typed plot document; render it with :func:`plot_matplotlib`.
    """

    result = _core.build_measurement_set_plot(
        measurement_set,
        preset,
        selection,
        {
            "data_column": data_column,
            "color_by": color_by,
            "avgchannel": avgchannel,
            "avgtime": avgtime,
            "avgscan": avgscan,
            "avgfield": avgfield,
            "avgbaseline": avgbaseline,
            "avgantenna": avgantenna,
            "avgspw": avgspw,
            "scalar": scalar,
            "iteraxis": iteraxis,
            "max_plot_points": max_plot_points,
        },
    )
    return _measurement_set_plot_data(result)


def plot_matplotlib(
    plot_data: MeasurementSetPlotData,
    *,
    figure: Any = None,
) -> tuple[Any, Any]:
    """Render MeasurementSet plot data into editable Matplotlib objects."""

    try:
        import matplotlib.pyplot  # noqa: F401
    except ImportError as error:  # pragma: no cover - environment dependent
        raise ImportError(
            "Matplotlib plotting requires `pip install casa-rs-python[plot]`"
        ) from error
    from ._plot_document import render_document

    document = {
        "title": plot_data.title, "show_legend": plot_data.show_legend,
        "panels": [{
            "title": panel.title,
            "axes": [{"label": axis.label, "lower": axis.lower, "upper": axis.upper} for axis in panel.axes],
            "layers": [{
                "kind": "scatter", "title": series.label,
                "x_values": series.x, "y_values": series.y,
                "symbol_size": 9, "opacity": 1,
            } for series in panel.series],
        } for panel in plot_data.panels],
    }
    if not document["panels"]:
        document["panels"] = [{"title": "", "axes": [], "layers": []}]
    return render_document(document, figure=figure, use_native_limits=False)


def _measurement_set_plot_data(result: dict[str, Any]) -> MeasurementSetPlotData:
    return MeasurementSetPlotData(
        title=result["title"],
        summary=result["summary"],
        header_lines=tuple(result["header_lines"]),
        show_legend=result["show_legend"],
        panels=tuple(_plot_panel(panel) for panel in result["panels"]),
        measurement_set=result["measurement_set"],
        preset=result["preset"],
        selection_summary=result["selection_summary"],
    )


def _plot_panel(panel: dict[str, Any]) -> PlotPanelData:
    return PlotPanelData(
        id=panel["id"],
        title=panel["title"],
        axes=tuple(PlotAxisData(**axis) for axis in panel["axes"]),
        series=tuple(
            PlotSeriesData(
                label=layer["title"],
                color_group=layer["color_group"],
                y_axis_id=layer["y_axis_id"],
                x=layer["x"],
                y=layer["y"],
                provenance=tuple(layer["provenance"]),
            )
            for layer in panel["layers"]
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

    def _direction_wcs(self) -> tuple[int, int, dict[str, Any]] | None:
        """The pixel axes of the image's direction and the FITS WCS keywords
        `exportfits` writes for the image, or None; for `casars.imexplore`."""

        return self._inner._direction_wcs()

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
    "linearmosaic",
    "measurement_set_plot",
    "plot_matplotlib",
    "protocol_info",
    "schema_bundle",
]
