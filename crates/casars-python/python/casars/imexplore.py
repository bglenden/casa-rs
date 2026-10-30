"""Python-native CASA image planes and optional Matplotlib/WCSAxes rendering."""

from __future__ import annotations

from dataclasses import dataclass
from os import PathLike, fspath
from typing import Any, TypeAlias

import numpy as np
import numpy.typing as npt

from .data import Image

StrPath: TypeAlias = str | PathLike[str]


@dataclass(frozen=True)
class ImagePlaneData:
    """One editable image plane with WCS, mask, beam, and overlay metadata.

    `values` and `mask` are indexed by the image's pixel axes `plane_axes`,
    in their order: `values[x, y]`. `direction_axes` are the image's
    direction (longitude, latitude) pixel axes, and `wcs_header` the FITS WCS
    keywords `exportfits` writes for the image; both are None without a
    direction coordinate.
    """

    values: npt.NDArray[Any]
    mask: npt.NDArray[np.bool_] | None
    coordinate_system: dict[str, Any]
    beam_metadata: dict[str, Any]
    overlays: tuple[dict[str, Any], ...]
    units: str
    image_path: str
    axis: int
    index: int
    stretch: str
    plane_axes: tuple[int, ...] = ()
    direction_axes: tuple[int, int] | None = None
    wcs_header: dict[str, Any] | None = None


def data(
    image_path: StrPath,
    *,
    axis: int,
    index: int,
    overlays: tuple[dict[str, Any], ...] = (),
    stretch: str = "linear",
) -> ImagePlaneData:
    """Read one image plane directly from casa-rs without a rendered image."""

    image = Image.open(image_path)
    shape = image.shape
    if axis < 0 or axis >= len(shape):
        raise IndexError(f"axis {axis} is outside image rank {len(shape)}")
    if index < 0 or index >= shape[axis]:
        raise IndexError(f"index {index} is outside axis length {shape[axis]}")
    start = [0] * len(shape)
    start[axis] = index
    slice_shape = list(shape)
    slice_shape[axis] = 1
    mask = image.get_mask_slice(start, slice_shape)
    direction = image._direction_wcs()
    return ImagePlaneData(
        values=np.squeeze(np.asarray(image.get_plane(axis, index))),
        mask=None if mask is None else np.squeeze(np.asarray(mask, dtype=np.bool_)),
        coordinate_system=image.coordinate_system,
        beam_metadata=image.image_info,
        overlays=tuple(overlays),
        units=image.units,
        image_path=fspath(image_path),
        axis=axis,
        index=index,
        stretch=stretch,
        # The axes `np.squeeze` leaves: all but `axis` and those of length 1.
        plane_axes=tuple(k for k, length in enumerate(shape) if k != axis and length > 1),
        direction_axes=None if direction is None else (direction[0], direction[1]),
        wcs_header=None if direction is None else dict(direction[2]),
    )


def imshow(
    plane: ImagePlaneData,
    *,
    figure: Any = None,
    wcs: Any = None,
    cmap: str = "viridis",
) -> tuple[Any, Any]:
    """Render a plane and return editable Matplotlib figure/axes objects.

    The plane's first axis runs across and its second up, as in CASA's
    viewer. A plane of the image's direction axes is drawn on their WCS
    unless `wcs` gives another; overlay `x` and `y` are pixels on the first
    and second axes.
    """

    try:
        from matplotlib import pyplot as plt
    except ImportError as error:  # pragma: no cover - environment dependent
        raise ImportError(
            "Image plotting requires `pip install casa-rs-python[plot]`"
        ) from error
    resolved_wcs = wcs if wcs is not None else _astropy_wcs(plane)
    if figure is None:
        figure = plt.figure()
    axes = figure.add_subplot(1, 1, 1, projection=resolved_wcs)
    image = axes.imshow(_display_values(plane), origin="lower", cmap=cmap)
    figure.colorbar(image, ax=axes, label=plane.units)
    for overlay in plane.overlays:
        if overlay.get("kind") == "point":
            axes.plot(overlay["x"], overlay["y"], marker=overlay.get("marker", "+"))
    return figure, axes


def _display_values(plane: ImagePlaneData) -> np.ma.MaskedArray:
    """`plane`'s values with masked pixels hidden, as Matplotlib and astropy
    draw an array (`data[y, x]`): the plane's first axis across."""

    values = np.ma.array(plane.values, mask=None if plane.mask is None else ~plane.mask)
    return values.T


def _is_direction_plane(plane: ImagePlaneData) -> bool:
    """`plane`'s two axes are the image's direction axes."""

    return (
        plane.wcs_header is not None
        and plane.direction_axes is not None
        and plane.plane_axes == plane.direction_axes
    )


def _astropy_wcs(plane: ImagePlaneData) -> Any:
    """The WCS of `plane`'s two axes when they are the image's direction
    axes, else None.

    It is the image's FITS WCS (`exportfits`'s keywords) cut to the plane's
    axes. A plane holding one direction axis has no celestial WCS of its own,
    so it gets none.
    """

    if not _is_direction_plane(plane):
        return None
    try:
        from astropy.wcs import WCS
    except ImportError as error:  # pragma: no cover - environment dependent
        raise ImportError(
            "WCSAxes plotting requires `pip install casa-rs-python[plot]`"
        ) from error
    return WCS(plane.wcs_header).sub([axis + 1 for axis in plane.plane_axes])


__all__ = ["ImagePlaneData", "data", "imshow"]
