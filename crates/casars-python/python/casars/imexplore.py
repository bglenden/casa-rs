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
    """One editable image plane with WCS, mask, beam, and overlay metadata."""

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
    )


def imshow(
    plane: ImagePlaneData,
    *,
    figure: Any = None,
    wcs: Any = None,
    cmap: str = "viridis",
) -> tuple[Any, Any]:
    """Render a plane and return editable Matplotlib figure/axes objects."""

    try:
        from matplotlib import pyplot as plt
    except ImportError as error:  # pragma: no cover - environment dependent
        raise ImportError(
            "Image plotting requires `pip install casa-rs-python[plot]`"
        ) from error
    resolved_wcs = wcs if wcs is not None else _astropy_wcs(plane.coordinate_system)
    if figure is None:
        figure = plt.figure()
    axes = figure.add_subplot(1, 1, 1, projection=resolved_wcs)
    values = np.ma.array(plane.values, mask=None if plane.mask is None else ~plane.mask)
    image = axes.imshow(values, origin="lower", cmap=cmap)
    figure.colorbar(image, ax=axes, label=plane.units)
    for overlay in plane.overlays:
        if overlay.get("kind") == "point":
            axes.plot(overlay["x"], overlay["y"], marker=overlay.get("marker", "+"))
    return figure, axes


def _astropy_wcs(coordinate_system: dict[str, Any]) -> Any:
    direction = next(
        (
            value
            for key, value in coordinate_system.items()
            if key.startswith("coordinate")
            and isinstance(value, dict)
            and value.get("coordinate_type") == "Direction"
        ),
        None,
    )
    if direction is None:
        return None
    try:
        from astropy.wcs import WCS
    except ImportError as error:  # pragma: no cover - environment dependent
        raise ImportError(
            "WCSAxes plotting requires `pip install casa-rs-python[plot]`"
        ) from error
    projection = str(direction.get("projection", "SIN"))
    reference = str(direction.get("direction_ref", "J2000"))
    wcs = WCS(naxis=2)
    wcs.wcs.crpix = np.asarray(direction["crpix"], dtype=float) + 1.0
    wcs.wcs.cdelt = np.rad2deg(np.asarray(direction["cdelt"], dtype=float))
    wcs.wcs.crval = np.rad2deg(np.asarray(direction["crval"], dtype=float))
    if reference in {"GALACTIC", "SUPERGAL"}:
        wcs.wcs.ctype = [f"GLON-{projection}", f"GLAT-{projection}"]
    else:
        wcs.wcs.ctype = [f"RA---{projection}", f"DEC--{projection}"]
    return wcs


__all__ = ["ImagePlaneData", "data", "imshow"]
