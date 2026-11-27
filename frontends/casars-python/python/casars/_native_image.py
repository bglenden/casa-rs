# SPDX-License-Identifier: LGPL-3.0-or-later
"""Native-width image access for Python consumers inside casa-rs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np

from . import _core
from .data import ArrayLike, RecordLike, StrPath


class NativeImageView:
    """Private typed image access for internal callers of the native tool."""

    def __init__(self, inner: _core.image, *, writable: bool = False) -> None:
        self._inner = inner
        self._writable = writable

    @classmethod
    def open(cls, path: StrPath, *, writable: bool = False) -> NativeImageView:
        """Open an existing persistent image.

        Set ``writable=True`` to enable pixel-slice updates with
        :meth:`put_slice`.
        """

        return cls(_core.image._data_open(path, writable), writable=writable)

    @property
    def shape(self) -> tuple[int, ...]:
        """Image shape in CASA/Rust axis order."""

        return tuple(self._inner._data_shape)

    @property
    def pixel_type(self) -> str:
        """Pixel dtype name such as ``float32`` or ``complex128``."""

        return self._inner._data_pixel_type

    @property
    def units(self) -> str:
        """Image unit string."""

        return self._inner._data_units

    @property
    def image_info(self) -> RecordLike:
        """Structured image-info record."""

        return self._inner._data_image_info

    @property
    def misc_info(self) -> RecordLike:
        """Structured miscellaneous-info record."""

        return self._inner._data_misc_info

    @property
    def coordinate_system(self) -> RecordLike:
        """Renderer-neutral coordinate metadata, including celestial WCS."""

        return self._inner._data_coordinate_system

    def _direction_wcs(self) -> tuple[int, int, dict[str, Any]] | None:
        """The pixel axes of the image's direction and the FITS WCS keywords
        `exportfits` writes for the image, or None; for `casars.imexplore`."""

        return self._inner._direction_wcs()

    @property
    def mask_names(self) -> list[str]:
        """Names of persisted masks attached to the image."""

        return list(self._inner._data_mask_names)

    @property
    def default_mask_name(self) -> str | None:
        """Name of the default mask, if any."""

        return self._inner._data_default_mask_name

    def get_slice(
        self,
        start: Sequence[int],
        shape: Sequence[int],
        *,
        stride: Sequence[int] | None = None,
    ) -> ArrayLike:
        """Read a rectangular image chunk as a NumPy array."""

        return self._inner._data_slice(list(start), list(shape), stride=None if stride is None else list(stride))

    def put_slice(self, data: ArrayLike, start: Sequence[int]) -> None:
        """Write a native-width pixel chunk into the attached image."""

        self._inner._data_put_slice(np.asarray(data), list(start), self._writable)

    def get_plane(self, axis: int, index: int) -> ArrayLike:
        """Read one plane orthogonal to ``axis``."""

        return np.squeeze(self._inner._data_plane(axis, index), axis=axis)

    def get_mask_slice(
        self,
        start: Sequence[int],
        shape: Sequence[int],
        *,
        stride: Sequence[int] | None = None,
    ) -> ArrayLike | None:
        """Read a rectangular chunk from the default mask."""

        return self._inner._data_mask_slice(
            list(start),
            list(shape),
            stride=None if stride is None else list(stride),
        )
