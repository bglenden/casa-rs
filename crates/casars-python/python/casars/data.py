"""Persistent image and table access for casa-rs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
import json
from os import PathLike
from typing import Any, TypeAlias

import numpy as np
import numpy.typing as npt

from . import _core

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
    "ProtocolInfo",
    "RecordLike",
    "StrPath",
    "Table",
    "protocol_info",
    "schema_bundle",
]
