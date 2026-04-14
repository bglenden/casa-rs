"""Python bindings for casa-rs."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from . import data, tasks
from .data import Image, Table

try:
    __version__ = version("casa-rs-python")
except PackageNotFoundError:  # pragma: no cover - editable local builds
    __version__ = "0+unknown"

__all__ = ["Image", "Table", "data", "tasks", "__version__"]
