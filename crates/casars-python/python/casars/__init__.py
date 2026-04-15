"""Python bindings for casa-rs."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from . import tasks

# Task wrappers do not require the native data extension. Keep `casars.tasks`
# importable from a source checkout even when `_core` has not been built yet.
try:
    from . import data
    from .data import Image, Table
except ImportError:  # pragma: no cover - exercised in source-only task tests
    data = None  # type: ignore[assignment]
    Image = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]

try:
    __version__ = version("casa-rs-python")
except PackageNotFoundError:  # pragma: no cover - editable local builds
    __version__ = "0+unknown"

__all__ = ["Image", "Table", "data", "tasks", "__version__"]
