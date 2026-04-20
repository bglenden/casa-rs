"""Python bindings for casa-rs."""

from __future__ import annotations

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from . import tasks

try:
    __version__ = version("casa-rs-python")
except PackageNotFoundError:  # pragma: no cover - editable local builds
    __version__ = "0+unknown"

__all__ = ["Image", "Table", "data", "tasks", "__version__"]


def __getattr__(name: str) -> Any:
    """Load the data surface lazily so task wrappers work without `_core`."""

    if name in {"data", "Image", "Table"}:
        data_module = import_module(".data", __name__)

        if name == "data":
            return data_module
        if name == "Image":
            return data_module.Image
        return data_module.Table
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
