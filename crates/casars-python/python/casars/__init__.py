"""Python bindings for casa-rs."""

from __future__ import annotations

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from . import parameters, tasks
from .parameters import SurfaceParameters, TaskParameters

try:
    __version__ = version("casa-rs-python")
except PackageNotFoundError:  # pragma: no cover - editable local builds
    __version__ = "0+unknown"

__all__ = [
    "Image",
    "imexplore",
    "SurfaceParameters",
    "TaskParameters",
    "data",
    "parameters",
    "tasks",
    "tools",
    "__version__",
]


def __getattr__(name: str) -> Any:
    """Load the data and tool surfaces lazily so task wrappers work without `_core`."""

    if name in {"imexplore", "tools"}:
        return import_module(f".{name}", __name__)
    if name in {"data", "Image"}:
        data_module = import_module(".data", __name__)

        if name == "data":
            return data_module
        return data_module.Image
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
