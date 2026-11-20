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

    if name in {"data", "imexplore", "tools"}:
        return import_module(f".{name}", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
