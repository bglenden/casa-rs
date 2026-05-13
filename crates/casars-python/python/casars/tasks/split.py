"""CASA-style split wrapper backed by the canonical ``mstransform`` executable."""

from __future__ import annotations

import json
import os
from os import PathLike
from typing import Any, TypeAlias

from .._task_runtime import (
    MsTransformInvocationError,
    configure_mstransform_binary,
    resolve_mstransform_binary,
    _run_process,
)

StrPath: TypeAlias = str | PathLike[str]
TaskResult: TypeAlias = dict[str, Any]


def configure(*, binary: StrPath | None) -> None:
    """Configure the default ``mstransform`` binary override for this module."""

    configure_mstransform_binary(binary)


def split(
    vis: StrPath,
    outputvis: StrPath,
    *,
    field: str | None = None,
    spw: str | None = None,
    width: int | str = 1,
    datacolumn: str = "data",
    keepflags: bool = True,
    binary: StrPath | None = None,
) -> TaskResult:
    """Materialize a selected MeasurementSet using CASA ``split`` parameters."""

    resolved = resolve_mstransform_binary(binary)
    argv = [
        resolved,
        "--vis",
        os.fspath(vis),
        "--outputvis",
        os.fspath(outputvis),
        "--datacolumn",
        datacolumn.upper(),
        "--width",
        str(width),
    ]
    if field not in (None, ""):
        argv.extend(["--field", str(field)])
    if spw not in (None, ""):
        argv.extend(["--spw", str(spw)])
    argv.append("--keepflags" if keepflags else "--no-keepflags")
    stdout = _run_process(argv, error_cls=MsTransformInvocationError)
    return json.loads(stdout)
