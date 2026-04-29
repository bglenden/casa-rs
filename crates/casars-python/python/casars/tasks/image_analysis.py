"""Image-analysis task wrappers backed by casa-rs executables."""

from __future__ import annotations

import os
from os import PathLike
from typing import Any, TypeAlias

from .._task_runtime import (
    ProtocolInfo,
    configure_exportfits_binary,
    configure_imexplore_binary,
    configure_immoments_binary,
    fetch_exportfits_schema,
    fetch_immoments_schema,
    get_exportfits_protocol_info,
    get_immoments_protocol_info,
    invoke_exportfits_task,
    invoke_imexplore_json_subcommand,
    invoke_immoments_task,
)

StrPath: TypeAlias = str | PathLike[str]
TaskResult: TypeAlias = dict[str, Any]


def configure(
    *,
    imexplore_binary: StrPath | None = None,
    immoments_binary: StrPath | None = None,
    exportfits_binary: StrPath | None = None,
) -> None:
    """Configure default image-analysis binary overrides for this module."""

    configure_imexplore_binary(imexplore_binary)
    configure_immoments_binary(immoments_binary)
    configure_exportfits_binary(exportfits_binary)


def immoments_protocol_info(*, binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol information for the selected ``immoments`` binary."""

    return get_immoments_protocol_info(binary=binary)


def exportfits_protocol_info(*, binary: StrPath | None = None) -> ProtocolInfo:
    """Return validated protocol information for the selected ``exportfits`` binary."""

    return get_exportfits_protocol_info(binary=binary)


def immoments_schema(*, binary: StrPath | None = None) -> dict[str, Any]:
    """Return the Rust-emitted ``immoments`` schema bundle."""

    return fetch_immoments_schema(binary=binary)


def exportfits_schema(*, binary: StrPath | None = None) -> dict[str, Any]:
    """Return the Rust-emitted ``exportfits`` schema bundle."""

    return fetch_exportfits_schema(binary=binary)


def imhead(imagename: StrPath, *, binary: StrPath | None = None) -> dict[str, Any]:
    """Run CASA-style ``imhead`` through ``imexplore imhead --json``."""

    return invoke_imexplore_json_subcommand("imhead", [os.fspath(imagename)], binary=binary)


def imstat(
    imagename: StrPath,
    *,
    box: str | None = None,
    chans: str | None = None,
    binary: StrPath | None = None,
) -> dict[str, Any]:
    """Run CASA-style ``imstat`` through ``imexplore imstat --json``."""

    argv = [os.fspath(imagename)]
    if box is not None:
        argv.extend(["--box", box])
    if chans is not None:
        argv.extend(["--chans", chans])
    return invoke_imexplore_json_subcommand("imstat", argv, binary=binary)


def immoments(
    imagename: StrPath,
    *,
    outfile: StrPath,
    moments: int = 0,
    chans: str | None = None,
    includepix: tuple[float, float] | list[float] | None = None,
    overwrite: bool = False,
    binary: StrPath | None = None,
) -> TaskResult:
    """Run CASA-style ``immoments`` through the Rust task binary."""

    request = {
        "imagename": os.fspath(imagename),
        "outfile": os.fspath(outfile),
        "moments": moments,
        "chans": chans,
        "includepix": None if includepix is None else list(includepix),
        "overwrite": overwrite,
    }
    return invoke_immoments_task(request=request, binary=binary)


def exportfits(
    imagename: StrPath,
    fitsimage: StrPath,
    *,
    velocity: bool = False,
    overwrite: bool = False,
    binary: StrPath | None = None,
) -> TaskResult:
    """Run CASA-style ``exportfits`` through the Rust task binary."""

    request = {
        "imagename": os.fspath(imagename),
        "fitsimage": os.fspath(fitsimage),
        "velocity": velocity,
        "overwrite": overwrite,
    }
    return invoke_exportfits_task(request=request, binary=binary)
