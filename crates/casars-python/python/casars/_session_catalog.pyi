"""Generated CASA-named wrappers for every catalog session.

Do not edit by hand; run ``scripts/generate-python-parameter-wrappers.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from os import PathLike
from typing import Any, Literal, TypeAlias

from .parameters import ParameterData, SessionParameters
from .sessions import JsonlSession

StrPath: TypeAlias = str | PathLike[str]


def tablebrowser(
    *,
    table: StrPath = ...,
    view: Literal['columns', 'keywords', 'rows', 'summary'] = ...,
    bookmark: str = ...,
    rowstart: int = ...,
    nrow: int = ...,
    linkedtable: str = ...,
    contentmode: Literal['auto', 'compact', 'detailed'] = ...,
    parameters: SessionParameters | None = ... ,
    profile: StrPath | None = ... ,
    source: Literal["defaults", "last"] = ... ,
    workspace: StrPath | None = ... ,
    save_last: bool = ... ,
    **options: Any,
) -> JsonlSession:
    ...

def imexplore(
    *,
    image: StrPath = ...,
    blc: str = ...,
    trc: str = ...,
    inc: str = ...,
    stretch: str = ...,
    autoscale: Literal['frozen', 'per_plane'] = ...,
    clip_low: str = ...,
    clip_high: str = ...,
    fps: int = ...,
    view: Literal['coordinates', 'metadata', 'plane', 'spectrum'] = ...,
    contentmode: Literal['raster', 'spreadsheet'] = ...,
    colormap: Literal['gray', 'inferno', 'viridis'] = ...,
    movieaxis: str = ...,
    profileaxis: str = ...,
    loop: bool = ...,
    region: str = ...,
    mask: str = ...,
    parameters: SessionParameters | None = ... ,
    profile: StrPath | None = ... ,
    source: Literal["defaults", "last"] = ... ,
    workspace: StrPath | None = ... ,
    save_last: bool = ... ,
    **options: Any,
) -> JsonlSession:
    ...

SESSION_SURFACES = ('tablebrowser', 'imexplore')

__all__ = ['SESSION_SURFACES', 'tablebrowser', 'imexplore']
