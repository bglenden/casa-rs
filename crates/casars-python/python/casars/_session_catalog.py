"""Generated CASA-named wrappers for every catalog session.

Do not edit by hand; run ``scripts/generate-python-parameter-wrappers.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from os import PathLike
from typing import Any, Literal, TypeAlias

from .parameters import ParameterData, SessionParameters

StrPath: TypeAlias = str | PathLike[str]
_UNSET = object()


def _explicit(values: Mapping[str, object], names: tuple[str, ...]) -> dict[str, ParameterData]:
    return {name: values[name] for name in names if values[name] is not _UNSET}  # type: ignore[misc]


def tablebrowser(
    *,
    table: StrPath | object = _UNSET,
    view: Literal['columns', 'keywords', 'rows', 'summary'] | object = _UNSET,
    bookmark: str | object = _UNSET,
    rowstart: int | object = _UNSET,
    nrow: int | object = _UNSET,
    linkedtable: str | object = _UNSET,
    contentmode: Literal['auto', 'compact', 'detailed'] | object = _UNSET,
    parameters: SessionParameters | None = None,
    profile: StrPath | None = None,
    source: Literal["defaults", "last"] = "defaults",
    workspace: StrPath | None = None,
    save_last: bool = True,
    **options: Any,
) -> Any:
    """browse arbitrary casacore tables"""
    from .sessions import open as open_session

    overrides = _explicit(locals(), ('table', 'view', 'bookmark', 'rowstart', 'nrow', 'linkedtable', 'contentmode'))
    return open_session(
        "tablebrowser",
        parameters=parameters,
        profile=profile,
        start=source,
        workspace=workspace,
        save_last=save_last,
        overrides=overrides,
        **options,
    )

def imexplore(
    *,
    image: StrPath | object = _UNSET,
    blc: str | object = _UNSET,
    trc: str | object = _UNSET,
    inc: str | object = _UNSET,
    stretch: str | object = _UNSET,
    autoscale: Literal['frozen', 'per_plane'] | object = _UNSET,
    clip_low: str | object = _UNSET,
    clip_high: str | object = _UNSET,
    fps: int | object = _UNSET,
    view: Literal['coordinates', 'metadata', 'plane', 'spectrum'] | object = _UNSET,
    contentmode: Literal['raster', 'spreadsheet'] | object = _UNSET,
    colormap: Literal['gray', 'inferno', 'viridis'] | object = _UNSET,
    movieaxis: str | object = _UNSET,
    profileaxis: str | object = _UNSET,
    loop: bool | object = _UNSET,
    region: str | object = _UNSET,
    mask: str | object = _UNSET,
    parameters: SessionParameters | None = None,
    profile: StrPath | None = None,
    source: Literal["defaults", "last"] = "defaults",
    workspace: StrPath | None = None,
    save_last: bool = True,
    **options: Any,
) -> Any:
    """browse persistent casacore images"""
    from .sessions import open as open_session

    overrides = _explicit(locals(), ('image', 'blc', 'trc', 'inc', 'stretch', 'autoscale', 'clip_low', 'clip_high', 'fps', 'view', 'contentmode', 'colormap', 'movieaxis', 'profileaxis', 'loop', 'region', 'mask'))
    return open_session(
        "imexplore",
        parameters=parameters,
        profile=profile,
        start=source,
        workspace=workspace,
        save_last=save_last,
        overrides=overrides,
        **options,
    )

SESSION_SURFACES = ('tablebrowser', 'imexplore')

__all__ = ['SESSION_SURFACES', 'tablebrowser', 'imexplore']
