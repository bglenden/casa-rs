"""Tutorial-scoped object helpers composed from raw provider task APIs.

This module is outside profile persistence. Use :mod:`casars.tasks.profiles`
when a reusable CASA-named task configuration and managed Last lifecycle are
required.
"""

from __future__ import annotations

import os
from os import PathLike
from typing import Any, TypeAlias

from . import image_analysis, imager, msexplore

StrPath: TypeAlias = str | PathLike[str]


def vla_ppdisk_dirty_analysis(
    measurement_set: StrPath,
    image_name: StrPath,
    *,
    image_size: int = 257,
    cell_arcsec: float = 0.00311,
    plot_output: StrPath | None = None,
    imager_binary: StrPath | None = None,
    imexplore_binary: StrPath | None = None,
    msexplore_binary: StrPath | None = None,
) -> dict[str, Any]:
    """Image and inspect a VLA ppdisk synthetic MS using casa-rs task surfaces."""

    measurement_set_path = os.fspath(measurement_set)
    image_prefix = os.fspath(image_name)
    image_product = f"{image_prefix}.image"

    imaging = imager.mfs(
        measurement_set_path,
        image_prefix,
        image_size=image_size,
        cell_arcsec=cell_arcsec,
        data_column="data",
        dirty_only=True,
        niter=0,
        binary=imager_binary,
    )
    header = image_analysis.imhead(image_product, binary=imexplore_binary)
    statistics = image_analysis.imstat(image_product, binary=imexplore_binary)
    plot = None
    if plot_output is not None:
        plot = msexplore.plot(
            measurement_set_path,
            plot_output,
            x_axis="Time",
            y_axis="Amplitude",
            data_column="data",
            title="VLA ppdisk synthetic amplitudes",
            binary=msexplore_binary,
        )
    return {
        "imaging": imaging,
        "imhead": header,
        "imstat": statistics,
        "plot": plot,
    }
