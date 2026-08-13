#!/usr/bin/env python3
"""Launch the bounded CASA AW DataToGrid input-hash interposer.

This script must be run only against a fresh copy-on-write clone of the frozen
4096 full-16-SPW CASA product bundle and with the diagnostic interposer in
DYLD_INSERT_LIBRARIES.  It zeros the cloned Taylor model images, requests one
residual-only restart, and expects the interposer to terminate with status 86
before CASA writes a grid, performs an FFT, or forms an image.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import numpy as np
from casatasks import casalog, tclean
from casatools import image, version_string

from vlass_reduced_casa_clean_4096_full_16_spw import IMAGE, TCLEAN_PARAMETERS


REQUIRED_SUFFIXES = (
    ".model.tt0",
    ".model.tt1",
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
)


def require_environment() -> Path:
    output = os.environ.get("CASA_AW_INPUT_HASH_OUTPUT")
    expected_nxy = os.environ.get("CASA_AW_INPUT_HASH_EXPECT_NXY")
    inserted = os.environ.get("DYLD_INSERT_LIBRARIES", "")
    if not output:
        raise RuntimeError("CASA_AW_INPUT_HASH_OUTPUT is required")
    receipt = Path(output)
    if not receipt.is_absolute():
        raise RuntimeError("CASA_AW_INPUT_HASH_OUTPUT must be absolute")
    if receipt.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {receipt}")
    if expected_nxy != "4096":
        raise RuntimeError(
            "CASA_AW_INPUT_HASH_EXPECT_NXY must be exactly 4096 for this frozen row"
        )
    if "casa_aw_datatogrid_input_hash_interpose" not in inserted:
        raise RuntimeError(
            "DYLD_INSERT_LIBRARIES does not name the bounded CASA AW interposer"
        )
    return receipt


def image_max_abs(path: Path) -> float:
    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open CASA image: {path}")
        statistics: dict[str, Any] = tool.statistics(robust=False, verbose=False)
    finally:
        tool.close()
    minimum = float(np.asarray(statistics["min"]).reshape(-1)[0])
    maximum = float(np.asarray(statistics["max"]).reshape(-1)[0])
    return max(abs(minimum), abs(maximum))


def zero_model(path: Path) -> None:
    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open cloned model image: {path}")
        if not tool.set(0.0):
            raise RuntimeError(f"failed to zero cloned model image: {path}")
    finally:
        tool.close()
    if image_max_abs(path) != 0.0:
        raise RuntimeError(f"cloned model image is not exactly zero: {path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--prepared-prefix",
        required=True,
        type=Path,
        help="fresh copy-on-write clone prefix; never the frozen CASA prefix",
    )
    args = parser.parse_args()
    receipt = require_environment()
    prepared = args.prepared_prefix.resolve()
    frozen = IMAGE.resolve()
    if not prepared.is_absolute():
        raise RuntimeError("--prepared-prefix must be absolute")
    if prepared == frozen:
        raise RuntimeError("refusing to run the abort probe in the frozen CASA bundle")
    if not prepared.parent.is_dir():
        raise RuntimeError(f"prepared scratch directory is missing: {prepared.parent}")
    for suffix in REQUIRED_SUFFIXES:
        product = Path(f"{prepared}{suffix}")
        if not product.is_dir():
            raise RuntimeError(f"prepared CASA product is missing: {product}")
    if version_string() != "6.7.5.18":
        raise RuntimeError(f"expected CASA 6.7.5.18, found {version_string()}")

    for suffix in (".model.tt0", ".model.tt1"):
        zero_model(Path(f"{prepared}{suffix}"))

    parameters = dict(TCLEAN_PARAMETERS)
    parameters.update(
        {
            "imagename": str(prepared),
            "niter": 0,
            "cycleniter": 1,
            "nmajor": 0,
            "calcres": True,
            "calcpsf": False,
            "restoration": False,
            "restart": True,
            "savemodel": "none",
            "parallel": False,
            "fullsummary": False,
        }
    )
    receipt.parent.mkdir(parents=True, exist_ok=True)
    casalog.filter("INFO")
    tclean(**parameters)
    raise RuntimeError(
        "tclean returned normally; the bounded DataToGrid interposer was not reached"
    )


if __name__ == "__main__":
    main()
