#!/usr/bin/env python3
"""Run the bounded one-block CASA AW DataToGrid bracket oracle.

The caller must provide a fresh copy-on-write clone of the frozen 4096-square,
full-16-SPW CASA product prefix.  The injected dylib hashes the ordered
residual inputs, invokes the unmodified CASA DComplex DataToGrid function for
exactly one TT0/TT1 block, hashes the cumulative raw grids and sumwt, writes an
atomic receipt, and exits with status 86.

The dylib also interposes AWProjectFT/AWProjectWBFT finalizeToSky and getImage.
Those hooks fail closed with status 87, so a loaded probe cannot continue into
normalization, FFT, or image formation if the expected DataToGrid pair is not
reached.
"""

from __future__ import annotations

import argparse
import ctypes
import os
from pathlib import Path
from typing import Any

import numpy as np
from casatasks import casalog, tclean
from casatools import image, version_string

from vlass_reduced_casa_clean_4096_full_16_spw import (
    EXPECTED_PRODUCTS,
    IMAGE,
    TCLEAN_PARAMETERS,
)


READY_MAGIC = 0x4341534141574231
REQUIRED_RESTART_SUFFIXES = (
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
    output = os.environ.get("CASA_AW_BRACKET_OUTPUT")
    expected_nxy = os.environ.get("CASA_AW_BRACKET_EXPECT_NXY")
    target_blocks = os.environ.get("CASA_AW_BRACKET_BLOCKS")
    terms = os.environ.get("CASA_AW_BRACKET_TERMS")
    inserted = os.environ.get("DYLD_INSERT_LIBRARIES", "")
    if not output:
        raise RuntimeError("CASA_AW_BRACKET_OUTPUT is required")
    receipt = Path(output)
    if not receipt.is_absolute():
        raise RuntimeError("CASA_AW_BRACKET_OUTPUT must be absolute")
    if receipt.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {receipt}")
    if expected_nxy != "4096":
        raise RuntimeError(
            "CASA_AW_BRACKET_EXPECT_NXY must be exactly 4096 for this frozen row"
        )
    if target_blocks != "1":
        raise RuntimeError(
            "CASA_AW_BRACKET_BLOCKS must be exactly 1 for this one-block launcher"
        )
    if terms != "2":
        raise RuntimeError(
            "CASA_AW_BRACKET_TERMS must be exactly 2 for the MT-MFS TT0/TT1 pair"
        )
    inserted_paths = [Path(entry) for entry in inserted.split(":") if entry]
    matching = [
        entry
        for entry in inserted_paths
        if entry.name == "casa_aw_datatogrid_bracket_interpose.dylib"
    ]
    if len(matching) != 1 or not matching[0].is_file():
        raise RuntimeError(
            "DYLD_INSERT_LIBRARIES must contain the built bracket interposer"
        )

    process = ctypes.CDLL(None)
    try:
        ready = process.casa_aw_datatogrid_bracket_ready_v1
    except AttributeError as error:
        raise RuntimeError(
            "the bracket interposer is not resident in this CASA Python process"
        ) from error
    ready.argtypes = []
    ready.restype = ctypes.c_uint64
    if ready() != READY_MAGIC:
        raise RuntimeError("the resident bracket interposer failed its ABI marker")
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


def product_suffixes(prefix: Path) -> list[str]:
    product_prefix = f"{prefix.name}."
    return sorted(
        f".{path.name.removeprefix(product_prefix)}"
        for path in prefix.parent.iterdir()
        if path.is_dir() and path.name.startswith(product_prefix)
    )


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
    products = product_suffixes(prepared)
    if products != EXPECTED_PRODUCTS:
        raise RuntimeError(
            f"prepared product inventory mismatch: {products}; "
            f"expected {EXPECTED_PRODUCTS}"
        )
    for suffix in REQUIRED_RESTART_SUFFIXES:
        product = Path(f"{prepared}{suffix}")
        if not product.is_dir():
            raise RuntimeError(f"prepared CASA product is missing: {product}")
    reported_version = version_string()
    if reported_version != "6.7.5-18":
        raise RuntimeError(
            f"expected canonical CASA version string 6.7.5-18, "
            f"found {reported_version}"
        )

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
        "tclean returned normally; the fail-closed DataToGrid/finalize/getImage "
        "interpositions were not reached"
    )


if __name__ == "__main__":
    main()
