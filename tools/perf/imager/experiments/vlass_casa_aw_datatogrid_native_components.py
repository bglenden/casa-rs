#!/usr/bin/env python3
"""Launch the bounded CASA first-TT0 native-component oracle.

The caller supplies a fresh copy-on-write clone of the frozen 4096-square,
full-16-SPW CASA products. The injected dylib observes the first non-PSF
DComplex AW DataToGrid call, records native flags, imaging weights, UVW, and
dphase, proves the frozen v5 stream/geometry hashes, and exits with status 86.

The interposer does not invoke the original DataToGrid implementation or
obtain grid storage. It therefore stops before gridding, sumwt accumulation,
normalization, FFT, or product formation.
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


READY_MAGIC = 0x4341534141574E31
INTERPOSER_NAME = "casa_aw_datatogrid_native_components_interpose.dylib"
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
    output = os.environ.get("CASA_AW_NATIVE_COMPONENTS_OUTPUT")
    expected_nxy = os.environ.get("CASA_AW_NATIVE_COMPONENTS_EXPECT_NXY")
    inserted = os.environ.get("DYLD_INSERT_LIBRARIES", "")
    if not output:
        raise RuntimeError("CASA_AW_NATIVE_COMPONENTS_OUTPUT is required")
    receipt = Path(output)
    if not receipt.is_absolute():
        raise RuntimeError("CASA_AW_NATIVE_COMPONENTS_OUTPUT must be absolute")
    if receipt.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {receipt}")
    if expected_nxy != "4096":
        raise RuntimeError("CASA_AW_NATIVE_COMPONENTS_EXPECT_NXY must be exactly 4096")
    inserted_paths = [Path(entry) for entry in inserted.split(":") if entry]
    matching = [
        entry
        for entry in inserted_paths
        if entry.name == INTERPOSER_NAME and entry.is_file()
    ]
    if len(matching) != 1:
        raise RuntimeError(
            f"DYLD_INSERT_LIBRARIES must contain exactly one built {INTERPOSER_NAME}"
        )

    process = ctypes.CDLL(None)
    try:
        ready = process.casa_aw_datatogrid_native_components_ready_v1
    except AttributeError as error:
        raise RuntimeError(
            "the CASA native-component interposer is not resident"
        ) from error
    ready.argtypes = []
    ready.restype = ctypes.c_uint64
    if ready() != READY_MAGIC:
        raise RuntimeError(
            "the resident native-component interposer failed its exact ABI "
            "and symbol-owner marker"
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
        raise RuntimeError("refusing to run against the frozen CASA products")
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
            f"expected canonical CASA version string 6.7.5-18, found {reported_version}"
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
        "tclean returned normally; the fail-closed first-TT0 interposer was not reached"
    )


if __name__ == "__main__":
    main()
