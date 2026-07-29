#!/usr/bin/env python3
"""Generate the one-time 4096-square full-16-SPW VLASS CASA oracle."""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import time

from casatasks import tclean


ROOT = pathlib.Path(
    os.environ.get(
        "CASA_RS_VLASS_EXPERIMENT_ROOT",
        "/Volumes/GLENDENNING/casa-rs-vlass/issue-446",
    )
)
MS = (
    ROOT
    / "data/frozen-clean-b80d5e87487a/"
    "VLASS1.2.sb36484946.eb36542800.58574.4235612037_"
    "ptgfix_split_bright_source.ms"
)
MASK = ROOT / "masks/vlass-single-field-peak-box-4096.mask"
CACHE = ROOT / "cf-cache/6.7.5.18/single-field-4096-full-16-spw"
IMAGE = ROOT / "casa-reduced-clean/4096-full-16-spw/casa"
RECEIPT = (
    ROOT
    / "receipts/runs/20260729-vlass-reduced-casa-clean-4096-full-16-spw.json"
)

EXPECTED_PRODUCTS = [
    ".alpha",
    ".alpha.error",
    ".image.tt0",
    ".image.tt1",
    ".mask",
    ".model.tt0",
    ".model.tt1",
    ".pb.tt0",
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual.tt0",
    ".residual.tt1",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".weight.tt0",
    ".weight.tt1",
    ".weight.tt2",
]

TCLEAN_PARAMETERS = {
    "vis": str(MS),
    "imagename": str(IMAGE),
    "field": "1525",
    "phasecenter": 1525,
    "spw": "2~17",
    "datacolumn": "data",
    "uvrange": "<12km",
    "intent": "OBSERVE_TARGET#UNSPECIFIED",
    "imsize": [4096, 4096],
    "cell": ["0.6arcsec", "0.6arcsec"],
    "stokes": "I",
    "projection": "SIN",
    "specmode": "mfs",
    "interpolation": "linear",
    "gridder": "awproject",
    "cfcache": str(CACHE),
    "wprojplanes": 32,
    "facets": 1,
    "psfphasecenter": "",
    "vptable": "",
    "aterm": True,
    "psterm": False,
    "wbawp": True,
    "conjbeams": True,
    "usepointing": True,
    "computepastep": 360.0,
    "rotatepastep": 360.0,
    "pointingoffsetsigdev": 0.0,
    "mosweight": False,
    "normtype": "flatnoise",
    "weighting": "briggs",
    "robust": 1.0,
    "perchanweightdensity": True,
    "deconvolver": "mtmfs",
    "nterms": 2,
    "scales": [0, 5, 12],
    "smallscalebias": 0.0,
    "niter": 2000,
    "gain": 0.1,
    "threshold": "0Jy",
    "nsigma": 5.0,
    "cycleniter": 2000,
    "cyclefactor": 3.0,
    "minpsffraction": 0.05,
    "maxpsffraction": 0.8,
    "pblimit": 0.0001,
    "pbcor": False,
    "restoration": True,
    "restoringbeam": "common",
    "interactive": False,
    "usemask": "user",
    "mask": "box[[575pix,2125pix],[638pix,2188pix]]",
    "restart": False,
    "savemodel": "none",
    "calcres": True,
    "calcpsf": True,
    "parallel": False,
}


def product_suffixes() -> list[str]:
    prefix = f"{IMAGE.name}."
    return sorted(
        f".{path.name.removeprefix(prefix)}"
        for path in IMAGE.parent.iterdir()
        if path.is_dir() and path.name.startswith(prefix)
    )


def main() -> None:
    if not MS.is_dir():
        raise RuntimeError(f"staged MeasurementSet is missing: {MS}")
    if not MASK.is_dir():
        raise RuntimeError(f"CASA mask is missing: {MASK}")
    if CACHE.exists():
        raise RuntimeError(f"refusing to reuse or overwrite CF cache: {CACHE}")
    if any(IMAGE.parent.glob(f"{IMAGE.name}.*")):
        raise RuntimeError(f"refusing to overwrite existing products: {IMAGE}.*")

    IMAGE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    RECEIPT.parent.mkdir(parents=True, exist_ok=True)
    parameters_json = json.dumps(
        TCLEAN_PARAMETERS, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    started = time.monotonic()
    tclean(**TCLEAN_PARAMETERS)
    elapsed_s = time.monotonic() - started
    products = product_suffixes()
    if products != EXPECTED_PRODUCTS:
        raise RuntimeError(
            f"CASA product inventory mismatch: {products}; expected {EXPECTED_PRODUCTS}"
        )
    result = {
        "kind": "vlass_reduced_casa_clean_correctness_oracle",
        "role": "reduced_turnaround_only_not_casa_baseline",
        "casa_version": "6.7.5.18",
        "elapsed_s": elapsed_s,
        "parameters_sha256": hashlib.sha256(parameters_json).hexdigest(),
        "parameters": TCLEAN_PARAMETERS,
        "mask_image_reference": str(MASK),
        "products": products,
        "pid": os.getpid(),
    }
    RECEIPT.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
