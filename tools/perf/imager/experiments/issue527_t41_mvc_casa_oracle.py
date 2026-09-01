#!/usr/bin/env python3
"""Generate the one frozen CASA T41 multi-SPW MVC oracle.

The raw CASA image tables stay outside the repository.  This recipe refuses to
reuse or replace an existing output prefix so an unchanged oracle is never run
twice accidentally.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
import sys
import time
from typing import Any


MS = Path("/tmp/t41-alma-ephemobj-icrs.ms")
ARTIFACT_ROOT = Path("/tmp/t41-mvc-casa-oracle")
PREFIX = ARTIFACT_ROOT / "casa"
PRODUCT_SUFFIXES = (
    ".psf",
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual",
    ".residual.tt0",
    ".residual.tt1",
    ".model",
    ".model.tt0",
    ".model.tt1",
    ".image.tt0",
    ".image.tt1",
    ".sumwt",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".pb",
    ".pb.tt0",
    ".alpha",
    ".alpha.error",
)


def json_safe(value: Any) -> Any:
    if hasattr(value, "tolist"):
        return json_safe(value.tolist())
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, complex):
        return [float(value.real), float(value.imag)]
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if math.isnan(value) else ("+Infinity" if value > 0 else "-Infinity")
    return value


def image_receipt(image_tool: Any, path: Path) -> dict[str, Any]:
    image = image_tool()
    coordinates = None
    try:
        if not image.open(str(path)):
            raise RuntimeError(f"cannot open CASA image {path}")
        coordinates = image.coordsys()
        statistics = image.statistics()
        return {
            "shape": [int(value) for value in image.shape()],
            "brightness_unit": str(image.brightnessunit()),
            "axis_coordinate_types": [
                str(value) for value in coordinates.axiscoordinatetypes()
            ],
            "axis_units": [str(value) for value in coordinates.units()],
            "reference_pixel": json_safe(coordinates.referencepixel()),
            "reference_value": json_safe(coordinates.referencevalue(format="n")),
            "increment": json_safe(coordinates.increment(format="n")),
            "maximum": float(statistics["max"][0]),
            "maximum_position": [int(value) for value in statistics["maxpos"]],
            "rms": float(statistics["rms"][0]),
            "sum": float(statistics["sum"][0]),
        }
    finally:
        if coordinates is not None:
            coordinates.done()
        image.done()


def main() -> None:
    if not MS.is_dir():
        raise RuntimeError(f"T41 MeasurementSet is missing: {MS}")
    if ARTIFACT_ROOT.exists():
        raise RuntimeError(
            f"refusing to replace or rerun the frozen oracle at {ARTIFACT_ROOT}"
        )
    ARTIFACT_ROOT.mkdir(parents=True)
    config = ARTIFACT_ROOT / "casa-config.py"
    log = ARTIFACT_ROOT / "casa.log"
    config.write_text(
        "\n".join(
            [
                'measurespath = "/Users/brianglendenning/.casa/data"',
                'datapath = ["/Users/brianglendenning/.casa/data"]',
                "data_auto_update = False",
                "measures_auto_update = False",
                "nogui = True",
                "nologger = True",
                "nologfile = False",
                f'logfile = "{log}"',
                "log2term = True",
                "",
            ]
        ),
        encoding="utf-8",
    )

    original_argv = sys.argv
    sys.argv = [sys.argv[0], "--configfile", str(config)]
    try:
        import casatasks
        import casatools
        from casatasks import casalog, tclean
        from casatools import image
    finally:
        sys.argv = original_argv

    casalog.setlogfile(str(log))
    kwargs = {
        "vis": str(MS),
        "imagename": str(PREFIX),
        "field": "1",
        "spw": "0,1,2,3",
        "datacolumn": "data",
        "imsize": [512, 512],
        "cell": ["0.1arcsec", "0.1arcsec"],
        "phasecenter": "TRACKFIELD",
        "stokes": "I",
        "specmode": "mvc",
        "nchan": 16,
        "outframe": "LSRK",
        "gridder": "standard",
        "deconvolver": "mtmfs",
        "nterms": 2,
        "scales": [0],
        "weighting": "natural",
        "niter": 4,
        "cycleniter": 2,
        "gain": 0.1,
        "threshold": "0Jy",
        "pblimit": -0.1,
        "pbcor": False,
        "interactive": False,
        "parallel": False,
    }
    started = time.perf_counter()
    result = tclean(**kwargs)
    wall_seconds = time.perf_counter() - started

    missing = [suffix for suffix in PRODUCT_SUFFIXES if not Path(f"{PREFIX}{suffix}").is_dir()]
    if missing:
        raise RuntimeError(f"CASA MVC oracle omitted products: {missing}")
    products = {
        suffix: image_receipt(image, Path(f"{PREFIX}{suffix}"))
        for suffix in PRODUCT_SUFFIXES
    }
    inventory = sorted(
        entry.name.removeprefix(PREFIX.name)
        for entry in ARTIFACT_ROOT.iterdir()
        if entry.is_dir() and entry.name.startswith(f"{PREFIX.name}.")
    )
    manifest = {
        "kind": "casa_rs_t41_multi_spw_mvc_oracle",
        "casatasks_version": casatasks.version_string(),
        "casatools_version": casatools.version_string(),
        "measurement_set": str(MS),
        "parameters": kwargs,
        "tclean_return": json_safe(result),
        "wall_seconds": wall_seconds,
        "product_inventory": inventory,
        "products": products,
    }
    (ARTIFACT_ROOT / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
