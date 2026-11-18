#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 imstat statistics for the #977 parameter work.

The images stay in this repository as CASA-written casacore tables, so the
casa-rs tests read CASA's own product and compare against the recorded
`expected.json`. Every pixel is a closed-form function of its index, so the
values are reproducible without this script's CASA run.

Run with CASA's own Python:

    "$CASA_PYTHON" -I tools/casa-compat/imstat_oracle.py
"""

import json
import math
from pathlib import Path

import numpy as np
from casatasks import imstat
from casatools import image, version_string


def pixel_value(index: int) -> float:
    """Deterministic spread with planted outliers for rejection algorithms."""
    value = ((index * 37) % 100) * 0.25 - 10.0
    if index == 5:
        return 500.0
    if index == 130:
        return -400.0
    if index == 200:
        return 350.0
    return value


def make_image(path: Path, *, beam: bool) -> None:
    """An 8 x 8 x 1 x 4 (RA, Dec, Stokes, frequency) image of the
    deterministic values, with CASA's default fromshape coordinates and a
    beam, or without one."""
    shape = [8, 8, 1, 4]
    data = np.array(
        [pixel_value(i) for i in range(math.prod(shape))], dtype=np.float32
    ).reshape(shape, order="F")
    ia = image()
    ia.fromshape(str(path), shape, overwrite=True)
    ia.putchunk(data)
    ia.setbrightnessunit("Jy/beam")
    if beam:
        ia.setrestoringbeam(major="3arcsec", minor="2arcsec", pa="10deg")
    ia.done()


def plain(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, float) and not math.isfinite(value):
        return "nan" if math.isnan(value) else "inf" if value > 0 else "-inf"
    return value


def record_case(cases: dict, name: str, **call) -> None:
    try:
        result = imstat(**call)
    except RuntimeError as error:
        message = str(error).split(" at File:", 1)[0]
        cases[name] = {"error": message}
        return
    cases[name] = {key: plain(value) for key, value in result.items()}


def main() -> None:
    root = Path("crates/casars-imexplore/tests/data/imstat")
    root.mkdir(parents=True, exist_ok=True)
    values = root / "values.im"
    unbeamed = root / "unbeamed.im"
    make_image(values, beam=True)
    make_image(unbeamed, beam=False)

    result = {"casa_version": version_string(), "values": {}, "unbeamed": {}}
    for name, call in (
        ("classic", {}),
        ("classic clmethod tiled", {"clmethod": "tiled"}),
        ("classic clmethod framework", {"clmethod": "framework"}),
        ("classic chans", {"chans": "1~2"}),
        ("classic box", {"box": "2,2,5,5"}),
        ("axes", {"axes": [0, 1]}),
        ("hinges-fences fence -1", {"algorithm": "hinges-fences", "fence": -1.0}),
        ("hinges-fences fence 0.5", {"algorithm": "hinges-fences", "fence": 0.5}),
        ("hinges-fences fence 1", {"algorithm": "hinges-fences", "fence": 1.0}),
        ("hinges-fences fence 2", {"algorithm": "hinges-fences", "fence": 2.0}),
        ("fit-half", {"algorithm": "fit-half"}),
        ("fit-half median", {"algorithm": "fit-half", "center": "median"}),
        ("fit-half zero", {"algorithm": "fit-half", "center": "zero"}),
        ("fit-half lside false", {"algorithm": "fit-half", "lside": False}),
        ("fit-half median lside false", {
            "algorithm": "fit-half",
            "center": "median",
            "lside": False,
        }),
        ("fit-half zero lside false", {
            "algorithm": "fit-half",
            "center": "zero",
            "lside": False,
        }),
        ("chauvenet", {"algorithm": "chauvenet"}),
        ("chauvenet zscore 1.5", {"algorithm": "chauvenet", "zscore": 1.5}),
        ("chauvenet zscore 2", {"algorithm": "chauvenet", "zscore": 2.0}),
        ("chauvenet maxiter 1", {"algorithm": "chauvenet", "maxiter": 1}),
        ("chauvenet maxiter 3", {"algorithm": "chauvenet", "maxiter": 3}),
        ("biweight", {"algorithm": "biweight"}),
        ("biweight niter 1", {"algorithm": "biweight", "niter": 1}),
        ("biweight niter -1", {"algorithm": "biweight", "niter": -1}),
        ("minimum-match c", {"algorithm": "c"}),
        ("minimum-match x", {"algorithm": "x"}),
        ("minimum-match empty", {"algorithm": ""}),
    ):
        call = dict(imagename=str(values), **call)
        record_case(result["values"], name, **call)
    for name, call in (
        ("classic", {}),
        ("classic box", {"box": "2,2,5,5"}),
        ("hinges-fences fence 1", {"algorithm": "hinges-fences", "fence": 1.0}),
        ("chauvenet zscore 1.5", {"algorithm": "chauvenet", "zscore": 1.5}),
        ("fit-half median", {"algorithm": "fit-half", "center": "median"}),
        ("biweight", {"algorithm": "biweight"}),
    ):
        call = dict(imagename=str(unbeamed), **call)
        record_case(result["unbeamed"], name, **call)

    (root / "expected.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    for lock in root.rglob("table.lock"):
        lock.unlink()


if __name__ == "__main__":
    main()
