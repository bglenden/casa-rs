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


def make_stokes_image(path: Path) -> None:
    """A 4 x 4 x 4 x 2 (RA, Dec, Stokes, frequency) image of a ramp with
    CASA's default fromshape coordinates: I, Q, U and V Stokes planes."""
    shape = [4, 4, 4, 2]
    data = np.arange(math.prod(shape), dtype=np.float32).reshape(shape, order="F")
    ia = image()
    ia.fromshape(str(path), shape, overwrite=True)
    ia.putchunk(data)
    ia.done()


def make_constant_image(path: Path, value: float, shape=(8, 8, 1, 4)) -> None:
    """An image of one value, with CASA's default fromshape coordinates."""
    ia = image()
    ia.fromshape(str(path), list(shape), overwrite=True)
    ia.putchunk(np.full(shape, value, dtype=np.float32, order="F"))
    ia.done()


def make_nan_image(path: Path) -> None:
    """An image of zeros with one NaN pixel."""
    shape = (8, 8, 1, 4)
    ia = image()
    ia.fromshape(str(path), list(shape), overwrite=True)
    data = np.zeros(shape, dtype=np.float32, order="F")
    data[0, 0, 0, 0] = np.nan
    ia.putchunk(data)
    ia.done()


def make_mask_image(path: Path, shape) -> None:
    """An image of zeros whose rows from the middle on are one."""
    ia = image()
    ia.fromshape(str(path), list(shape), overwrite=True)
    data = np.zeros(shape, dtype=np.float32, order="F")
    data[shape[0] // 2:, ...] = 1.0
    ia.putchunk(data)
    ia.done()


def make_ramp_image(path: Path, shape) -> None:
    """An image of a Fortran ramp with CASA's default coordinates."""
    data = np.arange(math.prod(shape), dtype=np.float32).reshape(shape, order="F")
    ia = image()
    ia.fromshape(str(path), list(shape), overwrite=True)
    ia.putchunk(data)
    ia.done()


def make_moment_image(path: Path) -> None:
    """A 2-D ramp in Jy/beam with a beam: a moment map, whose statistics
    carry the flux."""
    shape = [8, 8]
    data = np.arange(math.prod(shape), dtype=np.float32).reshape(shape, order="F")
    ia = image()
    ia.fromshape(str(path), shape, overwrite=True)
    ia.putchunk(data)
    ia.setbrightnessunit("Jy/beam")
    ia.setrestoringbeam(major="3arcsec", minor="2arcsec", pa="10deg")
    ia.done()


def fits_card(key: str, value, comment: str = "") -> bytes:
    """One 80-byte FITS header card."""
    if isinstance(value, bool):
        text = f"{key:<8}= {'T' if value else 'F':>20}"
    elif isinstance(value, int):
        text = f"{key:<8}= {value:>20d}"
    elif isinstance(value, float):
        text = f"{key:<8}= {value:>20.12G}"
    else:
        text = f"{key:<8}= '{value:<8}'"
    if comment:
        text += f" / {comment}"
    return text.ljust(80).encode("ascii")


def make_linear_fits(path: Path) -> None:
    """A FITS image with linear, non-direction coordinates: the CAS-2195
    scenario, read by imstat directly as the FITS file."""
    nx, ny = 5, 4
    header = b"".join([
        fits_card("SIMPLE", True, "conforms to FITS standard"),
        fits_card("BITPIX", -32, "float32"),
        fits_card("NAXIS", 2),
        fits_card("NAXIS1", nx),
        fits_card("NAXIS2", ny),
        fits_card("CTYPE1", "POSITION", "linear"),
        fits_card("CTYPE2", "POSITION", "linear"),
        fits_card("CUNIT1", "m"),
        fits_card("CUNIT2", "m"),
        fits_card("CRVAL1", 5.0),
        fits_card("CRVAL2", 7.0),
        fits_card("CRPIX1", 2.0),
        fits_card("CRPIX2", 2.0),
        fits_card("CDELT1", 3.0),
        fits_card("CDELT2", 2.0),
        fits_card("BUNIT", "Jy"),
        b"END".ljust(80),
    ])
    header += b" " * ((2880 - len(header) % 2880) % 2880)
    # FITS numbers are big-endian.
    data = np.arange(nx * ny, dtype=">f4").reshape(ny, nx)
    payload = data.tobytes()
    payload += b"\x00" * ((2880 - len(payload) % 2880) % 2880)
    path.write_bytes(header + payload)


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
        cases[name] = {"error": error_message(str(error))}
        return
    cases[name] = {key: plain(value) for key, value in result.items()}


def error_message(text: str) -> str:
    """CASA's own wording, without the throw site casacore appends."""
    for cut in (" thrown by", " at File:"):
        text = text.split(cut, 1)[0]
    if "SEVERE" in text:
        # A logged refusal: the wording follows the logger's name.
        text = text.split("SEVERE", 1)[1].lstrip("\t ")
        text = text.split("\t", 1)[1] if "\t" in text else text
    return (
        text.split("\n", 1)[0].strip().removeprefix("Exception: ").rstrip(".")
    )


def main() -> None:
    root = Path("crates/casars-imexplore/tests/data/imstat")
    root.mkdir(parents=True, exist_ok=True)
    values = root / "values.im"
    unbeamed = root / "unbeamed.im"
    make_image(values, beam=True)
    make_image(unbeamed, beam=False)

    result = {"casa_version": version_string(), "values": {}, "unbeamed": {}, "stokes4": {}}
    for name, call in (
        ("classic", {}),
        ("classic clmethod tiled", {"clmethod": "tiled"}),
        ("classic clmethod framework", {"clmethod": "framework"}),
        ("classic clmethod a", {"clmethod": "a"}),
        ("classic chans", {"chans": "1~2"}),
        ("classic box", {"box": "2,2,5,5"}),
        ("axes", {"axes": [0, 1]}),
        ("hinges-fences fence -1", {"algorithm": "hinges-fences", "fence": -1.0}),
        ("hinges-fences fence 0.5", {"algorithm": "hinges-fences", "fence": 0.5}),
        ("hinges-fences fence 1", {"algorithm": "hinges-fences", "fence": 1.0}),
        ("hinges-fences fence 2", {"algorithm": "hinges-fences", "fence": 2.0}),
        ("hinges-fences fence 0", {"algorithm": "hinges-fences", "fence": 0.0}),
        ("hinges-fences minimum h", {"algorithm": "hinges-fences", "fence": 1.0}),
        ("hinges-fences upper case", {"algorithm": "Hinges-Fences", "fence": 1.0}),
        ("fit-half", {"algorithm": "fit-half"}),
        ("fit-half median", {"algorithm": "fit-half", "center": "median"}),
        ("fit-half zero", {"algorithm": "fit-half", "center": "zero"}),
        ("fit-half center med", {"algorithm": "fit-half", "center": "med"}),
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
        ("chauvenet zscore 3", {"algorithm": "chauvenet", "zscore": 3.0}),
        ("chauvenet maxiter 1", {"algorithm": "chauvenet", "maxiter": 1}),
        ("chauvenet maxiter 3", {"algorithm": "chauvenet", "maxiter": 3}),
        ("chauvenet maxiter 0", {"algorithm": "chauvenet", "maxiter": 0}),
        ("chauvenet maxiter -5", {"algorithm": "chauvenet", "maxiter": -5}),
        ("biweight", {"algorithm": "biweight"}),
        ("biweight niter 1", {"algorithm": "biweight", "niter": 1}),
        ("biweight niter 2", {"algorithm": "biweight", "niter": 2}),
        ("biweight niter -1", {"algorithm": "biweight", "niter": -1}),
        ("biweight niter 0", {"algorithm": "biweight", "niter": 0}),
        ("biweight upper case", {"algorithm": "BIWEIGHT"}),
        ("biweight box", {"algorithm": "biweight", "box": "2,2,5,5"}),
        ("biweight single pixel", {"algorithm": "biweight", "box": "0,0,0,0", "chans": "0"}),
        ("classic single pixel", {"box": "0,0,0,0", "chans": "0"}),
        ("fit-half single pixel", {
            "algorithm": "fit-half",
            "center": "zero",
            "box": "0,0,0,0",
            "chans": "0",
        }),
        ("minimum-match c", {"algorithm": "c"}),
        ("minimum-match x", {"algorithm": "x"}),
        ("minimum-match empty", {"algorithm": ""}),
        ("minimum-match cla", {"algorithm": "cla"}),
        ("center m", {"algorithm": "fit-half", "center": "m"}),
        ("center bad", {"algorithm": "fit-half", "center": "bad"}),
        ("clmethod bad", {"clmethod": "bad"}),
        ("axes 0 1", {"axes": [0, 1]}),
        ("axes 1 0", {"axes": [1, 0]}),
        ("axes scalar 0", {"axes": 0}),
        ("axes scalar 3", {"axes": 3}),
        ("axes scalar -1", {"axes": -1}),
        ("axes all four", {"axes": [0, 1, 2, 3]}),
        ("axes 3", {"axes": [3]}),
        ("axes 2", {"axes": [2]}),
        ("axes empty list", {"axes": []}),
        ("axes minus one list", {"axes": [-1]}),
        ("axes 0 1 with box", {"axes": [0, 1], "box": "2,2,5,5"}),
        ("axes 0 1 with chans", {"axes": [0, 1], "chans": "1~2"}),
        ("axes minus one twice", {"axes": [-1, -1]}),
        ("axes negative -4 -3", {"axes": [-4, -3]}),
        ("axes out of range 4", {"axes": [4]}),
        ("axes out of range -5", {"axes": -5}),
        ("biweight with axes", {"algorithm": "biweight", "axes": [0, 1]}),
        ("fit-half with axes", {"algorithm": "fit-half", "axes": [0, 1]}),
        ("chauvenet with axes", {"algorithm": "chauvenet", "axes": [0, 1]}),
        (
            "hinges-fences with axes",
            {"algorithm": "hinges-fences", "fence": 1.0, "axes": [0, 1]},
        ),
        (
            "mask threshold",
            {"mask": f'"{values}" > 100'},
        ),
        (
            "mask threshold with stretch",
            {"mask": f'"{values}" > 100', "stretch": True},
        ),
        (
            "mask empty selection",
            {"mask": f'"{values}" < -1000'},
        ),
        ("mask bad expression", {"mask": "not an expression"}),
        ("listit false", {"listit": False}),
        ("verbose false", {"verbose": False}),
        ("listit and verbose false", {"listit": False, "verbose": False}),
    ):
        call = dict(imagename=str(values), **call)
        record_case(result["values"], name, **call)

    # The full LEL mask forms, on the fixture images beside the target.
    other = root / "other.im"
    make_constant_image(other, 2.0)
    nan_image = root / "nan.im"
    make_nan_image(nan_image)
    mask2d = root / "mask2d.im"
    make_mask_image(mask2d, (8, 8))
    mask3d = root / "mask3d.im"
    make_mask_image(mask3d, (8, 8, 1))
    small = root / "small.im"
    make_constant_image(small, 1.0, shape=(4, 4))
    for name, call in {
        "mask boolean and": {"mask": f'"{values}" > 3 && "{values}" < 10'},
        "mask boolean or": {"mask": f'"{values}" > 3 || "{values}" > 400'},
        "mask boolean not": {"mask": f'!("{values}" > 3)'},
        "mask isnan nan image": {"mask": f'isnan("{nan_image}")'},
        "mask isnan no nan": {"mask": f'isnan("{values}")'},
        "mask iif": {"mask": f'iif("{values}" > 100, "{values}", 0) > 50'},
        "mask sqrt threshold": {"mask": f'"{values}" > sqrt(100)'},
        "mask two images": {"mask": f'"{values}" > "{other}"'},
        "mask single quotes": {"mask": f"'{values}' > 100"},
        "mask pi comparison": {"mask": f'"{values}" > 2 * pi()'},
        "mask 2d stretch": {"mask": f'"{mask2d}" > 0.5', "stretch": True},
        "mask 2d no stretch": {"mask": f'"{mask2d}" > 0.5', "stretch": False},
        "mask 3d stretch": {"mask": f'"{mask3d}" > 0.5', "stretch": True},
        "mask unextendable": {"mask": f'"{small}" > 0.5', "stretch": True},
        "mask bad name": {"mask": '"does-not-exist.im" > 3'},
        "mask bare words": {"mask": "not an expression"},
        "mask unbalanced": {"mask": f'"{values}" > '},
    }.items():
        call = dict(imagename=str(values), **call)
        record_case(result["values"], name, **call)

    # CASA's logfile semantics: the statistics block is written whatever
    # `listit` holds, appended or overwritten as `append` selects.
    log = root / "stats.log"
    if log.exists():
        log.unlink()
    imstat(imagename=str(values), logfile=str(log), append=False, listit=False)
    first = log.read_text()
    imstat(imagename=str(values), logfile=str(log), append=True, listit=False)
    appended = log.read_text()
    imstat(imagename=str(values), logfile=str(log), append=False, listit=False)
    overwritten = log.read_text()
    result["logfile"] = {
        "listit false writes": True,
        "content head": first[:400],
        "first length": len(first),
        "appended length": len(appended),
        "overwritten length": len(overwritten),
    }
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

    stokes4 = root / "stokes4.im"
    make_stokes_image(stokes4)
    for name, call in (
        ("classic", {}),
        ("stokes I", {"stokes": "I"}),
        ("stokes IQUV", {"stokes": "IQUV"}),
        ("stokes QU", {"stokes": "QU"}),
        ("stokes reverse IU", {"stokes": "IU"}),
        ("stokes comma Q,U", {"stokes": "Q,U"}),
        ("stokes empty", {"stokes": ""}),
        ("stokes with axes", {"stokes": "QU", "axes": [0, 1]}),
        ("stokes with chans", {"stokes": "QU", "chans": "1"}),
        ("stokes numeric", {"stokes": "0"}),
        ("stokes numeric range", {"stokes": "0~2"}),
        ("stokes unknown", {"stokes": "Z"}),
        ("stokes out of range", {"stokes": "5"}),
    ):
        call = dict(imagename=str(stokes4), **call)
        record_case(result["stokes4"], name, **call)
    record_case(
        result["stokes4"],
        "stokes absent from image",
        imagename=str(values),
        stokes="Q",
    )

    # CASA's own imstat test suite (`casatasks/tests/tasks/
    # test_task_imstat.py`), as recorded cases on small fixtures: the
    # scenarios the parameter cases above did not already cover.
    multi = root / "multi.im"
    make_ramp_image(multi, [10, 10, 10])
    fourdim = root / "fourdim.im"
    make_ramp_image(fourdim, [2, 3, 4, 5])
    excl = root / "excl.im"
    make_ramp_image(excl, [10, 10, 110, 4])
    moment = root / "moment.im"
    make_moment_image(moment)
    linear = root / "linear_coords.fits"
    make_linear_fits(linear)
    result["upstream"] = {}
    for name, imagename, call in (
        # test_multiple_region_support: several boxes in one spec, with a
        # channel comparison in the same call.
        ("box double", str(multi), {"box": "0,0,2,2,4,4,6,6"}),
        ("box overlap", str(multi), {"box": "0,0,2,2,0,0,2,2"}),
        ("box triple", str(multi), {"box": "0,0,1,1,4,4,5,5,8,8,9,9"}),
        # test_box_param_can_have_spaces (CAS-2050).
        ("box spaces", str(multi), {"box": "0, 0,  1 ,   1"}),
        ("box double with chans", str(multi), {
            "box": "0,0,2,2,4,4,6,6",
            "chans": "0~4, 6, >8",
        }),
        ("box seven values", str(multi), {"box": "0,0,2,2,4,4,6"}),
        ("box inverted", str(multi), {"box": "5,5,2,2"}),
        ("box out of range", str(multi), {"box": "0,0,20,20"}),
        # The channel comparisons CASA's chans specification accepts.
        ("chans gt", str(multi), {"chans": ">8"}),
        ("chans lt", str(multi), {"chans": "<2"}),
        ("chans ge", str(multi), {"chans": ">=8"}),
        ("chans le", str(multi), {"chans": "<=2"}),
        ("chans both filters", str(multi), {"chans": ">8,<2"}),
        ("chans bad comparison", str(multi), {"chans": ">abc"}),
        ("chans float comparison", str(multi), {"chans": ">2.5"}),
        # test_specifying_axes_param's quartet, on a four-axis ramp.
        ("fourdim axes minus one", str(fourdim), {"axes": -1}),
        ("fourdim axes 0 1 2", str(fourdim), {"axes": [0, 1, 2]}),
        ("fourdim axes 0 1", str(fourdim), {"axes": [0, 1]}),
        ("fourdim axes 3", str(fourdim), {"axes": 3}),
        # test_moment_map_flux: a beamed 2-D moment map carries flux.
        ("moment map flux", str(moment), {}),
        # test_internal_region_exclusion: the ';' channel separator with
        # axes and a Stokes pair, as the upstream call holds them.
        (
            "exclusion chans semicolon",
            str(excl),
            {"axes": [0, 1], "chans": "10~20;60~90", "stokes": "IV"},
        ),
        # test_CAS_2195: imstat on a FITS image whose coordinates are
        # linear, not a direction plane.
        ("linear coordinates", str(linear), {}),
    ):
        record_case(result["upstream"], name, imagename=imagename, **call)

    (root / "expected.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    for lock in root.rglob("table.lock"):
        lock.unlink()


if __name__ == "__main__":
    main()
