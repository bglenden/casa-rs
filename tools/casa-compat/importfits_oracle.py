# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record small importfits products with CASA 6.7.6.14.

Run with the pinned CASA Python and pass the destination fixture directory.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
from astropy.io import fits
from casatasks import importfits, version_string
from casatools import image

RA = ("RA---SIN", "deg", 110.0, -0.01)
DEC = ("DEC--SIN", "deg", 42.0, 0.01)
FREQUENCY = ("FREQ", "Hz", 1.42e9, 2e6)
STOKES = ("STOKES", "", 1.0, 1.0)
DEFAULT_AXES = {"defaultaxes": True, "defaultaxesvalues": ["", "", "88.5GHz", "Q"]}
REORDER = {"defaultaxes": True, "defaultaxesvalues": ["", "", "", ""]}
CASES = {
    "zero_blanks": ("plane.fits", {}),
    "nan_blanks": ("plane.fits", {"zeroblanks": False}),
    "alternate_wcs": ("plane.fits", {"whichrep": 1}),
    "selected_hdu": ("extensions.fits", {"whichhdu": 1}),
    "first_valid_hdu": ("empty_primary.fits", {}),
    "default_stokes_and_spectral": ("plane.fits", DEFAULT_AXES),
    "default_direction_and_stokes": (
        "spectrum.fits",
        {"defaultaxes": True, "defaultaxesvalues": ["13.5h", "-2.5deg", "", "I"]},
    ),
    "numeric_direction": (
        "spectrum.fits",
        {"defaultaxes": True, "defaultaxesvalues": [202.5, -2.5, 0, "I"]},
    ),
    "round_direction_strings": (
        "spectrum.fits",
        {"defaultaxes": True, "defaultaxesvalues": ["12h34m56.78901s", "-2.50001234deg", "", "I"]},
    ),
    "zero_frequency": ("plane.fits", {"defaultaxes": True, "defaultaxesvalues": ["", "", "0Hz", "I"]}),
    "zero_frequency_float": ("plane.fits", {"defaultaxes": True, "defaultaxesvalues": ["", "", 0.0, "I"]}),
    "integer_frequency": ("plane.fits", {"defaultaxes": True, "defaultaxesvalues": ["", "", 100, "I"]}),
    "negative_frequency": ("plane.fits", {"defaultaxes": True, "defaultaxesvalues": ["", "", "-100Hz", "I"]}),
    "reorder_axes_and_mask": ("permuted.fits", REORDER),
    "extension_default_axes": ("extensions.fits", {"whichhdu": 1, **DEFAULT_AXES}),
    "inactive_axis_values": ("plane.fits", {"defaultaxesvalues": ["ignored"]}),
    "blank_stokes": (
        "plane.fits",
        {"defaultaxes": True, "defaultaxesvalues": ["", "", 88.5e9, ""]},
    ),
    "undefined_stokes": (
        "plane.fits",
        {"defaultaxes": True, "defaultaxesvalues": ["", "", "88.5GHz", "unknown"]},
    ),
    "beam_override": ("plane.fits", {"beam": ["11arcsec", "7arcsec", "35deg"]}),
    "per_plane_beams": ("cube.fits", {}),
    "per_plane_override": ("cube.fits", {"beam": ["11arcsec", "7arcsec", "35deg"]}),
    "double_fits": ("double.fits", {"zeroblanks": False}),
    "nonstandard_direction": ("galactic.fits", DEFAULT_AXES),
    "axis_count": ("plane.fits", {"defaultaxes": True, "defaultaxesvalues": ["", "", "1GHz"]}),
    "time_beam_angle": ("plane.fits", {"beam": ["5arcsec", "2arcsec", "1h"]}),
    "time_beam_bad_order": ("plane.fits", {"beam": ["2arcsec", "5arcsec", "1h"]}),
    "invalid_beam_order": ("plane.fits", {"beam": ["7arcsec", "11arcsec", "35deg"]}),
    "nonquantity_axis_string": (
        "spectrum.fits",
        {"defaultaxes": True, "defaultaxesvalues": ["202.5", "-2.5", "", "I"]},
    ),
}


def wcs(axes, suffix=""):
    header = fits.Header()
    for index, (ctype, unit, ref, inc) in enumerate(axes, 1):
        for key, value in zip(
            ("CTYPE", "CUNIT", "CRVAL", "CDELT", "CRPIX"), (ctype, unit, ref, inc, 1.0)
        ):
            header[f"{key}{index}{suffix}"] = value
    return header


def make_fits():
    plane = np.array([[1, 2, 3], [4, -32768, 6]], dtype=np.int16)
    header = wcs([RA, DEC])
    header.update(wcs([("RA---TAN", "deg", 111, -0.02), ("DEC--TAN", "deg", 43, 0.02)], "A"))
    header.update(BLANK=-32768, BMAJ=0.04, BMIN=0.02, BPA=12.0, BUNIT="Jy/BEAM")
    header.add_history("Fixture origin")
    fits.PrimaryHDU(plane, header=header).writeto("plane.fits")
    data = np.arange(12, dtype=np.float32).reshape(2, 2, 3)
    extension = fits.ImageHDU(data, header=wcs([RA, DEC, FREQUENCY]))
    fits.HDUList([fits.PrimaryHDU(plane, header=header), extension]).writeto("extensions.fits")
    fits.HDUList([fits.PrimaryHDU(), extension]).writeto("empty_primary.fits")
    pixels = np.arange(12, dtype=np.float32).reshape(1, 3, 2, 2)
    pixels[0, 1, 1, 0] = np.nan
    fits.PrimaryHDU(pixels, header=wcs([STOKES, FREQUENCY, RA, DEC])).writeto("permuted.fits")
    fits.PrimaryHDU(np.arange(3, dtype=np.float32), header=wcs([FREQUENCY])).writeto("spectrum.fits")
    fits.PrimaryHDU(
        plane, header=wcs([("GLON-SIN", "deg", 18.0, -0.01), ("GLAT-SIN", "deg", 3.0, 0.01)])
    ).writeto("galactic.fits")
    fits.PrimaryHDU(
        np.array([[1.23456789, 2, 3], [4, np.nan, 6]], dtype=np.float64), header=wcs([RA, DEC])
    ).writeto("double.fits")

    tool = image()
    try:
        assert tool.fromarray("cube.im", np.arange(64, dtype=np.float32).reshape(4, 4, 2, 2))
        assert tool.setbrightnessunit("Jy/beam")
        assert tool.setrestoringbeam("6arcsec", "2arcsec", "16deg", channel=0, polarization=0, log=False)
        assert tool.setrestoringbeam("5arcsec", "3arcsec", "12deg", channel=1, polarization=1, log=False)
        assert tool.tofits("cube.fits", history=True)
    finally:
        tool.done()


def snapshot(path):
    tool = image()
    assert tool.open(str(path))
    try:
        coords = tool.coordsys()
        kinds = coords.axiscoordinatetypes()
        shape = tool.shape().tolist()
        pixels = tool.getchunk().ravel(order="F")
        return {
            "shape": shape,
            "pixel_type": tool.pixeltype(),
            "pixels": [None if np.isnan(pixel) else float(pixel) for pixel in pixels],
            "mask": tool.getchunk(getmask=True).ravel(order="F").tolist(),
            "unit": tool.brightnessunit(),
            "names": coords.names(),
            "units": coords.units(),
            "reference": coords.referencevalue()["numeric"].tolist(),
            "increment": coords.increment()["numeric"].tolist(),
            "last_world": tool.toworld([length - 1 for length in shape])["numeric"].tolist(),
            "rest_frequency": coords.restfrequency()["value"].tolist() if "Spectral" in kinds else None,
            "stokes": coords.stokes() if "Stokes" in kinds else [],
            "beam": tool.restoringbeam(),
            "fits_history": [line for line in tool.history() if line == "Fixture origin"],
            "task_history": any(line.startswith("importfits(") for line in tool.history()),
        }
    finally:
        tool.done()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    dest = parser.parse_args().destination.resolve()
    dest.mkdir(parents=True, exist_ok=True)
    assert version_string() == "6.7.6.14", version_string()
    start = Path.cwd()
    with tempfile.TemporaryDirectory() as temporary:
        os.chdir(temporary)
        try:
            make_fits()
            snapshots = {}
            for name, (source, params) in CASES.items():
                product = f"{name}.im"
                case = {"fitsimage": source, "params": params}
                try:
                    result = importfits(source, product, **params)
                except (RuntimeError, TypeError) as error:
                    case["created_on_error"] = Path(product).exists()
                    case["error"] = type(error).__name__
                    case["message"] = str(error).splitlines()[0].split("\t")[-1]
                else:
                    assert result is None
                    case.update(snapshot(product))
                snapshots[name] = case
            for source in Path.cwd().glob("*.fits"):
                shutil.copy(source, dest)
            (dest / "expected.json").write_text(
                json.dumps({"casa_version": version_string(), "cases": snapshots}, indent=2, allow_nan=False) + "\n"
            )
        finally:
            os.chdir(start)


if __name__ == "__main__":
    main()
