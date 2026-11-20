# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record immutable inputs and reference products for CASA 6.7.6.14 immoments."""

import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
from casatasks import immoments, version_string
from casatools import image

CASES = {
    "compound": ("cube.im", {"moments": [-1], "mask": '"cube.im" > 4 && "cube.im" < 30'}),
    "math_mask": ("cube.im", {"moments": [-1], "mask": 'sqrt("cube.im") >= 4 && "cube.im" < 36'}),
    "stretch": ("cube.im", {"moments": [-1], "mask": '"plane.im" > 0.5', "stretch": True}),
    "mixed_masks": ("cube.im", {
        "moments": [-1], "mask": '"cube.im" > 4 && "plane.im" > 0.5', "stretch": True,
    }),
    "stretch_off": ("cube.im", {"moments": [-1], "mask": '"plane.im" > 0.5'}),
    "subset": ("cube.im", {"moments": [0], "box": "1,0,2,1", "chans": "1~4", "stokes": "Q"}),
    "crtf": ("cube.im", {"moments": [-1], "region": "circle[[1pix,1pix],1pix]"}),
    "region_file": ("cube.im", {"moments": [-1], "region": "circle.crtf"}),
    "gaps": ("cube.im", {"moments": [0], "chans": "0,2,4", "stokes": "IV"}),
    "spatial": ("cube.im", {"moments": [0], "axis": "Right Ascension"}),
    "spatial_maxcoord": ("cube.im", {"moments": [9], "axis": 0}),
    "stokes_axis": ("cube.im", {"moments": [0], "axis": "Stokes"}),
    "one_channel": ("cube.im", {"moments": [0], "chans": "1"}),
    "spectral_axis_minus_one": ("cube.im", {"moments": [0], "axis": -1}),
    "spectral_axis_minus_two": ("cube.im", {"moments": [0], "axis": -2}),
    "auto_axis_one_channel": ("cube.im", {"moments": [0], "axis": -1, "chans": "1"}),
    "convolved_spectral": ("beams.im", {"moments": [0], "axis": "spectral"}),
    "beams_direction": ("beams.im", {"moments": [0], "axis": 0}),
    "beams_stokes": ("polbeams.im", {"moments": [0], "axis": "stokes"}),
    "beams_rectangular_pixels": ("polbeams.im", {"moments": [0], "axis": "spectral"}),
    "pixel_beams_target_last": ("pixelbeams.im", {"moments": [0], "chans": "0~1"}),
    "pixel_beams_target_first": ("pixelbeams.im", {"moments": [0], "chans": "2~3"}),
    "pixel_beams_singular": ("pixelbeams.im", {"moments": [0], "chans": "4~5"}),
    "tabular_subset": ("tabular.im", {"moments": [0], "chans": "2~4"}),
    "median_100": ("tabular.im", {"moments": [3], "includepix": [0, 99]}),
    "median_101": ("tabular.im", {"moments": [3], "includepix": [0, 100]}),
    "median_102": ("tabular.im", {"moments": [3]}),
    "spectral_alias": ("cube.im", {"moments": [0], "axis": "spec"}),
    "median_needs_range": ("cube.im", {"moments": [4]}),
    "all_positive": ("cube.im", {"moments": list(range(-1, 12)), "includepix": [1, 120]}),
    "excluded_endpoints": ("cube.im", {"moments": [-1], "excludepix": [1, 3]}),
    "both_limits": ("cube.im", {"moments": [-1], "includepix": [0, 20], "excludepix": [1, 3]}),
    "repeat_moment": ("cube.im", {"moments": [0, 0]}),
    "invalid_axis": ("cube.im", {"moments": [0], "axis": 9}),
    "no_spectral": ("plane.im", {"moments": [0]}),
}
DEFAULT_INPUTS = (
    "cube.im", "plane.im", "beams.im", "polbeams.im", "pixelbeams.im", "tabular.im", "circle.crtf",
)


def make_inputs():
    for name, pixels in (
        ("cube.im", np.arange(120, dtype=np.float32).reshape(3, 2, 4, 5)),
        ("plane.im", np.array([[1, 0], [0, 1], [1, 0]], dtype=np.float32)),
    ):
        tool = image()
        try:
            assert tool.fromarray(name, pixels)
            assert tool.setbrightnessunit("Jy/pixel")
            assert tool.sethistory("fixture", "CASA immoments oracle input")
        finally:
            tool.done()
    maker = image()
    cube = np.zeros((13, 13, 1, 3), dtype=np.float32)
    cube[6, 6, 0, :] = [1, 2, 1]
    try:
        assert maker.fromarray("beams.im", cube)
        assert maker.setbrightnessunit("Jy/beam")
        assert maker.sethistory("fixture", "CASA immoments oracle input")
        for channel, (major, minor) in enumerate([(2.0, 1.5), (3.6, 2.8), (2.6, 1.8)]):
            assert maker.setrestoringbeam(
                f"{major}arcmin", f"{minor}arcmin", "0deg",
                channel=channel, polarization=0, log=False,
            )
    finally:
        maker.done()
    try:
        assert maker.fromarray("polbeams.im", np.ones((2, 2, 2, 2), dtype=np.float32))
        assert maker.setbrightnessunit("Jy/beam")
        coords = maker.coordsys()
        increments = coords.increment(type="direction")["numeric"]
        increments[1] *= 2
        assert coords.setincrement(increments, type="direction")
        assert maker.setcoordsys(coords.torecord())
        coords.done()
        for channel in range(2):
            for pol, width in enumerate([2.0, 3.0]):
                assert maker.setrestoringbeam(
                    f"{width}arcmin", "1.5arcmin", "0deg", channel=channel, polarization=pol, log=False,
                )
        assert maker.sethistory("fixture", "CASA immoments oracle input")
    finally:
        maker.done()
    try:
        pixels = np.zeros((5, 5, 1, 6), dtype=np.float32)
        pixels[2, 2, 0, :] = 1
        assert maker.fromarray("pixelbeams.im", pixels)
        assert maker.setbrightnessunit("Jy/pixel")
        beams = [(2.0, 1.5), (3.6, 2.8), (3.6, 2.8), (2.0, 1.5), (2.0, 1.5), (3.0, 1.5)]
        for channel, (major, minor) in enumerate(beams):
            assert maker.setrestoringbeam(
                f"{major}arcmin", f"{minor}arcmin", "0deg", channel=channel, polarization=0, log=False,
            )
        assert maker.sethistory("fixture", "CASA immoments oracle input")
    finally:
        maker.done()
    try:
        assert maker.fromarray("tabular.im", np.arange(102, dtype=np.float32).reshape(1, 1, 1, 102))
        assert maker.setbrightnessunit("Jy/pixel")
        coords = maker.coordsys()
        offsets = [0, 1, 3, 6, 10] + list(range(14, 402, 4))
        assert len(offsets) == 102
        assert coords.setspectral(frequencies={"value": [1.4e9 + x * 1e6 for x in offsets], "unit": "Hz"})
        assert maker.setcoordsys(coords.torecord())
        coords.done()
        assert maker.sethistory("fixture", "CASA immoments oracle input")
    finally:
        maker.done()
    Path("circle.crtf").write_text("#CRTFv0\ncircle[[1pix,1pix],1pix]\n")


def snapshot(path):
    tool = image()
    assert tool.open(str(path))
    try:
        cs = tool.coordsys()
        shape = tool.shape().tolist()
        return {
            "shape": shape,
            "names": cs.names(),
            "units": tool.brightnessunit(),
            "reference_pixel": cs.referencepixel()["numeric"].tolist(),
            "reference_value": cs.referencevalue()["numeric"].tolist(),
            "first_world": tool.toworld([0] * len(shape))["numeric"].tolist(),
            "pixels": [
                float(value) if np.isfinite(value) else None
                for value in tool.getchunk().ravel(order="F")
            ],
            "mask": tool.getchunk(getmask=True).ravel(order="F").tolist(),
            "default_mask": tool.maskhandler("default"),
            "beam": tool.restoringbeam(),
            "input_history": "CASA immoments oracle input" in tool.history(),
            "task_history": any(line.startswith("immoments(") for line in tool.history()),
        }
    finally:
        tool.done()


def run(name, source, params):
    stem = name + ".out"
    case = {"imagename": source, "params": params}
    try:
        value = immoments(source, outfile=stem, **params)
    except (RuntimeError, TypeError, ValueError, AssertionError) as error:
        case["error"] = type(error).__name__
        case["message"] = str(error).splitlines()[0].split("\t")[-1]
    else:
        assert value is None
        case["products"] = {
            path.name.removeprefix(stem): snapshot(path)
            for path in sorted(Path.cwd().glob(stem + "*"))
        }
        assert case["products"], name
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    destination = parser.parse_args().destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    assert version_string() == "6.7.6.14", version_string()
    original = Path.cwd()
    with tempfile.TemporaryDirectory() as scratch:
        os.chdir(scratch)
        try:
            make_inputs()
            cases = {name: run(name, source, args) for name, (source, args) in CASES.items()}
            for name in DEFAULT_INPUTS:
                if Path(name).is_dir():
                    shutil.copytree(name, destination / name, dirs_exist_ok=True,
                                    ignore=shutil.ignore_patterns("table.lock"))
                    for info in (destination / name).rglob("table.info"):
                        lines = [line.rstrip() for line in info.read_text().splitlines()]
                        info.write_text("\n".join(lines).rstrip() + "\n")
                else:
                    shutil.copy(name, destination)
            (destination / "expected.json").write_text(
                json.dumps({"casa_version": version_string(), "cases": cases},
                           indent=2, allow_nan=False) + "\n"
            )
        finally:
            os.chdir(original)


if __name__ == "__main__":
    main()
