# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 primary-beam correction for task-level comparisons."""

import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
from casatasks import exportfits, impbcor, version_string
from casatools import image, regionmanager

INPUTS = ("cube.im", "beam.im", "plane.im", "shifted.im", "singleton.im", "nearplane.im", "stripe.im",
          "circle.crtf", "cube.fits", "plane.fits")
INPUT_SHAPE = (4, 3, 2, 3)
PLANE = np.array([[0.0, 0.25, 0.5], [0.75, 1.0, -0.25], [0.4, 0.6, 0.8], [0.5, 0.9, 1.0]], dtype=np.float32)
BEAM = np.broadcast_to(PLANE[:, :, None, None], INPUT_SHAPE).copy() + np.arange(3, dtype=np.float32) * 0.125
CASES = {
    "full": {},
    "divide_cutoff": {"cutoff": 0.5},
    "nan_cutoff": {"cutoff": float("nan")},
    "multiply_cutoff": {"mode": "m", "cutoff": 0.5},
    "full_array": {"pbimage": BEAM},
    "nested_float_list": {"pbimage": PLANE.tolist()},
    "plane_image": {"pbimage": "plane.im", "cutoff": 0.1},
    "plane_array": {"pbimage": PLANE, "cutoff": 0.1},
    "selected_plane_array": {"pbimage": PLANE, "box": "1,0,3,2", "chans": "1~2", "stokes": "Q"},
    "singleton_image": {"pbimage": "singleton.im", "cutoff": 0.1},
    "singleton_direction_axis": {"pbimage": "stripe.im", "box": "1,0,3,2", "cutoff": 0.1},
    "near_plane": {"pbimage": "nearplane.im", "cutoff": 0.1},
    "near_full": {"imagename": "plane.im", "pbimage": "nearplane.im", "cutoff": 0.1},
    "selection": {"box": "1,0,3,2", "chans": "1~2", "stokes": "Q"},
    "gapped_channels": {"box": "1,0,3,2", "chans": "0,2", "stokes": "Q"},
    "multiple_boxes": {"box": "0,0,1,1,2,1,3,2"},
    "region_and_box": {"box": "1,0,3,2", "region": "circle[[1pix,1pix],1pix]"},
    "region_and_channels": {"region": "circle[[1pix,1pix],1pix]", "chans": "1~2"},
    "stokes_only": {"stokes": "Q"},
    "all_selectors": {"chans": "all", "stokes": "ALL"},
    "stokes_bracketed": {"stokes": "[I;Q]"},
    "selectors_without_axes": {"imagename": "plane.im", "pbimage": "plane.im", "chans": "1", "stokes": "Q"},
    "decimal_box": {"box": "1.3,0.6,2.75,2.0"},
    "region_text": {"region": "circle[[1pix,1pix],1pix]"},
    "region_file": {"region": "circle.crtf"},
    "bare_saved_region": {"region": "savedCircle"},
    "lel": {"mask": 'sqrt("cube.im") > 2 && "cube.im" < 45'},
    "frequency_beam_as_mask": {"mask": '"beam.im" > 0.21'},
    "stretch": {"mask": '"plane.im" > 0.3', "stretch": True},
    "stretch_off": {"mask": '"plane.im" > 0.3'},
    "shifted_beam": {"pbimage": "shifted.im"},
    "fits_input": {"imagename": "cube.fits", "pbimage": "plane.fits", "cutoff": 0.1},
    "fits_beam": {"pbimage": "plane.fits", "cutoff": 0.1},
    "wrong_array_shape": {"pbimage": np.ones((2, 2))},
    "integer_array": {"pbimage": np.ones((4, 3), dtype=np.int32)},
    "flat_array": {"pbimage": [1.0, 0.5]},
    "invalid_mode": {"mode": "unrecognized"},
}


def make_image(name, data, *, mask=None, increment=None, changed_frequency=False):
    tool = image()
    try:
        assert tool.fromarray(name, data)
        assert tool.sethistory("fixture", f"input history for {name}")
        if name == "cube.im":
            assert tool.setbrightnessunit("Jy/beam")
            assert tool.setrestoringbeam("5arcsec", "4arcsec", "12deg", log=False)
        if increment is not None or changed_frequency:
            cs = tool.coordsys()
            if increment is not None:
                increments = cs.increment(type="direction")["numeric"]
                increments[0] *= increment
                assert cs.setincrement(increments, type="direction")
            if changed_frequency:
                reference = cs.referencevalue(type="spectral")["numeric"]
                assert cs.setreferencevalue(reference + 2e6, type="spectral")
            assert tool.setcoordsys(cs.torecord())
            cs.done()
        if mask is not None:
            assert tool.putregion(pixelmask=mask)
    finally:
        tool.done()


def make_inputs():
    src = np.arange(1, np.prod(INPUT_SHAPE) + 1, dtype=np.float32).reshape(INPUT_SHAPE)
    make_image("cube.im", src, mask=src != 12)
    make_image("beam.im", BEAM, mask=BEAM != 0.4, changed_frequency=True)
    make_image("plane.im", PLANE)
    make_image("singleton.im", PLANE[:, :, None, None])
    make_image("shifted.im", BEAM, increment=2)
    make_image("nearplane.im", PLANE, increment=1 + 1e-7)
    rg = regionmanager()
    ia = image()
    assert ia.open("plane.im")
    stripe = ia.subimage("stripe.im", region=rg.box([0, 0], [0, 2]), dropdeg=False)
    stripe.done()
    ia.done()
    assert ia.open("cube.im")
    cs = ia.coordsys()
    circle = rg.fromtext("circle[[1pix,1pix],1pix]", ia.shape(), cs.torecord())
    cs.done()
    ia.done()
    assert rg.fromrecordtotable("cube.im", "savedCircle", circle)
    rg.done()
    Path("circle.crtf").write_text("#CRTFv0\ncircle[[1pix,1pix],1pix]\n")
    exportfits("cube.im", "cube.fits")
    exportfits("plane.im", "plane.fits")


def snapshot(path):
    tool = image()
    assert tool.open(str(path))
    try:
        coords = tool.coordsys()
        shape = tool.shape().tolist()
        history = tool.history()
        names = coords.names()
        result = {
            "shape": shape,
            "names": names,
            "stokes": coords.stokes() if "Stokes" in names else [],
            "reference_pixel": coords.referencepixel()["numeric"].tolist(),
            "reference_value": coords.referencevalue()["numeric"].tolist(),
            "units": tool.brightnessunit(),
            "beam": tool.restoringbeam(),
            "pixels": [float(x) if np.isfinite(x) else None for x in tool.getchunk().ravel(order="F")],
            "mask": tool.getchunk(getmask=True).ravel(order="F").tolist(),
            "default_mask": tool.maskhandler("default"),
            "history": [line for line in history if line.startswith("input history for ")],
            "task_history": any(line.startswith("impbcor(") for line in history),
        }
        coords.done()
        return result
    finally:
        tool.done()


def plain(value):
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (np.ndarray, np.generic)):
        return value.tolist()
    if isinstance(value, float) and np.isnan(value):
        return "nan"
    return value


def run_cases():
    rg = regionmanager()
    region = rg.box([1, 0, 0, 1], [2, 2, 1, 2])
    tool = image()
    assert tool.open("cube.im")
    coords = tool.coordsys()
    circle = rg.fromtext("circle[[1pix,1pix],1pix]", tool.shape(), coords.torecord())
    ellipse = rg.fromtext("ellipse[[1pix,1pix],[1.5pix,0.75pix],15deg]", tool.shape(), coords.torecord())
    coords.done()
    tool.done()
    rg.done()
    cases = {}
    records = {"region_record": region, "circle_record": circle, "ellipse_record": ellipse}
    for name, kwargs in {**CASES, **{key: {"region": value} for key, value in records.items()}}.items():
        params = {"imagename": "cube.im", "pbimage": "beam.im", **kwargs}
        entry = {"params": plain(params)}
        try:
            assert impbcor(**params, outfile=f"{name}.out") is None
        except (AssertionError, RuntimeError, ValueError, TypeError) as error:
            entry["error"] = type(error).__name__
            entry["message"] = str(error).splitlines()[0]
        else:
            entry["product"] = snapshot(f"{name}.out")
        cases[name] = entry
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    destination = parser.parse_args().destination.resolve()
    assert version_string() == "6.7.6.14"
    destination.mkdir(parents=True, exist_ok=True)
    original = Path.cwd()
    with tempfile.TemporaryDirectory() as temporary:
        os.chdir(temporary)
        try:
            make_inputs()
            cases = run_cases()
            for name in INPUTS:
                source = Path(name)
                if source.is_dir():
                    shutil.copytree(source, destination / name, dirs_exist_ok=True,
                                    ignore=shutil.ignore_patterns("table.lock"))
                    for info in (destination / name).rglob("table.info"):
                        info.write_text("\n".join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + "\n")
                else:
                    shutil.copyfile(source, destination / name)
            (destination / "expected.json").write_text(json.dumps(
                {"casa_version": version_string(), "cases": cases}, indent=2, allow_nan=False,
            ) + "\n")
        finally:
            os.chdir(original)


if __name__ == "__main__":
    main()
