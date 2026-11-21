# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 regrid products with shapes, world axes, masks and pixels."""

import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
from casatasks import imregrid, version_string
from casatools import coordsys, image

INPUTS = ("cube.im", "template.im", "single.im", "masked.im", "transposed.im", "cube_pol.im", "template_pol.im", "double.im",
          "complex.im", "dcomplex.im", "complex_sfl.im", "linear.im", "linear_template.im")
CASES = {
    "frequency": {},
    "velocity": {"asvelocity": True},
    "direction_only": {"axes": [0, 1]},
    "one_direction_axis": {"axes": [0]},
    "frequency_only": {"axes": [3]},
    "velocity_only": {"asvelocity": True, "axes": [3]},
    "explicit_shape": {"axes": [0, 1], "shape": [10, 9, 1, 5]},
    "empty_shape": {"axes": [0, 1], "shape": []},
    "wrong_shape": {"axes": [0, 1], "shape": [10, 9, 1, 4]},
    "unknown_axis": {"axes": [4]},
    "replicate": {"replicate": True},
    "nearest": {"interpolation": "nearest"},
    "cubic": {"interpolation": "cubic"},
    "decimated": {"decimate": 3},
    "oversized_decimation": {"axes": [0, 1], "decimate": 8},
    "singleton": {"imagename": "single.im", "axes": [3], "shape": [14, 12, 1, 4]},
    "mask": {"imagename": "masked.im", "interpolation": "cubic"},
    "transposed_template": {"template": "transposed.im"},
    "common_stokes": {"imagename": "cube_pol.im", "template": "template_pol.im"},
    "same_frame": {"template": "J2000"},
    "galactic_frame": {"template": "GALACTIC", "axes": [3], "shape": [1, 1, 1, 1]},
    "identity_spectral_cubic": {"imagename": "masked.im", "template": "masked.im", "axes": [3], "interpolation": "cubic"},
    "identity_decimation_refused": {"template": "cube.im", "axes": [0, 1], "decimate": 100},
    "complex_cubic": {"imagename": "complex.im", "interpolation": "cubic"},
    "complex_frequency": {"imagename": "complex.im", "axes": [3]},
    "complex_same_frame": {"imagename": "complex.im", "template": "J2000"},
    "complex_same_frame_existing": {"imagename": "complex.im", "template": "J2000", "overwrite": True},
    "complex_frame_refused": {"imagename": "complex.im", "template": "GALACTIC"},
    "complex_sfl_frame": {"imagename": "complex_sfl.im", "template": "GALACTIC"},
    "double_frame_refused": {"imagename": "double.im", "template": "GALACTIC"},
    "dcomplex_frequency": {"imagename": "dcomplex.im", "axes": [3], "interpolation": "cubic"},
    "dcomplex_spatial": {"imagename": "dcomplex.im", "axes": [0, 1]},
    "linear_plane": {"imagename": "linear.im", "template": "linear_template.im", "axes": [0, 1]},
    "one_linear_axis": {"imagename": "linear.im", "template": "linear_template.im", "axes": [0]},
    "replicate_linear_plane": {"imagename": "linear.im", "template": "linear_template.im", "axes": [0, 1], "replicate": True},
}


def cube(path, shape, frequency, step, rest, shifted=False, masked=False, polarizations=None):
    x, y, stokes, channel = np.indices(shape, dtype=np.float32)
    pixels = x**2 + 0.1 * y**2 + np.sin(x * y) + stokes + 20 * channel**2
    ia = image()
    try:
        assert ia.fromarray(path, pixels)
        assert ia.setbrightnessunit("Jy/beam")
        assert ia.setrestoringbeam("4arcsec", "3arcsec", "12deg", log=False)
        cs = ia.coordsys()
        try:
            assert cs.setrestfrequency(f"{rest}Hz")
            assert cs.setreferencevalue([frequency], type="spectral")
            assert cs.setincrement([step], type="spectral")
            assert cs.setreferencepixel([0.0], type="spectral")
            assert cs.setunits(["rad", "rad", "", "Hz"])
            assert cs.setincrement([-0.01, 0.01], type="direction")
            if polarizations:
                assert cs.setstokes(polarizations)
            if shifted:
                assert cs.setreferencevalue([0.012, 0.018], type="direction")
                assert cs.setincrement([-0.011, 0.009], type="direction")
            assert ia.setcoordsys(cs.torecord())
        finally:
            cs.done()
        if masked:
            assert ia.calcmask(f'"{path}" > 6 && "{path}" < 340')
        if path == "template.im":
            transposed = ia.transpose("transposed.im", "3012")
            transposed.done()
    finally:
        ia.done()


def plain(value):
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value.tolist() if isinstance(value, (np.ndarray, np.generic)) else value


def make_double():
    tool = image()
    assert tool.open("cube.im")
    coords = tool.coordsys()
    record = coords.torecord()
    shape = tool.shape()
    coords.done()
    tool.done()
    try:
        assert tool.fromshape("double.im", shape, csys=record, type="d")
        assert tool.putchunk(np.full(shape, 1e100, dtype=np.float64))
    finally:
        tool.done()


def make_complex():
    tool = image()
    assert tool.open("cube.im")
    cs = tool.coordsys()
    record = cs.torecord()
    shape = tool.shape()
    cs.done()
    tool.done()
    x, y, _, channel = np.indices(shape, dtype=np.float64)
    real = x**2 + 0.1 * y**2 + np.sin(x * y) + 20 * channel**2 + 1
    imag = 1 + 0.21 * x * y**2 + np.cos(x + channel * y) - 3 * channel**2
    for name, dtype, precision, scale in [
        ("complex.im", np.complex64, "f", 1.0),
        ("dcomplex.im", np.complex128, "d", 1e100),
    ]:
        try:
            pixels = ((real + 1j * imag) * scale).astype(dtype)
            assert tool.fromarray(name, pixels, csys=record, type=precision)
            if precision == "f":
                assert tool.setbrightnessunit("Jy/beam")
                assert tool.setrestoringbeam("4arcsec", "3arcsec", "12deg", log=False)
                assert tool.putregion(pixelmask=(x + 2 * y + channel) % 23 != 0)
        finally:
            tool.done()
    cs = coordsys()
    try:
        assert cs.fromrecord(record)
        assert cs.setprojection("SFL")
        pixels = (real + 1j * imag).astype(np.complex64)
        assert tool.fromarray("complex_sfl.im", pixels, csys=cs.torecord())
    finally:
        cs.done()
        tool.done()


def snapshot(path):
    ia = image()
    assert ia.open(path)
    try:
        cs = ia.coordsys()
        try:
            shape = ia.shape().tolist()
            beam = ia.restoringbeam()
            pixels = ia.getchunk().ravel(order="F")
            result = {
                "shape": shape,
                "names": cs.names(),
                "units": cs.units(),
                "direction_frame": cs.referencecode("direction")[0] if "Direction" in cs.coordinatetype() else None,
                "reference_pixel": plain(cs.referencepixel()["numeric"]),
                "reference_value": plain(cs.referencevalue()["numeric"]),
                "increment": plain(cs.increment()["numeric"]),
                "pixel0_world": plain(cs.toworld([0] * len(shape))["numeric"]),
                "brightness": ia.brightnessunit(),
                "mask": ia.getchunk(getmask=True).ravel(order="F").tolist(),
                "pixels": pixels.tolist(),
                "beam": {key: [item["value"], item["unit"]] for key, item in beam.items()},
            }
            if np.iscomplexobj(pixels):
                result["pixels"] = np.column_stack((pixels.real, pixels.imag)).tolist()
                result["pixel_type"] = ia.pixeltype()
            return result
        finally:
            cs.done()
    finally:
        ia.done()


def make_linear():
    for name, shape, units, increments, reference in [
        ("linear.im", (7, 6), ["m", "s"], [1.0, 2.0], [0.0, 0.0]),
        ("linear_template.im", (4, 4), ["km", "ms"], [0.00125, 1500.0], [0.00025, 1000.0]),
    ]:
        cs = coordsys().newcoordsys(linear=2)
        ia = image()
        try:
            assert cs.setunits(units, type="linear", overwrite=True)
            assert cs.setincrement(increments, type="linear")
            assert cs.setreferencevalue(reference, type="linear")
            if name == "linear_template.im":
                assert cs.setlineartransform(type="linear", value=[[1.0, 0.125], [-0.1, 1.0]])
            x, y = np.indices(shape, dtype=np.float32)
            assert ia.fromarray(name, x + 10 * y, csys=cs.torecord())
            assert ia.setbrightnessunit("Jy/pixel")
            if name == "linear.im":
                assert ia.putregion(pixelmask=(x + y != 7))
        finally:
            cs.done()
            ia.done()


def run_cases():
    cube("cube.im", (14, 12, 1, 5), 1.4e9, 2e6, 1.42e9)
    cube("template.im", (12, 10, 1, 4), 1.401e9, 2.3e6, 1.421e9, shifted=True)
    cube("single.im", (14, 12, 1, 1), 1.4e9, 2e6, 1.42e9)
    cube("masked.im", (14, 12, 1, 5), 1.4e9, 2e6, 1.42e9, masked=True)
    cube("cube_pol.im", (14, 12, 3, 5), 1.4e9, 2e6, 1.42e9, polarizations=["I", "Q", "U"])
    cube("template_pol.im", (12, 10, 2, 4), 1.401e9, 2.3e6, 1.421e9, shifted=True, polarizations=["Q", "U"])
    make_double()
    make_complex()
    make_linear()
    dictionary = imregrid(imagename="template.im", template="get")
    reference = imregrid(imagename="cube.im", template="get")
    def edit(direction=None, spectral=None):
        import copy
        record = copy.deepcopy(reference)
        if direction:
            record["csys"]["direction0"].update(direction)
        if spectral:
            for field, value in spectral.items():
                if field == "wcs":
                    record["csys"]["spectral2"][field].update(value)
                else:
                    record["csys"]["spectral2"][field] = value
        return record

    extra = {
        "dictionary": {"template": dictionary, "shape": [3, 4]},
        "b1950_same_numerical_grid": {"template": edit(direction={"system": "B1950", "conversionSystem": "B1950"}), "axes": [0, 1]},
        "optical_velocity": {"template": edit(spectral={"velType": 1, "wcs": {"crval": 1.402e9}}), "axes": [3], "asvelocity": True},
        "disjoint_replicated_spectrum": {"template": edit(spectral={"wcs": {"crval": 2e9}}), "axes": [3], "replicate": True},
        "double_spectrum": {"imagename": "double.im", "template": edit(spectral={"wcs": {"crpix": 0.25}}), "axes": [3]},
        "complex_dictionary": {"imagename": "complex.im", "template": dictionary, "interpolation": "nearest"},
    }
    cases = {}
    for name, options in {**CASES, **extra}.items():
        args = {"imagename": "cube.im", "template": "template.im", "asvelocity": False, "decimate": 0, **options}
        case = {"params": plain(args)}
        product = f"{name}.out"
        if name == "complex_same_frame_existing":
            case["existing"] = "template.im"
            shutil.copytree(case["existing"], product)
        try:
            assert imregrid(**args, output=product) is None
        except Exception as error:
            if not isinstance(error, (AssertionError, RuntimeError, ValueError, TypeError)):
                if name != "complex_sfl_frame" or "a projection of SFL" not in str(error):
                    raise
            case["error"] = str(error).splitlines()[0].replace(f"{Path.cwd()}/", "")
            if "existing" in case:
                assert snapshot(product) == snapshot(case["existing"])
        else:
            case["product"] = snapshot(product)
            if name == "singleton":
                original = snapshot("single.im")
                case["preserved_input_metadata"] = {key: original[key] for key in ("brightness", "beam")}
        cases[name] = case
    return {"casa_version": version_string(), "template": plain(dictionary), "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    destination = parser.parse_args().destination.resolve()
    assert version_string() == "6.7.6.14"
    destination.mkdir(parents=True, exist_ok=True)
    original = Path.cwd()
    with tempfile.TemporaryDirectory() as workspace:
        os.chdir(workspace)
        try:
            evidence = run_cases()
            for name in INPUTS:
                shutil.copytree(name, destination / name, dirs_exist_ok=True,
                                ignore=shutil.ignore_patterns("table.lock"))
                for path in (destination / name).rglob("table.info"):
                    path.write_text("\n".join(line.rstrip() for line in path.read_text().splitlines()).rstrip() + "\n")
            with (destination / "expected.json").open("w") as file:
                file.write(f'{{"casa_version":{json.dumps(evidence["casa_version"])},\n')
                file.write(f' "template":{json.dumps(evidence["template"], allow_nan=False)},\n "cases":{{\n')
                file.write(",\n".join(f"  {json.dumps(name)}:{json.dumps(case, allow_nan=False)}"
                                     for name, case in evidence["cases"].items()))
                file.write("\n }}\n")
        finally:
            os.chdir(original)


if __name__ == "__main__":
    main()
