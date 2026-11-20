# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 position-velocity slices for native task comparisons."""

import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
from casatasks import impv, version_string
from casatools import image, measures, regionmanager


INPUTS = ("cube.im", "big.im", "mask.im", "transposed.image", "rectangular.im", "beams.im", "full.crtf")
CASES = {
    "horizontal": {},
    "reverse": {"start": [8, 6], "end": [3, 6]},
    "vertical": {"start": [5, 3], "end": [5, 8]},
    "diagonal": {"start": [3, 4], "end": [8, 7]},
    "long_diagonal": {"imagename": "big.im", "start": [6, 7], "end": [24, 21]},
    "long_vertical": {"imagename": "big.im", "start": [15, 5], "end": [15, 25]},
    "long_reverse": {"imagename": "big.im", "start": [24, 15], "end": [6, 15]},
    "fractional_diagonal": {"start": [3.25, 4.1], "end": [8.1, 7.2]},
    "fractional_horizontal": {"start": [3.25, 6.3], "end": [8.4, 6.3]},
    "width_average": {"width": 3},
    "width_even": {"width": 2},
    "width_angle": {"width": "2arcmin"},
    "degrees": {"unit": "deg"},
    "quantity_record_width": {"width": {"value": 2.0, "unit": "arcmin"}},
    "coords_prefix": {"mode": "c"},
    "length_pixels": {"mode": "length", "center": [5.5, 6], "length": 5, "pa": "90deg"},
    "length_angle": {"mode": "length", "center": [5.5, 6], "length": "5arcmin", "pa": "90deg"},
    "channels": {"chans": "1~2"},
    "stokes": {"stokes": "Q"},
    "gapped_channels": {"chans": "1;3"},
    "full_region": {"region": "full.crtf"},
    "spatial_region_refused": {"region": "box[[1pix,1pix],[10pix,10pix]]"},
    "stretch": {"mask": '"mask.im" > 0.5', "stretch": True, "width": 3},
    "mask_requires_stretch": {"mask": '"mask.im" > 0.5', "width": 3},
    "all_masked": {"mask": '"cube.im" < -1'},
    "missing_spectral": {"imagename": "mask.im"},
    "rectangular_pixels": {"imagename": "rectangular.im"},
    "selected_beams": {"imagename": "beams.im", "chans": "1~2", "stokes": "Q"},
    "edge_refused": {"start": [1, 6]},
    "transposed": {"imagename": "transposed.image", "chans": "1~2"},
}


def make_inputs():
    x, y, stokes, chan = np.indices((12, 12, 2, 5), dtype=np.float32)
    pixels = x**2 + 0.3 * y**2 + np.sin(x * y) + 2 * x + 20 * stokes + 100 * chan
    ia = image()
    try:
        assert ia.fromarray("cube.im", pixels)
        assert ia.setbrightnessunit("Jy/beam")
        assert ia.setrestoringbeam("5arcsec", "4arcsec", "12deg", log=False)
        assert ia.sethistory("fixture", "CASA impv nonlinear input")
        cs = ia.coordsys()
        assert cs.setreferencevalue([1.414e9], type="spectral")
        assert cs.setreferencepixel([0], type="spectral")
        assert cs.setincrement([-1e6], type="spectral")
        assert ia.setcoordsys(cs.torecord())
        cs.done()
        transposed = ia.transpose("transposed.image", "2013")
        transposed.done()
        padded = ia.pad("padded.im", npixels=16, padmask=False)
        padded.done()
    finally:
        ia.done()
    try:
        assert ia.fromarray("rectangular.im", pixels)
        cs = ia.coordsys()
        delta = cs.increment(type="direction")["numeric"]
        delta[1] *= 2
        assert cs.setincrement(delta, type="direction")
        assert ia.setcoordsys(cs.torecord())
        cs.done()
    finally:
        ia.done()
    try:
        assert ia.fromarray("beams.im", pixels)
        for chan in range(5):
            for pol in range(2):
                diameter = 4 + chan * 2 + pol
                assert ia.setrestoringbeam(f"{diameter}arcsec", "3arcsec", "12deg",
                                           channel=chan, polarization=pol, log=False)
    finally:
        ia.done()
    try:
        mask = ((x[:, :, 0, 0] + y[:, :, 0, 0]) % 4 != 0).astype(np.float32)
        assert ia.fromarray("mask.im", mask)
    finally:
        ia.done()
    try:
        x, y, s, f = np.indices((32, 32, 2, 5), dtype=np.float32)
        assert ia.fromarray("big.im", x**2 + 0.3 * y**2 + np.sin(x * y) + 2 * x + 20 * s + 100 * f)
    finally:
        ia.done()
    Path("full.crtf").write_text("#CRTFv0\nbox[[0pix,0pix],[11pix,11pix]]\n")


def snapshot(path):
    ia = image()
    assert ia.open(path)
    try:
        coords = ia.coordsys()
        shape = ia.shape().tolist()
        secondary = ia.miscinfo().get("secondary_coordinates", {})
        secondary_direction = next((value for field, value in secondary.items()
                                    if field.startswith("direction")), {})
        def beams(record):
            if "major" in record:
                return {field: [entry["value"], entry["unit"]] for field, entry in record.items()}
            return {field: beams(entry) if isinstance(entry, dict) else int(entry)
                    for field, entry in record.items()}

        record = {
            "shape": shape,
            "names": coords.names(),
            "units": coords.units(),
            "reference_pixel": coords.referencepixel()["numeric"].tolist(),
            "reference_value": coords.referencevalue()["numeric"].tolist(),
            "increment": coords.increment()["numeric"].tolist(),
            "pixel0_world": coords.toworld([0] * len(shape))["numeric"].tolist(),
            "brightness": ia.brightnessunit(),
            "mask": ia.getchunk(getmask=True).ravel(order="F").tolist(),
            "pixels": [float(x) if np.isfinite(x) else None for x in ia.getchunk().ravel(order="F")],
            "coordinate_record_maps": {k: v.tolist() for k, v in coords.torecord().items()
                                       if k.startswith(("pixelmap", "worldmap"))},
            "secondary_direction": {key: secondary_direction[key].tolist()
                                    for key in ("crpix", "crval", "cdelt", "pc")},
            "beams": beams(ia.restoringbeam()),
            "history": any(line.startswith("impv(") for line in ia.history()),
        }
        coords.done()
        return record
    finally:
        ia.done()


def run_cases():
    expected = {}
    ia = image()
    assert ia.open("cube.im")
    cs = ia.coordsys()
    units = cs.units()[:2]
    world = [[f"{a}{unit}" for a, unit in zip(cs.toworld([x, 6, 0, 0])["numeric"][:2], units)]
             for x in (3, 8)]
    center = cs.toworld([5.5, 6, 0, 0])["numeric"][:2]
    me = measures()
    equatorial = me.direction("J2000", *(f"{a}{unit}" for a, unit in zip(center, units)))
    galactic = me.measure(equatorial, "GALACTIC")
    galactic_center = "GALACTIC " + " ".join(f"{galactic[k]['value']}rad" for k in ("m0", "m1"))
    me.done()
    cs.done()
    ia.done()
    rg = regionmanager()
    channel_region = rg.box([0, 0, 0, 1], [11, 11, 1, 2])
    rg.done()
    extra = {
        "world_pairs": {"start": world[0], "end": world[1]},
        "world_text": {"start": "J2000 " + " ".join(world[0]), "end": "J2000 " + " ".join(world[1])},
        "region_record_channels": {"region": channel_region},
        "galactic_length": {"mode": "length", "center": galactic_center,
                            "length": "5arcmin", "pa": "0deg"},
    }
    for name, overrides in {**CASES, **extra}.items():
        values = {"imagename": "cube.im", "start": [3, 6], "end": [8, 6], **overrides}
        if values.get("mode") == "length":
            del values["start"], values["end"]
        def plain(value):
            if isinstance(value, dict):
                return {key: plain(item) for key, item in value.items()}
            return value.tolist() if isinstance(value, (np.ndarray, np.generic)) else value

        case = {"params": plain(values)}
        try:
            assert impv(**values, outfile=f"{name}.out") is None
        except (AssertionError, RuntimeError, TypeError, ValueError, UnboundLocalError) as error:
            case["error"] = type(error).__name__
            case["message"] = str(error).splitlines()[0].replace(f"{Path.cwd()}/", "")
            if name in {"reverse", "vertical"}:
                padded = {**values, "imagename": "padded.im"}
                for key in ("start", "end"):
                    padded[key] = [v + 16 for v in values[key]]
                impv(**padded, outfile=f"{name}.padded.out")
                case["padded_reference"] = snapshot(f"{name}.padded.out")
        else:
            case["product"] = snapshot(f"{name}.out")
        expected[name] = case
    return expected


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
            expected = run_cases()
            for root in INPUTS:
                source = Path(root)
                if source.is_dir():
                    shutil.copytree(source, destination / root, dirs_exist_ok=True,
                                    ignore=shutil.ignore_patterns("table.lock"))
                    for info in (destination / root).rglob("table.info"):
                        info.write_text("\n".join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + "\n")
                else:
                    shutil.copyfile(source, destination / root)
            (destination / "expected.json").write_text(json.dumps(
                {"casa_version": version_string(), "cases": expected}, indent=2, allow_nan=False,
            ) + "\n")
        finally:
            os.chdir(original)


if __name__ == "__main__":
    main()
