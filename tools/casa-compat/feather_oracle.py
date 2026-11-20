# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 feather filtering with an elliptical dish beam."""

import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
from casatasks import feather, version_string
from casatools import image

CASES = {
    "default": {},
    "scale": {"sdfactor": 0.73},
    "diameter": {"effdishdiam": 1600.0},
    "diameter_and_switch": {"effdishdiam": 1600.0, "lowpassfiltersd": True},
    "too_large": {"effdishdiam": 4200.0},
    "filter": {"lowpassfiltersd": True},
    "circular_filter": {"lowres": "round.im", "lowpassfiltersd": True},
    "zero_diameter": {"effdishdiam": 0.0},
    "odd_diameter": {"highres": "odd_high.im", "lowres": "odd_low.im", "effdishdiam": 1600.0},
    "uneven_diameter": {"highres": "even_high.im", "lowres": "even_low.im", "effdishdiam": 1600.0},
    "low_mask": {"lowres": "masked_low.im", "effdishdiam": 1600.0},
    "high_mask": {"highres": "masked_high.im", "effdishdiam": 1600.0},
    "both_masks": {"highres": "masked_high.im", "lowres": "masked_low.im", "effdishdiam": 1600.0},
    "unmasked_smaller_low": {"lowres": "cropped_low.im", "effdishdiam": 1600.0},
}


HIGH_BEAM = ("6arcsec", "5arcsec", "7deg")
LOW_BEAM = ("20arcsec", "13arcsec", "24deg")


def make_image(path, pixels, beam, mask=None, longitude_shift=0):
    ia = image()
    assert ia.fromarray(path, pixels)
    try:
        assert ia.setbrightnessunit("Jy/beam")
        assert ia.setrestoringbeam(*beam, log=False)
        cs = ia.coordsys()
        try:
            assert cs.setunits(["rad", "rad", "", "Hz"])
            assert cs.setincrement(np.deg2rad([-2.0/3600, 2.0/3600]), type="direction")
            if longitude_shift:
                reference = cs.referencepixel(type="direction")["numeric"]
                reference[0] += longitude_shift
                assert cs.setreferencepixel(reference, type="direction")
            assert cs.setreferencepixel([0.0], type="spectral")
            assert cs.setreferencevalue([1.4e9], type="spectral")
            assert cs.setincrement([2e6], type="spectral")
            assert ia.setcoordsys(cs.torecord())
        finally:
            cs.done()
        if mask:
            assert ia.calcmask(f'"{path}" < {mask[1]}', name=mask[0])
    finally:
        ia.done()


def snapshot(path):
    ia = image()
    assert ia.open(path)
    try:
        beam = ia.restoringbeam()
        return {
            "shape": ia.shape().tolist(),
            "units": ia.brightnessunit(),
            "beam": {key: [item["value"], item["unit"]] for key, item in beam.items()},
            "pixels": ia.getchunk().ravel(order="F").tolist(),
            "mask": ia.getchunk(getmask=True).ravel(order="F").tolist(),
            "default_mask": ia.maskhandler("default"),
        }
    finally:
        ia.done()


def sky(nx, ny):
    x, y, _, channel = np.indices((nx, ny, 1, 2), dtype=np.float32)
    high = np.exp(-((x - 11.2)**2 + (y - 14.6)**2)/8) + 0.6*np.exp(-((x - 5)**2 + (y - 7)**2)/3)
    low = 4*np.exp(-((x - 12)**2/24 + (y - 12)**2/47)) + np.exp(-((x - 5)**2 + (y - 7)**2)/12)
    return high*(1+channel/4), low*(1+channel/4)


def run_cases():
    high, low = sky(24, 24)
    make_image("high.im", high, HIGH_BEAM)
    make_image("low.im", low, LOW_BEAM)
    make_image("round.im", low, ("20arcsec", "20arcsec", "0deg"))
    make_image("masked_high.im", high, HIGH_BEAM, mask=("highmask", 0.8))
    make_image("masked_low.im", low, LOW_BEAM, mask=("lowmask", 3.7))
    # Keep the source edge off the exact integer where linear extrapolation
    # switches to zero after a sky-coordinate round trip.
    make_image("cropped_low.im", sky(18, 24)[1], LOW_BEAM, longitude_shift=0.25)
    for prefix, dimensions in [("odd", (25, 27)), ("even", (26, 24))]:
        high, low = sky(*dimensions)
        make_image(f"{prefix}_high.im", high, HIGH_BEAM)
        make_image(f"{prefix}_low.im", low, LOW_BEAM)
    cases = {}
    for name, overrides in CASES.items():
        values = {"highres": "high.im", "lowres": "low.im", **overrides}
        case = {"params": values}
        product = f"{name}.out"
        try:
            assert feather(imagename=product, **values) is None
            case["product"] = snapshot(product)
        except (AssertionError, RuntimeError, ValueError, TypeError) as error:
            message = str(error).splitlines()[0].replace(f"{Path.cwd()}/", "")
            case["error"] = message.partition("Caught exception: ")[2] or message
        cases[name] = case
    return cases


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
            cases = run_cases()
            for name in sorted({case["params"][key] for case in cases.values() for key in ("highres", "lowres")}):
                shutil.copytree(name, destination / name, dirs_exist_ok=True,
                                ignore=shutil.ignore_patterns("table.lock"))
                for path in (destination / name).rglob("table.info"):
                    path.write_text("\n".join(line.rstrip() for line in path.read_text().splitlines()).rstrip() + "\n")
            with (destination / "expected.json").open("w") as file:
                file.write(f'{{"casa_version":{json.dumps(version_string())},\n "cases":{{\n')
                file.write(",\n".join(f"  {json.dumps(name)}:{json.dumps(case, allow_nan=False)}"
                                     for name, case in cases.items()))
                file.write("\n }}\n")
        finally:
            os.chdir(original)


if __name__ == "__main__":
    main()
