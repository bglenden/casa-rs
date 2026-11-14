#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Saved radial-beam squint applied to simulated components and Stokes images."""
import argparse
from pathlib import Path

import numpy as np
from casatasks import version_string
from casatools import image, measures, vpmanager

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_squint_oracle import observation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--polarized-images", action="store_true")
    parser.add_argument("--long-scan", action="store_true")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    me, vp = measures(), vpmanager()
    assert vp.reset()
    assert vp.setpbgauss(telescope="VLA", halfwidth="20arcmin", maxrad="100arcmin",
        reffreq="1.4GHz", squintdir=me.direction("AZEL", "-2arcmin", "1arcmin"),
        squintreffreq="1.4GHz", dosquint=True)
    assert vp.saveastable(str(inputs / "custom_gaussian.vp"))
    me.done()
    ia = image()
    ia.open(str(args.data / "sm_voltage_patterns/sky.image"))
    pixels, cs = ia.getchunk(), ia.coordsys()
    cs.setreferencevalue([1.4e9], type="spectral")
    cs.setincrement([.1e9], type="spectral")
    cs.setincrement([-2e-4, 2e-4], type="direction")
    ia.done()
    sky_name = "vla_polarized.image" if args.polarized_images else "vla.image"
    if args.polarized_images:
        pixels = np.concatenate([pixels, pixels*.1, pixels*.04, pixels*.03], axis=2)
        cs.setstokes("I Q U V")
    ia.fromarray(outfile=str(inputs / sky_name), pixels=pixels, csys=cs.torecord())
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()
    cases = []
    for operation, mode, squint, increment, mount in (
            ("components", "off", False, 360., "alt-az"),
            ("components", "on", True, 360., "alt-az"),
            ("components", "refresh", True, .01, "alt-az"),
            ("components", "equatorial", True, .01, "equatorial"),
            ("image", "off", False, 360., "alt-az"),
            ("image", "on", True, 360., "alt-az"),
            ("image", "refresh", True, .01, "alt-az")):
        if operation == "image" and mode == "refresh" and not args.polarized_images:
            continue
        if args.polarized_images and operation != "image":
            continue
        if args.long_scan and mode == "off":
            continue
        name = f"saved_gaussian_{'polarized_' if args.polarized_images else ''}{'long_' if args.long_scan else ''}{operation}_{mode}"
        path = root / f"{name}.ms"
        timing = dict(integration=1800., starts=(10800.,), duration=7200.) if args.long_scan else {}
        sm = observation(path, "VLA", 1.4e9, mount, **timing)
        before = snapshot(path)
        assert sm.setvp(dovp=True, usedefaultvp=False,
            vptable=str(inputs / "custom_gaussian.vp"), dosquint=squint,
            parangleinc=f"{increment}deg")
        arguments = {"imagename": str(inputs / sky_name)} if operation == "image" else {
            "complist": str(args.data / "sm_squint/vla.cl")}
        assert sm.predict(**arguments, incremental=False)
        sm.close()
        case = dict(name=name, telescope="VLA", frequency_hz=1.4e9, mount=mount,
            squint=squint, increment_deg=increment, native_created=True,
            saved_beam="custom_gaussian.vp", before=before, after=snapshot(path))
        if args.long_scan:
            case.update(integration_s=1800., scan_starts_s=[10800.], scan_duration_s=7200.)
        if operation == "image":
            case["sky_image"] = sky_name
        cases.append(case)
        write(args.fixture, dict(casa_version=version_string(),
            recipe="tools/simulation/sm_saved_squint_oracle.py", cases=cases))
        print(name, "passed", flush=True)
    signal = lambda case: np.asarray([row["data"] for row in case["after"]["rows"]])
    pairs = ((0, 1),) if args.long_scan else (((0, 1), (1, 2)) if args.polarized_images else ((0, 1), (1, 2), (2, 3), (4, 5)))
    for left, right in pairs:
        contrast = np.linalg.norm(signal(cases[left])-signal(cases[right])) / np.linalg.norm(signal(cases[right]))
        if args.long_scan:
            assert contrast > 1e-3, contrast
        elif not args.polarized_images:
            assert contrast > 1e-3 if left < 4 else contrast < 1e-6, contrast
        print(cases[left]["name"], cases[right]["name"], "contrast", contrast, flush=True)


if __name__ == "__main__":
    main()
