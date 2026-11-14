#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Saved Jones-image inputs applied to simulated sky signals."""
import argparse
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
from casatasks import version_string
from casatools import image, simulator, table, vpmanager

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_voltage_patterns_oracle import correct_component_phase
from sm_multiple_models_oracle import make_session


def beam_image(path, values, complex_image=False, reference_pixel=0.):
    ia = image()
    assert ia.fromshape(str(path), shape=list(values.shape), type="c" if complex_image else "f")
    cs = ia.coordsys()
    cs.setstokes(["XX", "XY", "YX", "YY"])
    cs.setreferencecode("AZEL", type="direction", adjust=False)
    cs.setreferencevalue([0., 0.], type="direction")
    cs.setreferencepixel([48., 48.], type="direction")
    cs.setincrement([2e-6, 2e-6], type="direction")
    cs.setreferencevalue([150e9], type="spectral")
    if reference_pixel:
        cs.setincrement([10e9], type="spectral")
        cs.setreferencepixel([reference_pixel], type="spectral")
    assert ia.setcoordsys(cs.torecord())
    cs.done()
    assert ia.putchunk(values)
    ia.done()


def inputs_for(inputs, reverse_jones=False, reference_pixel=0.):
    x, y = np.indices((96, 96))
    profile = np.exp(-((x-48.)/18.)**2-((y-48.)/27.)**2).astype(np.float32)
    re = np.zeros((96, 96, 4, 1), dtype=np.float32)
    im = np.zeros_like(re)
    for pol in (0, 3):
        re[:, :, pol, 0] = .9 * profile
        im[:, :, pol, 0] = .2 * profile
    beam_image(inputs / "real.image", re, reference_pixel=reference_pixel)
    beam_image(inputs / "imaginary.image", im, reference_pixel=reference_pixel)
    beam_image(inputs / "complex.image", re + 1j*im, True, reference_pixel)
    re[:, :, 3, 0] = .7 * profile
    re[:, :, 1, 0] = .03 * profile
    re[:, :, 2, 0] = -.02 * profile
    im[:, :, 1, 0] = .02 * profile
    im[:, :, 2, 0] = .01 * profile
    values = re + 1j*im
    beam_image(inputs / "jones.image", values[:, :, ::-1, :] if reverse_jones else values, True, reference_pixel)


def revisit_base(path):
    sm = make_session(path, stokes="XX XY YX YY")
    assert sm.observe(sourcename="target", spwname="band6", starttime="14363s", stoptime="14383s")
    sm.close()
    tb = table()
    tb.open(str(path / "OBSERVATION"), nomodify=False)
    first = {name: tb.getcell(name, 0) for name in tb.colnames() if tb.iscelldefined(name, 0)}
    if tb.nrows() < 3:
        tb.addrows(3-tb.nrows())
    for row in (1, 2):
        for name, value in first.items():
            tb.putcell(name, row, value)
    tb.close()
    tb.open(str(path), nomodify=False)
    times, fields = tb.getcol("TIME"), tb.getcol("FIELD_ID")
    arrays = np.where(times > times.min()+1000., 2, np.where(fields == 1, 1, 0)).astype(np.int32)
    tb.putcol("ARRAY_ID", arrays)
    tb.putcol("OBSERVATION_ID", arrays)
    for name in ("MODEL_DATA", "CORRECTED_DATA"):
        if name not in tb.colnames():
            desc = tb.getcoldesc("DATA")
            desc["name"] = name
            desc.pop("dataManagerType", None)
            desc.pop("dataManagerGroup", None)
            tb.addcols({name: desc})
        for row in range(tb.nrows()):
            tb.putcell(name, row, np.zeros_like(tb.getcell("DATA", row)))
    tb.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--sky", type=Path, required=True)
    parser.add_argument("--full-stokes", action="store_true")
    parser.add_argument("--beam")
    parser.add_argument("--reverse-jones", action="store_true")
    parser.add_argument("--reference-pixel", type=float, default=0.)
    parser.add_argument("--nondefault-pole", action="store_true")
    parser.add_argument("--revisit", action="store_true")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    inputs_for(inputs, args.reverse_jones, args.reference_pixel)
    if args.revisit:
        args.base = inputs / "revisit.ms"
        revisit_base(args.base)
    sky_image = args.sky / "sky.image"
    if args.nondefault_pole:
        ia = image()
        ia.open(str(sky_image))
        values = ia.getchunk()
        cs = ia.coordsys().torecord()
        ia.done()
        cs["direction0"]["longpole"] = 170.
        cs["direction0"]["latpole"] = 60.
        sky_image = inputs / "pole.image"
        ia.fromarray(outfile=str(sky_image), pixels=values, csys=cs)
        ia.setbrightnessunit("Jy/pixel")
        ia.done()
    if args.full_stokes:
        ia = image()
        ia.open(str(sky_image))
        values = ia.getchunk()
        cs = ia.coordsys()
        cs.setstokes(["I", "Q", "U", "V"])
        ia.done()
        sky_image = inputs / "full_stokes.image"
        ia.fromarray(outfile=str(sky_image), pixels=np.concatenate([values, np.zeros_like(values),
            np.zeros_like(values), np.zeros_like(values)], axis=2), csys=cs.torecord())
        ia.setbrightnessunit("Jy/pixel")
        ia.done()
        cs.done()
    cases = []
    for beam, parameters in (
            ("real", dict(realimage=str(inputs / "real.image"))),
            ("pair", dict(realimage=str(inputs / "real.image"), imagimage=str(inputs / "imaginary.image"))),
            ("complex", dict(compleximage=str(inputs / "complex.image"))),
            ("jones", dict(compleximage=str(inputs / "jones.image")))):
        if args.beam and beam != args.beam:
            continue
        vp = vpmanager()
        assert vp.reset()
        assert vp.setpbimage(telescope="ALMA", **parameters)
        voltage_table = inputs / f"{beam}.vp"
        assert vp.saveastable(str(voltage_table))
        for operation in ("image", "components"):
            if (args.full_stokes or args.nondefault_pole or args.revisit) and operation != "image":
                continue
            name = f"{beam}_{operation}" + ("_full_stokes" if args.full_stokes else "")
            if args.reference_pixel:
                name += "_reference_pixel"
            if args.nondefault_pole:
                name += "_pole"
            if args.revisit:
                name += "_revisit"
            path = root / f"{name}.ms"
            shutil.copytree(args.base, path, ignore=shutil.ignore_patterns("table.lock"))
            before = snapshot(path)
            sm = simulator()
            assert sm.openfromms(str(path))
            assert sm.setvp(dovp=True, usedefaultvp=False, vptable=str(voltage_table), dosquint=False)
            assert sm.setdata(fieldid=[0, 1] if args.revisit else [0])
            assert sm.predict(**{"imagename" if operation == "image" else "complist":
                str(sky_image if operation == "image" else args.sky / "sky.cl")}, incremental=False)
            sm.close()
            case = dict(name=name, beam=beam, operation=operation, before=before, after=snapshot(path))
            if args.full_stokes:
                case["sky_image"] = "full_stokes.image"
            if args.nondefault_pole:
                case["sky_image"] = "pole.image"
            if args.revisit:
                case["base_ms"] = "revisit.ms"
                case["all_fields_selected"] = True
                control_path = root / f"{name}_fresh_visits.ms"
                shutil.copytree(args.base, control_path, ignore=shutil.ignore_patterns("table.lock"))
                for array in range(3):
                    fresh = simulator()
                    assert fresh.openfromms(str(control_path))
                    assert fresh.setvp(dovp=True, usedefaultvp=False, vptable=str(voltage_table), dosquint=False)
                    assert fresh.setdata(fieldid=[], msselect=f"ARRAY_ID == {array}")
                    assert fresh.predict(imagename=str(sky_image), incremental=False)
                    fresh.close()
                case["corrected_rows"] = snapshot(control_path)["rows"]
                case["casa_defect"] = "PBMath2DImage::updateJones reuses a regridded beam when sky coordinates/shape are unchanged, ignoring the pointing and position angle. The independent CASA control predicts each visit with a fresh simulator; raw cached output is retained."
            assert all(np.isfinite(v) for row in case["after"]["rows"] for pair in row["data"] for v in pair), name
            assert any(v for row in case["after"]["rows"] if row["field"] == 0
                for pair in row["data"] for v in pair), name
            if operation == "components":
                correct_component_phase(case)
            cases.append(case)
            write(args.fixture, dict(casa_version=version_string(),
                recipe="tools/simulation/sm_image_beams_oracle.py", cases=cases))
            print(name, "passed", flush=True)
    if args.full_stokes or args.beam or args.reverse_jones or args.reference_pixel or args.nondefault_pole or args.revisit:
        return
    # Independent C++ controls expose the image/component Jones-index
    # discrepancy and the lost polarization of a one-Stokes input image.
    controls = []
    for label, flags in (("full-stokes", ["--full-stokes"]),
            ("polarization", ["--full-stokes", "--beam", "jones", "--reverse-jones"])):
        output = root / f"{label}.json.gz"
        subprocess.run([sys.executable, __file__, "--out", str(root / label),
            "--fixture", str(output), "--base", str(args.base), "--sky", str(args.sky), *flags], check=True)
        with gzip.open(output, "rt") as stream:
            controls.append(json.load(stream)["cases"])
    cases.extend(controls[0])
    shutil.copytree(root / "full-stokes/inputs/full_stokes.image", inputs / "full_stokes.image")
    for case in cases:
        if case["beam"] == "jones" and case["operation"] == "image":
            case["corrected_rows"] = controls[1][0]["after"]["rows"]
            case["polarization_control"] = controls[1][0]
            case["casa_defect"] = "PBMath2DImage image application reverses Jones correlation indexing relative to its component application; a one-Stokes sky also drops induced polarization. The independent CASA control supplies four Stokes planes (Q/U/V zero) and reverses beam correlation values to undo both defects. Raw output is retained."
    for label, flags in (("reference_pixel", ["--reference-pixel", "2"]),
            ("pole", ["--nondefault-pole"]), ("revisit", ["--revisit"])):
        output = root / f"{label}.json.gz"
        subprocess.run([sys.executable, __file__, "--out", str(root / label),
            "--fixture", str(output), "--base", str(args.base), "--sky", str(args.sky),
            "--beam", "real", *flags], check=True)
        shutil.copytree(root / label / "inputs", inputs / label,
            ignore=shutil.ignore_patterns("table.lock"))
        with gzip.open(output, "rt") as stream:
            for case in json.load(stream)["cases"]:
                case["inputs"] = label
                cases.append(case)
    write(args.fixture, dict(casa_version=version_string(),
        recipe="tools/simulation/sm_image_beams_oracle.py", cases=cases))


if __name__ == "__main__":
    main()
