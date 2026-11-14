#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA total-power sky inputs and sampling behavior, independent of Rust."""
import argparse
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys

from casatasks import version_string
from casatools import componentlist, image, measures, simulator, table, vpmanager
import numpy as np

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_image_layouts_oracle import earth_orientation

CASES = ["components", "image_components", "polarized", "distance",
    "galactic", "b1950", "icrs", "pb_alma", "pb_aca", "pb_ovro", "pb_sma", "moved_pb", "azel", "mixed", "saved_numeric", "saved_jones", "pb_vla", "pb_evla", "pb_ata", "pb_ngvla", "pb_unknown", "saved_jones_ft"]


def make_inputs(root, data):
    inputs = root / "inputs"
    inputs.mkdir(exist_ok=True)
    ia = image()
    ia.open(str(data / "sm_telescope_beams/sky.image"))
    values, original = ia.getchunk(), ia.coordsys().torecord()
    ia.done()
    for kind in CASES:
        if kind == "components":
            continue
        path = inputs / f"{kind}.image"
        if path.exists():
            continue
        ia.fromarray(pixels=values, csys=original)
        cs = ia.coordsys()
        pixels = values.copy()
        if kind in ("polarized", "mixed", "saved_jones", "saved_jones_ft"):
            cs.setstokes(["I", "Q", "U", "V"])
            pixels = np.concatenate([values, .2*values, -.1*values, .05*values], axis=2)
        if kind in ("galactic", "b1950", "icrs", "azel"):
            frame = kind.upper()
            centre = cs.referencevalue(type="direction")["numeric"]
            me = measures()
            if kind == "azel":
                before = snapshot(data / "sm_mosaic_coverage/outside.ms")
                tb = table()
                tb.open(str(data / "sm_mosaic_coverage/outside.ms/ANTENNA"))
                position = tb.getcell("POSITION", 0)
                tb.close()
                me.doframe(me.epoch("UTC", f"{before['rows'][0]['time']}s"))
                me.doframe(me.position("ITRF", *[f"{x}m" for x in position]))
            direction = me.measure(me.direction("J2000", f"{centre[0]}rad", f"{centre[1]}rad"), frame)
            cs.setreferencecode(frame, type="direction", adjust=False)
            cs.setreferencevalue([direction["m0"]["value"], direction["m1"]["value"]], type="direction")
        if kind == "azel":
            x, y = np.indices(values.shape[:2])
            pixels = np.exp(-((x-32.)**2+(y-32.)**2)/80.).astype(np.float32)[:, :, None, None]
        if kind in ("pb_vla", "pb_evla", "pb_ata", "pb_ngvla", "pb_unknown"):
            cs.setincrement([-1e-4, 1e-4], type="direction")
            cs.setreferencevalue([8e9+1.5*15.625e6], type="spectral")
        if kind.startswith("pb_") or kind == "moved_pb":
            pixels = np.pad(pixels, ((224, 224), (224, 224), (0, 0), (0, 0)))
            pixel = cs.referencepixel()["numeric"]
            pixel[:2] += 224
            cs.setreferencepixel(pixel)
        ia.done()
        ia.fromarray(outfile=str(path), pixels=pixels, csys=cs.torecord())
        ia.setbrightnessunit("Jy/pixel")
        if kind == "distance":
            ia.setmiscinfo({"distance": 1e8})
        ia.done()
        cs.done()
    path = inputs / "point.cl"
    if not path.exists():
        ia.open(str(data / "sm_telescope_beams/sky.image"))
        cs = ia.coordsys()
        centre = cs.referencevalue(type="direction")["numeric"]
        cs.done()
        ia.done()
        cl = componentlist()
        cl.addcomponent(flux=[.7, .1, -.05, .02], fluxunit="Jy", polarization="Stokes",
            dir=measures().direction("J2000", f"{centre[0]}rad", f"{centre[1]}rad"),
            shape="point", freq="230GHz")
        cl.rename(str(path))
        cl.done()


def mixed_controls(root):
    """Independent per-description CASA runs bypass its cached polarization map."""
    cache = root / "mixed-controls.json.gz"
    if cache.exists():
        with gzip.open(cache, "rt") as stream:
            return json.load(stream)
    controls = []
    base = root / "inputs/mixed.ms"
    descriptions = sorted({row["data_desc_id"] for row in snapshot(base)["rows"]})
    for description in descriptions:
        path = root / f"mixed-control-{description}.ms"
        shutil.copytree(base, path, ignore=shutil.ignore_patterns("table.lock"))
        sm = simulator()
        assert sm.openfromms(str(path))
        assert sm.setoptions(ftmachine="sd", gridfunction="BOX")
        assert sm.setvp(dovp=False, dosquint=False)
        assert sm.setdata(fieldid=[0], msselect=f"DATA_DESC_ID=={description}")
        assert sm.predict(imagename=str(root / "inputs/mixed.image"), incremental=False)
        controls.append(dict(description=description, after=snapshot(path)))
        sm.close()
    write(cache, controls)
    return controls


def jones_control(root, data, name):
    """Undo CASA's documented image/component Jones-index reversal."""
    cache = root / f"{name}-polarization-control.json.gz"
    if cache.exists():
        with gzip.open(cache, "rt") as stream:
            return json.load(stream)
    source = root / "inputs/reversed_jones.image"
    if not source.exists():
        ia = image()
        ia.open(str(data / "sm_image_beams/jones.image"))
        values, cs = ia.getchunk(), ia.coordsys().torecord()
        ia.done()
        ia.fromarray(outfile=str(source), pixels=values[:, :, ::-1, :], csys=cs)
        ia.done()
    beam = root / "inputs/reversed_jones.vp"
    if not beam.exists():
        manager = vpmanager()
        manager.reset()
        manager.setpbimage(telescope="ALMA", compleximage=str(source))
        manager.saveastable(str(beam))
    path = root / f"{name}-polarization-control.ms"
    shutil.copytree(data / "sm_mosaic_coverage/outside.ms", path, ignore=shutil.ignore_patterns("table.lock"))
    sm = simulator()
    assert sm.openfromms(str(path))
    assert sm.setoptions(ftmachine="ft" if name.endswith("_ft") else "sd", gridfunction="BOX")
    assert sm.setvp(dovp=True, usedefaultvp=False, vptable=str(beam), dosquint=False)
    assert sm.setdata(fieldid=[0])
    assert sm.predict(imagename=str(root / f"inputs/{name}.image"), incremental=False)
    control = snapshot(path)
    sm.close()
    write(cache, control)
    return control


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--worker", choices=CASES)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root, data = args.out.resolve(), args.data.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if args.worker is None:
        make_inputs(root, data)
        cases = []
        for name in CASES:
            path = root / f"{name}.json.gz"
            if not path.exists():
                with (root / f"{name}.log").open("w") as log:
                    result = subprocess.run([sys.executable, __file__, "--out", str(root),
                        "--fixture", str(args.fixture), "--data", str(data), "--worker", name],
                        stdout=log, stderr=subprocess.STDOUT)
                assert path.exists(), (name, result.returncode)
            with gzip.open(path, "rt") as stream:
                case = json.load(stream)["cases"][0]
            cases.append(case)
            write(args.fixture, dict(casa_version=version_string(), cases=cases))
            autos = [r["data"] for r in case["after"]["rows"] if r["field"] == 0 and r["antennas"][0] == r["antennas"][1]]
            print(name, case["outcome"], "first autocorrelation", autos[0][:4] if autos else [], case["error"], flush=True)
        references = {case["name"]: case for case in cases}
        for case in cases:
            if case["name"] in ("saved_jones", "saved_jones_ft"):
                control = jones_control(root, data, case["name"])
                case["polarization_control"] = control
                case["corrected_rows"] = control["rows"]
                case["casa_defect"] = "PBMath2DImage reverses Jones correlation indices for images relative to components, as documented. Independent CASA controls reverse the beam values to undo the defect; raw output is preserved."
            if case["name"] == "mixed":
                controls = mixed_controls(root)
                rows = json.loads(json.dumps(case["after"]["rows"]))
                for control in controls:
                    for row, reference in zip(rows, control["after"]["rows"]):
                        if row["field"] == 0 and row["data_desc_id"] == control["description"]:
                            for column in ("data", "model_data", "corrected_data"):
                                row[column] = reference[column]
                case["corrected_rows"] = rows
                case["per_description_controls"] = controls
                case["casa_defect"] = "SDGrid inherits the documented cached FTMachine polarization-map defect across aliased data descriptions. Independent per-description CASA predictions supply the declared receptor order; raw combined output is retained."
            if case["name"] == "azel":
                case["eop"] = earth_orientation(case["before"])
            if case["returned"] and case["name"] not in ("components", "image_components", "saved_jones_ft"):
                case["native_single_dish"] = True
            if case["name"] == "image_components":
                rows = json.loads(json.dumps(case["after"]["rows"]))
                for row, reference in zip(rows, references["components"]["after"]["rows"]):
                    if row["field"] == 0 and row["antennas"][0] != row["antennas"][1]:
                        for column in ("data", "corrected_data"):
                            row[column] = reference[column]
                case["corrected_rows"] = rows
                case["casa_defect"] = "SDGrid's documented uvw-sign cross-correlation corruption is corrected using the independent component-only prediction; raw CASA output is retained."
        write(args.fixture, dict(casa_version=version_string(), recipe="tools/simulation/sm_total_power_oracle.py", cases=cases))
        return
    name = args.worker
    path = root / f"{name}.ms"
    shutil.copytree(data / "sm_mosaic_coverage/outside.ms", path, ignore=shutil.ignore_patterns("table.lock"))
    tb = table()
    if name.startswith("pb_"):
        tb.open(str(path / "OBSERVATION"), nomodify=False)
        tb.putcell("TELESCOPE_NAME", 0, name.removeprefix("pb_").upper() if name != "pb_unknown" else "NO_SUCH_TELESCOPE")
        tb.close()
    if name in ("pb_vla", "pb_evla", "pb_ata", "pb_ngvla", "pb_unknown"):
        tb.open(str(path / "SPECTRAL_WINDOW"), nomodify=False)
        frequencies = tb.getcell("CHAN_FREQ", 0)
        tb.putcell("CHAN_FREQ", 0, 8e9+np.arange(len(frequencies))*15.625e6)
        tb.putcell("REF_FREQUENCY", 0, 8e9)
        tb.close()
    if name == "moved_pb":
        tb.open(str(path / "POINTING"), nomodify=False)
        for row in range(tb.nrows()):
            direction = tb.getcell("DIRECTION", row)
            direction[0] += 5e-6
            tb.putcell("DIRECTION", row, direction)
        tb.close()
    if name == "mixed":
        tb.open(str(path / "POLARIZATION"), nomodify=False)
        row = tb.nrows()
        values = {key: tb.getcell(key, 0) for key in tb.colnames()}
        tb.addrows(1)
        for key, value in values.items():
            if key == "CORR_TYPE": value = value[::-1]
            if key == "CORR_PRODUCT": value = value[:, ::-1]
            tb.putcell(key, row, value)
        tb.close()
        tb.open(str(path / "DATA_DESCRIPTION"), nomodify=False)
        alias = tb.nrows()
        values = {key: tb.getcell(key, 0) for key in tb.colnames()}
        tb.addrows(1)
        for key, value in values.items():
            tb.putcell(key, alias, row if key == "POLARIZATION_ID" else value)
        tb.close()
        tb.open(str(path), nomodify=False)
        times = tb.getcol("TIME")
        ids = tb.getcol("DATA_DESC_ID")
        ids[times > times.min()+5.] = alias
        tb.putcol("DATA_DESC_ID", ids)
        tb.close()
        shutil.copytree(path, root / "inputs/mixed.ms", ignore=shutil.ignore_patterns("table.lock"))
    before = snapshot(path)
    sm = simulator()
    assert sm.openfromms(str(path))
    returned, error, outcome = None, None, "Raises"
    try:
        assert sm.setoptions(ftmachine="ft" if name == "saved_jones_ft" else "sd", gridfunction="PB" if name.startswith("pb_") or name == "moved_pb" else "BOX")
        vp = dict(dovp=name in ("moved_pb", "saved_numeric", "saved_jones", "saved_jones_ft"), dosquint=False)
        if name == "saved_numeric":
            vp.update(usedefaultvp=False, vptable=str(data / "sm_saved_curves/numeric_power.vp"))
        if name in ("saved_jones", "saved_jones_ft"):
            if not (root / "inputs/saved_jones.vp").exists():
                manager = vpmanager()
                manager.reset()
                manager.setpbimage(telescope="ALMA", compleximage=str(data / "sm_image_beams/jones.image"))
                manager.saveastable(str(root / "inputs/saved_jones.vp"))
            vp.update(usedefaultvp=False, vptable=str(root / "inputs/saved_jones.vp"))
        assert sm.setvp(**vp)
        assert sm.setdata(fieldid=[0])
        kwargs = dict(incremental=False)
        if name != "components":
            kwargs["imagename"] = str(root / f"inputs/{name}.image")
        if name in ("components", "image_components"):
            kwargs["complist"] = str(root / "inputs/point.cl")
        returned = bool(sm.predict(**kwargs))
        outcome = "Success" if returned else "ReturnsFalse"
    except Exception as exc:
        error = str(exc)
    write(root / f"{name}.json.gz", dict(casa_version=version_string(), cases=[dict(
        name=name, returned=returned, outcome=outcome, error=error, before=before, after=snapshot(path))]))
    sm.close()


if __name__ == "__main__":
    main()
