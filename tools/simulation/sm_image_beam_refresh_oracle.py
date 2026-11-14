#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA saved-image beam orientation across integrations of one scan."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import version_string
from casatools import measures, simulator, table, vpmanager

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import make_session


def predict(path, sky, beam, increment, query=""):
    sm = simulator()
    assert sm.openfromms(str(path))
    assert sm.setvp(dovp=True, usedefaultvp=False, vptable=str(beam),
        dosquint=False, parangleinc=f"{increment}deg")
    assert sm.setdata(fieldid=[], msselect=query)
    assert sm.predict(imagename=str(sky), incremental=False)
    sm.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root, data = args.out.resolve(), args.data.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    base = inputs / "base.ms"
    full = root / "full.ms"
    sm = make_session(full, stokes="XX XY YX YY")
    me = measures()
    assert sm.settimes(integrationtime="1800s", usehourangle=False,
        referencetime=me.epoch("UTC", "2024/06/01/00:00:00"))
    assert sm.observe(sourcename="target", spwname="band6", starttime="10763s", stoptime="17963s")
    sm.close()
    me.done()
    tb = table()
    tb.open(str(full))
    selected = tb.query(f"SCAN_NUMBER == {int(tb.getcol('SCAN_NUMBER').max())}")
    selected.copy(str(base), deep=True, valuecopy=True)
    selected.close()
    tb.close()
    tb.open(str(base), nomodify=False)
    for column in ("MODEL_DATA", "CORRECTED_DATA"):
        if column not in tb.colnames():
            desc = tb.getcoldesc("DATA")
            desc["name"] = column
            desc.pop("dataManagerType", None)
            desc.pop("dataManagerGroup", None)
            tb.addcols({column: desc})
        for row in range(tb.nrows()):
            tb.putcell(column, row, np.zeros_like(tb.getcell("DATA", row)))
    times = sorted(set(tb.getcol("TIME").tolist()))
    assert len(times) == 4 and len(set(tb.getcol("SCAN_NUMBER"))) == 1
    tb.close()
    shutil.copytree(data / "sm_image_beams/real.image", inputs / "real.image")
    shutil.copytree(data / "sm_telescope_beams/sky.image", inputs / "sky.image")
    vp = vpmanager()
    assert vp.reset()
    assert vp.setpbimage(telescope="ALMA", realimage=str(inputs / "real.image"))
    assert vp.saveastable(str(inputs / "real.vp"))
    cases = []
    for name, increment in (("frozen", 360.), ("refresh", .01)):
        path = root / f"{name}.ms"
        shutil.copytree(base, path, ignore=shutil.ignore_patterns("table.lock"))
        before = snapshot(path)
        predict(path, inputs / "sky.image", inputs / "real.vp", increment)
        case = dict(name=f"real_image_{name}_within_scan", beam="real", operation="image_refresh",
            base_ms="base.ms", sky_image="sky.image", inputs="refresh", increment_deg=increment,
            all_fields_selected=True, before=before, after=snapshot(path))
        if name == "refresh":
            control = root / "fresh_integrations.ms"
            shutil.copytree(base, control, ignore=shutil.ignore_patterns("table.lock"))
            for time in times:
                predict(control, inputs / "sky.image", inputs / "real.vp", increment, f"TIME == {time:.17g}")
            case["corrected_rows"] = snapshot(control)["rows"]
            case["casa_defect"] = "PBMath2DImage::updateJones caches unchanged image geometry and fails to refresh orientation. Independent fresh simulators predict each integration at its selected time; raw cached CASA output is retained."
        cases.append(case)
        write(args.fixture, dict(casa_version=version_string(),
            recipe="tools/simulation/sm_image_beam_refresh_oracle.py", cases=cases))
        print(name, "accepted", flush=True)
    values = lambda rows: np.asarray([row["data"] for row in rows])
    fresh = values(cases[1]["corrected_rows"])
    contrast = np.linalg.norm(fresh-values(cases[0]["after"]["rows"])) / np.linalg.norm(fresh)
    assert contrast > 1e-3, contrast
    print("orientation refresh contrast", contrast, flush=True)


if __name__ == "__main__":
    main()
