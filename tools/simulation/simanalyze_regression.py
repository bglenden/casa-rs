#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run the upstream m51 multi-array/TP calculation recipe against CASA 6.7.6.14.

Run under CASA's Python with --model CASATESTDATA/image/m51ha.model --out
target/simanalyze-regression --native-bin-dir target/release. CASA is solely
the oracle and reader. The native tasks run through their public JSON API.
The documented UTC, SF beam and fidelity controls are explicit below.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import numpy as np
from simanalyze_multi_oracle import physical_sf_beam
from simanalyze_task_oracle import corrected_simanalyze


def native(binary, surface, parameters, directory):
    request = directory / (surface+"-request.json")
    request.write_text(json.dumps(dict(surface=surface, values=parameters)))
    result = subprocess.run([str(binary), "--json-run", str(request)],
                            text=True, capture_output=True, check=False)
    (directory / (surface+"-stderr.log")).write_text(result.stderr)
    reply = json.loads(result.stdout)
    if result.returncode or reply.get("status") != "succeeded":
        raise RuntimeError(reply)


def nrms(actual, reference):
    if actual.shape != reference.shape or not np.all(np.isfinite(actual)) or not np.all(np.isfinite(reference)):
        raise ValueError("incompatible shape or non-finite scientific data")
    return float(np.sqrt(np.sum(np.abs(actual-reference)**2) / max(np.sum(np.abs(reference)**2), np.finfo(float).tiny)))


def image_record(path):
    from casatools import image
    ia = image()
    ia.open(str(path))
    cs = ia.coordsys()
    record = dict(pixels=ia.getchunk(), mask=ia.getchunk(getmask=True), unit=ia.brightnessunit(),
                  reference=cs.referencevalue()["numeric"], increment=cs.increment()["numeric"],
                  beam=ia.restoringbeam())
    cs.done()
    ia.done()
    return record


def compare(casa, rust):
    from casatools import table
    report = dict(images={}, measurementsets={}, failures=[])
    for source in sorted(casa.iterdir()):
        target = rust / source.name
        if source.name.endswith(".ms"):
            tb = table()
            columns = {}
            for path in [source, target]:
                tb.open(str(path))
                columns[str(path)] = {name: tb.getcol(name) for name in ["DATA", "UVW", "FLAG", "FLAG_ROW", "TIME", "WEIGHT", "SIGMA"]}
                tb.close()
            expected, actual = columns[str(source)], columns[str(target)]
            result = {name: nrms(actual[name].astype(float) if actual[name].dtype == bool else actual[name],
                                 expected[name].astype(float) if expected[name].dtype == bool else expected[name])
                      for name in expected}
            valid = ~expected["FLAG"] & ~expected["FLAG_ROW"][None,None,:]
            result["DATA"] = nrms(actual["DATA"][valid], expected["DATA"][valid])
            assert result["DATA"] <= 1e-6, (source.name, result)
            assert result["UVW"] <= 1e-6, (source.name, result)
            assert np.array_equal(actual["FLAG"], expected["FLAG"])
            assert np.array_equal(actual["FLAG_ROW"], expected["FLAG_ROW"])
            assert result["WEIGHT"] <= 1e-6 and result["SIGMA"] <= 1e-6
            # Use Phase 5's existing column comparison, including its
            # relative term for seconds near 5e9 (not a new absolute gate).
            from compare_sm_ms import TOLERANCES, RELATIVE_TOLERANCE
            np.testing.assert_allclose(actual["TIME"], expected["TIME"], rtol=RELATIVE_TOLERANCE, atol=TOLERANCES["TIME"])
            result["max_time_error_s"] = float(np.max(np.abs(actual["TIME"]-expected["TIME"])))
            report["measurementsets"][source.name] = result
        elif source.is_dir() and (source / "table.dat").exists() :
            assert target.exists(), ("missing image", target)
            expected, actual = image_record(source), image_record(target)
            error = nrms(actual["pixels"][expected["mask"]], expected["pixels"][expected["mask"]])
            masks = np.array_equal(actual["mask"], expected["mask"])
            if error > 1e-3 or not masks:
                report["failures"].append(dict(image=source.name, nrms=error, masks_equal=masks))
            np.testing.assert_allclose(actual["reference"], expected["reference"], rtol=1e-12, atol=1e-12)
            np.testing.assert_allclose(actual["increment"], expected["increment"], rtol=1e-12, atol=1e-12)
            report["images"][source.name] = dict(nrms=error, masks_equal=masks, casa_unit=expected["unit"], native_unit=actual["unit"])
    return report


def main():
    from casatasks import simobserve, version_string
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--native-bin-dir", required=True, type=Path)
    parser.add_argument("--compare-only", action="store_true", help="compare completed products without rerunning calculations")
    parser.add_argument("--reuse-casa", action="store_true", help="reuse an unchanged completed CASA oracle")
    parser.add_argument("--reuse-observations", action="store_true", help="reuse unchanged native simulated MSs; time analysis alone")
    args = parser.parse_args()
    model, root, binaries = args.model.resolve(), args.out.resolve(), args.native_bin_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert version_string() == "6.7.6.14"
    physical_sf_beam()
    task, defaults = corrected_simanalyze()
    common = dict(project="m51c", skymodel=str(model), inbright="0.004",
                  indirection="B1950 23h59m59.96 -34d59m59.50", incell="0.1arcsec",
                  incenter="330.076GHz", inwidth="50MHz", integration="10s", mapsize="1arcmin",
                  refdate="2012/11/21/20:00:00", thermalnoise="", graphics="none", overwrite=True)
    from casatools import ctsys
    tpconfig = str(Path(ctsys.resolve("alma/simmos/aca.tp.cfg")))
    observations = [dict(maptype="hex", pointingspacing="9arcsec", totaltime="3600s", antennalist="alma;0.5arcsec"),
                    dict(maptype="square", pointingspacing="9arcsec", totaltime="2h", obsmode="sd", sdantlist=tpconfig, sdant=0),
                    dict(maptype="hex", pointingspacing="15arcsec", totaltime="3", antennalist="aca.i.cfg")]
    analyses = [dict(vis="$project.aca.i.ms,$project.aca.tp.sd.ms", modelimage="$project.sd.image"),
                dict(vis="$project.alma_0.5arcsec.ms", modelimage="$project.aca.i.image")]
    timing = {}
    for mode in ["casa", "native"]:
        work = root / mode
        work.mkdir(exist_ok=True)
        if args.compare_only or (mode == "casa" and args.reuse_casa):
            timing[mode] = json.loads((work/"timing.json").read_text())
            continue
        reuse = mode == "native" and args.reuse_observations
        if reuse:
            assert all((work/"m51c"/name).is_dir() for name in ["m51c.alma_0.5arcsec.ms", "m51c.aca.i.ms", "m51c.aca.tp.sd.ms"])
        else:
            shutil.rmtree(work / "m51c", ignore_errors=True)
        os.chdir(work)
        started = time.perf_counter()
        for observation in ([] if reuse else observations):
            parameters = dict(common, **observation)
            if mode == "casa": simobserve(**parameters)
            else:
                # At this date TAI-UTC is 35 s. Align physical epochs with
                # CASA's documented TAI-labelled-as-UTC simulation bug.
                native(binaries/"simobserve", "simobserve", dict(parameters, hourangle="35s"), work)
        observed = time.perf_counter()
        for analysis in analyses:
            parameters = dict(analysis, project="m51c", image=True, imsize=[512,512], cell="0.2arcsec",
                              analyze=True, showpsf=False, showresidual=False, showconvolved=True, graphics="none")
            if mode == "casa": task(**dict(defaults, **parameters))
            else: native(binaries/"simanalyze", "simanalyze", parameters, work)
        timing[mode] = dict(simobserve_s=None if reuse else observed-started, simanalyze_s=time.perf_counter()-observed)
        (work/"timing.json").write_text(json.dumps(timing[mode]))
        print(mode, timing[mode], flush=True)
    result = dict(casa_version=version_string(), timing=timing,
                  controls=["35 s physical epoch alignment", "C++ SDGrid SF beam", "full-precision fidelity floor"],
                  comparison=compare(root/"casa"/"m51c", root/"native"/"m51c"))
    (root/"report.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2), flush=True)
    assert not result["comparison"]["failures"], result["comparison"]["failures"]


if __name__ == "__main__":
    main()
