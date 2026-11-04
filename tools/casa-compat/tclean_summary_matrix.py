#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare tclean's `summaryminor`, row by row, with CASA's (issue 1068).

Run with CASA's Python, on a data root holding the MeasurementSets below with
any `XSym` links resolved (`tclean_case_matrix.py` says why):

    /Applications/CASA.app/Contents/MacOS/python3 -I \
        tools/casa-compat/tclean_summary_matrix.py \
        --data-root <root> --casars-python <venv with the wheel> \
        --imager-binary <binary> --output <new dir> [--case NAME ...]

It writes `<output>/summary.json`, with each case's return on both sides under
`<output>/casa` and `<output>/native`. A count agrees exactly. A peak or flux
agrees within 2e-3 of CASA's, or of 1e-3 where CASA's is smaller: these carry
the runs' ordinary differences. Native runs at `gridprecision='f64'`.
"""
import argparse
import json
import math
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tclean_compare import run_native  # noqa: E402

LIMIT = 2e-3
FLOOR = 1e-3
COUNTS = {"iterDone", "startIterDone", "cycleStartIters", "masksum", "mpiServer", "stopCode"}
LINE = "refim_point_withline.ms"
TWO = "refim_twopoints_twochan.ms"
POL = "refim_point_linRL.ms"
COMMON = {"fullsummary": True, "gain": 0.2, "cell": "8.0arcsec", "imsize": 100}
OUTLIER = "imagename={name}/outlier\nimsize=[60,60]\ncell=[8.0arcsec,8.0arcsec]\n" \
          "phasecenter=J2000 19:58:40.895 +40.55.58.543\n"
BAND = {"vis": LINE, "spw": "0:0~7"}
CUBE = {"vis": LINE, "specmode": "cube", "start": 8, "width": 1}
BOX = "box[[40pix,40pix],[60pix,60pix]]"
MOSAIC = {"vis": "refim_oneshiftpoint.mosaic.ms", "gridder": "mosaic", "imsize": 256,
          "cell": "10.0arcsec", "spw": "0", "phasecenter": 0}
# A case is its calls in order; each call's image name defaults to "im". CASA's
# own tests of these rows join them in `main`.
CASES = {
    "mfs": [dict(BAND, niter=30)],
    "mfs-seed": [
        dict(BAND, niter=10, imagename="a"),
        dict(BAND, niter=10, imagename="b", startmodel="{case}/a.model"),
    ],
    "mtmfs": [dict(BAND, niter=30, deconvolver="mtmfs")],
    "mtmfs-seed": [
        dict(BAND, niter=10, deconvolver="mtmfs", imagename="a"),
        dict(BAND, niter=10, deconvolver="mtmfs", imagename="b",
             startmodel=["{case}/a.model.tt0", "{case}/a.model.tt1"]),
    ],
    "mosaic-seed": [
        dict(MOSAIC, niter=20, pblimit=0.1, imagename="a"),
        dict(MOSAIC, niter=10, pblimit=0.9, imagename="b", startmodel="{case}/a.model"),
    ],
    "mosaic-seed-cube": [
        dict(MOSAIC, specmode="cube", niter=20, pblimit=0.1, imagename="a"),
        dict(MOSAIC, specmode="cube", niter=10, pblimit=0.9, imagename="b",
             startmodel="{case}/a.model"),
    ],
    "mosaic-seed-cube-no-cut": [
        dict(MOSAIC, specmode="cube", niter=20, pblimit=0.1, imagename="a"),
        dict(MOSAIC, specmode="cube", niter=10, pblimit=0.1, imagename="b",
             startmodel="{case}/a.model"),
    ],
    "clark-stokes": [{"vis": POL, "stokes": "IV", "niter": 20, "deconvolver": "clark"}],
    "clark": [dict(BAND, niter=30, deconvolver="clark")],
    "multiscale": [dict(BAND, niter=20, deconvolver="multiscale", scales=[0, 3])],
    "threshold": [dict(BAND, niter=40, threshold="0.05Jy")],
    "mask-empty": [dict(BAND, niter=10, usemask="user", mask="box[[0pix,0pix],[0pix,0pix]]")],
    "outlier": [dict(BAND, niter=20, outlierfile="{case}/outliers.txt")],
    "dirty": [dict(BAND, niter=0)],
    "dirty-mtmfs": [dict(BAND, niter=0, deconvolver="mtmfs")],
    "dirty-seed": [
        dict(BAND, niter=10, imagename="a"),
        dict(BAND, niter=0, imagename="b", startmodel="{case}/a.model"),
    ],
    "dirty-mask": [
        dict(BAND, niter=5, usemask="user", mask=BOX, imagename="a"),
        dict(BAND, niter=0, usemask="user", mask="{case}/a.mask", imagename="b"),
    ],
    "cube": [dict(CUBE, nchan=5, niter=40, deconvolver="hogbom")],
    "cube-mask": [dict(CUBE, nchan=3, niter=30, usemask="user", mask=BOX)],
    "cube-short": [dict(CUBE, nchan=3, niter=4)],
    "cube-empty-plane": [dict(CUBE, nchan=3, niter=20, usemask="user",
                              mask="box[[40pix,40pix],[60pix,60pix]], range=[1.445GHz,1.455GHz]")],
    "cube-dirty": [dict(CUBE, nchan=3, niter=0)],
    "cube-offband": [dict(CUBE, nchan=4, start=18, niter=20)],
    "threshold-cube": [dict(CUBE, nchan=3, niter=60, threshold="0.2Jy")],
    "restart-cube": [dict(CUBE, nchan=3, niter=5), dict(CUBE, nchan=3, niter=15)],
    "stokes": [{"vis": POL, "stokes": "IV", "niter": 20}],
    "stokes-cube": [{"vis": POL, "stokes": "IV", "specmode": "cube", "nchan": 1, "start": 1,
                     "width": 1, "niter": 20}],
    # Channel 0's PSF peaks away from its centre (issue 1124).
    "stokes-cube-off-centre": [{"vis": POL, "stokes": "IV", "specmode": "cube", "nchan": 2,
                                "niter": 20}],
    "nsigma": [{"vis": TWO, "niter": 30, "nsigma": 1.5, "specmode": "cube", "imsize": 64}],
    "nsigma-one-plane": [{"vis": TWO, "niter": 30, "nsigma": 1.5, "specmode": "cube", "nchan": 1,
                          "start": 1, "width": 1, "imsize": 64}],
}


def casa_test_cases():
    """The calls of CASA's own tests of these rows, from the extracted cases."""
    path = Path(__file__).resolve().parents[2] / "resources/casa-compat/tclean-casa-test-cases.json"
    cases = json.loads(path.read_text())["cases"]
    return {
        name: [{key: value for key, value in dict(case["params"], vis=case["vis"]).items()
                if key not in ("parallel", "imagename")}]
        for name, case in cases.items()
        if "fullsummary" in name
    }


def plain(value):
    if hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return value.item() if hasattr(value, "item") else str(value)


def expand(case, call, root):
    values = dict(call) if "." in case else dict(COMMON, **call)
    values["vis"] = str(root / "unittest/tclean" / values["vis"])
    values["imagename"] = f"{case}/{values.get('imagename', 'im')}"
    for key in ("startmodel", "mask", "outlierfile"):
        if isinstance(values.get(key), str):
            values[key] = values[key].replace("{case}", case)
        elif key in values:
            values[key] = [name.replace("{case}", case) for name in values[key]]
    return values


def run_case(case, calls, root, folder, call):
    (folder / case).mkdir(parents=True)
    (folder / case / "outliers.txt").write_text(OUTLIER.format(name=folder / case))
    reply = None
    for values in calls:
        reply = call(expand(case, values, root))
    (folder / f"{case}.json").write_text(json.dumps(reply))
    return reply


def differences(casa, native):
    """Each value of `native` further from `casa`'s than the limit allows,
    and the worst agreement elsewhere."""
    found, worst = [], 0.0
    for key in ("iterdone", "nmajordone", "stopcode"):
        if casa[key] != native[key]:
            found.append(f"{key}: {casa[key]} against {native[key]}")
    planes = {}
    for side, reply in (("casa", casa), ("native", native)):
        for field, channels in reply["summaryminor"].items():
            for channel, stokes in channels.items():
                for stoke, rows in stokes.items():
                    planes.setdefault((field, channel, stoke), {})[side] = rows
    for plane, sides in sorted(planes.items()):
        if len(sides) != 2:
            found.append(f"{plane}: only on {list(sides)}")
            continue
        names = set(sides["casa"]) ^ set(sides["native"])
        if names:
            found.append(f"{plane}: rows {sorted(names)} on one side only")
        for name, expected in sides["casa"].items():
            got = sides["native"].get(name, expected)
            if len(expected) != len(got):
                found.append(f"{plane} {name}: {len(expected)} against {len(got)} cycles")
                continue
            for theirs, ours in zip(expected, got):
                if math.isnan(theirs) or math.isnan(ours):
                    gap = 0.0 if math.isnan(theirs) and math.isnan(ours) else math.inf
                elif name in COUNTS:
                    gap = math.inf if theirs != ours else 0.0
                else:
                    gap = abs(theirs - ours) / max(abs(theirs), FLOOR)
                if gap > LIMIT:
                    found.append(f"{plane} {name}: {expected} against {got}")
                    break
                worst = max(worst, gap)
    return found, worst


def main():
    CASES.update(casa_test_cases())
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--casars-python", type=Path, required=True)
    parser.add_argument("--imager-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    args = parser.parse_args()
    args.output.mkdir(parents=True)
    casa_folder, native_folder = args.output / "casa", args.output / "native"
    casa_folder.mkdir()
    native_folder.mkdir()

    import casatasks

    casatasks.casalog.setlogfile(str(args.output / "casa.log"))

    def call_casa(values):
        previous = os.getcwd()
        os.chdir(casa_folder)
        try:
            return plain(casatasks.tclean(**values))
        finally:
            os.chdir(previous)

    def call_native(values):
        reply = run_native(args.casars_python, args.imager_binary,
                           dict(values, backend="cpu", gridprecision="f64"), native_folder)
        if "result" not in reply:
            raise RuntimeError(f"{values['imagename']}: {json.dumps(reply)[:1500]}")
        return reply["result"]

    summary = {}
    for case in args.case or CASES:
        casa = run_case(case, CASES[case], args.data_root, casa_folder, call_casa)
        native = run_case(case, CASES[case], args.data_root, native_folder, call_native)
        found, worst = differences(casa, native)
        summary[case] = {"passed": not found, "worst": worst, "differences": found}
        print(case, "passed" if not found else "DIFFERS", f"worst {worst:.1e}", flush=True)
        for line in found:
            print("   ", line[:240], flush=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=1))
    passed = sum(entry["passed"] for entry in summary.values())
    print(f"{passed}/{len(summary)} passed")


if __name__ == "__main__":
    main()
