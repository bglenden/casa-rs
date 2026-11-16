# SPDX-License-Identifier: LGPL-3.0-or-later
"""Stateful CASA 6.7.6.14 simobserve project controls (SIM-24/25).

Run in CASA Python with --out, --casars-bin and --atmosphere-model.
Reuse the task parity harness's numeric/product gates; compare preservation
within each implementation before comparing successful products between them.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np

import simobserve_parity as parity


BASE = dict(parity.SD_CORE, obsmode="int", sdantlist="", antennalist="vla.d.cfg",
            incenter="8GHz", inwidth="20MHz", totaltime="600s", mapsize=["1arcsec", "1arcsec"])
CASES = [
    dict(name="continue_manual", seed=dict(thermalnoise="tsys-manual"),
         changes=dict(obsmode="", thermalnoise="tsys-manual",
         leakage=0.04, tau0=0.065, t_sky=20.0, t_ground=275.0), replaces_noise=True),
    dict(name="continue_atmosphere", seed=dict(antennalist="alma.out01.cfg", incenter="230GHz",
         inwidth="15.625MHz", direction="J2000 10h00m00 -23d00m00",
         indirection="J2000 10h00m00 -23d00m00"),
         changes=dict(obsmode="", thermalnoise="tsys-atm", leakage=0.02, user_pwv=0.8, t_ground=269.0)),
    dict(name="continue_rest", seed=dict(outframe="REST"),
         changes=dict(obsmode="", thermalnoise="tsys-manual", t_sky=20.0)),
    dict(name="continue_undefined_request", seed=dict(outframe="BARY"),
         changes=dict(obsmode="", outframe="Undefined", thermalnoise="", leakage=0.03)),
    dict(name="continue_single_dish", seed=dict(obsmode="sd", antennalist="", sdantlist="aca.tp.cfg",
         incenter="230GHz", inwidth="1GHz", totaltime="3600s"),
         changes=dict(obsmode="", thermalnoise="tsys-manual", leakage=0.02)),
    dict(name="overwrite_refused", changes=dict(overwrite=False, inbright="0.5Jy/pixel"), refusal=True),
    dict(name="continue_overwrite_refused", changes=dict(obsmode="", overwrite=False,
         thermalnoise="tsys-manual"), refusal=True),
    dict(name="overwrite_replaces", changes=dict(inbright="0.5Jy/pixel"), replaces=True),
    dict(name="missing_ms", changes=dict(obsmode="", thermalnoise="tsys-manual"), missing=True, refusal=True),
    dict(name="ambiguous_lists", changes=dict(obsmode="", sdantlist="aca.tp.cfg"), refusal=True),
    dict(name="input_model_alias", changes=dict(obsmode="", thermalnoise="tsys-manual"),
         refusal=True, input_alias=True),
    dict(name="relative_path", changes=dict(obsmode="", thermalnoise="tsys-manual"), path="relative"),
    dict(name="absolute_path", changes=dict(obsmode="", thermalnoise="tsys-manual"), path="absolute"),
]


@contextlib.contextmanager
def directory(path):
    previous = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def snapshot(path):
    """Keep actual file bytes in memory; locks are transient, not products."""
    return {str(p.relative_to(path)): p.read_bytes() for p in Path(path).rglob("*")
            if p.is_file() and p.name != "table.lock"}


def science(t, project):
    """Original MS scientific columns must survive continuation exactly."""
    ms = parity.stem_of(str(project), ".ms") or parity.stem_of(str(project), ".sd.ms")
    if not ms:
        return {}
    tb = t["tb"]
    tb.open(ms)
    try:
        result = {key: np.array(tb.getcol(key), copy=True)
                  for key in ("DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA", "TIME", "UVW")}
    finally:
        tb.close()
    tb.open(ms + "/SPECTRAL_WINDOW")
    try:
        result.update({key: np.array(tb.getcol(key), copy=True)
                       for key in ("CHAN_FREQ", "CHAN_WIDTH", "MEAS_FREQ_REF")})
    finally:
        tb.close()
    return result


def noise_data(t, project):
    ms = parity.stem_of(str(project), ".noisy.ms")
    t["tb"].open(ms)
    try:
        return np.array(t["tb"].getcol("DATA"), copy=True)
    finally:
        t["tb"].close()


def run(t, side, work, project, params, args, tag):
    params = dict(parity.absolute(params, args.out), project=project, graphics="none")
    params.setdefault("overwrite", True)
    with directory(work):
        if side == "casa":
            from casatasks import simobserve
            try:
                result = simobserve(**params)
                return {"status": "succeeded", "returned": result}
            except Exception as error:
                return {"status": "failed", "error": str(error)}
        params = parity.casars_params(t, params)
        if params["skymodel"] == str(Path(args.out) / "inputs" / "sd-sky.image"):
            params["skymodel"] = str(Path(args.out) / "native-inputs" / "sd-sky.image")
        request = Path(work) / (tag + ".json")
        request.write_text(json.dumps({"surface": "simobserve", "values": params}))
        with (Path(work) / (tag + ".log")).open("w") as log:
            reply = subprocess.run([args.casars_bin, "--json-run", str(request)],
                                   stdout=subprocess.PIPE, stderr=log, text=True, check=False)
        parsed = json.loads(reply.stdout)
        return {"status": parsed["status"], "error": parsed.get("message")}


def one_case(t, case, args):
    name = case["name"]
    seed = dict(BASE, **case.get("seed", {}))
    final = dict(seed, **case["changes"])
    paths, before, outcomes = {}, {}, {}
    report = {"name": name, "seed": seed, "changes": case["changes"], "differences": []}
    differ = report["differences"].append
    for side in ("casa", "casars"):
        work = Path(args.out) / side
        work.mkdir(exist_ok=True)
        project = name
        if side == "casars" and case.get("path"):
            parent = work / "nested"
            parent.mkdir(exist_ok=True)
            project = str(Path("nested") / name)
            if case["path"] == "absolute":
                project = str(parent / name)
        dest = work / project
        shutil.rmtree(dest, ignore_errors=True)
        paths[side] = str(dest)
        if not case.get("missing"):
            result = run(t, side, work, project, seed, args, name + "-seed")
            if result["status"] != "succeeded":
                raise RuntimeError(f"{name}/{side} seed: {result}")
        (dest / "retained.txt").parent.mkdir(exist_ok=True)
        (dest / "retained.txt").write_text("An unrelated project product must survive.\n")
        before[side] = science(t, dest)
        noise_before = noise_data(t, dest) if case.get("replaces_noise") else None
        frozen = snapshot(dest)
        input_before = snapshot(Path(args.out) / ("inputs" if side == "casa" else "native-inputs"))
        invocation = dict(final)
        if case.get("input_alias"):
            invocation["skymodel"] = parity.stem_of(str(dest), ".skymodel")
        outcomes[side] = run(t, side, work, project, invocation, args, name + "-final")
        if input_before != snapshot(Path(args.out) / ("inputs" if side == "casa" else "native-inputs")):
            differ(f"{side}: input image changed")
        if (dest / "retained.txt").read_text() != "An unrelated project product must survive.\n":
            differ(f"{side}: unrelated product changed")
        after = science(t, dest)
        if case.get("input_alias") and side == "casa":
            report["casa_deleted_input"] = not Path(invocation["skymodel"]).exists()
        elif case.get("refusal") and not case.get("missing"):
            if frozen != snapshot(dest):
                differ(f"{side}: rejected operation changed project bytes")
        elif final["obsmode"] == "":
            if before[side].keys() != after.keys() or any(
                not np.array_equal(value, after[key]) for key, value in before[side].items()
            ):
                differ(f"{side}: continuation changed original MS science")
        if case.get("replaces") and np.array_equal(before[side]["DATA"], after["DATA"]):
            differ(f"{side}: overwrite did not replace DATA")
        if noise_before is not None and np.array_equal(noise_before, noise_data(t, dest)):
            differ(f"{side}: continuation did not replace existing noisy product")
    report["outcomes"] = outcomes
    if case.get("refusal"):
        a, b = outcomes["casa"], outcomes["casars"]
        if a["status"] != "failed" or b["status"] != "failed":
            differ("both tasks must refuse")
        elif case.get("input_alias"):
            expected = (f"skymodel {parity.stem_of(paths['casars'], '.skymodel')} "
                        "is the model image simobserve writes; copy it elsewhere first")
            if not report["casa_deleted_input"] or b["error"] != expected:
                differ("input-alias correction does not match the documented CASA loss/native refusal")
        elif a["error"] != b["error"]:
            differ(f"refusal message: {a['error']!r} against {b['error']!r}")
        return report
    if any(result["status"] != "succeeded" for result in outcomes.values()):
        differ("both tasks must succeed")
        return report
    # CASA deliberately gates leakage on predict=True. Continuation must
    # preserve that behavior, not invent a fullpol product or leakage draw.
    effective = dict(final)
    if final["obsmode"] == "":
        effective.update(obsmode=seed["obsmode"], leakage=0.0)
    comparison = parity.compare_case(t, {"name": name, "params": effective},
                                    paths["casa"], paths["casars"], args.out, args)
    report["comparison"] = comparison
    detail = Path(args.out) / "compare" / name / "report.json"
    if detail.exists():
        report["measurement_sets"] = json.loads(detail.read_text())
    report["differences"].extend(comparison["differences"])
    report["original_ms_preserved"] = final["obsmode"] == ""
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--casars-bin", required=True)
    parser.add_argument("--atmosphere-model", required=True)
    parser.add_argument("--only", nargs="*")
    args = parser.parse_args()
    args.out = os.path.abspath(args.out)
    args.casars_bin = os.path.abspath(args.casars_bin)
    args.atmosphere_model = os.path.abspath(args.atmosphere_model)
    args.uvw_tolerance_m = 1e-3
    Path(args.out).mkdir(parents=True, exist_ok=True)
    parity.copy_inputs(".", args.out, [{"params": BASE}])
    native = Path(args.out) / "native-inputs" / "sd-sky.image"
    if not native.exists():
        shutil.copytree(Path(args.out) / "inputs" / "sd-sky.image", native)
    t = parity.tools()
    reports = []
    for case in CASES:
        if args.only and case["name"] not in args.only:
            continue
        report = one_case(t, case, args)
        reports.append(report)
        print(f"{case['name']}: {report['differences'] or 'agree'}", flush=True)
        (Path(args.out) / "report.json").write_text(json.dumps(reports, indent=1) + "\n")
    return int(any(report["differences"] for report in reports))


if __name__ == "__main__":
    raise SystemExit(main())
