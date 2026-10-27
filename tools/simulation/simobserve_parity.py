#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA `simobserve` parity for casa-rs's (simulation Phase 5c, #841).

Run under CASA 6.7.6's Python:

    /Applications/CASA.app/Contents/MacOS/python3 tools/simulation/simobserve_parity.py \\
        --out OUT --casars-bin TARGET/release/simobserve \\
        --casatestdata /Volumes/home/casatestdata \\
        --atmosphere-model TARGET/release/examples/at_tool_quantities

Each case of `CASES` runs CASA's `simobserve` in `OUT/casa` and casa-rs's
task binary (`crates/casars-simulation`, through `--json-run`) in
`OUT/casars`, on the same inputs, copied from casatestdata to `OUT/inputs`.
Both find the antenna configurations in CASA's data directory
(`~/.casa/data/alma/simmos`). A case CASA refuses must be refused by
casa-rs with CASA's message, leaving the same files. The comparison
checks:

- **the files** each leaves in its project directory, without the graphics
  (`.png`, `.quick.psf`), which come in Phase 5e;
- **the pointing file**, `<stem>.ptg.txt`, byte for byte;
- **the images** (`.skymodel`, `.compskymodel` and their flat images):
  shape, brightness unit, coordinate values to 1e-12, pixels to 1e-6 of
  the peak;
- **the calibrator's component list**: its flux, direction, frequency,
  shape and label;
- **each MeasurementSet**, through `compare_sm_ms.py`: the noise-free one
  as a `model` scenario, the leaked `.fullpol.ms` as `leakage` against it,
  and the noisy one as `noisy` against the MeasurementSet it was copied
  from, each side's noise over the σ of the Tsys term `simobserve` sets
  (its efficiencies from CASA's `simutil.noisetemp`).

CASA's simulator stores TAI under the UTC label (`docs/CASA (C++) bugs.md`),
so casa-rs's run asks for an hour angle later by TAI − UTC at the reference
epoch, which CASA's measures give for each case.

Where casa-rs departs from CASA's task, the case says so and the reference
is CASA's own simulator doing what the task meant: a case with a
calibrator (`repredict`) has CASA's MeasurementSet predicted again over
every field, the calibrator's included, from the model image and the
component lists merged into one (`docs/CASA (C++) bugs.md`, "simobserve's
calibrator"). A file casa-rs does not write on purpose is listed in the
case's `casa_only`.

The report goes to `OUT/report.json`; the exit status is 1 when any case
differs.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))

# casatestdata's inputs, copied to OUT/inputs.
INPUTS = {
    "input50pc_672GHz.fits": "regression/sim_components_and_skymodel/input50pc_672GHz.fits",
    "core5ps.skymodel": "unittest/simobserve/core5ps.skymodel",
    "core5ps.clist": "unittest/simobserve/core5ps.clist",
    "flatone.model": "unittest/simobserve/flatone.model",
    "flatone.single.ptg.txt": "unittest/simobserve/flatone.single.ptg.txt",
}

PPDISK_VLA = {
    "skymodel": "inputs/input50pc_672GHz.fits",
    "inbright": "3e-5Jy/pixel",
    "incenter": "44GHz",
    "inwidth": "128MHz",
    "setpointings": True,
    "integration": "2s",
    "direction": "J2000 18h00m00.031s -22d59m59.6s",
    "mapsize": "0.78arcsec",
    "obsmode": "int",
    "antennalist": "vla.a.cfg",
    "hourangle": "transit",
    "totaltime": "3600s",
}
CORE = {
    "skymodel": "inputs/core5ps.skymodel",
    "setpointings": True,
    "integration": "10s",
    "maptype": "ALMA",
    "obsmode": "int",
    "antennalist": "alma.out01.cfg",
    "totaltime": "300s",
    "thermalnoise": "",
}

# The cases: CASA's parameters (graphics off, overwrite on), with paths
# relative to OUT. `complist_from` makes a component list in CASA first.
CASES = [
    # The VLA protoplanetary-disk CASA Guide (CASA 5.7.0), noiseless
    # (`psimvla1`) and with tsys-manual noise (`psimvla3`).
    {"name": "psimvla1", "params": dict(PPDISK_VLA, thermalnoise="")},
    {"name": "psimvla3", "params": dict(PPDISK_VLA, thermalnoise="tsys-manual", tau0=0.065, t_sky=20.0)},
    # CASA's regression `test_regression_sim_components_and_skymodel`
    # (`psim2`): the ppdisk at 672 GHz with a star, ALMA's out20, tsys-atm.
    {
        "name": "psim2",
        "complist_from": {"name": "star672GHz.cl", "dir": "J2000 18h00m00.031s -22d59m59.6s", "flux": 0.0003, "freq": "672GHz"},
        "params": {
            "skymodel": "inputs/input50pc_672GHz.fits",
            "direction": "J2000 18h00m00.031s -22d59m59.6s",
            "complist": "star672GHz.cl",
            "setpointings": True,
            "mapsize": "0.76arcsec",
            "pointingspacing": "0.5arcsec",
            "integration": "10s",
            "obsmode": "int",
            "antennalist": "alma.out20.cfg",
            "refdate": "2012/06/21/03:25:00",
            "totaltime": "1200s",
            "thermalnoise": "tsys-atm",
            "user_pwv": 0.5,
            "t_ground": 269.0,
            "maptype": "ALMA2012",
            "verbose": True,
        },
    },
    # Components alone, an ALMA resolution string, tsys-atm noise.
    {
        "name": "comp_atm",
        "params": {
            "complist": "inputs/core5ps.clist",
            "compwidth": "8GHz",
            "setpointings": True,
            "integration": "10s",
            "maptype": "hexagonal",
            "obsmode": "int",
            "antennalist": "alma;0.5arcsec",
            "totaltime": "300s",
            "thermalnoise": "tsys-atm",
        },
    },
    # Leakage, then tsys-manual noise on the leaked copy.
    {"name": "leak_manual", "params": dict(CORE, leakage=0.02, thermalnoise="tsys-manual")},
    # Leakage, then tsys-atm noise: CASA leaves `.noisier.T.cal`.
    {"name": "leak_atm", "params": dict(CORE, leakage=0.02, thermalnoise="tsys-atm"), "casa_only": [".noisier.T.cal"]},
    # A calibrator, for two whole passes over a small map.
    {
        "name": "calibrator",
        "repredict": True,
        "params": dict(
            CORE, mapsize=["20arcsec", "20arcsec"], totaltime="2", caldirection="J2000 19h00m00 -22d00m00",
            calflux="1Jy",
        ),
    },
    # The calibrator with a component list as well: two lists to predict.
    {
        "name": "calibrator_lists",
        "repredict": True,
        "params": dict(
            CORE, complist="inputs/core5ps.clist", mapsize=["20arcsec", "20arcsec"], totaltime="2",
            caldirection="J2000 19h00m00 -22d00m00", calflux="1Jy",
        ),
    },
    # A single dish.
    {
        "name": "single_dish",
        # An hour, so the noise statistics have samples enough.
        "params": dict(
            CORE, obsmode="sd", sdantlist="aca.tp.cfg", sdant=0, maptype="square", thermalnoise="tsys-atm",
            totaltime="3600s",
        ),
    },
    # Pointings read from a file, with a time of their own.
    {
        "name": "pointing_file",
        "params": dict(
            CORE, setpointings=False, ptgfile="inputs/flatone.single.ptg.txt", integration="2s", totaltime="60s"
        ),
    },
    # A model whose 4″ cells the configuration cannot sample: both refuse
    # with CASA's message, and remove the flat model.
    {
        "name": "coarse_cell",
        "params": dict(CORE, skymodel="inputs/flatone.model"),
    },
]

GRAPHICS = (".png", ".quick.psf")


def tools():
    from casatools import componentlist, image, measures, quanta, simulator, table

    return {
        "cl": componentlist(),
        "ia": image(),
        "me": measures(),
        "qa": quanta(),
        "sm": simulator(),
        "tb": table(),
    }


def tai_minus_utc_s(t, refdate: str) -> float:
    """TAI − UTC at `refdate`'s 00:00 UTC, the reference epoch of
    simobserve's `settimes`, from CASA's measures data; rounded to the
    millisecond, below which the MJD subtraction is noise."""
    date = re.match(r"(\d*/\d+/\d+)", refdate).group(1)
    utc = t["me"].epoch("UTC", date + "/00:00:00")
    tai = t["me"].measure(utc, "TAI")
    return round((tai["m0"]["value"] - utc["m0"]["value"]) * 86400.0, 3)


def hour_angle_s(t, hourangle: str) -> float:
    """simobserve's `haoffset`."""
    qa = t["qa"]
    if hourangle == "transit":
        return 0.0
    offset = None
    if qa.isquantity(hourangle + "h") and qa.compare(hourangle + "h", "s"):
        offset = qa.convert(qa.quantity(hourangle + "h"), "s")["value"]
    if qa.isquantity(hourangle):
        seconds = qa.convert(hourangle, "s")
        if qa.compare(seconds, "s"):
            offset = seconds["value"]
    return offset


def casars_params(t, params: dict) -> dict:
    """`params` with the hour angle later by TAI − UTC (see the module
    documentation)."""
    shifted = dict(params)
    refdate = params.get("refdate", "2014/01/01")
    offset = hour_angle_s(t, params.get("hourangle", "transit")) + tai_minus_utc_s(t, refdate)
    shifted["hourangle"] = f"{offset!r}s"
    return shifted


def make_complist(t, spec: dict, where: str) -> None:
    cl = t["cl"]
    path = os.path.join(where, spec["name"])
    shutil.rmtree(path, ignore_errors=True)
    cl.done()
    cl.addcomponent(dir=spec["dir"], flux=spec["flux"], freq=spec["freq"])
    cl.rename(path)
    cl.done()


def casa_params(case: dict, out: str) -> dict:
    """The case's parameters as CASA's run takes them: the inputs under
    `OUT/inputs` and a `complist_from` list in `OUT/casa`, all absolute, so
    they do not depend on the caller's working directory."""
    params = absolute(case["params"], out)
    if "complist_from" in case:
        params["complist"] = os.path.join(out, "casa", case["complist_from"]["name"])
    return params


def run_casa(t, case: dict, out: str) -> str:
    from casatasks import simobserve

    work = os.path.join(out, "casa")
    os.makedirs(work, exist_ok=True)
    params = casa_params(case, out)
    if "complist_from" in case:
        make_complist(t, case["complist_from"], work)
    cwd = os.getcwd()
    os.chdir(work)
    try:
        shutil.rmtree(case["name"], ignore_errors=True)
        simobserve(project=case["name"], graphics="none", overwrite=True, **params)
    finally:
        os.chdir(cwd)
    return os.path.join(work, case["name"])


def run_casars(t, case: dict, out: str, binary: str) -> tuple[str, dict]:
    """casa-rs's task binary on the case, through `--json-run`: its stem and
    elapsed time, or the message it refused the case with."""
    work = os.path.join(out, "casars")
    os.makedirs(work, exist_ok=True)
    params = absolute(case["params"], out)
    if "complist_from" in case:
        params["complist"] = os.path.join(out, "casa", case["complist_from"]["name"])
    params = casars_params(t, params)
    params.update(project=case["name"], graphics="none", overwrite=True)
    shutil.rmtree(os.path.join(work, case["name"]), ignore_errors=True)
    request = os.path.join(work, case["name"] + ".json")
    with open(request, "w", encoding="utf-8") as handle:
        json.dump({"kind": "run", "request": params}, handle, indent=1)
    command = [binary, "--json-run", request, "--managed-output", "true"]
    log_path = os.path.join(work, case["name"] + ".log")
    with open(log_path, "w", encoding="utf-8") as log:
        result = subprocess.run(command, cwd=work, stdout=subprocess.PIPE, stderr=log, text=True, check=False)
    if result.returncode == 0:
        report = json.loads(result.stdout)
        outcome = {"stem": report["run"]["stem"], "elapsed_s": report["run"]["elapsed_ns"] / 1e9}
    else:
        prefix = "Error: task execution failed: "
        with open(log_path, encoding="utf-8") as log:
            errors = [line[len(prefix):].rstrip("\n") for line in log if line.startswith(prefix)]
        outcome = {"error": errors[-1] if errors else f"exit status {result.returncode}"}
    return os.path.join(work, case["name"]), outcome


def absolute(params: dict, out: str) -> dict:
    """`params` with the input paths made absolute."""
    resolved = dict(params)
    for key in ("skymodel", "complist", "ptgfile"):
        if resolved.get(key, "").startswith("inputs/"):
            resolved[key] = os.path.join(out, resolved[key])
    return resolved


def products(project: str) -> list[str]:
    return sorted(name for name in os.listdir(project) if not name.endswith(GRAPHICS))


def stem_of(project: str, suffix: str) -> str | None:
    """The product `<stem><suffix>`, when it exists; the stem is the
    pointing file's, which every run writes."""
    stems = [name[: -len(".ptg.txt")] for name in os.listdir(project) if name.endswith(".ptg.txt")]
    if len(stems) != 1:
        return None
    path = os.path.join(project, stems[0] + suffix)
    return path if os.path.exists(path) else None


def compare_images(t, casa: str, casars: str) -> list[str]:
    ia = t["ia"]
    differences = []

    def read(path):
        ia.open(path)
        try:
            cs = ia.coordsys()
            record = {
                "shape": list(ia.shape()),
                "unit": ia.brightnessunit(),
                "refval": cs.referencevalue()["numeric"],
                "refpix": cs.referencepixel()["numeric"],
                "incr": cs.increment()["numeric"],
                "pixels": ia.getchunk(),
            }
            cs.done()
            return record
        finally:
            ia.close()

    a, b = read(casa), read(casars)
    name = os.path.basename(casa)
    for key in ("shape", "unit"):
        if a[key] != b[key]:
            differences.append(f"{name}: {key} {a[key]} against {b[key]}")
    for key in ("refval", "refpix", "incr"):
        if len(a[key]) != len(b[key]) or not np.allclose(a[key], b[key], rtol=1e-12, atol=0):
            differences.append(f"{name}: {key} {list(a[key])} against {list(b[key])}")
    # A NaN would make the largest difference NaN, which no bound fails.
    finite = True
    for side, record in (("CASA", a), ("casa-rs", b)):
        nonfinite = int(np.count_nonzero(~np.isfinite(record["pixels"])))
        if nonfinite:
            differences.append(f"{name}: {side} has {nonfinite} pixels that are not finite")
            finite = False
    if finite and a["pixels"].shape == b["pixels"].shape:
        peak = float(np.abs(a["pixels"]).max())
        largest = float(np.abs(a["pixels"] - b["pixels"]).max())
        if largest > 1e-6 * peak:
            differences.append(f"{name}: pixels differ by {largest:.3g}, peak {peak:.3g}")
    return differences


def compare_components(t, casa: str, casars: str) -> list[str]:
    cl = t["cl"]

    def read(path):
        cl.open(path)
        try:
            component = cl.getcomponent(0)
            return {
                "count": cl.length(),
                "flux": list(component["flux"]["value"]),
                "direction": [component["shape"]["direction"]["m0"]["value"], component["shape"]["direction"]["m1"]["value"]],
                "refer": component["shape"]["direction"]["refer"],
                "frequency_hz": t["qa"].convert(component["spectrum"]["frequency"]["m0"], "Hz")["value"],
                "shape": component["shape"]["type"],
                "label": component["label"],
            }
        finally:
            cl.close()

    a, b = read(casa), read(casars)
    name = os.path.basename(casa)
    differences = []
    for key in ("count", "refer", "shape", "label"):
        if a[key] != b[key]:
            differences.append(f"{name}: {key} {a[key]!r} against {b[key]!r}")
    for key in ("flux", "direction", "frequency_hz"):
        if not np.allclose(a[key], b[key], rtol=1e-12, atol=1e-15):
            differences.append(f"{name}: {key} {a[key]} against {b[key]}")
    return differences


def repredict(t, case: dict, project: str, out: str) -> None:
    """CASA's `.ms` predicted again over every field, from its model image
    and its component lists merged into one: what `simobserve` meant. The
    lists are the run's (`casa_params`), resolved against `OUT`."""
    sm, cl = t["sm"], t["cl"]
    ms = stem_of(project, ".ms")
    complist = casa_params(case, out).get("complist")
    lists = [path for path in (complist, stem_of(project, ".cal.cclist")) if path]
    merged = None
    if lists:
        merged = os.path.join(project, "merged.cl")
        shutil.rmtree(merged, ignore_errors=True)
        cl.done()
        for path in lists:
            cl.concatenate(path)
        cl.rename(merged)
        cl.done()
    image = stem_of(project, ".skymodel")
    sm.openfromms(ms)
    sm.setdata(fieldid=[])
    sm.setvp(dovp=True, usedefaultvp=False)
    sm.predict(imagename=image or "", complist=merged or "")
    sm.done()
    if merged:
        shutil.rmtree(merged)


def noise_term(t, params: dict, ms: str, uvmode: bool) -> dict:
    """The resolved `setnoise` term simobserve sets on `ms` (see
    `compare_sm_ms.tsys_term`)."""
    from casatasks.private.simutil import simutil

    tb = t["tb"]
    tb.open(ms + "/OBSERVATION")
    telescope = tb.getcol("TELESCOPE_NAME")[0]
    tb.close()
    tb.open(ms + "/SPECTRAL_WINDOW")
    frequency = float(tb.getcol("REF_FREQUENCY")[0])
    tb.close()
    eta_p, eta_s, eta_b, eta_t, eta_q, t_rx = simutil().noisetemp(
        telescope=telescope, freq=f"{frequency!r}Hz", correlator=params.get("correlator", "")
    )
    term = {
        "mode": params["thermalnoise"],
        "spillefficiency": float(eta_s),
        "correfficiency": float(eta_q),
        "antefficiency": float(eta_p * eta_s * eta_b * eta_t),
        "trx": float(t_rx),
        "tground": float(params.get("t_ground", 270.0)),
        "tcmb": 2.725,
        "senscoeff": -1.0 if uvmode else 1.0,
    }
    if term["mode"] == "tsys-manual":
        term.update(tau=float(params.get("tau0", 0.1)), tatmos=float(params.get("t_sky", 260.0)))
    else:
        term.update(pwv=float(params.get("user_pwv", 0.5)), relhum=20.0)
    return term


def scenarios(t, case: dict, casa: str, casars: str, links: str) -> list[dict]:
    """The comparator's scenarios for the case's MeasurementSets, with
    casa-rs's linked into `links` under the scenarios' names."""
    params = case["params"]
    found = []

    def add(name, suffix, **extra):
        casa_ms, casars_ms = stem_of(casa, suffix), stem_of(casars, suffix)
        if casa_ms is None or casars_ms is None:
            return None
        os.symlink(casars_ms, os.path.join(links, name + ".ms"))
        found.append(dict({"name": name, "ms": casa_ms}, **extra))
        return name

    uvmode = params.get("obsmode") != "sd"
    plain = add(case["name"], ".ms" if uvmode else ".sd.ms", data="model")
    source = plain
    if params.get("leakage", 0) > 0:
        table = os.path.basename(stem_of(casa, ".fullpol.cal") or "")
        target = stem_of(casars, ".fullpol.cal")
        if target:
            os.symlink(target, os.path.join(links, table))
        source = add(
            case["name"] + "-fullpol", ".fullpol.ms", data="leakage", twin=plain,
            # `setleakage(amplitude=a)`: the scale (a, a) of the real and
            # imaginary parts.
            leakage_amplitude=[params["leakage"], params["leakage"]], tables=[table],
        )
    if params.get("thermalnoise"):
        suffix = ".noisy.sd.ms" if not uvmode else (".noisier.ms" if params.get("leakage", 0) > 0 else ".noisy.ms")
        casa_noisy = stem_of(casa, suffix)
        if casa_noisy:
            term = noise_term(t, params, casa_noisy, uvmode)
            add(case["name"] + "-noisy", suffix, data="noisy", twin=source,
                calls=[{"op": "setnoise", "returned": True, "resolved": term}])
    return found


def compare_case(t, case: dict, casa: str, casars: str, out: str, args) -> dict:
    report = {"name": case["name"], "differences": []}
    differ = report["differences"].append
    casa_files = [name for name in products(casa) if not any(name.endswith(only) for only in case.get("casa_only", []))]
    if casa_files != products(casars):
        differ(f"files: CASA {casa_files} against casa-rs {products(casars)}")
    for suffix in (".ptg.txt",):
        a, b = stem_of(casa, suffix), stem_of(casars, suffix)
        if a and b:
            with open(a, "rb") as one, open(b, "rb") as two:
                if one.read() != two.read():
                    differ(f"{os.path.basename(a)}: not byte-identical")
    for suffix in (".skymodel", ".skymodel.flat", ".compskymodel", ".compskymodel.flat"):
        a, b = stem_of(casa, suffix), stem_of(casars, suffix)
        if a and b:
            report["differences"].extend(compare_images(t, a, b))
    a, b = stem_of(casa, ".cal.cclist"), stem_of(casars, ".cal.cclist")
    if a and b:
        report["differences"].extend(compare_components(t, a, b))
    compare = os.path.join(out, "compare", case["name"])
    shutil.rmtree(compare, ignore_errors=True)
    links = os.path.join(compare, "casars")
    os.makedirs(links)
    with open(os.path.join(compare, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump({"scenarios": scenarios(t, case, casa, casars, links)}, handle, indent=1)
    command = [sys.executable, os.path.join(HERE, "compare_sm_ms.py"), "--casa", compare, "--casars", links,
               "--json", os.path.join(compare, "report.json")]
    if args.atmosphere_model:
        command += ["--atmosphere-model", args.atmosphere_model]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
    report["ms"] = result.stdout.strip().splitlines()
    if result.returncode != 0:
        differ("MeasurementSets: " + " | ".join(line for line in report["ms"] if not line.startswith("    known")))
    return report


def copy_inputs(casatestdata: str, out: str) -> None:
    inputs = os.path.join(out, "inputs")
    os.makedirs(inputs, exist_ok=True)
    for name, path in INPUTS.items():
        target = os.path.join(inputs, name)
        if os.path.exists(target):
            continue
        source = os.path.join(casatestdata, path)
        if os.path.isdir(source):
            shutil.copytree(source, target)
        else:
            shutil.copyfile(source, target)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True)
    parser.add_argument("--casars-bin", required=True, help="casa-rs's simobserve task binary")
    parser.add_argument("--casatestdata", required=True, help="a casatestdata clone")
    parser.add_argument("--atmosphere-model", help="casa-atmosphere's at_tool_quantities example, for tsys-atm")
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--skip-casa", action="store_true", help="reuse OUT/casa")
    args = parser.parse_args()
    out = os.path.abspath(args.out)
    copy_inputs(args.casatestdata, out)
    t = tools()
    reports = []
    for case in CASES:
        if args.only and case["name"] not in args.only:
            continue
        casa = os.path.join(out, "casa", case["name"])
        error_file = casa + ".error"
        casa_s = None
        if not args.skip_casa:
            if os.path.exists(error_file):
                os.remove(error_file)
            started = time.perf_counter()
            try:
                casa = run_casa(t, case, out)
                casa_s = time.perf_counter() - started
                if case.get("repredict"):
                    repredict(t, case, casa, out)
            except Exception as error:  # noqa: BLE001 - CASA's task raises what it refuses
                with open(error_file, "w", encoding="utf-8") as handle:
                    handle.write(str(error))
        casa_error = open(error_file, encoding="utf-8").read() if os.path.exists(error_file) else None
        casars, outcome = run_casars(t, case, out, os.path.abspath(args.casars_bin))
        if casa_error is not None or "error" in outcome:
            report = {"name": case["name"], "differences": [], "refused": casa_error}
            if casa_error != outcome.get("error"):
                report["differences"].append(f"CASA raised {casa_error!r}, casa-rs {outcome.get('error')!r}")
            elif products(casa) != products(casars):
                report["differences"].append(f"files: CASA {products(casa)} against casa-rs {products(casars)}")
        else:
            report = compare_case(t, case, casa, casars, out, args)
        # The task's own times: CASA's simobserve call, casa-rs's run report.
        report["seconds"] = {"casa": casa_s, "casa-rs": outcome.get("elapsed_s")}
        reports.append(report)
        status = "OK" if not report["differences"] else f"{len(report['differences'])} differences"
        timing = "".join(
            f", {side} {seconds:.1f} s" for side, seconds in report["seconds"].items() if seconds is not None
        )
        print(f"{case['name']}: {status}{timing}")
        for line in report.get("ms", []):
            print("   ", line)
        for line in report["differences"]:
            print("   !", line)
    with open(os.path.join(out, "report.json"), "w", encoding="utf-8") as handle:
        json.dump(reports, handle, indent=1)
    return 1 if any(report["differences"] for report in reports) else 0


if __name__ == "__main__":
    sys.exit(main())
