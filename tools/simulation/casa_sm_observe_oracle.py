#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA oracle for simulation Phases 1c and 1d (#841): `sm` MSs.

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3`).
For each scenario in a scenario file (`sm_observe_scenarios.json` by default,
`sm_predict_scenarios.json` for prediction and noise, `sm_noise_scenarios.json`
for `tsys-manual` noise) the script creates an MS
with CASA's `sm` tool: `setconfig` (from a CASA `.cfg` through
`simutil.readantenna`, or local coordinates), then the scenario's calls, then
`close`. It writes `<out>/<scenario>.ms` and `<out>/manifest.json`.

The manifest records every call with its arguments resolved to plain numbers:
quantities in SI units, J2000 directions in radians, UTC epochs as MJD days and
antenna positions as ITRF metres. The casa-rs driver runs the same calls from
those numbers, so it needs no CASA string parsing, and `compare_sm_ms.py`
compares the two MSs. Each call records what CASA returned, or the exception
it raised.

A scenario file's `complists` (`sm_components_scenarios.json`) are written
with CASA's `cl` tool under `<out>/complists/` before any scenario runs, and
`predict`'s `complist` names one of them; CASA reads a list read-only, so the
manifest records that table for casa-rs too.

`predict` names its model images relative to `--model-root`. CASA's predict
rewrites the image it reads (its spectral units and frame, in `table.dat`), so
CASA predicts from a copy in `<out>/`, and the manifest records the pristine
image for casa-rs. The copies are made, and every call's arguments resolved,
before the scenario's clock starts at `open`, so its `seconds` (and each
call's) time only CASA's `sm` work. A scenario's `data` (`zero`, `model` or `noisy`), `twin`
(the noise-free scenario a noisy one is compared with) and `noise_variance`
(the variance its noise should have, Jy²) are copied to the manifest for
`compare_sm_ms.py`.

A two-stage scenario (`sm_openfromms_scenarios.json`) calls `close` and then
`openfromms` among its calls: the first stage's MS is copied to
`<out>/<scenario>.stage1.ms` before `openfromms` reopens it, and the later
calls rewrite its rows. Its `openfromms_ms` (`casa-rs` by default) tells the
casa-rs driver which first stage to reopen: its own, or (`casa`) a copy of
CASA's, so that only the later calls' DATA differs.

A scenario with `close_aborts` runs in a child process (`run_in_child`):
CASA 6.7.6's `sm.close()` aborts the process after a mosaic predict, once
DATA is written. The child turns the abort into a quiet exit, so macOS
shows no crash report; the abort is accepted only when every `predict`
returned True, and `close` is recorded as aborted.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import shutil
import signal
import subprocess
import sys
import time

from casatasks.private import simutil
from casatools import componentlist, measures, quanta, simulator

HERE = os.path.dirname(os.path.abspath(__file__))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="output directory (replaced)")
    parser.add_argument(
        "--scenarios",
        default=os.path.join(HERE, "sm_observe_scenarios.json"),
        help="scenario file",
    )
    parser.add_argument(
        "--config-root",
        default=os.path.expanduser("~/.casa/data/alma/simmos"),
        help="directory holding CASA antenna configuration files",
    )
    parser.add_argument(
        "--model-root",
        default=os.getcwd(),
        help="directory the scenarios' predict image names are relative to",
    )
    parser.add_argument("--only", nargs="*", help="run only these scenarios")
    parser.add_argument(
        "--child",
        help="run this one scenario into the parent oracle's --out (run_in_child)",
    )
    return parser.parse_args()


QUANTITY_UNITS = {
    "freq": "Hz",
    "deltafreq": "Hz",
    "freqresolution": "Hz",
    "integrationtime": "s",
    "starttime": "s",
    "stoptime": "s",
    "elevationlimit": "rad",
    "simplenoise": "Jy",
    "pwv": "mm",
}


def resolve_config(config: dict, args: argparse.Namespace, me) -> dict:
    """The setconfig arguments, and their resolved form."""
    mount = config.get("mount", "alt-az")
    if "cfg" in config:
        path = os.path.join(args.config_root, config["cfg"])
        util = simutil.simutil()
        x, y, z, diam, pads, names, telescope, posobs = util.readantenna(path)
        position = me.measure(posobs, "ITRF")
        kwargs = dict(
            # A configuration may name another telescope for the same pads
            # (EVLA for a VLA .cfg).
            telescopename=config.get("telescopename", telescope),
            x=list(map(float, x)),
            y=list(map(float, y)),
            z=list(map(float, z)),
            dishdiameter=list(map(float, diam)),
            mount=[mount],
            antname=list(names),
            padname=list(pads),
            coordsystem="global",
            referencelocation=posobs,
        )
        resolved = dict(kwargs, referencelocation=itrf_metres(position), cfg=config["cfg"])
        return {"kwargs": kwargs, "resolved": resolved}
    local = config["local"]
    reference = me.observatory(local["referencelocation"])
    kwargs = dict(
        telescopename=local["telescopename"],
        x=local["x"],
        y=local["y"],
        z=local["z"],
        dishdiameter=local["dishdiameter"],
        mount=[mount],
        antname=local["antname"],
        padname=local["padname"],
        coordsystem="local",
        referencelocation=reference,
    )
    resolved = dict(kwargs, referencelocation=itrf_metres(me.measure(reference, "ITRF")))
    return {"kwargs": kwargs, "resolved": resolved}


def itrf_metres(position: dict) -> list[float]:
    qa = quanta()
    lon = position["m0"]["value"] if position["m0"]["unit"] == "rad" else qa.convert(position["m0"], "rad")["value"]
    lat = position["m1"]["value"] if position["m1"]["unit"] == "rad" else qa.convert(position["m1"], "rad")["value"]
    radius = qa.convert(position["m2"], "m")["value"]
    import math

    return [
        radius * math.cos(lat) * math.cos(lon),
        radius * math.cos(lat) * math.sin(lon),
        radius * math.sin(lat),
    ]


def model_images(names, args: argparse.Namespace, out_prefix: str) -> tuple[list[str], list[str]]:
    """The copies CASA predicts from and the pristine images, for `imagename`."""
    names = [names] if isinstance(names, str) else list(names)
    copies, pristine = [], []
    for index, name in enumerate(names):
        if not name:
            copies.append(name)
            pristine.append(name)
            continue
        source = os.path.abspath(os.path.join(args.model_root, name))
        copy = f"{out_prefix}.model{index}.image"
        shutil.rmtree(copy, ignore_errors=True)
        shutil.copytree(source, copy)
        copies.append(copy)
        pristine.append(source)
    return copies, pristine


def resolve_call(call: dict, me, qa) -> tuple[str, dict, dict]:
    """CASA keyword arguments and their resolved numbers for one call."""
    op = call["op"]
    kwargs = {key: value for key, value in call.items() if key != "op"}
    resolved = dict(kwargs)
    for key, unit in QUANTITY_UNITS.items():
        if key in kwargs:
            resolved[key] = qa.convert(qa.quantity(kwargs[key]), unit)["value"]
    if "sourcedirection" in kwargs:
        direction = me.direction(*kwargs["sourcedirection"])
        kwargs["sourcedirection"] = direction
        j2000 = me.measure(direction, "J2000")
        resolved["sourcedirection"] = [j2000["m0"]["value"], j2000["m1"]["value"]]
    # observemany's lists: times as seconds, and each direction (a string,
    # as CASA takes it) as J2000 radians, or None where it is empty.
    for key in ("starttimes", "stoptimes"):
        if key in kwargs:
            resolved[key] = [qa.convert(qa.quantity(value), "s")["value"] for value in kwargs[key]]
    if "directions" in kwargs:
        resolved["directions"] = []
        for text in kwargs["directions"]:
            if not text:
                resolved["directions"].append(None)
                continue
            j2000 = me.measure(me.direction(*text.split()), "J2000")
            resolved["directions"].append([j2000["m0"]["value"], j2000["m1"]["value"]])
    if "referencetime" in kwargs:
        epoch = me.epoch(*kwargs["referencetime"])
        kwargs["referencetime"] = epoch
        resolved["referencetime"] = me.measure(epoch, "UTC")["m0"]["value"]
    return op, kwargs, resolved


def run_scenario(
    scenario: dict, args: argparse.Namespace, record_path: str | None = None
) -> dict:
    """Run `scenario`'s calls and `close`, writing its record to
    `record_path` first when given."""
    me = measures()
    qa = quanta()
    ms = os.path.join(args.out, scenario["name"] + ".ms")
    # The inputs are resolved, and the models CASA predicts from copied,
    # before the clock starts (#853 review: a cube's copy is about 1 GiB of
    # work casa-rs does not do).
    config = resolve_config(scenario["config"], args, me)
    resolved_calls = []
    for number, call in enumerate(scenario["calls"]):
        op, kwargs, resolved = resolve_call(call, me, qa)
        if op == "predict":
            prefix = os.path.join(args.out, f"{scenario['name']}.call{number}")
            kwargs["imagename"], resolved["imagename"] = model_images(
                kwargs.get("imagename", []), args, prefix
            )
            if kwargs.get("complist"):
                # CASA reads a list read-only, so both sides read the one
                # the oracle wrote.
                path = os.path.join(os.path.abspath(args.out), "complists", kwargs["complist"])
                kwargs["complist"] = resolved["complist"] = path
        resolved_calls.append((op, kwargs, resolved))
    sm = simulator()
    calls = []
    started = time.perf_counter()
    returned = sm.open(ms)
    calls.append({"op": "open", "resolved": {"ms": ms}, "returned": bool(returned)})
    returned = sm.setconfig(**config["kwargs"])
    calls.append({"op": "setconfig", "resolved": config["resolved"], "returned": bool(returned)})
    for op, kwargs, resolved in resolved_calls:
        call_started = time.perf_counter()
        record = {"op": op, "resolved": resolved}
        try:
            if op == "close":
                # A two-stage scenario's first stage: its MS is kept as
                # `<name>.stage1.ms`, which casa-rs reopens for an
                # `openfromms_ms: casa` scenario.
                record["returned"] = bool(sm.close())
                stage_one = ms[: -len(".ms")] + ".stage1.ms"
                shutil.rmtree(stage_one, ignore_errors=True)
                shutil.copytree(ms, stage_one)
                record["resolved"] = {"stage1": stage_one}
            elif op == "openfromms":
                record["returned"] = bool(sm.openfromms(ms))
                record["resolved"] = {"ms": ms}
            else:
                record["returned"] = bool(getattr(sm, op)(**kwargs))
        except RuntimeError as error:
            record["returned"] = None
            record["raised"] = str(error).split("\n", 1)[0]
        record["seconds"] = time.perf_counter() - call_started
        calls.append(record)
    result = {
        "name": scenario["name"],
        "ms": ms,
        "ignore": scenario.get("ignore", []),
        "data": scenario.get("data", "zero"),
        "twin": scenario.get("twin"),
        "noise_variance": scenario.get("noise_variance"),
        "openfromms_ms": scenario.get("openfromms_ms", "casa-rs"),
        "calls": calls,
        "seconds": time.perf_counter() - started,
    }
    if record_path is not None:
        with open(record_path, "w", encoding="utf-8") as handle:
            json.dump(result, handle)
    sm.close()
    calls.append({"op": "close", "resolved": {}, "returned": True})
    result["seconds"] = time.perf_counter() - started
    return result


def exit_quietly_on_abort() -> None:
    """Make SIGABRT end the process with `_exit(SIGABRT)`: a C handler, so
    `abort()` never returns to its default action, and macOS reports no
    crash. A Python handler would not do: `abort()` raises again when one
    returns."""
    libc = ctypes.CDLL(None)
    libc.signal.argtypes = [ctypes.c_int, ctypes.c_void_p]
    libc.signal.restype = ctypes.c_void_p
    libc.signal(signal.SIGABRT, ctypes.cast(libc._exit, ctypes.c_void_p))


def run_in_child(scenario: dict, args: argparse.Namespace) -> dict:
    """Run a `close_aborts` scenario in a process of its own. CASA 6.7.6's
    `sm.close()` aborts the process after a mosaic predict ("Pure virtual
    function called!") once DATA is written (`docs/CASA (C++) bugs.md`), so
    the child saves the scenario's record before `close`, and exits with
    status SIGABRT through `exit_quietly_on_abort`. The abort is accepted only
    when every `predict` returned True; `close` is then recorded as
    aborted."""
    record = os.path.join(args.out, scenario["name"] + ".record.json")
    command = [
        sys.executable,
        os.path.abspath(__file__),
        "--out", args.out,
        "--scenarios", args.scenarios,
        "--config-root", args.config_root,
        "--model-root", args.model_root,
        "--child", scenario["name"],
    ]
    completed = subprocess.run(command, check=False)
    if not os.path.exists(record):
        raise SystemExit(f"{scenario['name']}: the child exited {completed.returncode} before close")
    with open(record, encoding="utf-8") as handle:
        result = json.load(handle)
    os.remove(record)
    if completed.returncode == 0:
        result["calls"].append({"op": "close", "resolved": {}, "returned": True})
        return result
    predicts = [call for call in result["calls"] if call["op"] == "predict"]
    if completed.returncode != signal.SIGABRT or not all(
        call.get("returned") is True for call in predicts
    ):
        raise SystemExit(
            f"{scenario['name']}: close exited {completed.returncode} after predicts {predicts}"
        )
    result["calls"].append(
        {"op": "close", "resolved": {}, "returned": None, "aborted": completed.returncode}
    )
    return result


def write_complists(specs: dict, out: str) -> None:
    """Write each of the scenario file's `complists` with CASA's `cl` tool:
    a name, under `<out>/complists/`, and the keyword arguments of each
    `cl.addcomponent`, a `dir` given as `[frame, longitude, latitude]`."""
    me = measures()
    root = os.path.join(out, "complists")
    os.makedirs(root, exist_ok=True)
    for name, components in specs.items():
        cl = componentlist()
        for spec in components:
            kwargs = dict(spec)
            if "dir" in kwargs:
                kwargs["dir"] = me.direction(*kwargs["dir"])
            cl.addcomponent(**kwargs)
        cl.rename(os.path.join(root, name))
        cl.close()
        cl.done()


def main() -> int:
    args = parse_args()
    with open(args.scenarios, encoding="utf-8") as handle:
        document = json.load(handle)
    scenarios = document["scenarios"]
    if args.child:
        exit_quietly_on_abort()
        scenario = next(scenario for scenario in scenarios if scenario["name"] == args.child)
        run_scenario(scenario, args, os.path.join(args.out, args.child + ".record.json"))
        return 0
    shutil.rmtree(args.out, ignore_errors=True)
    os.makedirs(args.out)
    write_complists(document.get("complists", {}), args.out)
    results = []
    for scenario in scenarios:
        if args.only and scenario["name"] not in args.only:
            continue
        if scenario.get("close_aborts"):
            result = run_in_child(scenario, args)
        else:
            result = run_scenario(scenario, args)
        results.append(result)
        print(result["name"], f"{result['seconds']:.2f} s")
    manifest = {
        "schema_version": 1,
        "purpose": f"simulation Phase 1 sm oracle (#841): {os.path.basename(args.scenarios)}",
        "casa_tai_minus_utc_s": 37.0,
        "scenarios": results,
    }
    with open(os.path.join(args.out, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
