#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA oracle for simulation Phases 1c and 1d (#841): `sm` MSs.

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3`).
For each scenario in a scenario file (`sm_observe_scenarios.json` by default,
`sm_predict_scenarios.json` for prediction and noise) the script creates an MS
with CASA's `sm` tool: `setconfig` (from a CASA `.cfg` through
`simutil.readantenna`, or local coordinates), then the scenario's calls, then
`close`. It writes `<out>/<scenario>.ms` and `<out>/manifest.json`.

The manifest records every call with its arguments resolved to plain numbers:
quantities in SI units, J2000 directions in radians, UTC epochs as MJD days and
antenna positions as ITRF metres. The casa-rs driver runs the same calls from
those numbers, so it needs no CASA string parsing, and `compare_sm_ms.py`
compares the two MSs. Each call records what CASA returned, or the exception
it raised.

`predict` names its model images relative to `--model-root`. CASA's predict
rewrites the image it reads (its spectral units and frame, in `table.dat`), so
CASA predicts from a copy in `<out>/`, and the manifest records the pristine
image for casa-rs. A scenario's `data` (`zero`, `model` or `noisy`), `twin`
(the noise-free scenario a noisy one is compared with) and `noise_variance`
(the variance its noise should have, Jy²) are copied to the manifest for
`compare_sm_ms.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time

from casatasks.private import simutil
from casatools import measures, quanta, simulator

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
            telescopename=telescope,
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
    if "referencetime" in kwargs:
        epoch = me.epoch(*kwargs["referencetime"])
        kwargs["referencetime"] = epoch
        resolved["referencetime"] = me.measure(epoch, "UTC")["m0"]["value"]
    return op, kwargs, resolved


def run_scenario(scenario: dict, args: argparse.Namespace) -> dict:
    me = measures()
    qa = quanta()
    ms = os.path.join(args.out, scenario["name"] + ".ms")
    sm = simulator()
    calls = []
    started = time.perf_counter()
    returned = sm.open(ms)
    calls.append({"op": "open", "resolved": {"ms": ms}, "returned": bool(returned)})
    config = resolve_config(scenario["config"], args, me)
    returned = sm.setconfig(**config["kwargs"])
    calls.append({"op": "setconfig", "resolved": config["resolved"], "returned": bool(returned)})
    for number, call in enumerate(scenario["calls"]):
        op, kwargs, resolved = resolve_call(call, me, qa)
        if op == "predict":
            prefix = os.path.join(args.out, f"{scenario['name']}.call{number}")
            kwargs["imagename"], resolved["imagename"] = model_images(
                kwargs.get("imagename", []), args, prefix
            )
        call_started = time.perf_counter()
        record = {"op": op, "resolved": resolved}
        try:
            record["returned"] = bool(getattr(sm, op)(**kwargs))
        except RuntimeError as error:
            record["returned"] = None
            record["raised"] = str(error).split("\n", 1)[0]
        record["seconds"] = time.perf_counter() - call_started
        calls.append(record)
    sm.close()
    calls.append({"op": "close", "resolved": {}, "returned": True})
    return {
        "name": scenario["name"],
        "ms": ms,
        "ignore": scenario.get("ignore", []),
        "data": scenario.get("data", "zero"),
        "twin": scenario.get("twin"),
        "noise_variance": scenario.get("noise_variance"),
        "calls": calls,
        "seconds": time.perf_counter() - started,
    }


def main() -> int:
    args = parse_args()
    with open(args.scenarios, encoding="utf-8") as handle:
        scenarios = json.load(handle)["scenarios"]
    shutil.rmtree(args.out, ignore_errors=True)
    os.makedirs(args.out)
    results = []
    for scenario in scenarios:
        if args.only and scenario["name"] not in args.only:
            continue
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
