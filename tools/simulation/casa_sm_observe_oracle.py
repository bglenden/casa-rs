#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA oracle for simulation Phase 1c (#841): observe-only `sm` MSs.

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3`).
For each scenario in `sm_observe_scenarios.json` the script creates an MS with
CASA's `sm` tool: `setconfig` (from a CASA `.cfg` through
`simutil.readantenna`, or local coordinates), then the scenario's calls, then
`close`. It writes `<out>/<scenario>.ms` and `<out>/manifest.json`.

The manifest records every call with its arguments resolved to plain numbers:
quantities in SI units, J2000 directions in radians, UTC epochs as MJD days and
antenna positions as ITRF metres. The casa-rs driver runs the same calls from
those numbers, so it needs no CASA string parsing, and `compare_sm_ms.py`
compares the two MSs.
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
    for call in scenario["calls"]:
        op, kwargs, resolved = resolve_call(call, me, qa)
        call_started = time.perf_counter()
        returned = getattr(sm, op)(**kwargs)
        calls.append(
            {
                "op": op,
                "resolved": resolved,
                "returned": bool(returned),
                "seconds": time.perf_counter() - call_started,
            }
        )
    sm.close()
    calls.append({"op": "close", "resolved": {}, "returned": True})
    return {
        "name": scenario["name"],
        "ms": ms,
        "ignore": scenario.get("ignore", []),
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
        "purpose": "simulation Phase 1c observe-only oracle (#841)",
        "casa_tai_minus_utc_s": 37.0,
        "scenarios": results,
    }
    with open(os.path.join(args.out, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
