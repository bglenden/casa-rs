#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 showframe strings and independent velocity controls."""
import argparse
import json
from pathlib import Path

from casaconfig import config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--measures-data", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
config.data_auto_update = config.measures_auto_update = False
config.measurespath = str(args.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]

from casatools import measures, version_string

assert version_string() == "6.7.6-14"
m = measures()
epoch = m.epoch("UTC", "2011/01/03/17:00:00")
tai = m.epoch("TAI", {"value": 55564.123456, "unit": "d"})
position = m.observatory("ALMA")
itrf = m.position("ITRF", "100m", "200m", "300m")
direction = m.direction("J2000", "10deg", "20deg")
galactic = m.direction("GALACTIC", "44deg", "-31deg")
azel = m.direction("AZEL", "135deg", "45deg")
velocity = m.radialvelocity("LSRK", "15km/s")
velocity_offset = dict(velocity, offset=m.radialvelocity("LSRK", "1km/s"))
ganymede = "JPL-Horizons/Ganymede_55437-56293dUTC.tab"

states = {
    "fresh": [],
    "empty": ["done"],
    "epoch": ["done", epoch],
    "tai": ["done", tai],
    "position": ["done", position],
    "itrf": ["done", itrf],
    "direction": ["done", direction],
    "galactic": ["done", galactic],
    "azel_missing": ["done", azel],
    "azel_with_frame": ["done", epoch, position, azel],
    "velocity": ["done", velocity],
    "velocity_offset": ["done", velocity_offset],
    "all": ["done", epoch, position, direction, velocity, ganymede],
    "comet_only": ["done", ganymede],
    "comet_failed": ["done", ganymede, "absent-data.tab"],
    "comet_after_done": ["done", ganymede, "done"],
    "replacement": ["done", tai, direction, epoch, galactic],
    "inverse_activation_order": ["done", velocity, direction, position, epoch, ganymede],
}
corrected = {}
for reference in ["BARY", "LSRD", "GEO", "TOPO", "GALACTO", "LGROUP", "CMB"]:
    value = m.radialvelocity(reference, "15.123km/s")
    states[f"velocity_{reference}_missing"] = ["done", value]
    case = f"velocity_{reference}_explicit"
    states[case] = ["done", epoch, position, direction, value]
    corrected[case] = value


def set_element(tool, element):
    if element == "done":
        return tool.done()
    if isinstance(element, str):
        return tool.framecomet(element)
    return tool.doframe(element)


cases = []
for name, state in states.items():
    tool = measures()
    case = {"name": name, "setup": state}
    if name in corrected:
        assert all(set_element(tool, step) for step in state[:-1])
        base_text = tool.showframe()
        value = corrected[name]
        control = tool.measure(value, "LSRK")
        assert control["refer"] == "LSRK" and control["m0"]["unit"] == "m/s"
        rendered = format(control["m0"]["value"] / 1e3, ".6g")
        case["velocity_control"] = control
        case["native_result"] = (
            base_text + "\n       Radialvelocity: 15123\n"
            f"        (LSR velocity = {rendered} km/s)"
        )
        tool.done()
    case["setup_results"] = [set_element(tool, step) for step in state]
    try:
        case["result"] = tool.showframe()
    except RuntimeError as error:
        case["error"] = {"type": type(error).__name__, "message": str(error)}
    if name == "fresh":
        case["native_result"] = "Frame: "
        case["owner_decision"] = "The native tool starts with an explicit empty frame."
    if name in corrected:
        assert "error" in case
    cases.append(case)
    tool.done()

# Read every state before changing the next field to test cache invalidation.
tool = measures()
assert tool.done()
transitions = []
for element in [
    "done", epoch, position, direction, velocity, ganymede,
    "absent-data.tab", "JPL-Horizons/Ganymede_55197-59214dUTC_J2000.tab",
    tai, "done",
]:
    result = set_element(tool, element)
    transitions.append({"element": element, "setup_result": result, "result": tool.showframe()})
tool.done()
m.done()
args.out.write_text(json.dumps({
    "casa_version": version_string(), "cases": cases, "transitions": transitions,
}, indent=2) + "\n")
print(f"{len(cases)} CASA frame-display states, {len(transitions)} state transitions")
