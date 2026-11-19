#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture shift records and protocol behavior from CASA 6.7.6.14."""
import argparse
import copy
import json
from pathlib import Path
import resource

from casaconfig import config
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--measures-data", required=True, type=Path)
parser.add_argument("--out", required=True, type=Path)
options = parser.parse_args()
root = options.measures_data.resolve()
config.data_auto_update = config.measures_auto_update = False
config.measurespath = str(root)
config.datapath = [str(root), *config.datapath]
from casatools import measures, quanta, version_string
import numpy as np
assert version_string() == "6.7.6-14"
m = measures()
assert m.done()
q = quanta()
v = m.direction("J2000", "4h20m30s", "+30.20.30")
vec = copy.deepcopy(v)
for key in ("m0", "m1"):
    vec[key]["value"] = [vec[key]["value"], vec[key]["value"] + 0.1]
off = m.direction("ICRS", "12deg", "28deg")
offset_record = dict(v, offset=off)
double_offset = dict(v, offset=dict(off, offset=off))
tests = {
    "default": (),
    "default_angles": (v,),
    "default_pa": (v, "20arcmin"),
    "none_v": (None,"20arcmin","0deg"),
    "nonrecord_v": ([],"20arcmin","0deg"),
    "wrong_measure": (m.epoch("UTC","2011/01/03"),"1deg","0deg"),
    "malformed_v": ({},"1deg","0deg"),
    "missing_m0": ({"type":"direction","refer":"J2000","m1":v["m1"]},"1deg","0deg"),
    "scalar_numeric_offset": (v,1,"0deg"),
    "time_offset": (v,"1s","0deg"),
    "bad_offset_text": (v,"opaque","0deg"),
    "bad_pa_text": (v,"1deg","opaque"),
    "bad_offset_record": (v, {"value":1,"unit":"bogusXX"}, "0deg"),
    "bad_offset_type": (v, {"items":[1,2]}, "0deg"),
    "quantity_record_int": (v, {"value":1,"unit":"deg"}, "0deg"),
    "quantity_record_vector_1": (v, {"value":[1.],"unit":"deg"}, "0deg"),
    "quantity_record_vector_2": (v, {"value":[1.,2.],"unit":"deg"}, "0deg"),
    "quantity_vector": (v, [1.,2.], "0deg"),
    "bytes_angles": (v, b"20arcmin", b"90deg"),
    "none_angles": (v, None, None),
    "pa_time": (v, "1deg", "1s"),
    "unknown_reference": (dict(v,refer="opaque"),"1deg","0deg"),
    "vector": (vec,"1deg","0deg"),
    "offset_record": (offset_record, "20arcmin", "90deg"),
    "nested_offset": (double_offset, "20arcmin", "90deg"),
    "large_east": (m.direction("J2000","0deg","0deg"),"120deg","90deg"),
    "pole_crossing": (m.direction("J2000","0deg","80deg"),"20deg","0deg"),
    "upstream_north": (v,"20arcmin","0deg"),
    "upstream_east": (v,"20arcmin","90deg"),
    "upstream_south": (v,"20arcmin","180deg"),
}

tests.update({
    "zero_offset": (v, "0deg", "70deg"),
    "north_pole": (m.direction("J2000", "10deg", "90deg"), "5deg", "135deg"),
    "south_pole": (m.direction("J2000", "-10deg", "-90deg"), "5deg", "180deg"),
    "negative_offset": (v, "-31arcsec", "37deg"),
    "west": (v, "10arcmin", "-90deg"),
    "record_pa": (v, {"value": 4.1, "unit": "arcmin"}, {"value": 18., "unit": "deg"}),
    "galactic": (m.direction("GALACTIC", "91deg", "-35deg"), "15arcmin", "54deg"),
    "azimuth": (m.direction("AZEL", "181deg", "70deg"), "3deg", "131deg"),
    "comet_label": (dict(v, refer="COMET"), "20arcmin", "90deg"),
})

controls = {
    "large_east": (m.direction("J2000", "120deg", "0deg"), 120.),
    "pole_crossing": (m.direction("J2000", "180deg", "80deg"), 20.),
}


def clean(value):
    if isinstance(value, bytes):
        return {"__python_bytes__": list(value)}
    if isinstance(value, dict):
        return {key: clean(element) for key, element in value.items()}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [clean(element) for element in value]
    return value


cases = []
for name, arguments in tests.items():
    case = {"name": name, "args": clean(arguments)}
    try:
        result = m.shift(*arguments)
        case["result"] = clean(result)
        if name == "upstream_north":
            assert result == dict(v, m1=q.add(v["m1"], "20arcmin"))
        if name in controls:
            target, offset = controls[name]
            control_sep = m.separation(arguments[0], target)["value"]
            raw_sep = m.separation(arguments[0], result)["value"]
            assert abs(control_sep - offset) < 1e-5 and abs(raw_sep - offset) > 1.
            case["native_result"] = clean(target)
            case["separation_control_deg"] = {"requested": offset, "casa": raw_sep, "target": control_sep}
    except (RuntimeError, TypeError, ValueError) as error:
        case["error"] = {"type": type(error).__name__, "message": str(error)}
    cases.append(case)

m.done()
q.done()
options.out.write_text(json.dumps(
    {"casa_version": version_string(), "cases": cases}, indent=2, allow_nan=False
) + "\n")
print(f"{len(cases)} CASA shift cases")
