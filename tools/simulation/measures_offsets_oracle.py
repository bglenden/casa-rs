#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 output reference and constructor offsets."""
import argparse
import json
import math
from pathlib import Path

from casaconfig import config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--measures-data", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
config.data_auto_update = config.measures_auto_update = False
config.measurespath = str(args.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]

from casatools import measures, quanta, version_string

assert version_string() == "6.7.6-14"
tool = measures()
qa = quanta()
assert tool.done()
frame = [
    tool.observatory("ALMA"),
    tool.epoch("UTC", "2011/01/03/17:00:00"),
    tool.direction("J2000", "10deg", "20deg"),
]
assert all(tool.doframe(element) for element in frame)


def clean(value):
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if hasattr(value, "tolist"):
        return clean(value.tolist())
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    return value


def vector(record):
    return {
        key: dict(value, value=[float(value["value"]), float(value["value"]) * 1.1])
        if key.startswith("m") else value
        for key, value in record.items()
    }


def record(name, method, arguments):
    return {
        "name": name,
        "method": method,
        "args": arguments,
        "result": clean(getattr(tool, method)(*arguments)),
    }


def magnetic_record(reference, quantities):
    strength, longitude, latitude = [
        qa.convert(value, unit)["value"]
        for value, unit in zip(quantities, ("nT", "rad", "rad"), strict=True)
    ]
    xyz = [
        strength * math.cos(latitude) * math.cos(longitude),
        strength * math.cos(latitude) * math.sin(longitude),
        strength * math.sin(latitude),
    ]
    value = {f"m{i}": {"value": item, "unit": "nT"} for i, item in enumerate(xyz)}
    return tool.measure(dict(value, refer=reference, type="earthmagnetic"), reference)


def magnetic_xyz(value):
    return [value[key]["value"] for key in ("m0", "m1", "m2")]


def magnetic_offset_control(source, origin, target, result):
    # The previously recorded CASA field-norm defect applies to each
    # independently rotated vector. Restore both norms before subtracting.
    absolute = tool.measure(source, target)
    offset = tool.measure(origin, target)
    def restore_norm(before, after):
        xyz = magnetic_xyz(after)
        factor = math.hypot(*magnetic_xyz(before)) / math.hypot(*xyz)
        return [part * factor for part in xyz]
    v = restore_norm(source, absolute)
    o = restore_norm(origin, offset)
    control = {f"m{i}": {"unit": "nT", "value": a - b}
               for i, (a, b) in enumerate(zip(v, o, strict=True))}
    return dict(result, **control), [absolute, offset]


cases = []
types = [
    ("epoch", "UTC", ("2011/01/03/17:00:00",), ("2011/01/01/17:00:00",), "TAI"),
    ("direction", "J2000", ("10deg", "20deg"), ("30deg", "-6deg"), "ICRS"),
    ("position", "ITRF", ("100m", "200m", "300m"), ("10m", "20m", "30m"), "WGS84"),
    ("frequency", "LSRK", ("1GHz",), ("1MHz",), "BARY"),
    ("radialvelocity", "LSRK", ("1km/s",), ("10m/s",), "BARY"),
    ("doppler", "RADIO", ("10km/s",), ("100m/s",), "BETA"),
    ("baseline", "ITRF", ("100m", "200m", "300m"), ("10m", "20m", "30m"), "J2000"),
    ("uvw", "J2000", ("100m", "200m", "300m"), ("10m", "20m", "30m"), "ICRS"),
    ("earthmagnetic", "ITRF", ("3000nT", "10deg", "20deg"),
     ("14nT", "-8deg", "3deg"), "J2000"),
]
bad = {"m0": {"unit": "m", "value": 1.0}}
for kind, refer, source, origin, target in types:
    method = getattr(tool, kind)
    v, off = method(refer, *source), method(refer, *origin)
    if kind == "earthmagnetic":
        # Keep the separate Cartesian control from the geometry oracle.
        v, off = magnetic_record(refer, source), magnetic_record(refer, origin)
    assert v and off, (kind, v, off)
    wrong = frame[0] if kind == "epoch" else frame[1]
    for label, measure, output, offset in (
        ("measure_same", v, refer, off),
        ("measure_cross", v, target, off),
        ("measure_nested", v, refer, dict(off, offset=off)),
        ("measure_vector", vector(v), refer, vector(off)),
        ("measure_both", dict(v, offset=off), refer, off),
        ("measure_malformed", v, refer, bad),
        ("measure_mismatch", v, refer, wrong),
    ):
        case = record(f"{kind}_{label}", "measure", [measure, output, offset])
        if kind == "earthmagnetic" and label == "measure_cross":
            case["native_result"], case["rotation_controls"] = magnetic_offset_control(
                v, off, target, case["result"],
            )
        cases.append(case)
    for label, offset in (
        ("define_offset", off), ("define_malformed", bad), ("define_mismatch", wrong)
    ):
        case = record(f"{kind}_{label}", kind, [refer, *source, offset])
        if kind == "uvw" and label == "define_offset":
            case["native_result"] = clean(tool.measure(v, refer, off))
        if kind == "earthmagnetic" and case["result"]:
            case["native_result"] = dict(v, offset=off) if label == "define_offset" else v
        cases.append(case)

for ref in ("R_UTC", "R_LAST", "r_tai", "R_INVALID"):
    cases.append(record(f"epoch_define_{ref}", "epoch", [ref, "2011/01/03/17:00:00"]))
    cases.append(record(f"epoch_measure_{ref}", "measure", [frame[1], ref]))
raw_raze = dict(frame[1], refer="R_UTC")
cases.append(record("epoch_raze_on_input", "measure", [raw_raze, "LAST"]))
cases.append(record("epoch_raze_same_reference", "measure", [raw_raze, "R_UTC"]))

for name, value in (
    ("epoch", frame[1]),
    ("epoch_raze", tool.epoch("R_LAST", "2011/01/03/17:00:00")),
    ("position", tool.position("ITRF", "100m", "200m", "300m")),
    ("geodetic_position", tool.position("WGS84", "17deg", "-40deg", "5061m")),
    ("frequency", tool.frequency("REST", "1GHz")),
    ("frequency_with_offset", tool.frequency("BARY", "1GHz", tool.frequency("BARY", "1MHz"))),
):
    cases.append(record("dirshow_" + name, "dirshow", [value]))

for name, operands in (
    ("cartesian", [("m", [100., 200.]), ("m", [200., 100.]), ("m", [300., 400.])]),
    ("length_first", [("m", [100., 200.]), ("deg", [2., 12.]), ("deg", [16., 35.])]),
    ("angle_first", [("deg", [2., 12.]), ("deg", [16., 35.]), ("m", [100., 200.])]),
    ("one", [("m", [100.]), ("m", [200.]), ("m", [300.])]),
    ("mismatch", [("m", [100., 200.]), ("m", [200.]), ("m", [300., 400.])]),
):
    arg = ["ITRF", *[{"unit": unit, "value": values} for unit, values in operands]]
    cases.append(record("position_vector_" + name, "position", arg))
    if name == "cartesian":
        origin = tool.position("ITRF", "10m", "20m", "30m")
        cases.append(record("position_vector_with_offset", "position", [*arg, origin]))

raw_direction = tool.direction("J2000", "10deg", "20deg")
direction_with_offset = dict(raw_direction, offset=tool.direction("J2000", "30deg", "-6deg"))
icrs = tool.direction("ICRS", "16deg", "-20deg")
icrs_with_offset = dict(icrs, offset=tool.direction("ICRS", "47deg", "-25deg"))
other = tool.direction("J2000", "22deg", "-8deg")
for method in ("separation", "posangle"):
    for name, first, second in (
        ("same_reference_offset", direction_with_offset, other),
        ("cross_reference_offset", raw_direction, icrs_with_offset),
        ("vector_first", vector(raw_direction), other),
        ("solar_system", tool.direction("MOON"), other),
        ("two_models", tool.direction("SUN"), tool.direction("MOON")),
    ):
        cases.append(record(f"{method}_{name}", method, [first, second]))
cases.append(record("source_with_offset", "source", [direction_with_offset]))

relative_frame = [
    tool.position("ITRF", "100m", "200m", "300m",
                  tool.position("ITRF", "1m", "20m", "10m")),
    tool.epoch("UTC", "2011/01/03/17:00:00", tool.epoch("UTC", "2011/01/01/17:00:00")),
    tool.direction("J2000", "10deg", "20deg", tool.direction("J2000", "30deg", "-6deg")),
]
tool.done()
assert all(tool.doframe(element) for element in relative_frame)
for name, method, arguments in (
    ("showframe_with_offsets", "showframe", []),
    ("epoch_offset_absolute", "measure", [relative_frame[1], "UTC"]),
    ("frequency_with_frame_offsets", "measure", [tool.frequency("LSRK", "1GHz"), "BARY"]),
    ("ambient_direction_with_frame_offsets", "measure", [tool.direction("APP", "10deg", "20deg"), "J2000"]),
):
    case = record(name, method, arguments)
    case["frame"] = relative_frame
    cases.append(case)
tool.done()
vector_frame = [
    vector(frame[1]),
    vector(tool.position("ITRF", "100m", "200m", "300m")),
    vector(raw_direction),
]
assert all(tool.doframe(element) for element in vector_frame)
case = record("showframe_with_vectors", "showframe", [])
case["frame"] = vector_frame
cases.append(case)
tool.done()
for model in ("SUN", "PLUTO"):
    model_direction = dict(tool.direction(model, "10deg", "20deg"),
                           offset=tool.direction(model, "13deg", "7deg"))
    model_frame = [*frame[:2], model_direction]
    assert all(tool.doframe(element) for element in model_frame)
    for case in (
        record(f"{model}_frame_display", "showframe", []),
        record(f"{model}_frame_barycentric_frequency", "measure",
               [tool.frequency("LSRK", "1GHz"), "BARY"]),
    ):
        case["frame"] = model_frame
        cases.append(case)
    tool.done()

for refer, origin in (("J2000", "MOON"), ("APP", "J2000")):
    assert all(tool.doframe(element) for element in frame)
    direction = dict(tool.direction(refer, "10deg", "20deg"),
                     offset=tool.direction(origin))
    case = record(f"frame_{refer}_{origin}_origin_refused", "doframe", [direction])
    assert case["result"] is False
    cases.append(case)
    tool.done()
    assert all(tool.doframe(element) for element in frame)
    case = record(f"measure_{refer}_{origin}_origin_resolved", "measure", [direction, refer])
    assert case["result"]
    cases.append(case)
    tool.done()

for x in (42000, 42500, 42700, 43500):
    cartesian = tool.position("ITRF", f"{x}m", "0m", "1m")
    cases.append(record(f"position_near_geocentre_{x}", "measure", [cartesian, "WGS84"]))

args.out.write_text(json.dumps({"casa_version": version_string(), "frame": frame, "cases": cases},
                               indent=2, allow_nan=False) + "\n")
print(len(cases), "CASA offset cases")
