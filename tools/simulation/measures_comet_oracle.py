#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture CASA 6.7.6.14's MeasComet tool states and values."""
import argparse
import json
import math
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory

import numpy as np
from casaconfig import config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--measures-data", required=True, type=Path)
parser.add_argument("--out", type=Path)
parser.add_argument("--without-epoch", choices=["cometdist", "cometangdiam"])
parser.add_argument("--after-done", action="store_true")
args = parser.parse_args()

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
config.data_auto_update = config.measures_auto_update = False
config.measurespath = str(args.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]

from casatools import measures, table, version_string

assert version_string() == "6.7.6-14"
GANYMEDE = "JPL-Horizons/Ganymede_55437-56293dUTC.tab"
EPHEMERIDES = Path(config.measurespath) / "ephemerides"
os.environ["CASA_RS_TEST_COMET_ROOT"] = str(EPHEMERIDES)

if args.without_epoch:
    tool = measures()
    assert tool.done() and tool.framecomet(str(EPHEMERIDES / GANYMEDE))
    if args.after_done:
        assert tool.doframe(tool.epoch("UTC", "2011/01/03/17:00:00"))
        assert tool.done() and tool.cometname() == "Ganymede"
    print("loaded the comet table with an empty frame", flush=True)
    print(getattr(tool, args.without_epoch)(), flush=True)
    raise SystemExit(0)

assert args.out is not None
QUERIES = ("cometname", "comettype", "comettopo", "cometdist", "cometangdiam")
SYNTHETIC = {
    "columns": {
        "MJD": [55564., 55565., 55566.],
        "RA": [359., 1., 2.],
        "DEC": [12., 14., 17.],
        "Rho": [5., 5.01, 5.03],
        "RadVel": [0.01, .02, .02],
    },
    "keywords": {
        "MJD0": 55563., "dMJD": 1., "NAME": "Test object",
        "GeoDist": 6378.25, "GeoLong": -67.25, "GeoLat": 25.5,
        "meanrad": {"unit": "km", "value": 2400.},
        "VS_CREATE": "2013/02/20/10:04", "VS_DATE": "2013/02/20/18:18",
        "VS_TYPE": "Table of comet/planetary positions", "VS_VERSION": "0003.0001",
    },
    "variants": {
        "topocentric": {},
        "explicit_topocentric": {"posrefsys": "TOPO"},
        "explicit_j2000": {"posrefsys": "ICRF/J2000.0"},
        "explicit_icrs": {"posrefsys": "ICRS"},
        "explicit_b1950": {"posrefsys": "B1950"},
        "no_radius": {"meanrad": None},
        "bad_grid": {"MJD0": 55000.},
        "bad_version_date": {"VS_DATE": "not a date"},
        "one_sample": {},
        "numeric_version_date": {"VS_DATE": "55564"},
        "time_unit_version_date": {"VS_DATE": "1s"},
        "zero_range": {},
        "nonfinite_direction": {},
        "bad_optional_column": {},
        "nonconformant_radius": {"meanrad": {"unit": "s", "value": 1.}},
        "integer_metadata": {
            "MJD0": 55563, "dMJD": 1, "GeoDist": 6378, "GeoLat": 25, "GeoLong": -67,
            "meanrad": {"unit": "km", "value": 2400},
        },
    },
    "row_counts": {"one_sample": 1},
    "column_overrides": {
        "zero_range": {"Rho": [0., 5.01, 5.03]},
        "nonfinite_direction": {"RA": [None, 1., 2.]},
        "bad_optional_column": {"DiskLong": ["invalid", "invalid", "invalid"]},
    },
}


def make_table(path, changes, count, variant):
    columns = SYNTHETIC["columns"] | SYNTHETIC["column_overrides"].get(variant, {})
    metadata = SYNTHETIC["keywords"] | changes
    tb = table()
    descriptors = {name: {"valueType": "string" if isinstance(values[0], str) else "double"}
                   for name, values in columns.items()}
    assert tb.create(str(path), descriptors, nrow=count)
    for name, values in columns.items():
        if isinstance(values[0], str):
            for row, value in enumerate(values[:count]):
                tb.putcell(name, row, value)
        else:
            tb.putcol(name, np.asarray(values[:count], dtype=float))
    tb.putkeywords({name: value for name, value in metadata.items() if value is not None})
    tb.putinfo({"type": "IERS"})
    tb.close()


def clean(value):
    if isinstance(value, np.ndarray):
        return [clean(item) for item in value.tolist()]
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def query_steps():
    return [(query,) for query in QUERIES]


def epoch(value, refer="UTC"):
    if value.endswith("d"):
        value = {"value": float(value[:-1]), "unit": "d"}
    return ("set_epoch", refer, value)


def explicit(file=GANYMEDE, form="text"):
    return ("framecomet", {"ephemeris": file, "form": form})


def run_case(name, steps, synthetic):
    tool = measures()
    assert tool.done()
    output = []
    for method, *values in steps:
        entry = {"method": method, "args": values}
        if method == "set_epoch":
            result = tool.doframe(tool.epoch(*values))
        elif method == "set_alma":
            result = tool.doframe(tool.observatory("ALMA"))
        else:
            def decode(value):
                if not isinstance(value, dict) or not {"ephemeris", "synthetic"} & value.keys():
                    return value
                if "synthetic" in value:
                    return str(synthetic / (value["synthetic"] + ".tab"))
                path = str(EPHEMERIDES / value["ephemeris"])
                if value["form"] == "bytes":
                    return path.encode()
                if value["form"] == "nul":
                    return path + "\0ignored"
                if value["form"] == "home":
                    return "~/" + os.path.relpath(path, Path.home())
                if value["form"] == "plain_environment":
                    return "$CASA_RS_TEST_COMET_ROOT/" + value["ephemeris"]
                if value["form"] == "environment":
                    return "${CASA_RS_TEST_COMET_ROOT}/" + value["ephemeris"]
                return path

            try:
                result = getattr(tool, method)(*(decode(value) for value in values))
            except (TypeError, RuntimeError, ValueError) as error:
                output.append(entry | {"error": type(error).__name__, "message": str(error)})
                continue
        output.append(entry | {"result": clean(result), "python_type": type(result).__name__})
    tool.done()
    return {"name": name, "steps": output}


sequences = {
    "unloaded_and_defaults": [*query_steps(), ("framecomet",), *query_steps()],
    "upstream_ganymede": [explicit(), epoch("2011/01/03/17:00:00"), ("set_alma",), *query_steps()],
    "path_protocol": [epoch("55564.7083333333d"), explicit(form="bytes"), *query_steps(),
                      explicit(form="nul"), ("cometname",), ("framecomet", GANYMEDE), *query_steps()],
    "path_expansion": [explicit(form="home"), ("cometname",),
                       explicit(form="environment"), ("cometname",),
                       explicit(form="plain_environment"), ("cometname",)],
    "protocol_errors_keep_frame": [epoch("55564.7083333333d"), explicit(),
                                   ("framecomet", None), ("framecomet", 42), ("framecomet", []),
                                   ("framecomet", False), *query_steps()],
    "replacement_and_done": [epoch("55564.7083333333d"), explicit(), ("cometdist",),
                             explicit("JPL-Horizons/absent.tab"), *query_steps(), explicit(),
                             ("cometdist",), ("done",), ("cometname",), ("comettype",),
                             ("comettopo",), epoch("55564.7083333333d"), *query_steps()],
    "dates_and_ref": [explicit(), epoch("55436.9d"), ("cometdist",),
                      epoch("55437d"), ("cometdist",), ("cometangdiam",),
                      epoch("55564.7083333333d", "TAI"), ("cometdist",),
                      epoch("55565.3d"), ("cometdist",), epoch("55564.8d"), ("cometdist",),
                      epoch("56292.9999d"), ("cometdist",), epoch("56293d"), ("cometdist",),
                      ("cometangdiam",)],
    "explicit_table_reference": [
        explicit("JPL-Horizons/Ganymede_55197-59214dUTC_J2000.tab"),
        epoch("55564.7083333333d"), *query_steps(),
    ],
}
for variant in SYNTHETIC["variants"]:
    sequences[variant] = [
        ("framecomet", {"synthetic": variant}),
        epoch("55564d" if variant == "zero_range" else "55564.5d"),
        *query_steps(),
    ]

definitions = measures()
direction = definitions.direction("COMET")
shifted = definitions.direction("COMET", "17deg", "35deg")
vla = definitions.observatory("VLA")
frequency = definitions.frequency("TOPO", "1GHz")
velocity = definitions.radialvelocity("TOPO", "10km/s")
geo = definitions.radialvelocity("GEO", "0m/s")
uvw = definitions.uvw("J2000", "1m", "21m", "30m")
definitions.done()

def conversions(*references, source=direction):
    return [("measure", source, reference) for reference in references]

def comet_frame(table_entry, position, date="2011/01/03/17:00:00"):
    steps = [epoch(date)] + ([position] if position else [])
    if table_entry:
        steps.append(table_entry)
    return steps


targets = ("APP", "J2000", "ICRS", "TOPO", "HADEC", "AZEL",
           "B1950", "B1950_VLA", "BMEAN", "BTRUE")
ordinary = explicit()
j2000 = explicit("JPL-Horizons/Ganymede_55197-59214dUTC_J2000.tab")
alma = ("set_alma",)
show = [("doframe", direction), ("showframe",)]
for label, source in (("app", ordinary), ("j2000", j2000)):
    for site, position in (("alma", alma), ("vla", ("doframe", vla))):
        sequences[f"direction_{label}_{site}"] = (
            comet_frame(source, position) + conversions(*targets) + show
        )
    sequences[f"direction_{label}_without_observatory"] = (
        comet_frame(source, None) + conversions(*targets)
    )
for variant in ("topocentric", "explicit_topocentric", "explicit_icrs", "explicit_b1950"):
    sequences[f"direction_{variant}"] = (
        comet_frame(("framecomet", {"synthetic": variant}), alma, "55564.5d")
        + conversions(*targets) + show + [("rise", direction), ("riseset", direction)]
    )
sequences.update({
    "direction_outside_table": (
        comet_frame(ordinary, alma, "2009/01/03/17:00:00")
        + conversions("APP", "TOPO", "J2000") + show
    ),
    "direction_no_table": comet_frame(None, alma) + conversions("APP", "TOPO", "J2000", "COMET"),
    "direction_shifted_record": (
        comet_frame(ordinary, alma) + conversions("J2000", "TOPO", source=shifted)
    ),
    "direction_rise_set": (
        comet_frame(ordinary, alma) + [("rise", direction), ("riseset", direction)]
    ),
    "direction_spectral_context": (
        comet_frame(ordinary, alma) + show
        + [("measure", frequency, "BARY"), ("measure", velocity, "LSRK"),
           ("measure", uvw, "ITRF")]
    ),
    "direction_replacement_context": (
        comet_frame(("framecomet", {"synthetic": "explicit_b1950"}), alma, "55564.5d")
        + show + [("measure", geo, "TOPO"),
                  ("framecomet", {"synthetic": "explicit_topocentric"}),
                  ("showframe",), ("measure", geo, "TOPO"),
                  ("doframe", direction), ("showframe",), ("measure", geo, "TOPO")]
    ),
    "direction_table_lifetime": (
        comet_frame(ordinary, alma) + show + conversions("J2000")
        + [explicit("JPL-Horizons/absent.tab"), ("showframe",)] + conversions("J2000")
        + [("done",), ("showframe",)] + comet_frame(None, alma) + conversions("J2000")
    ),
})

with TemporaryDirectory() as temp:
    scratch = Path(temp)
    for variant, changes in SYNTHETIC["variants"].items():
        count = SYNTHETIC["row_counts"].get(variant, len(SYNTHETIC["columns"]["MJD"]))
        make_table(scratch / (variant + ".tab"), changes, count, variant)
    cases = [run_case(name, steps, scratch) for name, steps in sequences.items()]
    replacement = next(case for case in cases if case["name"] == "direction_replacement_context")
    for stale, fresh in zip(replacement["steps"][-5:-3], replacement["steps"][-2:], strict=True):
        assert stale["method"] == fresh["method"] and stale["result"] != fresh["result"]
        stale["native_result"] = fresh["result"]

for query, unit in (("cometdist", "AU"), ("cometangdiam", "rad")):
    for after_done in (False, True):
        probe = subprocess.run(
            [sys.executable, __file__, "--measures-data", args.measures_data,
             "--without-epoch", query, *(["--after-done"] if after_done else [])],
            text=True, capture_output=True, check=False,
        )
        assert probe.returncode == -signal.SIGSEGV and "loaded the comet table" in probe.stdout, probe
        steps = [
            {"method": "framecomet", "args": [explicit()[1]], "result": True, "python_type": "bool"},
        ]
        if after_done:
            steps += [
                {"method": "set_epoch", "args": ["UTC", "2011/01/03/17:00:00"],
                 "result": True, "python_type": "bool"},
                {"method": "done", "args": [], "result": True, "python_type": "bool"},
            ]
        steps.append({"method": query, "args": [], "casa_signal": "SIGSEGV",
                      "native_result": {"unit": unit, "value": -1.}, "python_type": "dict"})
        cases.append({"name": f"{query}_{'after_done' if after_done else 'no_epoch'}", "steps": steps})

args.out.write_text(json.dumps({
    "casa_version": version_string(), "synthetic": SYNTHETIC, "cases": cases,
}, indent=2, allow_nan=False) + "\n")
print(f"{len(cases)} cases, {sum(len(case['steps']) for case in cases)} steps")
