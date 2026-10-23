#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA oracle for component-list tables (simulation Phase 2, #841).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3`).
Four subcommands:

`fixture --out DIR`
    Writes, with CASA's `cl` tool, the component lists casa-components'
    tests read, each with a JSON record of what CASA reads back:

    - `DIR/components.cl` and `components.json`: every shape (point, Gaussian,
      disk, limb-darkened disk), every spectral model (constant, spectral
      index with and without Stokes indices, tabular, power-log-polynomial),
      every polarisation representation, five direction frames, nonzero
      errors of every kind, and labels. `cl` cannot set a reference-frequency
      error, so two rows get one through casacore's table tool afterwards.
    - `DIR/old_format.cl` and `old_format.json`: a list without the error
      columns and `Spectral_Record`, as lists were before CASA added them,
      made by removing those columns with the table tool.

`dump TABLE --json FILE`
    Records what CASA reads from a component list (`cl.open(nomodify=True)`
    then `cl.getcomponent(i)`), and every cell, column description and keyword
    of the table (`tb.getcell`).

`rewrite TABLE`
    Opens a component list for update and closes it, so that CASA's
    `writeTable` rewrites it in place.

`tool --json FILE`
    Records what CASA's `cl` tool returns for a fixed script
    (`TOOL_CALLS`): each `addcomponent` call's keyword arguments, what it
    returned or the first line of what it raised, and the list's length
    after it; then `torecord`, and for every component `getcomponent`,
    `getspectrum`, `getrefdir`, `getfluxvalue`, `getfluxunit`, `getlabel`,
    `shapetype` and `spectrumtype`; and the outcomes of the edge cases in
    `TOOL_EDGES`. Complex arguments are recorded as `{"complex": [re, im]}`.
    `casars.tools.componentlist`'s tests replay the same script
    (`crates/casars-python/python/tests/data/casa_componentlist_tool.json`).

A JSON record holds `components` (each `getcomponent(i)`), `cells` (column
name to a list with one entry per row: the cell, or null when it is
undefined), `column_names` (in table order), `columns` (each column's
description and keywords), `data_managers`, `info` and `casa_version`.
Complex numbers are `[re, im]` pairs and arrays are lists.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil

import numpy as np
from casatools import componentlist, table
import casatools

ERROR_COLUMNS = [
    "Flux_Error",
    "Direction_Error",
    "Direction_Error_Units",
    "Shape_Error",
    "Frequency_Error",
    "Frequency_Error_Units",
    "Spectral_Error",
    "Spectral_Record",
]


def jsonable(value):
    """Convert CASA tool output into JSON values."""
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, np.ndarray):
        return [jsonable(item) for item in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, (complex, np.complexfloating)):
        return [float(value.real), float(value.imag)]
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value)
    if isinstance(value, bytes):
        return value.decode()
    return value


def quantity(value, unit):
    return {"value": value, "unit": unit}


def direction(frame, longitude, latitude, unit="deg", error=None):
    record = {
        "type": "direction",
        "refer": frame,
        "m0": quantity(longitude, unit),
        "m1": quantity(latitude, unit),
    }
    if error is not None:
        record["error"] = error
    return record


def frequency(frame, value_ghz):
    return {"type": "frequency", "refer": frame, "m0": quantity(value_ghz, "GHz")}


def axes(shape, major, minor, pa, errors=None):
    record = {
        "type": shape,
        "majoraxis": quantity(major, "arcsec"),
        "minoraxis": quantity(minor, "arcsec"),
        "positionangle": quantity(pa, "deg"),
    }
    errors = errors or (0.0, 0.0, 0.0)
    record["majoraxiserror"] = quantity(errors[0], "arcsec")
    record["minoraxiserror"] = quantity(errors[1], "arcsec")
    record["positionangleerror"] = quantity(errors[2], "deg")
    return record


def api_components(cl):
    """Components made through the cl tool's own setters."""
    cl.addcomponent(
        flux=[1.5, 0.1, 0.05, 0.02],
        fluxunit="Jy",
        polarization="Stokes",
        dir="J2000 20h00m00.0 -30d00m00.0",
        shape="point",
        freq="LSRK 1.4GHz",
        spectrumtype="constant",
        label="point, Stokes, J2000 at RA 20h",
    )
    cl.addcomponent(
        flux=2.0,
        dir="B1950 01h00m00.0 20d00m00.0",
        shape="gaussian",
        majoraxis="12arcsec",
        minoraxis="6arcsec",
        positionangle="200deg",
        freq="TOPO 100GHz",
        spectrumtype="spectral index",
        index=-0.7,
        label="Gaussian, B1950, PA 200 deg",
    )
    cl.addcomponent(
        flux=0.5,
        dir="J2000 05h35m17.3 -05d23m28.0",
        shape="disk",
        majoraxis="30arcsec",
        minoraxis="20arcsec",
        positionangle="45deg",
        freq="LSRK 1.0GHz",
        label="disk, tabular",
    )
    cl.setspectrum(
        2,
        "tabular",
        tabularfreqs=[1.0e9, 1.5e9, 2.0e9, 3.0e9],
        tabularflux=[1.0, 0.8, 0.6, 0.4],
        tabularframe="LSRK",
    )
    cl.addcomponent(
        flux=[3.0, 0.3, 0.2, 0.0],
        dir="J2000 12h00m00.0 45d00m00.0",
        shape="gaussian",
        majoraxis="4arcsec",
        minoraxis="4arcsec",
        positionangle="0deg",
        freq="LSRK 1.4GHz",
        label="Gaussian, Stokes spectral indices",
    )
    cl.setstokesspectrum(3, "spectral index", index=[-0.8, 0.1, 50.0, 0.0])
    cl.addcomponent(
        flux=1.0,
        dir="GALACTIC 120deg 2deg",
        shape="point",
        freq="LSRK 5GHz",
        label="point, GALACTIC, plp",
    )
    cl.setspectrum(4, "plp", index=[0.1, -0.5, 0.02])


def record_components():
    """Components only a record can express: limb-darkened disks, complex
    linear and circular fluxes, and direction errors."""
    ldisk = axes("limbdarkeneddisk", 40.0, 30.0, 60.0, (0.4, 0.3, 1.5))
    ldisk["direction"] = direction(
        "AZEL",
        30.0,
        45.0,
        error={"latitude": quantity(0.25, "arcsec"), "longitude": quantity(30.0, "mas")},
    )
    linear_disk = axes("disk", 20.0, 10.0, -30.0, (0.2, 0.1, 0.5))
    linear_disk["direction"] = direction(
        "GALACTIC",
        10.0,
        -5.0,
        error={"latitude": quantity(1.0e-6, "rad"), "longitude": quantity(2.0e-6, "rad")},
    )
    circular_gaussian = axes("gaussian", 8.0, 8.0, -30.0)
    circular_gaussian["direction"] = direction("J2000", 200.0, -60.0)
    round_disk = axes("disk", 15.0, 15.0, 0.0)
    round_disk["direction"] = direction("B1950", 350.0, 89.0)
    return [
        {
            "flux": {
                "polarisation": "Circular",
                "unit": "Jy",
                "value": [complex(1.1, 0), complex(0.05, 0.02), complex(0.05, -0.02), complex(0.9, 0)],
                "error": [complex(0.01, 0), complex(0.001, 0.001), complex(0.001, 0.001), complex(0.01, 0)],
            },
            "shape": ldisk,
            "spectrum": {
                "type": "Power Logarithmic Polynomial",
                "frequency": frequency("TOPO", 230.0),
                "coeffs": [0.5, -0.1, 0.01],
                "error": [0.05, 0.01, 0.001],
            },
            "optionalParameters": [0.7],
            "label": "limb-darkened disk, circular, AZEL, plp",
        },
        {
            "flux": {
                "polarisation": "Linear",
                "unit": "mJy",
                "value": [complex(900.0, 0), complex(40.0, 25.0), complex(40.0, -25.0), complex(700.0, 0)],
                "error": [complex(5.0, 0), complex(1.0, 1.0), complex(1.0, 1.0), complex(5.0, 0)],
            },
            "shape": linear_disk,
            "spectrum": {
                "type": "Spectral Index",
                "frequency": frequency("BARY", 8.4),
                "index": 0.3,
                "error": 0.05,
            },
            "label": "disk, linear mJy, GALACTIC, PA -30 deg",
        },
        {
            "flux": {"polarisation": "Stokes", "unit": "Jy", "value": [0.25, 0.0, 0.0, 0.01],
                     "error": [0.02, 0.0, 0.0, 0.001]},
            "shape": circular_gaussian,
            "spectrum": {
                "type": "Tabular Spectrum",
                "frequency": frequency("BARY", 1.5),
                "freqRef": {"type": "frequency", "refer": "BARY", "m0": quantity(0.0, "Hz")},
                "tabFreqVal": [1.0e9, 2.0e9, 4.0e9],
                "ival": [1.0, 0.5, 0.25],
                "qval": [0.1, 0.05, 0.0],
                "uval": [0.0, 0.02, 0.01],
                "vval": [0.01, 0.0, 0.0],
                "referenceFreq": 1.5e9,
                "maxFreq": 4.0e9,
                "minFreq": 1.0e9,
            },
            "label": "circular Gaussian, PA -30 deg, tabular in BARY",
        },
        {
            "flux": {"polarisation": "Stokes", "unit": "Jy", "value": [4.0, 0.0, 0.0, 0.0]},
            "shape": round_disk,
            "spectrum": {"type": "Constant", "frequency": frequency("LSRK", 43.0)},
            "label": "",
        },
    ]


def make_components(path):
    cl = componentlist()
    api_components(cl)
    record = cl.torecord()
    cl.done()
    extra = record_components()
    count = int(record["nelements"])
    for offset, component in enumerate(extra):
        record["component%d" % (count + offset)] = component
    record["nelements"] = count + len(extra)
    cl = componentlist()
    cl.fromrecord(record)
    cl.rename(path)
    cl.close(log=False)
    # cl cannot set a reference-frequency error; casacore's table tool can.
    tb = table()
    tb.open(path, nomodify=False)
    tb.putcell("Frequency_Error", 1, 5.0)
    tb.putcell("Frequency_Error_Units", 1, "kHz")
    tb.putcell("Frequency_Error", 6, 0.002)
    tb.putcell("Frequency_Error_Units", 6, "GHz")
    tb.close()


def make_old_format(path, scratch):
    cl = componentlist()
    cl.addcomponent(
        flux=[1.0, 0.0, 0.0, 0.0],
        dir="J2000 03h00m00.0 10d00m00.0",
        shape="point",
        freq="LSRK 1.4GHz",
        label="old point",
    )
    cl.addcomponent(
        flux=0.75,
        dir="B1950 18h00m00.0 -20d00m00.0",
        shape="gaussian",
        majoraxis="10arcsec",
        minoraxis="5arcsec",
        positionangle="10deg",
        freq="TOPO 22GHz",
        spectrumtype="spectral index",
        index=-1.2,
        label="old gaussian",
    )
    cl.addcomponent(
        flux=[1.0, 0.2, 0.1, 0.9],
        polarization="Linear",
        dir="GALACTIC 30deg 0.5deg",
        shape="disk",
        majoraxis="2arcmin",
        minoraxis="1arcmin",
        positionangle="-10deg",
        freq="LSRK 5GHz",
        label="old disk",
    )
    cl.rename(scratch)
    cl.close(log=False)
    tb = table()
    tb.open(scratch)
    copy = tb.copy(path, deep=True, valuecopy=True)
    tb.close()
    copy.close()
    tb.open(path, nomodify=False)
    tb.removecols(ERROR_COLUMNS)
    tb.close()
    shutil.rmtree(scratch)


def dump(path, json_path):
    cl = componentlist()
    cl.open(path, nomodify=True)
    components = [cl.getcomponent(index) for index in range(cl.length())]
    cl.close(log=False)
    tb = table()
    tb.open(path)
    rows = tb.nrows()
    cells = {}
    columns = {}
    tb_column_names = tb.colnames()
    for column in tb_column_names:
        cells[column] = [
            jsonable(tb.getcell(column, row)) if tb.iscelldefined(column, row) else None
            for row in range(rows)
        ]
        columns[column] = jsonable(tb.getcoldesc(column))
    info = jsonable(tb.info())
    dminfo = jsonable(tb.getdminfo())
    tb.close()
    document = {
        "casa_version": casatools.version_string(),
        "components": jsonable(components),
        "cells": cells,
        "column_names": list(tb_column_names),
        "columns": columns,
        "info": info,
        "data_managers": sorted(
            ({"type": manager["TYPE"], "columns": sorted(manager["COLUMNS"])}
             for manager in dminfo.values()),
            key=lambda manager: manager["type"],
        ),
    }
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=1, sort_keys=True)
        handle.write("\n")


def rewrite(path):
    cl = componentlist()
    cl.open(path, nomodify=False)
    cl.close(log=False)


TOOL_CALLS = [
    {"flux": 1.0, "dir": "J2000 10h00m00.0 -30d00m00.0", "shape": "point", "freq": "230GHz", "label": "point"},
    {
        "flux": [0.5, 0.1, 0.0, 0.0], "fluxunit": "mJy", "dir": ["J2000", "150deg", "-30deg"],
        "shape": "gaussian", "majoraxis": "1arcsec", "minoraxis": "0.5arcsec", "positionangle": "30deg",
        "freq": "230GHz", "spectrumtype": "spectral index", "index": -0.7, "label": "Gaussian",
    },
    {
        "flux": 0.3, "dir": "B1950 10h00m00.0 -30d00m00.0", "shape": "disk", "majoraxis": "2arcsec",
        "minoraxis": "1.5arcsec", "positionangle": "60deg", "freq": "LSRK 100GHz", "spectrumtype": "plp",
        "index": [-0.5, 0.2],
    },
    {"flux": [1.0, 0.1, 0.2, 0.9], "polarization": "circular", "dir": "J2000 1rad 0.5rad", "freq": "1.4GHz"},
    {},
    {"shape": "nosuch", "flux": 0.25},
    {
        "shape": "gauss", "majoraxis": "10arcsec", "minoraxis": "5arcsec", "positionangle": "-20deg",
        "freq": {"type": "frequency", "refer": "TOPO", "m0": {"unit": "GHz", "value": 43.0}},
        "dir": {"type": "direction", "refer": "J2000", "m0": {"unit": "deg", "value": 10.0},
                "m1": {"unit": "deg", "value": 20.0}},
    },
    {"shape": "Gaussian", "flux": 0.5},
    {"shape": "limbdarkeneddisk", "majoraxis": "3arcsec", "minoraxis": "2arcsec"},
    {"flux": [1.0, 2.0]},
    {"spectrumtype": "tabular"},
    {"dir": "nonsense"},
    {"spectrumtype": "plp", "index": 0.5},
    {"spectrumtype": "nonsense"},
    {"spectrumtype": "spectral index"},
    {"spectrumtype": "spectral index", "index": [0.3, 0.1]},
    {"polarization": "circ"},
    {"fluxunit": "K"},
    {"flux": 2.0, "dir": "GALACTIC 120deg 2deg", "freq": 1.4e9, "label": "a number as freq"},
    {"freq": "BARY 5GHz"},
    {"shape": "disk", "majoraxis": "1arcsec", "minoraxis": "2arcsec"},
    # Added for the review of PR #873: spaced frequencies, shapes whose
    # constructors raise, axes that do not read, and complex fluxes.
    {"flux": 1.0, "freq": "1.4 GHz", "label": "a spaced frequency"},
    {"flux": 1.0, "freq": "foo 1.4GHz"},
    {"shape": "gaussian", "majoraxis": "2arcsec", "minoraxis": "1arcsec", "positionangle": "400deg"},
    {"shape": "gaussian", "majoraxis": "30arcsec", "minoraxis": "2arcmin", "positionangle": "0deg"},
    {"shape": "gaussian", "majoraxis": "1arcsec", "minoraxis": "2arcsec", "positionangle": "0deg"},
    {"shape": "gaussian", "majoraxis": "2arcsec", "minoraxis": "0arcsec", "positionangle": "0deg"},
    {"shape": "disk", "majoraxis": "1arcsec", "minoraxis": "2arcsec", "positionangle": "0deg"},
    {"shape": "gaussian", "majoraxis": "2Jy", "minoraxis": "1arcsec", "positionangle": "garbage"},
    {"shape": "gaussian", "majoraxis": 1j, "minoraxis": "1arcsec", "positionangle": "0deg"},
    {"shape": "gaussian", "majoraxis": "2Jy", "minoraxis": "1arcsec", "positionangle": "0deg"},
    {"flux": [1 + 0j, 0.1 + 0.2j, 0j, 0.5j], "label": "complex values"},
    {"flux": 2 + 3j},
    {"flux": [1, 0.1j, 0, 0]},
    {"flux": [1 + 1j, 0.1, 0.2, 0.9], "polarization": "linear"},
    {"flux": 1 + 1j, "polarization": "circular"},
]

TOOL_ACCESSORS = ("getcomponent", "getspectrum", "getrefdir", "getfluxvalue", "getfluxunit",
                  "getlabel", "shapetype", "spectrumtype")


def outcome(call):
    """What a call returned, or the first line of what it raised."""
    try:
        return {"returned": jsonable(call())}
    except Exception as error:  # noqa: BLE001 - CASA raises RuntimeError and others
        return {"raised": str(error).split("\n", 1)[0]}


def tagged(value):
    """A script argument for JSON, with each complex number as
    `{"complex": [re, im]}` so that it reads back as complex."""
    if isinstance(value, complex):
        return {"complex": [value.real, value.imag]}
    if isinstance(value, dict):
        return {key: tagged(item) for key, item in value.items()}
    if isinstance(value, list):
        return [tagged(item) for item in value]
    return value


def tool(json_path):
    cl = componentlist()
    calls = []
    for kwargs in TOOL_CALLS:
        result = outcome(lambda: cl.addcomponent(**kwargs))
        calls.append(dict(result, kwargs=tagged(kwargs), length=cl.length()))
    components = []
    for index in range(cl.length()):
        components.append({name: outcome(lambda: getattr(cl, name)(index)) for name in TOOL_ACCESSORS})
    document = {
        "casa_version": casatools.version_string(),
        "calls": calls,
        "torecord": outcome(cl.torecord),
        "components": components,
        "edges": {
            "getcomponent_out_of_range": outcome(lambda: cl.getcomponent(cl.length())),
            "getfluxvalue_out_of_range": outcome(lambda: cl.getfluxvalue(-1)),
            "getrefdir_out_of_range": outcome(lambda: cl.getrefdir(cl.length())),
        },
    }
    cl.done()
    document["edges"]["torecord_empty"] = outcome(cl.torecord)
    document["edges"]["length_empty"] = outcome(cl.length)
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=1, sort_keys=True)
        handle.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    fixture = commands.add_parser("fixture")
    fixture.add_argument("--out", required=True)
    dumping = commands.add_parser("dump")
    dumping.add_argument("table")
    dumping.add_argument("--json", required=True)
    rewriting = commands.add_parser("rewrite")
    rewriting.add_argument("table")
    tooling = commands.add_parser("tool")
    tooling.add_argument("--json", required=True)
    args = parser.parse_args()
    if args.command == "tool":
        tool(args.json)
        return
    if args.command == "fixture":
        os.makedirs(args.out, exist_ok=True)
        components = os.path.join(args.out, "components.cl")
        old_format = os.path.join(args.out, "old_format.cl")
        for path in (components, old_format):
            shutil.rmtree(path, ignore_errors=True)
        make_components(components)
        make_old_format(old_format, os.path.join(args.out, "old_format_source.cl"))
        dump(components, os.path.join(args.out, "components.json"))
        dump(old_format, os.path.join(args.out, "old_format.json"))
        for path in (components, old_format):
            lock = os.path.join(path, "table.lock")
            if os.path.exists(lock):
                os.remove(lock)
    elif args.command == "dump":
        dump(args.table, args.json)
    else:
        rewrite(args.table)


if __name__ == "__main__":
    main()
