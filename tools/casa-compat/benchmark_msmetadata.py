#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Time paired public msmetadata workloads on CASA's MSMetaData.ms fixture."""

import argparse
from collections import defaultdict
from hashlib import file_digest
import json
import os
from pathlib import Path
from statistics import median
from time import perf_counter_ns, process_time_ns

import casatools
import numpy as np

from casars import _core, tools


def lifecycle(tool):
    return (
        tool.nantennas(), tool.nfields(), tool.nobservations(), tool.nstates(),
        tool.fieldnames(), tool.observatorynames(), tool.observers(), tool.projects(),
    )


def identifiers(tool):
    return (
        tool.namesforfields([0, 1, 5]), tool.fieldsforname("3C279"),
        tool.scannumbers(), tool.namesforspws([0, 9, 10]),
        tool.spwsfornames(tool.namesforspws(9)[0]),
        tool.antennanames([0, 2, 3]), tool.antennanames(),
        tool.ncorrforpol(), tool.nsources(),
    )


def spectral(tool):
    return (
        tool.nchan(0), tool.chanfreqs(0, "MHz"), tool.chanwidths(0),
        tool.meanfreq(0), tool.reffreq(0), tool.sideband(0),
        tool.datadescids(), tool.spwfordatadesc(), tool.polidfordatadesc(),
        tool.ncorrforpol(), tool.corrtypesforpol(0), tool.corrprodsforpol(0),
    )


def spectral_classes(tool):
    return (
        tool.chanres(9, asvel=True), tool.chaneffbws(9, "MHz"),
        tool.almaspws(fdm=True, sqld=True), tool.chanavgspws(), tool.tdmspws(),
        tool.baseband(9), tool.spwsforbaseband(2, "only"),
        tool.corrbit([2, 1]), tool.rxbands(), tool.subwindows(),
    )


def main_scans(tool):
    scans = tool.scannumbers()
    scan = int(scans[len(scans) // 2])
    return (
        scans, tool.nscans(), tool.narrays(), tool.fieldsforname(),
        tool.fieldsforscan(scan), tool.fieldsforscans(scans),
        tool.scansforfield(0), tool.antennasforscan(scan),
        tool.nbaselines(True), tool.baselines(),
    )


def main_membership(tool):
    scans = tool.scannumbers()
    scan = int(scans[len(scans) // 2])
    return (
        scans, tool.spwsforscan(scan), tool.statesforscan(scan), tool.timesforscan(scan),
        tool.spwsforfield(0), tool.fieldsforspw(0), tool.scansforspw(0),
        tool.timesforfield(0), tool.timesforspws(0), tool.timesforscans([scan]),
    )


def intents(tool):
    intents = tool.intents()
    intent = intents[0]
    return (
        intents, tool.fieldsforintent(intent, asnames=True), tool.intentsforfield(0),
        tool.intentsforscan(1), tool.intentsforspw(0),
        tool.scansforintent(intent), tool.spwsforintent("*TARGET*"), tool.timesforintent(intent),
    )


def source_metadata(tool):
    return {
        "sourceids": tool.sourceidsfromsourcetable(),
        "names": tool.sourcenames(),
        "fields": tool.fieldsforsource(0, True),
        "source_fields": tool.fieldsforsources(False),
        "directions": tool.sourcedirs(),
        "times": tool.sourcetimes(),
        "motions": tool.propermotions(),
        "frequencies": tool.restfreqs(0, 34),
        "transitions": tool.transitions(0, 34),
    }


def antenna_observation(tool):
    return {
        "ids": tool.antennaids(["DA*", "DV1*"], mindiameter="10m", obsid=0),
        "diameters": tool.antennadiameter(),
        "stations": tool.antennastations([4, 3, 0], obsid=0),
        "position": tool.antennaposition(3),
        "offset": tool.antennaoffset(4),
        "observatory": tool.observatoryposition(),
        "schedule": tool.schedule(0),
        "time_range": tool.timerangeforobs(0),
    }


def field_pointing(tool):
    return {
        "field_phase_center": tool.phasecenter(0),
        "field_reference_direction": tool.refdir(0),
        "pointing_direction": tool.pointingdirection(500),
        "shifted_pointing_direction": tool.pointingdirection(501, initialrow=501),
    }


def pointing_span(tool):
    return tool.pointingdirection(7680), tool.pointingdirection(15362)


def main_metadata(tool):
    return {
        "total_rows": tool.nrows(),
        "cross_rows": tool.nrows(autoc=False),
        "unflagged_rows": tool.nrows(flagged=False),
        "summary": tool.summary(),
        "first_exposure": tool.exposuretime(scan=17, spwid=10),
        "effective_exposure": tool.effexposuretime(),
    }


WORKLOADS = {workload.__name__: workload for workload in (
    lifecycle, identifiers,
    spectral,
    spectral_classes,
    main_scans,
    main_membership,
    intents,
    source_metadata,
    antenna_observation,
    field_pointing,
    pointing_span,
    main_metadata,
)}


def equivalent(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys(), (actual.keys(), expected.keys())
        for name, item in expected.items():
            if name == "value" and expected.get("unit") == "m" and isinstance(item, float):
                # Retain the antenna/observatory position comparison's
                # absolute tolerance in metres.
                np.testing.assert_allclose(actual[name], item, rtol=1e-12, atol=2e-7)
            else:
                equivalent(actual[name], item)
    elif isinstance(expected, (tuple, list)):
        assert type(actual) is type(expected), (type(actual), type(expected))
        for value, item in zip(actual, expected, strict=True):
            equivalent(value, item)
    elif isinstance(expected, np.ndarray):
        assert actual.dtype == expected.dtype, (actual.dtype, expected.dtype)
        assert actual.shape == expected.shape, (actual.shape, expected.shape)
        if expected.dtype.kind == "f":
            np.testing.assert_allclose(actual, expected, rtol=1e-12, atol=1e-10)
        else:
            np.testing.assert_array_equal(actual, expected, strict=True)
    elif isinstance(expected, float):
        np.testing.assert_allclose(actual, expected, rtol=1e-12, atol=1e-10)
    else:
        assert actual == expected, (actual, expected)


def attached_run(api, dataset, workload):
    tool = api.msmetadata()
    wall_start, cpu_start = perf_counter_ns(), process_time_ns()
    assert tool.open(str(dataset))
    query_start = perf_counter_ns()
    try:
        result = workload(tool)
    finally:
        assert tool.done()
    wall_end, cpu_end = perf_counter_ns(), process_time_ns()
    return result, {
        "total_wall_ns": wall_end - wall_start,
        "total_cpu_ns": cpu_end - cpu_start,
        "open_wall_ns": query_start - wall_start,
    }


def paired_runs(dataset, workload, repetitions):
    backends = [("CASA", casatools), ("casa-rs", tools)]
    timings = defaultdict(list)
    for trial in range(repetitions + 1):
        results = {}
        for name, api in backends[::1 if trial % 2 == 0 else -1]:
            results[name], sample = attached_run(api, dataset, workload)
            if trial:
                timings[name].append(sample)
        equivalent(results["casa-rs"], results["CASA"])
    median_wall_ms = {
        name: median(sample["total_wall_ns"] for sample in measured) / 1e6
        for name, measured in timings.items()
    }
    return {"median_wall_ms": median_wall_ms, "samples": timings}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("measurement_set", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workload", choices=WORKLOADS, action="append")
    parser.add_argument("--repeat", type=int, default=10)
    parser.add_argument("--casa-rs-revision")
    args = parser.parse_args()
    version = casatools.version_string().replace("-", ".")
    if version != "6.7.6.14":
        parser.error(f"CASA 6.7.6.14 is required, found {version}")
    if args.repeat < 1:
        parser.error("--repeat must be positive")
    casatools.logsink().filter("SEVERE")
    with Path(_core.__file__).open("rb") as extension:
        extension_sha256 = file_digest(extension, "sha256").hexdigest()
    report = {
        "casa_version": version,
        "extension_sha256": extension_sha256,
        "casa_rs_revision": args.casa_rs_revision,
        "measurement_set": args.measurement_set.name,
        "worker_environment": {
            name: os.environ.get(name) for name in
            ("RAYON_NUM_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")
        },
        "workloads": {},
    }
    for name in args.workload or WORKLOADS:
        measured = paired_runs(args.measurement_set.resolve(), WORKLOADS[name], args.repeat)
        report["workloads"][name] = measured
        print(name, measured["median_wall_ms"], flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
