#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check installed simobserve stores against a CASA component mosaic.

Run `casa ROOT CASATESTDATA` with CASA 6.7.6.14's Python, `create ROOT`
with the installed casa-rs wheel, `msv2 ROOT` with CASA's Python, and
`msv4 ROOT` with XRADIO and arcae. Use a fresh ROOT for each wheel.
"""

import argparse
import json
import os
from pathlib import Path

import numpy as np

from simobserve_parity import CASES, casa_params, casars_params, compare_case, copy_inputs, run_casa, tools
from sm_creation_oracle import compare_msv4


CASE = next(case for case in CASES if case["name"] == "components_cube")
PROJECT = CASE["name"]
STEM = f"{PROJECT}.alma.out01"
SIGNED_UV_REFUSAL = "An interferometer observation is requested but only AUTO-correlation"
CONFIG = Path(__file__).resolve().parents[2] / "libs/casa/casa-simulation/tests/data/readantenna/alma.out01.cfg"


def rejected(task, error_type, diagnostic, **parameters):
    try:
        task(**parameters)
    except error_type as error:
        assert diagnostic in str(error), error
    else:
        raise AssertionError(f"simobserve accepted a run expected to fail: {diagnostic}")


def prepare(root, data):
    from casatasks import simobserve, version_string

    assert version_string() == "6.7.6.14"
    case = dict(CASE, params=dict(CASE["params"], antennalist=str(CONFIG)))
    t = tools()
    copy_inputs(str(data), str(root), [case])
    run_casa(t, case, str(root))
    parameters = casars_params(t, casa_params(case, str(root)))
    parameters.update(project=PROJECT, graphics="none", overwrite=True,
                      complist=str(root / "native-inputs" / f"{PROJECT}.cl"))
    (root / "params.json").write_text(json.dumps(parameters))

    # The first pair has nonzero cross-correlations, but both signed u and
    # v are negative at all six samples of the independent CASA dataset.
    config = root / "negative-inputs" / CONFIG.name
    config.parent.mkdir()
    config.write_text("".join(CONFIG.read_text().splitlines(keepends=True)[:8]))
    work = root / "casa-negative"
    work.mkdir()
    os.chdir(work)
    negative = dict(casa_params(case, str(root)), antennalist=str(config))
    rejected(simobserve, Exception, SIGNED_UV_REFUSAL,
             project=PROJECT, graphics="none", overwrite=True, **negative)
    t["tb"].open(str(work / PROJECT / f"{STEM}.ms"))
    try:
        uv = t["tb"].getcol("UVW")[:2]
        assert uv.shape == (2, 6) and np.all(uv < 0)
        print(f"CASA refused six physical cross-correlations with largest signed u/v {uv.max()} m")
    finally:
        t["tb"].close()


def create(root):
    from casars import tasks

    parameters = json.loads((root / "params.json").read_text())
    negative = dict(parameters, antennalist=str(root / "negative-inputs" / CONFIG.name))
    for folder, fmt in (("default", {}), ("msv2", {"outformat": "msv2"}),
                        ("msv4", {"outformat": "msv4"}), ("negative", {"outformat": "msv4"})):
        work = root / folder
        work.mkdir()
        os.chdir(work)
        if folder == "negative":
            rejected(tasks.simobserve, tasks.TaskExecutionError, SIGNED_UV_REFUSAL, **negative, **fmt)
            assert not list((work / PROJECT).glob("*ps.zarr*"))
        else:
            assert tasks.simobserve(**parameters, **fmt) is None

    work = root / "msv4"
    store = work / PROJECT / f"{STEM}.ps.zarr"
    marker = store / ".preserve-on-failure"
    marker.write_text("previous store")
    os.chdir(work)
    rejected(tasks.simobserve, tasks.TaskExecutionError, "overwrite=F",
             **dict(parameters, overwrite=False), outformat="msv4")
    assert marker.read_text() == "previous store"
    rejected(tasks.simobserve, tasks.TaskExecutionError, SIGNED_UV_REFUSAL, **negative, outformat="msv4")
    assert marker.read_text() == "previous store"
    result = tasks.run("simobserve", workspace=work, **parameters, outformat="msv4")
    assert result.value is None and result.successful
    assert "MSv4: SOURCE.REST_FREQUENCY is not carried" in result.logs
    assert not marker.exists()
    assert not list(store.parent.glob(".*ps.zarr*"))
    measurement_sets = [p for p in result.products if p.resource_kind.name == "MEASUREMENT_SET"]
    product, = measurement_sets
    assert product.exists and Path(product.path) == store

    work = root / "foreign"
    foreign = work / PROJECT / f"{STEM}.ps.zarr" / "zarr.json"
    foreign.parent.mkdir(parents=True)
    unrelated = '{"zarr_format":3,"node_type":"group","attributes":{"type":"image"}}'
    foreign.write_text(unrelated)
    os.chdir(work)
    rejected(tasks.simobserve, tasks.TaskExecutionError, "is not an MSv4 processing set",
             **parameters, outformat="msv4")
    assert foreign.read_text() == unrelated


def compare_msv2(root):
    t = tools()
    casa = root / "casa" / PROJECT
    for folder in ("default", "msv2"):
        result = root / folder / PROJECT
        report = compare_case(t, CASE, str(casa), str(result), str(root),
                              argparse.Namespace(uvw_tolerance_m=1e-3, atmosphere_model=None))
        assert not report["differences"], report
        t["tb"].open(str(result / f"{STEM}.ms" / "SPECTRAL_WINDOW"))
        try:
            reference, = t["tb"].getcol("REF_FREQUENCY")
            assert reference == 345e9 and t["tb"].getcell("CHAN_FREQ", 0)[0] == 342e9
        finally:
            t["tb"].close()
        t["tb"].open(str(result / f"{STEM}.ms" / "SOURCE"))
        try:
            assert (t["tb"].getcol("REST_FREQUENCY") == reference).all()
        finally:
            t["tb"].close()
        print(folder, *report["ms"], sep="\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("casa", "create", "msv2", "msv4"))
    parser.add_argument("root", type=Path)
    parser.add_argument("casatestdata", nargs="?", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    if args.operation == "casa":
        if args.casatestdata is None:
            parser.error("casa requires CASATESTDATA")
        prepare(root, args.casatestdata.resolve())
    elif args.operation == "create":
        create(root)
    elif args.operation == "msv2":
        compare_msv2(root)
    else:
        compare_msv4(root / "casa" / PROJECT / f"{STEM}.ms",
                     root / "msv4" / PROJECT / f"{STEM}.ps.zarr",
                     observer="LSRK", datasets=6, flagged_rows=0, offset_reference=True)
