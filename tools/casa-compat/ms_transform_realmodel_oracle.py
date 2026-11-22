#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare mstransform virtual-model output with public CASA 6.7.6.14."""

import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casatools import componentlist
from casacore.tables import table
import numpy as np

from casars import tasks
from ms_combine_spws_oracle import make_pair
from ms_hanning_oracle import NOISE, VISIBILITY, compare_table
from ms_selection_tasks_oracle import MAIN, SUBTABLES
from ms_transform_callib_oracle import library, snapshot
from ms_transform_mms_oracle import member_paths, referenced_members


def install_model(root, source):
    with table(str(source / "FIELD"), ack=False) as fields:
        ra, dec = np.asarray(fields.getcell("PHASE_DIR", 0)).ravel()
    path = root / "point.cl"
    component = componentlist()
    try:
        component.addcomponent(flux=2.0, fluxunit="Jy", shape="point",
                               dir=f"J2000 {ra}rad {dec}rad", freq="100GHz")
        component.rename(str(path))
    finally:
        component.done()
    assert casatasks.ft(vis=str(source), field="0", complist=str(path), usescratch=False) is None


def compare_products(expected, actual, case, *, metadata=True):
    with table(str(expected), ack=False) as reference, table(str(actual), ack=False) as native:
        wanted = (VISIBILITY | NOISE) & set(reference.colnames())
        assert wanted == (VISIBILITY | NOISE) & set(native.colnames()), case
    compare_table(expected, actual, (set(MAIN) - VISIBILITY) | wanted, case)
    if metadata:
        for name in ("FIELD", "SPECTRAL_WINDOW", "DATA_DESCRIPTION", "POLARIZATION", "SOURCE", "FEED"):
            compare_table(expected / name, actual / name, SUBTABLES[name], f"{case} {name}")


def verify_model_keywords(expected, actual):
    def model_keys(path):
        with table(str(path), ack=False) as ms:
            keys = set(ms.getkeywords())
        return {key for key in keys if key.startswith(("definedmodel_field_", "model_"))}

    for left, right in ((expected, actual), (expected / "SOURCE", actual / "SOURCE")):
        assert model_keys(left) == model_keys(right), (left, model_keys(left), model_keys(right))
    with table(str(expected / "SOURCE"), ack=False) as reference, table(str(actual / "SOURCE"), ack=False) as native:
        assert ("SOURCE_MODEL" in reference.colnames()) == ("SOURCE_MODEL" in native.colnames())


def paired(root, source, case, *, createmms=False, **options):
    expected, actual = (root / f"{case}-{side}.ms" for side in ("casa", "native"))
    common = dict(vis=str(source), datacolumn="model", realmodelcol=True, createmms=createmms) | options
    before = snapshot(source)
    for engine, output in ((casatasks, expected), (tasks, actual)):
        assert engine.mstransform(outputvis=str(output), **common) is None
        assert snapshot(source) == before
    compare_products(expected, actual, case)
    if createmms:
        left, right = member_paths(expected), member_paths(actual)
        assert len(left) == len(right) == 2
        assert referenced_members(actual) == [path.name for path in right]
        for index, (ref_part, native_part) in enumerate(zip(left, right)):
            compare_products(ref_part, native_part, f"{case} member {index}")
    verify_model_keywords(expected, actual)
    with table(str(actual / "HISTORY"), ack=False) as history:
        lines = {"".join(line.split()) for line in history.getcol("MESSAGE")}
        assert f'realmodelcol={common["realmodelcol"]}' in lines
    print(f"passed {case}", flush=True)
    return expected, actual


def refusing_and_retry(root, source):
    before = snapshot(source)
    for engine in (casatasks, tasks):
        output = root / f"retry-{engine.__name__}.ms"
        arguments = dict(vis=str(source), outputvis=str(output), datacolumn="model")
        try:
            engine.mstransform(**arguments)
        except RuntimeError as error:
            assert "MODEL_DATA" in str(error) and not output.exists()
        else:
            raise AssertionError("accepted an unavailable physical model")
        assert snapshot(source) == before
        assert engine.mstransform(**arguments, realmodelcol=True) is None
        assert snapshot(source) == before
    print("passed absent physical model and retry", flush=True)


def main(cases):
    assert casatasks.version() == [6, 7, 6, 14]
    selected = lambda name: not cases or name in cases
    with TemporaryDirectory(prefix="mstransform-model-") as temporary:
        root = Path(temporary)
        source = make_pair(root, "input", 7, counts=(7, 7), spectra=True)
        if selected("stored"):
            paired(root, source, "stored", realmodelcol=False)
        install_model(root, source)
        for case, options in (
            ("priority-default", dict(realmodelcol=False)),
            ("priority-explicit", {}),
            ("all-stored", dict(datacolumn="all", realmodelcol=False)),
            ("stored-fallback", dict(field="1")),
            ("time-average", dict(timeaverage=True, timebin="24s")),
            ("channel-average", dict(chanaverage=True, chanbin=2)),
            ("take-channels", dict(spw="0:1~4")),
            ("continuum", dict(douvcontsub=True, fitspw="NONE")),
        ):
            if selected(case):
                paired(root, source, case, **options)
        if selected("callibrary"):
            gain = library(root, source, "gain")
            paired(root, source, "callibrary", datacolumn="all", docallib=True, callib=str(gain))
        with table(str(source), ack=False, readonly=False) as ms:
            ms.removecols("MODEL_DATA")
        for case, options in (
            ("promote", {}),
            ("take-correlation", dict(correlation="XX")),
            ("promote-all", dict(datacolumn="all")),
            ("all-unpromoted", dict(datacolumn="all", realmodelcol=False)),
            ("data-only", dict(datacolumn="data")),
            ("unit-fallback", dict(field="1")),
            ("hanning", dict(hanning=True)),
            ("multi-ms", dict(datacolumn="all", createmms=True, separationaxis="scan",
                             disableparallel=True, numsubms=2)),
        ):
            if not selected(case):
                continue
            _, actual = paired(root, source, case, **options)
            if case in ("promote", "unit-fallback"):
                with table(str(actual), ack=False) as ms:
                    np.testing.assert_array_equal(ms.getcell("DATA", 0), 2 if case == "promote" else 1)
            if case == "promote":
                assert subprocess.check_output(
                    ["taql", f"select DATA_DESC_ID from [{actual}] limit 1"], text=True,
                ).splitlines()[-1] == "0"
        if selected("retry"):
            refusing_and_retry(root, source)


if __name__ == "__main__":
    main(set(sys.argv[1:]))
