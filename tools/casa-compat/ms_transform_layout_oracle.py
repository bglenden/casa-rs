#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public mstransform output layout and ID handling with CASA 6.7.6.14."""

from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from ms_combine_spws_oracle import make_pair
from ms_hanning_oracle import NOISE, compare_table
from ms_selection_tasks_oracle import MAIN, SUBTABLES, casa_table


def layout(path):
    result = {}
    with casa_table(path) as ms:
        for manager in ms.getdminfo().values():
            spec = manager.get("SPEC", {})
            if "DEFAULTTILESHAPE" not in spec:
                continue
            default = tuple(spec["DEFAULTTILESHAPE"])
            shapes = sorted(
                (tuple(cube["CellShape"]), tuple(cube["TileShape"]))
                for cube in spec.get("HYPERCUBES", {}).values()
            )
            for column in manager["COLUMNS"]:
                result[column] = manager["TYPE"], default, shapes
    return result


def compare_layout(casa, native):
    reference = layout(casa)
    actual = layout(native)
    for column in (
        "DATA", "FLAG", "FLAG_CATEGORY", "WEIGHT", "SIGMA", "WEIGHT_SPECTRUM", "SIGMA_SPECTRUM", "UVW"
    ):
        assert actual[column] == reference[column], (column, actual[column], reference[column])


def paired(root, source, label, *, expected_ddi_offset=None, **options):
    reference, native = (root / f"{label}-{side}.ms" for side in ("casa", "native"))
    for engine, destination in ((casatasks, reference), (tasks, native)):
        assert engine.mstransform(vis=str(source), outputvis=str(destination), datacolumn="data",
                                 usewtspectrum=True, **options) is None
    compare_table(reference, native, set(MAIN) | NOISE, label)
    compare_layout(reference, native)
    for name in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION", "POLARIZATION", "FIELD",
                 "FEED", "SOURCE", "OBSERVATION", "ANTENNA"):
        if name == "DATA_DESCRIPTION" and expected_ddi_offset is not None:
            with (
                table(str(reference / name), ack=False) as expected,
                table(str(native / name), ack=False) as got,
                table(str(native), ack=False) as native_ms,
            ):
                # CASA leaves MAIN IDs dangling when the task is used
                # outside its internal Multi-MS assembly. Native reserves
                # the preceding IDs without changing the published MAIN IDs.
                offset = expected_ddi_offset
                assert got.nrows() == expected.nrows() + offset
                np.testing.assert_array_equal(got.getcol("FLAG_ROW")[:offset], True)
                for col in SUBTABLES[name]:
                    np.testing.assert_array_equal(got.getcol(col)[offset:], expected.getcol(col))
                assert native_ms.getcol("DATA_DESC_ID").max() < got.nrows()
        else:
            compare_table(reference / name, native / name, SUBTABLES[name], f"{label} {name}")
    print(f"passed {label}", flush=True)
    return reference, native


def rejected(root, source):
    for case, tileshape, diagnostic in (
        ("tile-mode", [2], "either 0 or 1"),
        ("tile-length", [1, 2], "1 or 3 elements"),
    ):
        for engine in (casatasks, tasks):
            target = root / f"{case}-{engine.__name__}.ms"
            try:
                engine.mstransform(vis=str(source), outputvis=str(target), datacolumn="data",
                                  tileshape=tileshape)
            except ValueError as error:
                assert diagnostic in str(error), str(error)
                assert not target.exists()
            else:
                raise AssertionError(f"{case} succeeded for {engine.__name__}")
        print(f"passed {case}", flush=True)


def main(cases):
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="mstransform-layout-") as scratch:
        root = Path(scratch)
        source = make_pair(root, "source", 7, counts=(7, 9), spectra=True)
        inputs = [
            ("default", {}),
            ("tile-mosaic", dict(tileshape=[1])),
            ("tile-explicit", dict(tileshape=[1, 4, 17])),
            ("tile-wide", dict(tileshape=[4, 12, 23])),
            ("tile-small", dict(tileshape=[1, 1, 1])),
            ("preserve", dict(reindex=False, field="1", spw="1:1~5", antenna="A1")),
            ("preserve-averaged", dict(reindex=False, field="1", spw="1:1~5",
                                       chanaverage=True, chanbin=2)),
            ("preserve-regridded", dict(reindex=False, field="1", spw="1:1~5",
                                        regridms=True, start=1, nchan=3)),
            ("preserve-combine", dict(reindex=False, spw="0:0~6,1:0~6", combinespws=True)),
            ("ddi-start", dict(ddistart=3, spw="1", expected_ddi_offset=3)),
            ("ddi-start-zero", dict(ddistart=0, spw="1")),
            ("ddi-ignore", dict(ddistart=3, reindex=False, spw="1")),
        ]
        for label, options in inputs:
            if not cases or label in cases:
                reference, native = paired(root, source, label, **options)
                if label == "tile-explicit":
                    for product in (reference, native):
                        output = subprocess.check_output(
                            ["taql", f"select DATA_DESC_ID from [{product}] limit 1"], text=True
                        )
                        assert output.splitlines()[-1] == "0", output
                    print("passed standalone-casacore-reopen", flush=True)
        if not cases or "rejected" in cases:
            rejected(root, source)


if __name__ == "__main__":
    main(set(sys.argv[1:]))
