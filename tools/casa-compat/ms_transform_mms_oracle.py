#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public mstransform Multi-MS behavior with CASA 6.7.6.14."""

from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casatools import ms
from casacore.tables import table
import numpy as np

from casars import tasks
from ms_combine_spws_oracle import make_pair
from ms_hanning_oracle import NOISE, compare_table
from ms_selection_tasks_oracle import MAIN, SUBTABLES
from ms_transform_layout_oracle import compare_layout


def axis(path):
    info = (path / "table.info").read_text().splitlines()
    return next((line.split(" = ")[1] for line in info if line.startswith("AxisType = ")), None)


def member_paths(path):
    return sorted((path / "SUBMSS").glob("*.ms"))


def referenced_members(path):
    tool = ms()
    try:
        tool.open(str(path))
        assert tool.ismultims()
        return [Path(member).name for member in tool.getreferencedtables()]
    finally:
        tool.close()


def compare_members(expected, got, name, *, frequency_tolerance=None):
    reference, native = member_paths(expected), member_paths(got)
    assert len(reference) == len(native), (name, reference, native)
    assert axis(expected) == axis(got), name
    if reference:
        expected_ids = [path.name.rsplit(".", 2)[1] for path in reference]
        native_ids = [path.name.rsplit(".", 2)[1] for path in native]
        assert native_ids == expected_ids, (name, native_ids, expected_ids)
        assert referenced_members(got) == [path.name for path in native], name
    for left, right in [(expected, got), *zip(reference, native)]:
        with table(str(left), ack=False) as expected_ms, table(str(right), ack=False) as actual_ms:
            columns = set(MAIN) | (NOISE & set(expected_ms.colnames()))
            assert NOISE & set(expected_ms.colnames()) == NOISE & set(actual_ms.colnames())
            assert set(expected_ms.colnames()) == set(actual_ms.colnames())
        compare_table(left, right, columns, f"{name} {right.name}")
        for subtable in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION", "POLARIZATION", "FIELD", "SOURCE", "FEED"):
            values = None
            if subtable == "SPECTRAL_WINDOW" and frequency_tolerance is not None:
                def frequency_check(column):
                    def compare(row, actual, reference):
                        np.testing.assert_allclose(
                            actual, reference.getcell(column, row), rtol=0, atol=frequency_tolerance,
                            err_msg=f"{name} {column} row {row}",
                        )
                    return compare
                values = {column: frequency_check(column) for column in (
                    "CHAN_FREQ", "REF_FREQUENCY", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION",
                    "TOTAL_BANDWIDTH",
                )}
            compare_table(left / subtable, right / subtable, SUBTABLES[subtable], f"{name} {subtable}", values)
        with table(str(left / "HISTORY"), ack=False) as expected_history, table(
            str(right / "HISTORY"), ack=False
        ) as actual_history:
            def history_parameters(history):
                return {
                    key.strip(): value.strip()
                    for message in history.getcol("MESSAGE")
                    if "=" in message
                    for key, value in (message.split("=", 1),)
                }

            expected_history = history_parameters(expected_history)
            actual_history = history_parameters(actual_history)
            for key in ("createmms", "scan", "spw", "taql", "hanning", "monolithic_processing",
                        "separationaxis", "numsubms", "disableparallel"):
                assert expected_history[key] == actual_history.get(key), (name, key, expected_history, actual_history)


def run_pair(root, sources, label, *, returns=None, empty=False, frequency_tolerance=None, **options):
    outputs = [root / f"{label}-{side}.ms" for side in ("casa", "native")]
    for engine, source, output in zip((casatasks, tasks), sources, outputs):
        value = engine.mstransform(vis=str(source), outputvis=str(output), datacolumn="data",
                                   **options)
        assert value is returns, (label, engine.__name__, value)
        assert output.exists() != empty, (label, output)
    if not empty:
        compare_members(*outputs, label, frequency_tolerance=frequency_tolerance)
    print(f"passed {label}", flush=True)
    return outputs


def rejected(root, source, label, exception, message, options):
    for engine in (casatasks, tasks):
        output = root / f"{label}-{engine.__name__}.ms"
        try:
            engine.mstransform(vis=str(source), outputvis=str(output), datacolumn="data",
                               disableparallel=True, createmms=True, **options)
        except exception as error:
            assert message in str(error), str(error)
            assert not output.exists()
        else:
            raise AssertionError(f"{label} succeeded for {engine.__name__}")
    print(f"passed {label}", flush=True)


def main(cases):
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="mstransform-mms-") as scratch:
        root = Path(scratch)
        source = make_pair(root, "source", 7, counts=(7, 7), spectra=True)
        selected = lambda *names: not cases or any(name in cases for name in names)
        if selected("scan", "retain", "empty", "flat", "span", "reopen", "member-taql",
                    "retain-mixed-span", "retain-field-combine"):
            scan = run_pair(root, (source, source), "scan", createmms=True,
                            separationaxis="scan", numsubms=2, disableparallel=True)
            if selected("retain"):
                run_pair(root, scan, "retain", returns=True, field="1", hanning=True, disableparallel=True)
            if selected("empty"):
                run_pair(root, scan, "empty", returns=True, empty=True, scan="907", disableparallel=True)
            if selected("flat"):
                run_pair(root, scan, "flat", monolithic_processing=True, disableparallel=True)
            if selected("span"):
                run_pair(root, scan, "span", timeaverage=True, timespan="scan",
                         timebin="120s", disableparallel=True)
            if selected("member-taql"):
                run_pair(root, scan, "member-taql", returns=True, taql="ROWNUMBER() < 10",
                         disableparallel=True)
            if selected("retain-mixed-span"):
                run_pair(root, scan, "retain-mixed-span", returns=True, timeaverage=True,
                         timespan="scan,state", timebin="120s", disableparallel=True)
            if selected("retain-field-combine"):
                run_pair(root, scan, "retain-field-combine", returns=True, field="0",
                         combinespws=True, disableparallel=True)
            if selected("reopen"):
                renamed = root / "moved.ms"
                scan[1].rename(renamed)
                assert len(referenced_members(renamed)) == 2
                assert subprocess.check_output(
                    ["taql", f"select gcount() as n from [{renamed}]"], text=True
                ).splitlines()[-1] == "240"
                print("passed reopen", flush=True)
        if selected("spw", "member", "combination", "flatten-cross", "retain-spw-combine"):
            spw = run_pair(root, (source, source), "spw", createmms=True,
                           separationaxis="spw", numsubms=2, disableparallel=True)
            if selected("member"):
                run_pair(root, spw, "member", returns=True, spw="1:0~5", regridms=True,
                         nchan=3, disableparallel=True)
            if selected("combination"):
                run_pair(root, spw, "combination", combinespws=True, disableparallel=True)
            if selected("retain-spw-combine"):
                run_pair(root, spw, "retain-spw-combine", returns=True, spw="0",
                         combinespws=True, disableparallel=True)
            if selected("flatten-cross"):
                run_pair(root, spw, "flatten-cross", combinespws=True, timeaverage=True,
                         timespan="scan", timebin="60s", disableparallel=True)
        if selected("spw-mixed", "spw-reordered"):
            mixed = make_pair(root, "mixed", 7, counts=(7, 9), spectra=True)
            for label, spw in (
                ("spw-mixed", "0:1~5,1:0~6"),
                ("spw-reordered", "1:0~6,0:1~5"),
            ):
                if selected(label):
                    products = run_pair(root, (mixed, mixed), label, createmms=True, spw=spw,
                                        separationaxis="spw", numsubms=2, disableparallel=True,
                                        chanaverage=True, chanbin=[2, 3], usewtspectrum=True)
                    for left, right in zip(*[member_paths(product) for product in products]):
                        compare_layout(left, right)
        if selected("spw-offset"):
            run_pair(root, (source, source), "spw-offset", createmms=True, separationaxis="spw",
                     numsubms=2, ddistart=2, disableparallel=True)
        if selected("spw-epoch", "spw-range"):
            epochs = make_pair(root, "epochs", 7, counts=(7, 9), spectra=True,
                               time_offset=45 * 86400)
            if selected("spw-epoch"):
                run_pair(root, (epochs, epochs), "spw-epoch", createmms=True, separationaxis="spw",
                         numsubms=2, regridms=True, outframe="BARY", nchan=4, disableparallel=True,
                         frequency_tolerance=0.001)
            if selected("spw-range"):
                for engine, widths in ((casatasks, [7, 9]), (tasks, [5, 5])):
                    product = root / f"spw-range-{engine.__name__}.ms"
                    assert engine.mstransform(
                        vis=str(epochs), outputvis=str(product), datacolumn="data", createmms=True,
                        separationaxis="spw", numsubms=2, spw="0~1:1~5", disableparallel=True,
                    ) is None
                    with table(str(product / "SPECTRAL_WINDOW"), ack=False) as spw_table:
                        np.testing.assert_array_equal(spw_table.getcol("NUM_CHAN"), widths)
                print("passed SPW range qualifier deviation", flush=True)
        for name, options in (
            ("baseline", dict(separationaxis="baseline", numsubms=2, disableparallel=True)),
            ("auto", dict(numsubms=2, disableparallel=True)),
            ("count-auto", dict(separationaxis="scan", disableparallel=True)),
            ("standalone", dict(separationaxis="scan", numsubms=2)),
        ):
            if selected(name):
                run_pair(root, (source, source), name, createmms=True, **options)
        if selected("errors"):
            rejected(root, source, "invalid-axis", AssertionError, "unallowed value",
                     dict(separationaxis="unknown", numsubms=2))
            rejected(root, source, "invalid-count", ZeroDivisionError, "division by zero",
                     dict(separationaxis="scan", numsubms=0))
            rejected(root, source, "invalid-spw-combination", Exception, "Cannot create MMS",
                     dict(separationaxis="spw", combinespws=True, numsubms=2))
            rejected(root, source, "no-scans", ValueError, "No Scans", dict(separationaxis="scan", scan="907"))
        if selected("unflagged"):
            flagged = make_pair(root, "flagged", 7, counts=(7, 7), spectra=True)
            with table(str(flagged), readonly=False, ack=False) as main:
                scans = main.getcol("SCAN_NUMBER")
                main.putcol("FLAG_ROW", scans == 1)
            run_pair(root, (flagged, flagged), "partly-flagged", createmms=True, keepflags=False,
                     separationaxis="scan", numsubms=2, disableparallel=True)
            with table(str(flagged), readonly=False, ack=False) as main:
                main.putcol("FLAG_ROW", np.ones(main.nrows(), dtype=bool))
            run_pair(root, (flagged, flagged), "fully-flagged", createmms=True, keepflags=False,
                     separationaxis="spw", numsubms=2, disableparallel=True, empty=True)
            expected, native = (root / f"auto-flagged-{side}.ms" for side in ("casa", "native"))
            for engine, destination in ((casatasks, expected), (tasks, native)):
                assert engine.mstransform(
                    vis=str(flagged), outputvis=str(destination), createmms=True, keepflags=False,
                    datacolumn="data", numsubms=2, disableparallel=True,
                ) is None
            assert expected.exists()
            with table(str(expected), ack=False) as rows:
                assert np.all(rows.getcol("FLAG_ROW"))
            assert not native.exists()
            print("passed automatic-partition keepflags deviation", flush=True)


if __name__ == "__main__":
    main(set(sys.argv[1:]))
