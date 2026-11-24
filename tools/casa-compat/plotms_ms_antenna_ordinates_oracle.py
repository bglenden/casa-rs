#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare independent PlotMS visibility ordinates after per-antenna averaging."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_baseline_channels_oracle import make_fixture
from plotms_ms_channel_ordinates_oracle import compare_sides
from plotms_ms_columns_oracle import compare_text, copy_ms, opened, plotms_launcher, reference, request, rows
from plotms_ms_spw_average_oracle import add_second_field


def compare_antenna_ordinate_lists(root, native, *, only):
    root.mkdir()
    vis, _, _, spectra_flags = make_fixture(root)
    if only is not None:
        compare_antenna_ordinate_list_spans(root, native, vis, spectra_flags, edges_only=only == "span-edges")
        return
    controls = request(vis) | dict(antenna="A0&A1;A0&A2", xaxis="channel", xdatacolumn="", spw="0:0~3",
                                    avgantenna=True, yaxis=["real"] * 3,
                                    ydatacolumn=["data", "corrected", "model"],
                                    yaxislocation=["left", "right", "left"])
    time_channel = dict(vis=str(spectra_flags), avgtime="20", avgchannel="2")
    for name, overrides, count, first in (
        ("vector-antenna", {}, 162, (3.7, 37., 370.)),
        ("scalar-antenna", dict(scalar=True), 162, (3.7, 37., 370.)),
        ("vector-time-channel-antenna", time_channel, 36, (3.3, 30., 300.)),
        ("scalar-time-channel-antenna", time_channel | dict(scalar=True), 36, (2.84, 26., 260.)),
    ):
        options = controls | overrides | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == count
        assert {tuple(point[5:9]) for point in selected} == {
            (str(antenna), "-1", f"A{antenna}@P{antenna}", "*") for antenna in (0, 1, 2)
        }
        assert {point[12] for point in selected} == {"XX"}
        np.testing.assert_allclose([float(selected[index * (count // 3)][1]) for index in range(3)],
                                   first, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching antenna-averaged ordinate coordinates", flush=True)
        if name == "vector-time-channel-antenna":
            figure = controls | overrides | dict(xlabel="Channel bin")
            compare_sides(root, native, figure, expected_x="Channel bin", name=name)
    compare_antenna_ordinate_edges(root, native, vis, controls)
    compare_antenna_row_flags(root, native, vis, controls)
    compare_antenna_ordinate_list_spans(root, native, vis, spectra_flags)


def compare_antenna_ordinate_edges(root, native, vis, controls):
    from casars.tasks import TaskParameterError

    complex_data, flagged = (root / f"{name}.ms" for name in ("complex-ordinates", "all-cube-flagged"))
    copy_ms(vis, complex_data)
    copy_ms(vis, flagged)
    with opened(complex_data, True) as table:
        high = table.getcol("ANTENNA2") == 2
        for column, low, upper in (
            ("DATA", 1 + 2j, 4 + 3j),
            ("CORRECTED_DATA", 2 - 3j, -5 + 7j),
            ("MODEL_DATA", 6 - 2j, -7 - 6j),
        ):
            values = np.where(high, upper, low).astype(np.complex64)
            table.putcol(column, np.broadcast_to(values, table.getcol(column).shape))
    with opened(flagged, True) as table:
        flags = table.getcol("FLAG")
        flags[:] = True
        table.putcol("FLAG", flags)

    four = dict(vis=str(complex_data), xaxis="real", xdatacolumn="corrected",
                yaxis=["real", "imag", "amp", "phase"],
                ydatacolumn=["data", "model", "corrected", "data"],
                yaxislocation=["left", "right", "left", "right"])
    for mode, scalar in (("vector", False), ("scalar", True)):
        for case, overrides, count in (("complex", four, 216), ("empty", dict(vis=str(flagged)), 0)):
            name = f"{mode}-{case}"
            options = controls | overrides | dict(scalar=scalar, expformat="txt")
            expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**options, plotfile=str(expected)) is None
            assert len(rows(expected)) == count
            assert native(**options, plotfile=str(actual)) is None
            if count:
                assert compare_text(actual, expected) == count
            else:
                assert expected.read_text().count("# No data to export") == 3
                assert actual.read_bytes() == expected.read_bytes()
            print(f"{name}: {count if count else 'three empty sections'} matched", flush=True)

    for name, overrides in (("overlay", dict(showatm=True)), ("iteration", dict(iteraxis="corr"))):
        output = root / f"antenna-list-{name}.txt"
        options = controls | overrides | dict(plotfile=str(output), expformat="txt")
        try:
            native(**options)
        except TaskParameterError as error:
            assert "#1032" in str(error), error
        else:
            raise AssertionError(f"PlotMS accepted the pending antenna-averaged list with {name}")
        assert not output.exists()
        print(f"antenna-list-{name}: existing refusal preserved", flush=True)


def compare_antenna_row_flags(root, native, source, controls):
    for name, cube_flagged, count in (("row-only", False, 162), ("row-and-cube", True, 0)):
        vis = root / f"{name}.ms"
        copy_ms(source, vis)
        with opened(vis, True) as table:
            row_flags = table.getcol("FLAG_ROW")
            row_flags[:] = True
            table.putcol("FLAG_ROW", row_flags)
            if cube_flagged:
                flags = table.getcol("FLAG")
                flags[:] = True
                table.putcol("FLAG", flags)
        for mode, scalar in (("vector", False), ("scalar", True)):
            case = f"{mode}-{name}"
            options = controls | dict(vis=str(vis), scalar=scalar, expformat="txt")
            expected, actual = (root / f"{case}.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**options, plotfile=str(expected)) is None
            assert len(rows(expected)) == count
            assert native(**options, plotfile=str(actual)) is None
            if count:
                assert compare_text(actual, expected) == count
            else:
                assert expected.read_text().count("# No data to export") == 3
                assert actual.read_bytes() == expected.read_bytes()
            print(f"{case}: {count if count else 'three empty sections'} matched", flush=True)


def compare_antenna_ordinate_list_spans(root, native, source, masked_spectra, *, edges_only=False):
    for case, scan, field, channels in (
        ("scan", True, False, False), ("field", False, True, False),
        ("both", True, True, False), ("both-channel-bins", True, True, True),
    ):
        if edges_only and case != "both":
            continue
        vis = root / f"list-span-{case}.ms"
        copy_ms(masked_spectra if channels else source, vis)
        with opened(vis / "FIELD") as table:
            has_second_field = table.nrows() > 1
        if field and not has_second_field:
            add_second_field(vis)
        with opened(vis, True) as table:
            times = table.getcol("TIME")
            start = times.min()
            late = times >= start + 29
            for column, active, value in (("SCAN_NUMBER", scan, 7), ("FIELD_ID", field, 1)):
                if active:
                    ids = table.getcol(column)
                    ids[late] = value
                    table.putcol(column, ids)
            for column in ("DATA", "CORRECTED_DATA", "MODEL_DATA"):
                table.putcol(column, (table.getcol(column) * (1 + late)).astype(np.complex64))
        controls = request(vis) | dict(
            antenna="A0&A1;A0&A2", field="0,1", xaxis="time", xdatacolumn="",
            spw="0:0~3" if channels else "0:1", avgantenna=True, avgtime="80",
            avgchannel="2" if channels else "", avgscan=scan, avgfield=field,
            msselect=f"TIME < {start + 51:.3f}", yaxis=["real"] * 3,
            ydatacolumn=["data", "corrected", "model"], yaxislocation=["left", "right", "left"],
            expformat="txt",
        )
        if case == "both":
            compare_antenna_span_edges(root, native, vis, controls, start)
        if edges_only:
            return
        count = 18 if channels else 9
        for mode, scalar in (("vector", False), ("scalar", True)):
            name = f"{mode}-{case}-antenna-ordinates"
            expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
            options = controls | dict(scalar=scalar)
            assert reference(**options, plotfile=str(expected)) is None
            selected = rows(expected)
            assert len(selected) == count
            scan_id = "-1" if scalar and scan else "1"
            assert {tuple(point[3:5]) for point in selected} == {(scan_id, "0")}
            assert {tuple(point[5:9]) for point in selected} == {
                (str(antenna), "-1", f"A{antenna}@P{antenna}", "*") for antenna in (0, 1, 2)
            }
            if not channels:
                np.testing.assert_allclose([float(point[1]) for point in selected],
                                           np.outer([1, 10, 100], [8.55, 4.5, 9]).ravel(), rtol=1.2e-5)
            assert native(**options, plotfile=str(actual)) is None
            assert compare_text(actual, expected) == count
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                                       [float(point[0]) for point in selected], rtol=0, atol=.00051)
            print(f"{name}: {count} matching per-antenna span coordinates", flush=True)
            if channels and not scalar:
                compare_sides(root, native, options, expected_x="Time (MJD seconds)", name=name)


def compare_antenna_span_edges(root, native, source, controls, start):
    from casars.tasks import TaskParameterError

    short, complex_data = (root / f"list-span-{name}.ms" for name in ("short", "complex"))
    copy_ms(source, short)
    copy_ms(source, complex_data)
    with opened(short, True) as table:
        late = table.getcol("TIME") >= start + 19
        for column, value in (("SCAN_NUMBER", 7), ("FIELD_ID", 1)):
            ids = table.getcol(column)
            ids[late] = value
            table.putcol(column, ids)
    short_controls = controls | dict(vis=str(short), avgtime="20")
    expected, actual = (root / f"vector-short-span-control.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**short_controls, plotfile=str(expected)) is None
    assert native(**short_controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 18
    np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                               [float(point[0]) for point in rows(expected)], rtol=0, atol=.00051)
    print("vector-short-span-control: 18 matching uncolored coordinates", flush=True)
    for axis in ("scan", "field"):
        output = root / f"vector-short-span-{axis}.native.txt"
        try:
            native(**short_controls, coloraxis=axis, plotfile=str(output))
        except TaskParameterError as error:
            assert "#1032" in str(error), error
        else:
            raise AssertionError(f"PlotMS accepted pending vector antenna spans colored by {axis}")
        assert not output.exists()
        print(f"vector-short-span-{axis}: pending color grouping refusal verified", flush=True)

    with opened(complex_data, True) as table:
        late = table.getcol("TIME") >= start + 29
        for column, early, later in (
            ("DATA", 1 + 2j, -2 + 3j), ("CORRECTED_DATA", 1 - 3j, 3 + 2j),
            ("MODEL_DATA", -2 + 1j, 2 - 3j),
        ):
            values = table.getcol(column) * np.where(late, later, early)
            table.putcol(column, values.astype(np.complex64))
    complex_controls = controls | dict(vis=str(complex_data), xaxis="real", xdatacolumn="corrected",
                                       yaxis=["real", "imag", "phase"], ydatacolumn=["data", "model", "corrected"])
    for axis in ("scan", "field"):
        for mode, scalar in (("vector", False), ("scalar", True)):
            name = f"{mode}-antenna-keep-{axis}"
            options = complex_controls | dict(avgscan=axis == "field", avgfield=axis == "scan", scalar=scalar)
            expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**options, plotfile=str(expected)) is None
            assert {tuple(point[3:5]) for point in rows(expected)} == {("1", "0"), ("7", "1")}
            assert native(**options, plotfile=str(actual)) is None
            assert compare_text(actual, expected) == 18
            print(f"{name}: 18 matching independent coordinates in retained groups", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("spans", "span-edges"))
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    calibrate = args.calibrate_bin or args.native_bin.with_name("calibrate")
    os.environ["CASARS_CALIBRATE_BIN"] = str(calibrate.resolve())
    from casars.tasks import plotms as native

    with chdir(root), plotms_launcher(args.app_run):
        compare_antenna_ordinate_lists(root / "ordinate-lists", native, only=args.only)


if __name__ == "__main__":
    main()
