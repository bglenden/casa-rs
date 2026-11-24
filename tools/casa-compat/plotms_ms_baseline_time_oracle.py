#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public PlotMS vector time and baseline averages with pinned CASA."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_baseline_channels_oracle import make_fixture as make_channel_baseline_fixture
from plotms_ms_baseline_channels_oracle import make_reversed_partial_spectrum
from plotms_ms_columns_oracle import (
    compare_ordinate_figure,
    compare_text,
    copy_ms,
    opened,
    plotms_launcher,
    reference,
    request,
    rows,
    time_average_fixture,
)


def make_fixture(root):
    source, integration = time_average_fixture(root)
    with opened(source, True) as table:
        high = table.getcol("ANTENNA2") == 2
        sequence = integration % 6
        shape = table.getcol("DATA").shape
        amplitude = np.where(high, 3 + 2 * sequence, 1 + sequence).astype(np.complex64)
        for index, name in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            table.putcol(name, np.broadcast_to(amplitude * 10**index, shape).copy())
        for name, values in (
            ("WEIGHT", np.where(high, 1 + (sequence % 3) * 2, 9 - sequence)),
            ("SIGMA", np.where(sequence % 2 == 0, 1, 0.5)),
        ):
            table.putcol(name, np.broadcast_to(values.astype(np.float32), (shape[0], shape[2])).copy())
        start = table.getcol("TIME").min()
    flagged = root / "flags.ms"
    copy_ms(source, flagged)
    with opened(flagged, True) as table:
        flag = table.getcol("FLAG")
        flag[0, 1, ~high & np.isin(sequence, [1, 3, 4])] = True
        table.putcol("FLAG", flag)
    return source, flagged, start


def compare(root, native):
    source, flagged, start = make_fixture(root)
    controls = request(source) | dict(
        antenna="A0&A1;A0&A2", xaxis="real", xdatacolumn="data",
        yaxis=["real", "real"], ydatacolumn=["corrected", "model"],
        yaxislocation=["left", "right"], spw="0:1", avgbaseline=True, avgtime="20",
    )
    late_baseline = dict(msselect=f"ANTENNA1 != 0 || ANTENNA2 != 2 || TIME > {start + 0.5:.3f}")
    late_low_antenna = f"ANTENNA1 != 0 || ANTENNA2 != 1 || TIME > {start + 0.5:.3f}"
    cases = (
        ("equal-endpoint", {}, [(3.75, 33), (9, 1470 / 17)], [15, 45]),
        ("row-flags", dict(vis=str(flagged)), [(100 / 26, 920 / 26), (10.4, 1220 / 12)], [10, 45]),
        ("row-flags-time", dict(vis=str(flagged), xaxis="time"),
         [(start + 10, 920 / 26), (start + 45, 1220 / 12)], [10, 45]),
        ("retained-time-boundary", dict(avgtime="15"),
         [(49 / 21, 430 / 21), (101 / 19, 890 / 19), (9, 1470 / 17)], [5, 25, 45]),
        ("singleton-time-stage", dict(avgtime="4"),
         [(v, 10 * v) for v in (1.2, 31 / 11, 56 / 12, 33 / 7, 58 / 8, 89 / 9)], list(range(0, 60, 10))),
        ("late-baseline", late_baseline, [(123.2 / 38, 1200 / 38), (8.6, 1560 / 18)], [15, 40]),
        ("flagged-late-baseline", late_baseline | dict(vis=str(flagged)),
         [(3.05, 800 / 24), (126.6 / 13, 1310 / 13)], [10, 40]),
        ("final-buffer-order", dict(xaxis="time", avgtime="120", msselect=late_low_antenna),
         [(start + 25, 56.25)], [25]),
    )
    for name, options, corrected, offsets in cases:
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        options = controls | options | dict(expformat="txt")
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 2 * len(offsets)
        assert {tuple(point[5:9]) for point in selected} == {("-1", "-1", "*", "*")}
        assert [point[9] for point in selected] == [f"{start + offset:.3f}" for offset in offsets] * 2
        model = [(x, 10 * y) for x, y in corrected]
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   corrected + model, rtol=1.2e-5)
        if options["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in selected],
                                       [start + offset for offset in offsets] * 2, rtol=0, atol=0.00051)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching time-and-baseline coordinates", flush=True)
    compare_ordinate_figure(root, "equal-endpoint", native, controls, "Real", ["Real:corrected", "Real:model"])

    expected, actual = (root / f"single-ordinate.{engine}.txt" for engine in ("casa", "native"))
    options = controls | dict(yaxis="real", ydatacolumn="corrected", yaxislocation="left", expformat="txt")
    assert reference(**options, plotfile=str(expected)) is None
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 2
    compare_partial_spectrum(root, native, controls)


def compare_partial_spectrum(root, native, controls):
    work = root / "partial-spectrum"
    work.mkdir()
    source, *_ = make_channel_baseline_fixture(work)
    vis = make_reversed_partial_spectrum(source, work / "reversed-partial-spectrum.ms")
    expected, actual = (root / f"spectrum-availability.{engine}.txt" for engine in ("casa", "native"))
    options = controls | dict(vis=str(vis), expformat="txt")
    assert reference(**options, plotfile=str(expected)) is None
    selected = rows(expected)
    assert len(selected) == 4
    np.testing.assert_array_equal(np.array([point[:2] for point in selected], dtype=float),
                                  [(10, 45), (10, 55), (10, 450), (10, 550)])
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == len(selected)
    print(f"spectrum-availability: {len(selected)} matching time-and-baseline coordinates", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
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
        compare(root, native)


if __name__ == "__main__":
    main()
