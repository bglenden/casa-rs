#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Verify public MeasurementSet PlotMS display and documented static controls."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import casa, compare_text, fixture, opened, plotms_launcher, reference, request, rows


def refused(plot, options, dest, kind, expected, *, exact=False):
    try:
        plot(**options, plotfile=str(dest))
    except Exception as error:
        assert type(error) is kind, error
        if exact:
            assert str(error) == expected, error
        else:
            assert expected in str(error), error
    else:
        raise AssertionError(f"unexpected output for {dest}")
    assert not dest.exists(), dest


def compare_refusals(root, native, base):
    cases = (
        ("plotindex", dict(plotindex=1, clearplots=False), 0),
        ("clearplots", dict(clearplots=False), True),
        ("extendflag", dict(extendflag=True), False),
        ("extcorr", dict(extcorr=True), False),
        ("extchannel", dict(extchannel=True), False),
    )
    for parameter, options, default in cases:
        output = root / f"refused-{parameter}.txt"
        try:
            native(**(base | options), plotfile=str(output))
        except ValueError as error:
            assert type(error) is ValueError
            message = str(error)
            assert f'parameter "{parameter}" is not implemented by casa-rs yet (#1032)' in message
            assert f"default {default} is accepted" in message
        else:
            raise AssertionError(f"accepted the nondefault {parameter}")
        assert not output.exists()
    for index, message in ((-1, "a negative plotindex is not valid."),
                           (1, "a nonzero plotindex is not valid when clearing plots.")):
        for engine, task in (("casa", reference), ("native", native)):
            refused(task, base | dict(plotindex=index), root / f"index-{index}.{engine}.txt",
                    ValueError, message, exact=True)
    print("Five documented default-only controls and both pinned invalid-index cases verified.", flush=True)


def baseline_text(root, native, base):
    expected, actual = (root / f"selected.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**base, plotfile=str(expected)) is None
    assert native(**base, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
    return expected


def compare_display(root, native, base, expected):
    from matplotlib import pyplot as plt
    from matplotlib.figure import Figure

    options = base | dict(width=500, height=360, dpi=100)
    products = [root / f"display-off.{engine}.png" for engine in ("casa", "native")]
    assert reference(**options, plotfile=str(products[0])) is None
    figure_numbers = plt.get_fignums()
    shown = []

    def present(figure):
        shown.append(figure)

    with patch.object(Figure, "show", autospec=True, side_effect=present) as show:
        assert native(**options, plotfile=str(products[1])) is None
        show.assert_not_called()
        assert plt.get_fignums() == figure_numbers
        with patch.dict(os.environ):
            os.environ.pop("DISPLAY", None)
            refused(reference, base, root / "missing-display.casa.txt", RuntimeError,
                    "ERROR: DISPLAY environment variable is not set! Cannot run plotms.", exact=True)
            dest = root / "missing-display.native.txt"
            assert native(**base, plotfile=str(dest)) is None
            assert compare_text(dest, expected) == 12
            current_files = set(root.iterdir())
            assert native(**(base | dict(showgui=True))) is None
            assert set(root.iterdir()) == current_files
        show.assert_called_once()
    assert len(shown) == 1
    figure = shown.pop()
    try:
        assert len(figure.axes) == 1
        axis = figure.axes[0]
        assert axis.get_xlabel() == "Channel"
        assert axis.get_ylabel() == "Amp"
        coords = np.array([point[:2] for point in rows(expected)], dtype=float)
        np.testing.assert_allclose(axis.lines[0].get_xydata(), coords, rtol=1.2e-5)
    finally:
        plt.close(figure)
    assert plt.get_fignums() == figure_numbers
    for dest in products:
        with Image.open(dest) as image:
            assert image.size == (500, 360)
    print("Headless files and one mocked Matplotlib display preserve the selected coordinates.", flush=True)


def compare_disabled_paths(root, native, base, expected):
    unfiltered = root / "unfiltered.casa.txt"
    assert reference(**(base | dict(field="", antenna="", spw="", correlation="")),
                     plotfile=str(unfiltered)) is None
    selected = base | dict(selectdata=False, field="unmatched-field", antenna="unknown-antenna",
                           spw="unknown-spw", correlation="unknown-correlation", scan="not-a-scan")
    refused(reference, selected, root / "disabled-selection.casa.txt", Exception,
            "updateImmediately must be a boolean", exact=True)
    dest = root / "disabled-selection.native.txt"
    assert native(**selected, plotfile=str(dest)) is None
    count = compare_text(dest, unfiltered)
    assert count == 1800 and {row[4] for row in rows(unfiltered)} == {"0", "1"}
    average = base | dict(averagedata=False, avgchannel="2", avgtime="80", avgscan=True,
                          avgfield=True, avgspw=True, avgantenna=True, avgbaseline=True, scalar=True)
    for engine, task in (("casa", reference), ("native", native)):
        dest = root / f"disabled-average.{engine}.txt"
        assert task(**average, plotfile=str(dest)) is None
        assert compare_text(dest, expected) == 12
    print(f"Disabled selection preserves {count} unfiltered samples and disabled averaging preserves 12.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    from casars.tasks import plotms as native

    root = args.work.resolve()
    root.mkdir(parents=True)
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    with chdir(root), plotms_launcher(args.app_run.resolve() if args.app_run else None):
        vis, inputs = fixture(root)
        base = request(vis) | dict(xaxis="channel", xdatacolumn="", yaxis="amp", ydatacolumn="data")
        expected = baseline_text(root, native, base)
        compare_refusals(root, native, base)
        compare_display(root, native, base, expected)
        compare_disabled_paths(root, native, base, expected)
        with opened(vis) as table:
            for name, values in inputs.items():
                np.testing.assert_array_equal(table.getcol(name), values)
    print("The static controls leave visibility data and flags unchanged.", flush=True)


if __name__ == "__main__":
    main()
