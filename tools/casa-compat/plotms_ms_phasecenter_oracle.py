#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare fixed MeasurementSet phase-center shifts through public PlotMS."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import numpy as np

from plotms_ms_columns_oracle import (
    casa, compare_figure, compare_text, fixture, opened, plotms_launcher, reference, request, rows,
)


PHASE_CENTER = "J2000 00h00m02.00s -29d59m40.00s"


def compare_values(root, native, vis, shifted, name, axes):
    controls = request(vis) | axes | dict(expformat="txt")
    original, expected, actual = (root / f"{name}.{engine}.txt"
                                  for engine in ("original", "casa", "native"))
    assert reference(**(controls | dict(transform=False, phasecenter=PHASE_CENTER)),
                     plotfile=str(original)) is None
    assert reference(**(controls | dict(vis=str(shifted))), plotfile=str(expected)) is None
    original_rows, expected_rows = rows(original), rows(expected)
    assert len(original_rows) == len(expected_rows) == 12
    assert [point[2:] for point in original_rows] == [point[2:] for point in expected_rows]
    assert {(point[2], point[12]) for point in expected_rows} == {("1", "XX"), ("3", "XX")}
    before, after = (np.array([point[:2] for point in selected], dtype=float)
                     for selected in (original_rows, expected_rows))
    assert np.all(np.max(np.abs(after - before), axis=0) > 1e-4), (name, before, after)

    assert native(**controls, transform=True, phasecenter=PHASE_CENTER, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
    inactive = root / f"{name}.inactive.txt"
    assert native(**controls, transform=False, phasecenter=PHASE_CENTER, plotfile=str(inactive)) is None
    assert compare_text(inactive, original) == 12
    print(f"{name}: 12 shifted and 12 unchanged selected coordinates", flush=True)
    return expected


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
        vis, original_columns = fixture(root)
        with opened(vis, True) as table:
            row = np.arange(table.nrows())
            uvw = np.array([-30 - row / 16, 42 + row / 8, 125 - row / 4])
            table.putcol("UVW", uvw)
        shifted = root / "shifted.ms"
        # Direct PlotMS text export stalls after its selection header. The
        # phaseshift task uses the same tviphaseshift iterator and materializes
        # its coordinates into a separate MS readable by pinned PlotMS.
        casa.phaseshift(vis=str(vis), outputvis=str(shifted), phasecenter=PHASE_CENTER,
                        datacolumn="all")
        compare_values(root, native, vis, shifted, "independent", dict(xaxis="real", yaxis="imag"))
        for axis in ("u", "v", "w"):
            controls = dict(xaxis=axis, yaxis="phase", xdatacolumn="", ydatacolumn="data")
            expected = compare_values(root, native, vis, shifted, axis, controls)
            if axis == "u":
                image = request(vis) | controls | dict(transform=True, phasecenter=PHASE_CENTER,
                                                       title="Shifted visibility")
                compare_figure(root, native, image, expected,
                               ("Shifted visibility", "U (m)", "Phase (degrees)"))
        compare_values(root, native, vis, shifted, "model",
                       dict(xaxis="real", yaxis="imag", xdatacolumn="model"))
        with opened(vis) as table:
            for name, values in (original_columns | {"UVW": uvw}).items():
                np.testing.assert_array_equal(table.getcol(name), values)


if __name__ == "__main__":
    main()
