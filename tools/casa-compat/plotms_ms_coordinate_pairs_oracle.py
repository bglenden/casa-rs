#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare paired coordinate axes through public static PlotMS."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import casatasks
import numpy as np
from PIL import Image

from plotms_calibration_oracle import compare_text, plotms_launcher, reference
from plotms_ms_columns_oracle import opened, rows
from plotms_ms_metadata_axes_oracle import casa_ephemeris_export, controls, make_fixture

def compare_dual_ephemeris(work, source, native, fresh_reference):
    from casars.tasks import _plotms_plot
    from gaincal_oracle import ephemeris_observation

    root = work / "two-ephemeris-ordinates"
    root.mkdir()
    vis, _ = ephemeris_observation(root, source, offset=(0.002, -0.004))
    request = controls(vis) | dict(
        xaxis="time", yaxis=["Distance (rho)", "Radial Velocity"],
        yaxislocation=["left", "right"], antenna="A0&A1;A0&A2", spw="0:1~3",
    )
    expected, actual = (root / f"{engine}.txt" for engine in ("casa", "native"))
    fresh_reference(**request, expformat="txt", plotfile=str(expected))
    native(**request, expformat="txt", plotfile=str(actual))
    assert compare_text(actual, expected) == 12
    data = np.array([row[:2] for row in rows(expected)], dtype=float)
    reference_png, native_png = (root / f"{engine}.png" for engine in ("casa", "native"))
    figure_args = request | dict(expformat="png", width=760, height=480, dpi=100)
    fresh_reference(**figure_args, plotfile=str(reference_png))

    configure = _plotms_plot.configure
    captured = []

    def record(figure, axes, document, options):
        configure(figure, axes, document, options)
        captured.extend(figure.axes)

    with patch.object(_plotms_plot, "configure", record):
        native(**figure_args, plotfile=str(native_png))
    for path in (reference_png, native_png):
        with Image.open(path) as image:
            assert image.size == (760, 480)
    assert [axis.yaxis.get_label_position() for axis in captured] == ["left", "right"]
    assert [axis.get_ylabel() for axis in captured] == ["Distance (rho) (km)", "Radial Velocity (km/s)"]
    for axis, selected, rounding in zip(captured, np.split(data, 2), (5000.001, 5.001e-5), strict=True):
        values = axis.lines[0].get_xydata()
        np.testing.assert_allclose(values[:, 0], selected[:, 0], rtol=0, atol=0.00051)
        np.testing.assert_allclose(values[:, 1], selected[:, 1], rtol=0, atol=rounding)
    print("two-ephemeris-ordinates: both rendered axes and 12 selected points match", flush=True)


def compare_uv(root, native):
    from casars.tasks import _plotms_plot
    from record_casa_returns import make_cal_ms

    work = root / "uv"
    work.mkdir()
    vis = work / "tracks.ms"
    make_cal_ms(str(vis), channels=8, scan_seconds=100, integration_seconds=10)
    with opened(vis, True) as table:
        time = table.getcol("TIME")
        slot = np.searchsorted(np.unique(time), time)
        a1, a2 = (table.getcol(name) for name in ("ANTENNA1", "ANTENNA2"))
        uvw = np.array([30 + slot + 20 * a1, -70 + slot * 2 - 12 * a2, 100 + slot])
        table.putcol("UVW", uvw.astype(float))
    base = dict(vis=str(vis), field="FLUX", antenna="A0&A1;A1&A2", spw="0:1~6",
                correlation="XX", showgui=False, overwrite=True, verbose=False,
                customsymbol=True, symbolshape="square", symbolsize=5,
                symbolcolor="dd2255", symbolfill="fill", symboloutline=False)
    configure = _plotms_plot.configure
    for label, extra, mirrored in (
        ("metres", dict(xaxis="u", yaxis="v"), True),
        ("wavelengths", dict(xaxis="uwave", yaxis="vwave"), True),
        ("w-and-u", dict(xaxis="w", yaxis="u"), False),
        ("u-and-distance", dict(xaxis="u", yaxis="uvdist"), False),
        ("averaged-metres", dict(xaxis="u", yaxis="v", averagedata=True,
                                 avgchannel="3", avgtime="30"), True),
    ):
        request = base | extra
        expected, actual = (work / f"{label}-{engine}.txt" for engine in ("casa", "native"))
        reference(**request, expformat="txt", plotfile=str(expected))
        native(**request, expformat="txt", plotfile=str(actual))
        points = np.asarray(rows(expected), dtype=float)
        compared = np.asarray(rows(actual), dtype=float)
        np.testing.assert_allclose(
            compared[np.lexsort(compared.T)], points[np.lexsort(points.T)],
            rtol=1.2e-5, atol=1e-7,
        )
        reference_png, native_png = (work / f"{label}-{engine}.png" for engine in ("casa", "native"))
        render = request | dict(expformat="png", width=620, height=480, dpi=100)
        reference(**render, plotfile=str(reference_png))
        captured = []

        def record(figure, axes, document, options):
            configure(figure, axes, document, options)
            captured.append(axes)

        with patch.object(_plotms_plot, "configure", record):
            native(**render, plotfile=str(native_png))
        [axis] = captured
        rendered = np.concatenate([line.get_xydata() for line in axis.lines])
        expected_rendered = np.concatenate([points, -points]) if mirrored else points
        np.testing.assert_allclose(
            rendered[np.lexsort(rendered.T)], expected_rendered[np.lexsort(expected_rendered.T)],
            rtol=1.2e-5, atol=1e-7,
        )
        if mirrored:
            assert axis.get_xlim() == axis.get_ylim()
        for path in (reference_png, native_png):
            with Image.open(path) as image:
                assert image.size == (620, 480)
                pixels = np.asarray(image.convert("RGB"))
                mark = np.all(pixels == [221, 34, 85], axis=2)
                assert np.any(mark)
                if mirrored:
                    assert np.any(mark[:, :310]) and np.any(mark[:, 310:])
        print(f"{label}: {len(points)} matching selected coordinates and rendered PNG", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("uv", "ephemeris"))
    args = parser.parse_args()
    work = args.work.resolve()
    work.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native

    assert casatasks.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    with chdir(work), plotms_launcher(args.app_run):
        if args.only in (None, "ephemeris"):
            source, *_ = make_fixture(work)
            compare_dual_ephemeris(
                work, source, native, lambda **options: casa_ephemeris_export(args.app_run, options),
            )
        if args.only in (None, "uv"):
            compare_uv(work, native)


if __name__ == "__main__":
    main()
