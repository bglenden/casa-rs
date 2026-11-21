#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native PlotMS with pinned CASA on real BPOLY and GSPLINE products."""

import argparse
from contextlib import chdir
import hashlib
import os
from pathlib import Path
import shutil
from unittest.mock import patch

from casaplotms import plotms as reference
import casatasks as casa
import numpy as np
from PIL import Image

from plotms_calibration_oracle import plotms_launcher
from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms


def fixture(root):
    vis = root / "source.ms"
    make_cal_ms(str(vis), channels=20, scan_seconds=150)
    with opened(vis, True) as table:
        time = table.getcol("TIME")
        elapsed = (time - time.min()) / np.ptp(time)
        channel = np.linspace(-1, 1, 20)[:, None]
        gain = lambda ant: (1 + 0.08 * ant) * np.exp(
            1j * ant * (0.03 + 0.04 * elapsed + 0.02 * channel))
        data = table.getcol("DATA")
        data[1] *= gain(table.getcol("ANTENNA1")) * gain(table.getcol("ANTENNA2")).conj()
        table.putcol("DATA", data)
    tables = {kind: root / f"{kind}.cal" for kind in ("bpoly", "gspline")}
    casa.bandpass(str(vis), str(tables["bpoly"]), bandtype="BPOLY", refant="A0",
                  minsnr=0.0, degamp=2, degphase=2, maskedge=0)
    casa.gaincal(str(vis), str(tables["gspline"]), gaintype="GSPLINE", refant="A0",
                 minsnr=0.0, splinetime=60.0, calmode="ap")
    tables["flagged"] = root / "bpoly-flagged.cal"
    shutil.copytree(tables["bpoly"], tables["flagged"])
    with opened(tables["flagged"], True) as table:
        for row in np.flatnonzero(table.getcol("ANTENNA1") == 2):
            for component in ("AMP", "PHASE"):
                name = f"POLY_COEFF_{component}"
                coefficients = table.getcell(name, int(row))
                coefficients[..., table.getcell(f"N_POLY_{component}", int(row)):] = 0
                table.putcell(name, int(row), coefficients)
    tables["two-spw"] = root / "gspline-two-spw.cal"
    add_second_window(vis, tables["gspline"], tables["two-spw"])
    return vis, tables


def append_copy(table, **updates):
    columns = {name: table.getcell(name, 0) for name in table.colnames()
               if table.iscelldefined(name, 0)}
    columns.update(updates)
    row = table.nrows()
    table.addrows(1)
    for name, value in columns.items():
        table.putcell(name, row, value)


def add_second_window(vis, source, destination):
    shutil.copytree(source, destination)
    with opened(vis / "SPECTRAL_WINDOW", True) as table:
        append_copy(table, NAME="SPW1")
    with opened(vis / "DATA_DESCRIPTION", True) as table:
        append_copy(table, SPECTRAL_WINDOW_ID=1)
    with opened(vis, True) as table:
        append_copy(table, DATA_DESC_ID=1)
    with opened(destination / "CAL_DESC", True) as table:
        table.putcell("SPECTRAL_WINDOW_ID", 0, [0])
        append_copy(table, SPECTRAL_WINDOW_ID=[1])
    with opened(destination, True) as table:
        ids = (table.getcol("FIELD_ID") == 1).astype(np.int32)
        table.putcol("CAL_DESC_ID", ids)


def snapshot(paths):
    files = {}
    for root in paths:
        for path in root.rglob("*"):
            if path.is_file() and path.name != "table.lock":
                with path.open("rb") as stream:
                    files[path] = hashlib.file_digest(stream, "sha256").hexdigest()
    return files


def read_export(path):
    headers, samples = [], []
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            headers.append(line)
        elif line.strip():
            samples.append(line.split())
    return headers, samples


def compare_exports(expected, actual, name):
    reference_headers, reference_rows = read_export(expected)
    native_headers, native_rows = read_export(actual)
    assert native_headers == reference_headers, (name, native_headers, reference_headers)
    assert native_rows and len(native_rows) == len(reference_rows), (
        name, len(native_rows), len(reference_rows))
    for native, casa_row in zip(native_rows, reference_rows, strict=True):
        assert native[2:] == casa_row[2:], (name, native, casa_row)
    target = np.array([row[:2] for row in reference_rows], dtype=float)
    observed = np.array([row[:2] for row in native_rows], dtype=float)
    np.testing.assert_array_equal(observed[:, 0], target[:, 0], err_msg=name)
    np.testing.assert_allclose(observed[:, 1], target[:, 1], rtol=1.2e-5,
                               atol=5e-6, err_msg=name)
    if name == "gspline-ratio":
        assert np.max(np.abs(target[:, 1] - 1)) > 0.05
    if name == "bpoly-flagged":
        assert set(target[:, 1]) == {0.0, 1.0}
        assert {row[-2] for row in reference_rows if row[1] == "1"} == {"1"}
    error = np.linalg.norm(observed[:, 1] - target[:, 1])
    nrms = error / max(np.linalg.norm(target[:, 1]), 1e-30)
    assert nrms <= 3e-6, (name, nrms)
    print(f"{name}: {len(target)} points, NRMS {nrms:.6g}", flush=True)
    return target, nrms


def plots():
    for kind in ("bpoly", "gspline"):
        for axis in ("amp", "phase"):
            yield f"{kind}-{axis}", kind, dict(antenna="A0", yaxis=axis)
    yield "bpoly-all", "bpoly", dict(yaxis="amp")
    yield "bpoly-stride", "bpoly", dict(antenna="A2", yaxis="phase", spw="0:2~9^2")
    yield "bpoly-field", "bpoly", dict(antenna="A2", yaxis="phase", field="PHASE")
    yield "bpoly-flagged", "flagged", dict(antenna="A2", yaxis="flag", customflaggedsymbol=True)
    yield "gspline-all", "gspline", dict(yaxis="phase")
    yield "gspline-field", "gspline", dict(antenna="A2", yaxis="phase", field="PHASE")
    yield "gspline-time", "gspline", dict(antenna="A2", yaxis="phase",
                                         timerange="12:00:00~12:00:10")
    yield "gspline-pol", "gspline", dict(antenna="A2", yaxis="phase", polarization="Y")
    yield "gspline-ratio", "gspline", dict(antenna="A2", yaxis="amp", correlation="/")
    yield "gspline-flag", "gspline", dict(antenna="A2", yaxis="flag")
    yield "gspline-feed", "gspline", dict(antenna="A2", yaxis="feed1")
    yield "gspline-feed-all", "gspline", dict(yaxis="feed1")
    yield "gspline-antenna-axis", "gspline", dict(antenna="A2", yaxis="antenna1")
    yield "gspline-legacy-antenna", "gspline", dict(antenna="A2&A3", yaxis="phase")
    yield "gspline-secondary-spw", "two-spw", dict(antenna="A2", spw="1", yaxis="phase")
    yield "gspline-spw-with-channels", "two-spw", dict(antenna="A2", spw="1:3~8", yaxis="phase")


def requests(tables):
    for name, kind, options in plots():
        yield name, dict(caltable=str(tables[kind]), showgui=False, **options)


def negative_antenna(root, engine, table, expected):
    destination = root / "gspline-negative-antenna.txt"
    assert engine(caltable=str(table), antenna="!A2", yaxis="phase", showgui=False,
                  plotfile=str(destination), expformat="txt") is None
    _, rows = read_export(destination)
    assert {int(row[4]) for row in rows} == expected


def refused(root, engine, tables):
    cases = (
        ("bpoly-average", "bpoly", dict(averagedata=True, avgchannel="2")),
        ("gspline-frequency", "gspline", dict(xaxis="freq", yaxis="amp")),
        ("gspline-unset-window", "gspline", dict(spw="0")),
        ("gspline-unset-wildcard-window", "gspline", dict(spw="*")),
    )
    for name, kind, options in cases:
        destination = root / f"{name}.txt"
        try:
            engine(caltable=str(tables[kind]), showgui=False, plotfile=str(destination),
                   expformat="txt", **options)
        except (RuntimeError, ValueError) as error:
            assert str(error), name
        except Exception as error:
            from grpc import StatusCode

            assert callable(getattr(error, "code", None)), (name, error)
            assert error.code() == StatusCode.INVALID_ARGUMENT, (name, error)
        else:
            raise AssertionError(f"{name} unexpectedly succeeded")
        assert not destination.exists(), destination


def render(root, native, name, request, expected):
    from casars.tasks import _plotms_plot

    captured = []
    configure = _plotms_plot.configure

    def record(fig, axes, document, options):
        configure(fig, axes, document, options)
        visible = [line.get_xydata() for axis in fig.axes for line in axis.lines
                   if len(line.get_xydata())]
        captured.append(np.concatenate(visible))

    png = root / f"{name}.png"
    with patch.object(_plotms_plot, "configure", record):
        assert native(**request, plotfile=str(png), width=500, height=360, dpi=100) is None
    [points] = captured
    np.testing.assert_allclose(points, expected, rtol=1.2e-5, atol=5e-6, err_msg=name)
    with Image.open(png) as image:
        assert image.size == (500, 360)
        assert image.convert("L").getextrema()[0] < 128
    return len(points)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    args = parser.parse_args()
    root = args.work.resolve()
    casa_dir, native_dir = root / "casa", root / "native"
    root.mkdir()
    casa_dir.mkdir()
    native_dir.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native

    assert casa.version_string() == "6.7.6.14"
    vis, tables = fixture(root)
    images = ("bpoly-amp", "gspline-field")
    with chdir(root), plotms_launcher(args.app_run):
        for name, request in requests(tables):
            assert reference(**request, plotfile=str(casa_dir / f"{name}.txt"),
                             expformat="txt") is None
            if name in images:
                image = casa_dir / f"{name}.png"
                assert reference(**request, plotfile=str(image), width=500, height=360,
                                 dpi=100) is None
                with Image.open(image) as picture:
                    assert picture.convert("L").getextrema()[0] < 128
        refused(casa_dir, reference, tables)
        negative_antenna(casa_dir, reference, tables["gspline"], {2})
    inputs = [vis, *tables.values()]
    before = snapshot(inputs)
    points, visible, worst, cases = 0, 0, 0.0, 0
    with chdir(root):
        for name, request in requests(tables):
            actual = native_dir / f"{name}.txt"
            assert native(**request, plotfile=str(actual), expformat="txt") is None
            expected, nrms = compare_exports(casa_dir / f"{name}.txt", actual, name)
            points += len(expected)
            cases += 1
            worst = max(worst, nrms)
            if name in images:
                visible += render(native_dir, native, name, request, expected)
        refused(native_dir, native, tables)
        negative_antenna(native_dir, native, tables["gspline"], {0, 1, 3, 4})
    assert snapshot(inputs) == before, "legacy calibration plot modified persistent inputs"
    print(f"{cases} paired exports, {points} points, worst NRMS {worst:.6g}, "
          f"{visible} drawn coordinates, four paired refusals, antenna negation checked, "
          "persistent inputs unchanged.")


if __name__ == "__main__":
    main()
