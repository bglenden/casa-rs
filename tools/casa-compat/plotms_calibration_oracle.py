#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare calibration data exported by installed CASA PlotMS and casa-rs."""

import argparse
from contextlib import contextmanager
import inspect
import os
from pathlib import Path
import subprocess
from time import perf_counter
from unittest.mock import patch

import casatasks as casa
from casaplotms import plotms as reference
import numpy as np

from calibration_rerefant_oracle import reference_fixture
from calibration_smoothcal_oracle import fixture


@contextmanager
def plotms_launcher(executable):
    """Use an extracted CASA AppImage on a host without FUSE."""
    if executable is None:
        yield
        return
    popen = subprocess.Popen

    def launch(argv, *args, **kwargs):
        if str(argv[0]).endswith(".AppImage"):
            argv = [str(executable), *argv[1:]]
        return popen(argv, *args, **kwargs)

    with patch.object(subprocess, "Popen", launch):
        yield


def exports(root, products, native):
    def request(source, destination, **overrides):
        return dict(caltable=str(source), antenna="A0", spw="0", showgui=False,
                    plotfile=str(destination), expformat="txt", overwrite=True) | overrides

    cases = [
        ("bandpass-default", "bandpass", {}),
        ("bandpass-amp", "bandpass", dict(xaxis="channel", yaxis="amp")),
        ("bandpass-gainamp", "bandpass", dict(xaxis="channel", yaxis="gainamp")),
        ("bandpass-gainreal", "bandpass", dict(xaxis="frequency", yaxis="gainreal")),
        ("bandpass-phase", "bandpass", dict(xaxis="time", yaxis="phase")),
        ("bandpass-gainphase", "bandpass", dict(xaxis="time", yaxis="gainphase")),
        ("channel-selection", "bandpass", dict(spw="0:2~4")),
        ("field-selection", "bandpass", dict(xaxis="scan", yaxis="amp", field="PHASE")),
        ("channel-average", "bandpass", dict(averagedata=True, avgchannel="2")),
        ("channel-average-selection", "bandpass", dict(averagedata=True, avgchannel="2", spw="0:3~7")),
        ("gain-time", "gain", dict(xaxis="time", yaxis="gainamp")),
        ("bandpass-all-antennas", "bandpass", dict(antenna="")),
        ("bandpass-snr", "bandpass", dict(xaxis="antenna1", yaxis="snr")),
        ("delay", "delay", dict(xaxis="antenna1", yaxis="delay")),
        ("gain-all-antennas-flags", "gain", dict(antenna="", xaxis="time", yaxis="gainamp")),
        ("right-receptor", "bandpass", dict(correlation="R")),
        ("left-receptor", "bandpass", dict(correlation="L")),
        ("linear-polarization", "bandpass", dict(polarization="X")),
        ("amplitude-and-phase", "bandpass", dict(xaxis="channel", yaxis=["amp", "phase"])),
        ("quiet-text", "bandpass", dict(verbose=False)),
        ("caltable-in-vis", "bandpass", dict(vis=str(products["bandpass"]), caltable="")),
        ("gain-default", "gain", {}),
        ("delay-default", "delay", {}),
        ("receptor-ratio", "bandpass", dict(correlation="/")),
        ("solution-time-selection", "bandpass",
         dict(timerange="2019/12/31/21:52:35~2019/12/31/21:52:45")),
        ("no-channel-compression", "bandpass", dict(avgchannel="1")),
        ("time-average", "bandpass", dict(avgtime="120", avgscan=True, avgfield=True)),
        ("scan-boundaries", "bandpass", dict(avgtime="120")),
        ("scalar-average", "bandpass", dict(avgchannel="2", scalar=True)),
        ("baseline-average", "bandpass", dict(antenna="", avgbaseline=True)),
        ("antenna-average", "bandpass", dict(antenna="", avgantenna=True)),
        ("scan-current-text", "bandpass", dict(iteraxis="scan", exprange="all")),
        ("scan-grid-text", "bandpass", dict(iteraxis="scan", gridcols=2)),
        ("antenna-grid-text", "bandpass", dict(iteraxis="antenna", gridcols=2, antenna="")),
        ("polarization-grid-text", "bandpass", dict(iteraxis="poln", gridcols=2)),
    ]
    total = 0
    for name, kind, options in cases:
        expected, actual = [root / f"{name}.{mode}.txt" for mode in ("casa", "native")]
        start = perf_counter()
        assert reference(**request(products[kind], expected, **options)) is None
        reference_time = perf_counter() - start
        start = perf_counter()
        assert native(**request(products[kind], actual, **options)) is None
        native_time = perf_counter() - start
        count = compare_text(actual, expected)
        total += count
        print(f"{name}: {count} samples, CASA {reference_time:.3f}s, casa-rs {native_time:.3f}s",
              flush=True)
    return len(cases), total


def compare_text(actual, expected):
    a, b = actual.read_text().splitlines(), expected.read_text().splitlines()
    headers = lambda lines: [line for line in lines if line.startswith("#")]
    assert headers(a) == headers(b), (headers(a), headers(b))
    rows = lambda lines: [line.split() for line in lines if not line.startswith("#")]
    a, b = rows(a), rows(b)
    assert len(a) == len(b), (len(a), len(b))
    for native, reference in zip(a, b, strict=True):
        assert native[2:] == reference[2:], (native, reference)
    coordinates = lambda rows: np.array([r[:2] for r in rows], dtype=float)
    np.testing.assert_allclose(coordinates(a), coordinates(b), rtol=1.2e-5, atol=1e-7)
    return len(a)


def figure(root, products, native):
    from PIL import Image
    from casars import _plot_document

    expected = root / "figure-points.casa.txt"
    reference(caltable=str(products["bandpass"]), xaxis="frequency", yaxis="amp",
              antenna="A0", spw="0", showgui=False, plotfile=str(expected), expformat="txt")
    coordinates = np.array([line.split()[:2] for line in expected.read_text().splitlines()
                            if not line.startswith("#")], dtype=float)
    captured = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        fig, axis = render(*args, **kwargs)
        captured.append((axis.get_xlabel(), axis.get_ylabel(), axis.lines[0].get_xydata()))
        return fig, axis

    png = root / "calibration.png"
    with patch.object(_plot_document, "render_document", record):
        assert native(caltable=str(products["bandpass"]), xaxis="frequency", yaxis="amp",
                      antenna="A0", spw="0", showgui=False, plotfile=str(png),
                      width=500, height=360, dpi=100) is None
    [(xlabel, ylabel, points)] = captured
    assert (xlabel, ylabel) == ("Frequency (GHz)", "Amp")
    np.testing.assert_allclose(points, coordinates, rtol=1.2e-5, atol=1e-7)
    with Image.open(png) as actual:
        assert actual.size == (500, 360)
    print(f"Rendered PNG matched {len(points)} exported coordinates and the requested pixel size.")


def plot_pages(root, products, native):
    source = str(products["bandpass"])
    for engine, task in (("casa", reference), ("native", native)):
        directory = root / engine
        directory.mkdir()
        assert task(caltable=source, spw="0", antenna="A0", showgui=False,
                    iteraxis="scan", exprange="all", plotfile=str(directory / "plot.png")) is None
        assert sorted(file.name for file in directory.iterdir()) == ["plot_Scan1.png", "plot_Scan2_2.png"]
        assert all(file.stat().st_size > 1000 for file in directory.iterdir())

    unfiltered = root / "unfiltered.txt"
    reference_file = root / "unfiltered.casa.txt"
    try:
        reference(caltable=source, selectdata=False, field="missing",
                  showgui=False, plotfile=str(reference_file))
    except Exception as error:
        assert str(error) == "updateImmediately must be a boolean", str(error)
    else:
        raise AssertionError("CASA accepted selectdata=False")
    assert native(caltable=source, selectdata=False, field="missing", antenna="wrong",
                  plotfile=str(unfiltered), showgui=False) is None
    assert reference(caltable=source, showgui=False, plotfile=str(reference_file)) is None
    assert compare_text(unfiltered, reference_file) == 160
    print("Matched iteration image names and verified that disabled selectors leave every point available.")


def errors(root, products, native):
    plotfile = str(root / "invalid.txt")
    path = str(products["bandpass"])
    cases = [
        (RuntimeError, "Cannot set both vis and caltable parameters", dict(vis=path, caltable=path)),
        (RuntimeError, "vis and caltable not set, cannot export empty plot", {}),
        (RuntimeError, "Cannot select both correlation and polarization.",
         dict(caltable=path, correlation="X", polarization="Y")),
        (RuntimeError, f"Input file not found:\n{root / 'missing.cal'}",
         dict(caltable=str(root / "missing.cal"))),
        (ValueError, "a negative plotindex is not valid.", dict(caltable=path, plotindex=-1)),
        (ValueError, "a nonzero plotindex is not valid when clearing plots.",
         dict(caltable=path, plotindex=1)),
        (ValueError, "cannot average negative number of channels", dict(caltable=path, avgchannel="-1")),
        (ValueError, "avgtime value must be numerical string in seconds (no units)",
         dict(caltable=path, avgtime="5s")),
        (ValueError, "averaging over baselines is mutually exclusive with per-antenna averaging.",
         dict(caltable=path, avgantenna=True, avgbaseline=True)),
    ]
    for task in (reference, native):
        for error_type, message, options in cases:
            try:
                task(showgui=False, expformat="txt", plotfile=plotfile, **options)
            except error_type as error:
                assert str(error) == message, (str(error), message)
            else:
                raise AssertionError(options)
    assert not Path(plotfile).exists()
    return len(cases)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native

    expected = inspect.signature(reference)
    actual = inspect.signature(native)
    assert casa.version_string() == "6.7.6.14"
    assert [p.replace(annotation=inspect.Parameter.empty) for p in actual.parameters.values()] == list(expected.parameters.values())
    vis = root / "source.ms"
    fixture(vis, root / "solved.cal")
    products = {name: root / f"{name}.cal" for name in ("gain", "bandpass", "delay")}
    reference_fixture(vis, products["gain"], "G Jones")
    casa.bandpass(str(vis), str(products["bandpass"]), refant="A0", solint="inf", minsnr=0.0)
    casa.gencal(str(vis), str(products["delay"]), "sbd", parameter=[2.5])
    with plotms_launcher(args.app_run):
        cases, points = exports(root, products, native)
        error_cases = errors(root, products, native)
        figure(root, products, native)
        plot_pages(root, products, native)
    print(f"{cases} exports, {points} points and {error_cases} error cases matched.")


if __name__ == "__main__":
    main()
