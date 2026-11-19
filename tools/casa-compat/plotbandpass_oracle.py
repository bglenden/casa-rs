#!/usr/bin/env python3
"""Compare CASA 6.7.6.14 and casars plotbandpass data and products.

Run with the pinned CASA interpreter, a built casars Python package, and
CASARS_CALIBRATE_BIN pointing at calibrate. The PDF cases use the local
ImageMagick, Ghostscript and pdfinfo executables.
"""

from collections.abc import Mapping
from pathlib import Path
from shutil import copytree
import argparse
import subprocess
import tempfile
from unittest.mock import patch

import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
import numpy as np

from casatasks import bandpass, plotbandpass as reference
from casatasks.private import task_plotbandpass
from casars.tasks import plotbandpass as native

from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms


def fixtures(root):
    ms = root / "source.ms"
    make_cal_ms(str(ms))
    weather = root / "weather.txt"
    weather.write_text("ANTENNA_ID TIME INTERVAL TEMPERATURE PRESSURE REL_HUMIDITY WIND_DIRECTION WIND_SPEED\n"
                       "I D D R R R R R\n"
                       "0 5084550000 60 270 940 22 0 2\n")
    from casatools import table
    tool = table()
    tool.fromascii(str(ms / "WEATHER"), str(weather), sep=" ")
    tool.close()

    current = root / "solutions.bcal"
    bandpass(str(ms), str(current), solint="inf", refant="A0", minsnr=0.0)
    with opened(current, write=True) as tool:
        tool.putkeyword("MSName", str(ms))
        values = tool.getcol("CPARAM")
        ripple = np.array([1.00, 1.01, 1.015, 1.013, 1.025, 1.03, 1.026, 1.035])
        values *= ripple[None, :, None]
        tool.putcol("CPARAM", values)

    scaled = root / "scaled.bcal"
    copytree(current, scaled)
    with opened(scaled, write=True) as tool:
        tool.putcol("CPARAM", np.float32(0.8) * tool.getcol("CPARAM"))
    polynomial = root / "fit.bcal"
    bandpass(str(ms), str(polynomial), field="FLUX", solint="inf", refant="A0",
             minsnr=0.0, bandtype="BPOLY", degamp=2, degphase=1)
    return ms, current, scaled, polynomial


def artist_data(figure):
    return [{
        "title": axes.get_title(),
        "labels": (axes.get_xlabel(), axes.get_ylabel()),
        "limits": (axes.get_xlim(), axes.get_ylim()),
        "texts": [text.get_text() for text in axes.texts],
        "artists": [(np.asarray(line.get_xdata(), float), np.asarray(line.get_ydata(), float))
                    for line in axes.lines],
    } for axes in figure.axes]


def capture(task, root, options, keys=None):
    root.mkdir()
    arguments = dict(options)
    if "figfile" not in arguments:
        arguments["figfile"] = str(root / "fixture")
    elif arguments["figfile"] is False:
        arguments["figfile"] = ""
    figures, prompts = [], []
    present = Figure.savefig
    answers = iter(keys or [])

    def save(figure, filename, **kwargs):
        figures.append((Path(filename).name, kwargs.get("dpi"), artist_data(figure)))
        return present(figure, filename, **kwargs)

    def answer(prompt):
        prompts.append(prompt)
        try:
            return next(answers)
        except StopIteration:
            raise EOFError("EOF when reading a line") from None

    asking = (patch.object(task_plotbandpass, "input", answer) if task is reference
              else patch("builtins.input", answer))
    with patch.object(Figure, "savefig", save), asking:
        try:
            value = task(**arguments)
        except Exception as error:
            value = type(error).__name__, str(error)
    plt.close("all")
    return value, figures, prompts, sorted(p.name for p in root.iterdir())


def assert_returns(left, right, label):
    assert type(left) == type(right), (label, left, right)
    if isinstance(left, Mapping):
        assert set(left) == set(right), (label, left, right)
        for key in left:
            assert_returns(left[key], right[key], (label, key))
    elif isinstance(left, np.floating):
        np.testing.assert_allclose(right, left, rtol=2e-5, atol=1e-7, equal_nan=True, err_msg=str(label))
    else:
        assert left == right, (label, left, right)


def assert_figures(left, right, label):
    assert len(left) == len(right), (label, len(left), len(right))
    panels = 0
    for (name, dpi, reference_axes), (native_name, native_dpi, native_axes) in zip(left, right, strict=True):
        assert (name, dpi) == (native_name, native_dpi), (label, name, native_name, dpi, native_dpi)
        assert len(reference_axes) == len(native_axes), label
        for expected, plotted in zip(reference_axes, native_axes, strict=True):
            assert expected["title"] == plotted["title"], (label, expected["title"], plotted["title"])
            assert expected["labels"] == plotted["labels"], (label, expected["labels"], plotted["labels"])
            np.testing.assert_allclose(plotted["limits"], expected["limits"], rtol=2e-5, atol=1e-5, err_msg=label)
            assert len(expected["artists"]) == len(plotted["artists"]), label
            for (x, y), (actual_x, actual_y) in zip(expected["artists"], plotted["artists"], strict=True):
                np.testing.assert_allclose(actual_x, x, rtol=1e-9, atol=1e-7, err_msg=label)
                np.testing.assert_allclose(actual_y, y, rtol=2e-5, atol=1e-5, err_msg=label)
            panels += 1
    return panels


def pair(root, label, options, keys=None, compare_plots=True):
    expected = capture(reference, root / f"{label}-casa", options, keys)
    actual = capture(native, root / f"{label}-native", options, keys)
    assert_returns(expected[0], actual[0], label)
    assert expected[2:] == actual[2:], (label, expected[2:], actual[2:])
    panels = assert_figures(expected[1], actual[1], label) if compare_plots else 0
    print(f"verified {label}: {panels} panels, {len(expected[2])} terminal prompts", flush=True)
    return panels


def comparisons(root):
    ms, current, scaled, polynomial = fixtures(root)
    common = dict(caltable=str(current), vis=str(ms), antenna="A0", spw="0",
                  subplot="11", interactive=False)
    plots = {
        "channels": {},
        "frequency": dict(xaxis="freq"),
        "phase": dict(yaxis="phase", antenna=2, poln="XX"),
        "decibels": dict(yaxis="ampdb", antenna="A0"),
        "both": dict(yaxis="both", xaxis="freq", phase=[-10, 5], subplot=22,
                     figfileSequential=True),
        "style": dict(showpoints=True, showlines=False, density=93, markersize=5),
        "field": dict(field="PH*", antenna=[0]),
        "timerange": dict(timeranges="!1"),
        "limits": dict(xaxis="freq", chanrange="2~5", chanrangeSetXrange=True),
        "central-percent": dict(xaxis="freq", chanrange="50%"),
        "plotrange": dict(xaxis="freq", plotrange=[100.001, 100.004, 0.5, 2.4]),
        "scaled": dict(xaxis="freq", caltable2=str(scaled), zoom="intersect"),
        "polynomial": dict(caltable=str(polynomial), xaxis="freq", field="FLUX"),
        "polynomial-overlay": dict(xaxis="freq", caltable2=str(polynomial), field="FLUX",
                                   timeranges="0"),
        "atmosphere": dict(xaxis="freq", showatm=True, pwv=1.1, timeranges="0"),
        "sky": dict(xaxis="freq", showtsky=True, pwv="auto", timeranges="0"),
        "image": dict(xaxis="freq", showatm=True, showimage=True, lo1="99.8", pwv=1.1,
                      showatmpoints=True, timeranges="0"),
        "derivative": dict(channeldiff=4, edge=1, antenna="A2"),
        "derivative-phase": dict(channeldiff=3, edge=1, yaxis="phase", antenna="A2"),
        "debug": dict(debug=True, solutionTimeThresholdSeconds=45.0, timeranges="0"),
    }
    failures = []

    def check(label, options, keys=None, compare_plots=True):
        try:
            return pair(root, label, common | options, keys, compare_plots)
        except AssertionError as error:
            failures.append(label)
            print(f"comparison failed for {label}: {error}", flush=True)
            return 0

    panels = sum(check(label, options) for label, options in plots.items())

    prompts = {
        "forward": (["", ""], {}),
        "back": (["", "b", "", "q"], {}),
        "quit": (["q"], {}),
        "quit-derivative": (["q"], dict(channeldiff=5, edge=1)),
        "quit-combined": (["q"], dict(yaxis="both", channeldiff=5, edge=1)),
        "quit-derivative-no-file": (["q"], dict(channeldiff=5, edge=1, figfile=False)),
        "eof": ([], {}),
    }
    for label, (keys, options) in prompts.items():
        panels += check(label, dict(interactive=True, figfileSequential=True) | options, keys)
    domains = {
        "bad-axis": dict(xaxis="FREQ"),
        "bad-y-axis": dict(yaxis="delay"),
        "no-window": dict(spw="42"),
        "no-field": dict(field="MISSING"),
        "no-scan": dict(scans=[99]),
        "no-time": dict(timeranges="9"),
        "no-baseband": dict(basebands=[9]),
        "absent-bbc": dict(showBasebandNumber=True),
        "bad-phase": dict(phase=[1.5, 2.5]),
        "bad-plotrange": dict(plotrange=[1]),
        "bad-subplot": dict(subplot="other"),
        "short-subplot": dict(subplot=2),
        "unsigned-edge": dict(edge=-1),
        "invalid-zoom": dict(zoom="other"),
        "missing-caltable": dict(caltable=str(root / "missing")),
        "missing-companion": dict(caltable2=str(root / "missing")),
        "missing-companion-three": dict(caltable3=str(root / "missing")),
        "multi-type": dict(figfile=None, showflagged=4, timeranges=None),
        "float-lo": dict(showimage=True, lo1=99.8),
        "resampling-error": dict(channeldiff=5, resample=2),
        "empty-string-return": dict(resample=0),
    }
    for label, options in domains.items():
        check(label, options, compare_plots=False)

    pdf = dict(buildpdf=True, figfileSequential=True, density=108, convert="convert",
               gs="gs", pdftk="pdftk")
    panels += check("pdf", pdf)
    for implementation in ("casa", "native"):
        summary = subprocess.check_output(
            ["pdfinfo", str(root / f"pdf-{implementation}" / "fixture.pdf")], text=True
        )
        pages = next(line for line in summary.splitlines() if line.startswith("Pages:"))
        assert pages.split()[-1] == "2", summary
    print(f"{len(plots)} plot cases, {len(prompts)} terminal cases, {len(domains)} domain cases, "
          f"{panels} rendered panels and paired two-page PDFs", flush=True)
    assert not failures, failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, help="New directory for retained products")
    options = parser.parse_args()
    if options.directory is not None:
        options.directory.mkdir(parents=True)
        comparisons(options.directory.resolve())
    else:
        with tempfile.TemporaryDirectory(prefix="casars-plotbandpass-") as directory:
            comparisons(Path(directory))


if __name__ == "__main__":
    main()
