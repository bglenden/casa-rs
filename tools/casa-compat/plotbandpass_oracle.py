#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
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
from matplotlib.colors import to_rgba
from matplotlib.figure import Figure
import numpy as np

from casatasks import bandpass, plotbandpass as reference
from casatasks.private import task_plotbandpass
from casatools import table
from casars.tasks import plotbandpass as native

from bpoly_oracle import multiwindow_data
from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms


def fixtures(root):
    ms = root / "source.ms"
    make_cal_ms(str(ms))
    weather = root / "weather.txt"
    weather.write_text("ANTENNA_ID TIME INTERVAL TEMPERATURE PRESSURE REL_HUMIDITY WIND_DIRECTION WIND_SPEED\n"
                       "I D D R R R R R\n"
                       "0 5084550000 60 270 940 22 0 2\n")
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


def shifted_companion(root, scaled):
    shifted = root / "shifted.bcal"
    copytree(scaled, shifted)
    with opened(shifted / "SPECTRAL_WINDOW", write=True) as tool:
        tool.putcol("CHAN_FREQ", tool.getcol("CHAN_FREQ") + 2e6)
    return shifted


def review_fixtures(root, source, current, scaled):
    offset = root / "offset.bcal"
    copytree(scaled, offset)
    with opened(offset, write=True) as tool:
        tool.putcol("TIME", tool.getcol("TIME") + 1e6)

    circular = root / "circular.ms"
    make_cal_ms(str(circular), stokes="RR LL")
    circular_cal = root / "circular.bcal"
    bandpass(str(circular), str(circular_cal), refant="A0", solint="inf", minsnr=0.0)
    wide = root / "wide-atmosphere.ms"
    make_cal_ms(str(wide), channels=1024)
    copytree(source / "WEATHER", wide / "WEATHER")
    wide_cal = root / "wide-atmosphere.bcal"
    bandpass(str(wide), str(wide_cal), refant="A0", solint="inf", minsnr=0.0)
    for solution, ms in [(circular_cal, circular), (wide_cal, wide)]:
        with opened(solution, write=True) as tool:
            tool.putkeyword("MSName", str(ms))

    flagged = root / "all-flagged.bcal"
    copytree(current, flagged)
    with opened(flagged, write=True) as tool:
        tool.putcol("FLAG", np.ones_like(tool.getcol("FLAG"), dtype=bool))

    return {
        "offset-companion": dict(xaxis="freq", caltable2=str(offset)),
        "circular-hand": dict(caltable=str(circular_cal), vis=str(circular), poln="LL"),
        "wide-atmosphere": dict(caltable=str(wide_cal), vis=str(wide), showatm=True,
                                pwv=1.1, timeranges="0"),
        "all-flagged": dict(caltable=str(flagged)),
        "show-all-flagged": dict(caltable=str(flagged), showflagged=True),
    }


def multiband_fixture(root, source):
    ms = root / "multi.ms"
    multiwindow_data(source, ms)
    with opened(ms / "SPECTRAL_WINDOW", write=True) as tool:
        tool.addcols({"BBC_NO": {"valueType": "int"}})
        tool.putcol("BBC_NO", [1, 1])
        tool.addrows(1)
        frequency = np.arange(96) * 0.04e6 + 100.002e9
        widths = np.full(96, 0.04e6)
        for column, value in {
            "BBC_NO": 1, "CHAN_FREQ": frequency, "REF_FREQUENCY": frequency[30],
            "NUM_CHAN": 96, "NAME": "EXTRA_FDM", "CHAN_WIDTH": widths,
            "RESOLUTION": widths, "EFFECTIVE_BW": widths, "MEAS_FREQ_REF": 5,
            "NET_SIDEBAND": 1, "TOTAL_BANDWIDTH": 96 * 0.04e6, "FLAG_ROW": False,
        }.items():
            tool.putcell(column, 2, value)
    cal = root / "multi.bcal"
    bandpass(str(ms), str(cal), refant="A0", solint="inf", minsnr=0.0)
    with opened(cal, write=True) as tool:
        tool.putkeyword("MSName", str(ms))
    return ms, cal


def masked_fixture(root, current):
    masked = root / "masked.bcal"
    copytree(current, masked)
    with opened(masked, write=True) as tool:
        flags = tool.getcol("FLAG")
        flags[0, 2, :] = True
        tool.putcol("FLAG", flags)
        tool.putkeyword("MSName", "unavailable.ms")
    return masked


def tsys_fixture(root, current):
    tsys = root / "tsys.cal"
    copytree(current, tsys)
    with opened(tsys, write=True) as tool:
        values = 70.0 + 10.0 * np.abs(tool.getcol("CPARAM")).astype(np.float32)
        tool.removecols(["CPARAM"])
        tool.addcols({"FPARAM": {"valueType": "float", "ndim": 2}})
        tool.putcol("FPARAM", values)
        tool.putkeyword("ParType", "Float")
        tool.putkeyword("VisCal", "B TSYS")
    return tsys


def atmospheric_metadata(root, ms, calibration):
    parent = root / "parent.ms"
    multiwindow_data(ms, parent)
    with opened(calibration) as tool:
        epoch = float(tool.getcell("TIME", 0))
    path = root / "wvr.txt"
    path.write_text("startValidTime antennaName water\nD A D\n"
                    f"{epoch - 1:.3f} A0 0.001\n{epoch:.3f} A1 0.0014\n"
                    f"{epoch + 1:.3f} A2 0.0018\n")
    tool = table()
    tool.fromascii(str(ms / "ASDM_CALWVR"), str(path), sep=" ")
    tool.close()
    with opened(parent / "SPECTRAL_WINDOW", write=True) as tool:
        tool.putcell("NAME", 0, "UNUSED")
        tool.addcols({"BBC_NO": {"valueType": "int"}})
        tool.putcol("BBC_NO", [1, 1])
    shapes = {"numLO": "int", "name": "string", "frequencyBand": "string",
              "receiverId": "int", "spectralWindowId": "string"}
    descriptor = {name: {"valueType": dtype} for name, dtype in shapes.items()}
    descriptor["freqLO"] = {"valueType": "double", "ndim": 1}
    descriptor["sidebandLO"] = {"valueType": "string", "ndim": 1}
    tool = table()
    tool.create(str(ms / "ASDM_RECEIVER"), descriptor, nrow=2)
    try:
        for row, local_frequency in enumerate((98.4e9, 99.8e9)):
            for name, value in {
                "numLO": 3, "name": f"Receiver_{row}", "frequencyBand": "ALMA_RB_03",
                "receiverId": row, "spectralWindowId": f"SpectralWindow_{row}",
                "freqLO": np.array([local_frequency, 2e9, 1e8]),
                "sidebandLO": ["USB", "USB", "USB"],
            }.items():
                tool.putcell(name, row, value)
    finally:
        tool.close()
    return parent


def check_platforming(root):
    ms, cal = root / "wide.ms", root / "wide.bcal"
    make_cal_ms(str(ms), channels=64)
    bandpass(str(ms), str(cal), field="FLUX", refant="A0", solint="inf", minsnr=0.0)
    with opened(cal, write=True) as tool:
        values = tool.getcol("CPARAM")
        values[:, 32:, :] *= 1.5
        tool.putcol("CPARAM", values)
    options = dict(caltable=str(cal), vis=str(ms), antenna="A0", interactive=False,
                   channeldiff=5, edge=1, platformingSigma=0.0, figfile=False)
    error, *_ = capture(reference, root / "platforming-casa", options)
    assert error == ("TypeError", "'float' object cannot be interpreted as an integer"), error
    for threshold, detected in [(10.0, True), (100.0, False)]:
        result, *_ = capture(native, root / f"platforming-{threshold:.0f}", options | {
            "platformingThreshold": threshold,
        })
        assert bool(result["platforming"]) == detected, result
        if detected:
            assert set(result["platforming"]["A0"][0]) == {0, 1}
    print("verified analytic interior-step platforming at two thresholds")


def check_missing_weather(root, options):
    expected, *_ = capture(reference, root / "missing-weather-casa", options)
    assert expected == ("NameError", "name 'needToClose_mymsmd' is not defined"), expected
    result, figures, _, _ = capture(native, root / "missing-weather-native", options)
    assert result == () and len(figures) == 1, (result, figures)
    [axes] = figures[0][2]
    assert len(axes["artists"]) == 2
    assert any(text.startswith("PWV ") for text in axes["texts"])
    print("verified default atmospheric conditions when WEATHER is absent")


def artist_data(figure):
    return [{
        "title": axes.get_title(),
        "labels": (axes.get_xlabel(), axes.get_ylabel()),
        "limits": (axes.get_xlim(), axes.get_ylim()),
        "texts": [text.get_text() for text in axes.texts],
        "artists": [(np.asarray(line.get_xdata(), float), np.asarray(line.get_ydata(), float))
                    for line in axes.lines],
        "styles": [(to_rgba(line.get_color()), line.get_marker(), line.get_linestyle())
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
            if label in {"antenna-overlay", "overlay-phase", "overlay-time", "circular-hand"}:
                assert expected["styles"] == plotted["styles"], (label, expected["styles"], plotted["styles"])
                assert set(expected["texts"]) == set(plotted["texts"]), (label, expected["texts"], plotted["texts"])
            if label == "offset-companion":
                names = {text for text in expected["texts"] if text.startswith("c1=") or "-c" in text}
                assert names and names <= set(plotted["texts"]), (label, names, plotted["texts"])
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
    multi_ms, multi = multiband_fixture(root, ms)
    multiband = dict(caltable=str(multi), vis=str(multi_ms), xaxis="freq", timeranges="0")
    masked = masked_fixture(root, current)
    tsys = tsys_fixture(root, current)
    shifted = shifted_companion(root, scaled)
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
        "intersect": dict(xaxis="freq", caltable2=str(shifted), zoom="intersect"),
        "polynomial": dict(caltable=str(polynomial), xaxis="freq", field="FLUX"),
        "polynomial-overlay": dict(xaxis="freq", caltable2=str(polynomial), field="FLUX",
                                   timeranges="0"),
        "three-tables": dict(xaxis="freq", caltable2=str(polynomial), caltable3=str(polynomial),
                             field="FLUX", timeranges="0"),
        "mask": dict(caltable=str(masked)),
        "show-flagged": dict(caltable=str(masked), showflagged=True),
        "tsys": dict(caltable=str(tsys), xaxis="freq", yaxis="tsys"),
        "atmosphere": dict(xaxis="freq", showatm=True, pwv=1.1, timeranges="0"),
        "sky": dict(xaxis="freq", showtsky=True, pwv="auto", timeranges="0"),
        "image": dict(xaxis="freq", showatm=True, showimage=True, lo1="99.8", pwv=1.1,
                      showatmpoints=True, timeranges="0"),
        "derivative": dict(channeldiff=4, edge=1, antenna="A2"),
        "derivative-phase": dict(channeldiff=3, edge=1, yaxis="phase", antenna="A2"),
        "debug": dict(debug=True, solutionTimeThresholdSeconds=45.0, timeranges="0"),
        "fdm": multiband | dict(showfdm=True, showBasebandNumber=True, basebands=[1]),
        "spw-overlay": multiband | dict(overlay="spw", spw=[0, 1]),
        "baseband-overlay": multiband | dict(overlay="baseband", spw="", scans=[2]),
        "antenna-time": multiband | dict(overlay="antenna,time", antenna=[0, 2], spw=[1]),
        "antenna-overlay": multiband | dict(overlay="antenna", antenna=[0, 2], spw=[0]),
        "overlay-phase": multiband | dict(overlay="antenna", antenna=[0, 2], spw=[0], yaxis="phase"),
        "overlay-time": multiband | dict(overlay="time", timeranges="0,1", spw=[0]),
        "mixed-spw": multiband | dict(spw="0,2"),
    }
    plots.update(review_fixtures(root, ms, current, scaled))
    failures = []

    def check(label, options, keys=None, compare_plots=True):
        try:
            return pair(root, label, common | options, keys, compare_plots)
        except AssertionError as error:
            failures.append(label)
            print(f"comparison failed for {label}: {error}", flush=True)
            return 0

    panels = sum(check(label, options) for label, options in plots.items())
    parent = atmospheric_metadata(root, ms, current)
    atmosphere = dict(xaxis="freq", showatm=True, timeranges="0", pwv="auto")
    panels += check("measured-water", atmosphere)
    panels += check("reference-field", atmosphere | dict(overlay="time", showatmfield="FLUX"))
    panels += check("parent-receiver", atmosphere | dict(showimage=True, parentms=str(parent)))

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
        "bad-window-name": dict(spw="UNKNOWN"),
        "negated-window": dict(spw="!1"),
        "bad-time-wildcard": dict(timeranges="*"),
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
    check_platforming(root)
    check_missing_weather(root, common | plots["circular-hand"] | dict(
        showatm=True, xaxis="freq", pwv=1.1, timeranges="0",
    ))
    print(f"{len(plots) + 3} plot cases, {len(prompts)} terminal cases, {len(domains)} domain cases, "
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
