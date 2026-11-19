#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare calibration-table PlotMS atmospheric overlays with CASA."""

import argparse
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
from casaplotms import plotms as reference
from casatools import table as tabletool
import numpy as np

from calibration_rerefant_oracle import reference_fixture
from calibration_smoothcal_oracle import fixture, opened
from plotms_calibration_oracle import compare_text, plotms_launcher


def attached_table(ms, name, columns, rows):
    description = {}
    for key, kind, unit in columns:
        vector = kind == "vector"
        description[key] = dict(
            valueType="double" if vector else kind,
            dataManagerType="StandardStMan", dataManagerGroup="StandardStMan",
            option=4 if vector else 0, maxlen=0, comment="",
            keywords={} if unit is None else {"QuantumUnits": np.array([unit])},
        )
        if vector:
            description[key].update(ndim=1, shape=np.array([1], dtype=np.int32))
    child = ms / name
    table = tabletool()
    table.create(str(child), tabledesc=description, nrow=len(rows))
    try:
        if name == "WEATHER":
            table.putcolkeyword("TIME", "MEASINFO", {"type": "epoch", "Ref": "UTC"})
        for index, row in enumerate(rows):
            for (key, _, _), value in zip(columns, row, strict=True):
                table.putcell(key, index, value)
    finally:
        table.close()
    with opened(ms, True) as table:
        table.putkeyword(name, f"Table: {child}")


def atmosphere_inputs(ms):
    with opened(ms) as table:
        times = [(table.getcol("TIME")[table.getcol("SCAN_NUMBER") == scan]).mean()
                 for scan in (1, 2)]
    attached_table(ms, "WEATHER", [
        ("TIME", "double", "s"), ("ANTENNA_ID", "int", None),
        ("INTERVAL", "double", "s"), ("PRESSURE", "float", "Pa"),
        ("TEMPERATURE", "float", "K"), ("REL_HUMIDITY", "float", "%"),
    ], [(times[0], 0, 10.0, 56000.0, 281.0, 14.0),
        (times[1], 0, 10.0, 60000.0, 275.0, 27.0)])
    attached_table(ms, "ASDM_CALWVR", [
        ("startValidTime", "double", None), ("water", "double", "m"),
    ], [(times[0], 0.0025), (times[1], 0.0018)])
    attached_table(ms, "ASDM_RECEIVER", [
        ("spectralWindowId", "string", None), ("name", "string", None),
        ("freqLO", "vector", "Hz"),
    ], [("SpectralWindow_0", "science", np.array([9.9e10]))])


def export(root, native, base, name, changes, *, measured=False):
    expected = root / f"{name}.casa.txt"
    actual = root / f"{name}.native.txt"
    request = base | changes
    assert reference(**request, plotfile=str(expected), expformat="txt") is None
    assert native(**request, plotfile=str(actual), expformat="txt") is None
    # The measured-weather cases use a pointwise bound tighter than the
    # diagnostic ticket's 1e-3 normalized RMS acceptance.
    count = compare_text(actual, expected, value_rtol=2e-4 if measured else 1.2e-5)
    assert count > 0
    if measured:
        curves = lambda path: np.array([
            float(line.split()[1]) for line in path.read_text().splitlines()
            if not line.startswith("#")
        ])[32:]
        a, b = curves(actual), curves(expected)
        residual = np.linalg.norm(a - b) / np.linalg.norm(b)
        print(f"{name}: {count} points, overlay normalized RMS {residual:.6g}", flush=True)
    else:
        print(f"{name}: {count} points", flush=True)
    return count


def figure(root, request):
    from PIL import Image
    from casars import _plot_document, tasks

    destination = root / "atmosphere.png"
    expected = root / "sky.casa.txt"
    values = np.array([row.split()[:2] for row in expected.read_text().splitlines()[-16:]],
                      dtype=float)
    captured = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, x_axis = render(*args, **kwargs)
        [atmosphere] = [axis for axis in figure.axes if axis.get_ylabel() == "Tsky (K)"]
        captured.extend(line.get_xydata() for line in atmosphere.lines)
        return figure, x_axis

    with patch.object(_plot_document, "render_document", record):
        assert tasks.plotms(**request, plotfile=str(destination), width=500, height=360, dpi=100) is None
    np.testing.assert_allclose(np.concatenate(captured), values, rtol=1.2e-5, atol=1e-7)
    with Image.open(destination) as image:
        assert image.size == (500, 360)
    print(f"Rendered {len(values)} matching atmospheric points.", flush=True)


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

    assert casa.version_string() == "6.7.6.14"
    vis = root / "source.ms"
    gain = root / "gain.cal"
    bandpass = root / "bandpass.cal"
    fixture(vis, root / "solved.cal")
    reference_fixture(vis, gain, "G Jones")
    casa.bandpass(str(vis), str(bandpass), refant="A0", solint="inf", minsnr=0.0)
    base = dict(caltable=str(bandpass), yaxis="amp", spw="0", antenna="A0",
                showgui=False, overwrite=True)
    frequency = dict(xaxis="frequency", showatm=True)
    initial = [
        ("default", dict(showatm=True)),
        ("channel", dict(xaxis="channel", showatm=True)),
        ("frequency", frequency),
        ("sky", dict(xaxis="frequency", showtsky=True)),
        ("both", frequency | dict(showtsky=True)),
        ("image-only", dict(xaxis="frequency", showimage=True)),
        ("missing-receiver", frequency | dict(showimage=True)),
        ("singleton", frequency | dict(showimage=True, spw="0:2")),
        ("stride", dict(xaxis="frequency", showtsky=True, spw="0:1~7^2")),
        ("average", frequency | dict(avgchannel="2")),
        ("all-antennas", frequency | dict(antenna="")),
        ("scan-panels", frequency | dict(iteraxis="scan", gridrows=2)),
        ("gain-default", dict(caltable=str(gain), showatm=True)),
    ]
    measured = [
        ("weather-transmission", frequency),
        ("weather-sky", dict(xaxis="frequency", showtsky=True)),
        ("signal-and-image", frequency | dict(showimage=True)),
        ("sky-and-image", dict(xaxis="frequency", showtsky=True, showimage=True)),
    ]
    previous = Path.cwd()
    os.chdir(root)
    try:
        with plotms_launcher(args.app_run):
            points = sum(export(root, native, base, name, settings) for name, settings in initial)
            figure(root, base | dict(xaxis="frequency", showtsky=True))
            for api in (reference, native):
                destination = root / "non-spectral-axis.txt"
                try:
                    api(**base, xaxis="time", showatm=True, plotfile=str(destination), expformat="txt")
                except ValueError as error:
                    assert str(error) == "xaxis must be channel or frequency for showatm and showtsky"
                else:
                    raise AssertionError("Atmospheric overlay accepted an explicit time axis")
                assert not destination.exists()
            atmosphere_inputs(vis)
            points += sum(export(root, native, base, name, settings, measured=True)
                          for name, settings in measured)
    finally:
        os.chdir(previous)
    print(f"{len(initial) + len(measured)} exports, {points} points and one error matched.")


if __name__ == "__main__":
    main()
