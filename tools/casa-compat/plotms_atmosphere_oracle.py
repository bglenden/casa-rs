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
from calibration_smoothcal_oracle import copy_ms, fixture, opened
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


def export(root, native, base, name, changes, *, physical_overlay=False):
    expected = root / f"{name}.casa.txt"
    actual = root / f"{name}.native.txt"
    request = base | changes
    assert reference(**request, plotfile=str(expected), expformat="txt") is None
    assert native(**request, plotfile=str(actual), expformat="txt") is None
    # Keep the reference bound for visibility axes. The ATM line-band and
    # measured-weather cases retain a 2e-4 pointwise bound on overlays.
    if physical_overlay:
        lines = expected.read_text().splitlines()
        separator = next(index for index, line in enumerate(lines) if line.startswith("# ")
                         and (" Atm Transmission " in line or " Tsky " in line))
        start = sum(not line.startswith("#") for line in lines[:separator])
        count = compare_text(actual, expected, overlay_rtol=2e-4, overlay_start=start)

        def curves(path):
            return np.array([float(line.split()[1]) for line in path.read_text().splitlines()
                             if not line.startswith("#")])[start:]

        a, b = curves(actual), curves(expected)
        residual = np.linalg.norm(a - b) / np.linalg.norm(b)
        assert residual <= 2e-4
        print(f"{name}: {count} points, overlay normalized RMS {residual:.6g}", flush=True)
    else:
        count = compare_text(actual, expected)
        print(f"{name}: {count} points", flush=True)
    assert count > 0
    return count


def figure(root, request, name, label, count):
    from PIL import Image
    from casars import _plot_document, tasks

    destination = root / f"{name}.png"
    expected = root / f"{name}.casa.txt"
    values = np.array([row.split()[:2] for row in expected.read_text().splitlines()[-count:]],
                      dtype=float)
    captured = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, x_axis = render(*args, **kwargs)
        [atmosphere] = [axis for axis in figure.axes if axis.get_ylabel() == label]
        captured.extend(line.get_xydata() for line in atmosphere.lines)
        return figure, x_axis

    with patch.object(_plot_document, "render_document", record):
        assert tasks.plotms(**request, plotfile=str(destination), width=500, height=360, dpi=100) is None
    np.testing.assert_allclose(np.concatenate(captured), values, rtol=1.2e-5, atol=1e-7)
    with Image.open(destination) as image:
        assert image.size == (500, 360)
    print(f"{name}: rendered {len(values)} matching atmospheric points.", flush=True)


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
    flagged = root / "partially-flagged.cal"
    copy_ms(bandpass, flagged)
    with opened(flagged, True) as table:
        antennas = table.getcol("ANTENNA1") == 0
        flags = table.getcol("FLAG")
        flags[1, 0, antennas] = True
        flags[0, 1, antennas] = True
        table.putcol("FLAG", flags)
    wide = root / "wide.cal"
    copy_ms(bandpass, wide)
    with opened(wide / "SPECTRAL_WINDOW", True) as table:
        table.putcell("CHAN_FREQ", 0, np.linspace(179e9, 186e9, 8))
        table.putcell("CHAN_WIDTH", 0, np.full(8, 1e9))
        table.putcell("REF_FREQUENCY", 0, 182.5e9)
    odd = root / "odd-band.cal"
    copy_ms(wide, odd)
    with opened(odd, True) as table:
        for row in range(table.nrows()):
            for column in ("CPARAM", "PARAMERR", "FLAG", "SNR"):
                table.putcell(column, row, table.getcell(column, row)[:, :7])
    with opened(odd / "SPECTRAL_WINDOW", True) as table:
        for column in ("CHAN_FREQ", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION"):
            table.putcell(column, 0, table.getcell(column, 0)[:7])
        table.putcell("NUM_CHAN", 0, 7)
    base = dict(caltable=str(bandpass), yaxis="amp", spw="0", antenna="A0",
                showgui=False, overwrite=True)
    frequency = dict(xaxis="frequency", showatm=True)
    initial = [
        ("default", dict(showatm=True)),
        ("channel", dict(xaxis="Channel", showatm=True)),
        ("frequency", dict(xaxis="Frequency", showatm=True)),
        ("sky", dict(xaxis="frequency", showtsky=True)),
        ("both", frequency | dict(showtsky=True)),
        ("image-only", dict(xaxis="frequency", showimage=True)),
        ("missing-receiver", frequency | dict(showimage=True)),
        ("singleton", frequency | dict(showimage=True, spw="0:2")),
        ("stride", dict(xaxis="frequency", showtsky=True, spw="0:1~7^2")),
        ("wide-selection", dict(caltable=str(wide), xaxis="frequency", showtsky=True,
                                spw="0:0;3;6")),
        ("wide-time-average", dict(caltable=str(wide), xaxis="frequency", showtsky=True,
                                   spw="0:0;3;6", avgtime="120")),
        ("average", frequency | dict(avgchannel="2")),
        ("all-antennas", frequency | dict(antenna="")),
        ("partially-flagged-channels", frequency | dict(caltable=str(flagged))),
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
            points = sum(export(root, native, base, name, settings,
                                physical_overlay=name.startswith("wide-"))
                         for name, settings in initial)
            figure(root, base | dict(xaxis="frequency", showtsky=True), "sky", "Tsky (K)", 16)
            figure(root, base | dict(caltable=str(gain), showatm=True),
                   "gain-default", "Atm Transmission (%)", 3)
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
            points += sum(export(root, native, base, name, settings, physical_overlay=True)
                          for name, settings in measured)
            with opened(vis / "ASDM_RECEIVER", True) as table:
                table.putcell("freqLO", 0, np.array([182e9]))
            points += export(root, native, base, "odd-image",
                             dict(caltable=str(odd), xaxis="frequency", showtsky=True, showimage=True),
                             physical_overlay=True)
    finally:
        os.chdir(previous)
    print(f"{len(initial) + len(measured) + 1} exports, {points} points and one error matched.")


if __name__ == "__main__":
    main()
