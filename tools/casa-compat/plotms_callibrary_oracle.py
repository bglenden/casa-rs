#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare read-only, on-the-fly public PlotMS calibration with CASA."""

import argparse
import os
from pathlib import Path
from time import perf_counter
from unittest.mock import patch

import casatasks as casa
from casaplotms import plotms as reference
import numpy as np

from calibration_gencal_tsys_oracle import attach_syscal
from calibration_smoothcal_oracle import copy_ms, opened
from plotms_calibration_oracle import compare_text, plotms_launcher
from record_casa_returns import make_cal_ms


def fixture(root):
    vis = root / "input.ms"
    make_cal_ms(str(vis))
    amplitude = root / "amplitude.cal"
    casa.gencal(str(vis), str(amplitude), "amp", parameter=[2.0])
    bandpass = root / "bandpass.cal"
    casa.bandpass(str(vis), str(bandpass), refant="A0", solint="inf", minsnr=0.0)
    with opened(bandpass, True) as table:
        parameters = table.getcol("CPARAM")
        parameters[:] = 1
        parameters[:, 0::2, :] = 2
        table.putcol("CPARAM", parameters)
        table.putcol("FLAG", np.zeros_like(table.getcol("FLAG")))
    with opened(vis, True) as table:
        weights = table.getcol("WEIGHT")
        weights[:] = np.array([[2.0], [3.0]])
        table.putcol("WEIGHT", weights)
        times = table.getcol("TIME")
        slots = np.searchsorted(np.unique(times), times)
        sigma = table.getcol("SIGMA")
        sigma[0, :] = np.where(slots % 2 == 0, 1.0, 2.0)
        table.putcol("SIGMA", sigma)
    flagged = root / "flagged.cal"
    copy_ms(bandpass, flagged)
    with opened(flagged, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 0, table.getcol("ANTENNA1") == 0] = True
        table.putcol("FLAG", flags)
    flagged_gain = root / "flagged-amplitude.cal"
    copy_ms(amplitude, flagged_gain)
    with opened(flagged_gain, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 0, table.getcol("ANTENNA1") == 0] = True
        table.putcol("FLAG", flags)
    return vis, amplitude, bandpass, flagged, flagged_gain


def snapshot(vis):
    with opened(vis) as table:
        columns = tuple(table.colnames())
        present = (name for name in ("DATA", "CORRECTED_DATA", "FLAG", "FLAG_ROW", "WEIGHT",
                                     "WEIGHT_SPECTRUM", "SIGMA", "SIGMA_SPECTRUM") if name in columns)
        return columns, {name: table.getcol(name) for name in present}


def verify_source(vis, before):
    columns, data = snapshot(vis)
    assert columns == before[0]
    for name, expected in before[1].items():
        np.testing.assert_array_equal(data[name], expected, err_msg=f"MS column {name} changed")


def library_entry(table, calwt=False):
    return f"caltable='{table}' calwt={calwt} tinterp='nearest' finterp='linear'"


def requests(root, amplitude, bandpass, flagged, flagged_gain):
    amplitude_inline = library_entry(amplitude)
    callibrary = root / "apply.txt"
    callibrary.write_text(amplitude_inline + "\n", encoding="utf-8")
    for mode, library in (("file", str(callibrary)), ("inline", amplitude_inline),
                          ("list", [amplitude_inline])):
        yield f"{mode}-all", {"callib": library, "spw": "0"}
        yield f"{mode}-channel", {"callib": library}
    yield "mixed-data-and-corrected", {
        "callib": str(callibrary), "xaxis": "amp", "xdatacolumn": "data",
    }
    for axis in ("x", "y"):
        yield f"independent-{axis}-data-column", {
            "callib": str(callibrary), "xaxis": "amp", "ydatacolumn": "",
            f"{axis}datacolumn": "corrected",
        }
    yield "mixed-data-and-corrected-average", {
        "callib": str(callibrary), "xaxis": "amp", "xdatacolumn": "data",
        "spw": "0:0~1", "avgchannel": "2",
    }
    for label, spw in (("offset", "0:3~6"), ("strided", "0:0~7^2"),
                       ("segmented", "0:0~2;5~7"), ("single", "0:2")):
        yield f"{label}-channel-average", {
            "callib": str(callibrary), "xaxis": "channel",
            "spw": spw, "avgchannel": "2",
        }
    yield "unplotted-time-average", {
        "callib": str(callibrary), "xaxis": "channel", "avgtime": "30",
    }
    yield "multiple-library-terms", {
        "callib": [amplitude_inline, library_entry(bandpass)], "spw": "0:0~1",
    }
    yield "library-field-slices", {
        "callib": [amplitude_inline + " field='0'", amplitude_inline + " field='1'"],
    }
    yield "library-covers-one-field", {"callib": amplitude_inline + " field='0'"}
    yield "library-covers-no-selected-field", {
        "field": "1", "callib": amplitude_inline + " field='0'",
    }
    yield "flagged-solution-channel", {
        "callib": library_entry(flagged), "spw": "0:0~2",
    }
    yield "flagged-gain-baseline", {
        "callib": library_entry(flagged_gain), "antenna": "A0&A1;A2&A3",
    }
    for seconds in (20, 30, 120):
        yield f"sigma-weighted-time-{seconds}", {
            "callib": library_entry(amplitude, True), "avgtime": str(seconds),
        }
    for name, table in (("amplitude", amplitude), ("bandpass", bandpass)):
        for calwt in (False, True):
            library = library_entry(table, calwt)
            for axis in ("weight", "weightspectrum", "amp"):
                yield f"{name}-{calwt}-{axis}", {
                    "callib": library, "xaxis": "channel", "yaxis": axis,
                    "spw": "0:0~1", "avgchannel": "2" if axis == "amp" else "",
                }


def spectral_weight_fixture(root, vis):
    spectral = root / "spectral-weights.ms"
    copy_ms(vis, spectral)
    with opened(spectral, True) as table:
        sigma = np.ones(table.getcol("DATA").shape, dtype=np.float32)
        sigma[:, 1::2, :] = 4.0
        for name, values in (("SIGMA_SPECTRUM", sigma), ("WEIGHT_SPECTRUM", 1.0 / sigma**2)):
            if name not in table.colnames():
                description = table.getcoldesc("DATA")
                description.update(valueType="float", dataManagerType="StandardStMan",
                                   dataManagerGroup=name, option=4,
                                   shape=np.array(sigma.shape[:2], dtype=np.int32))
                table.addcols({name: description}, {"TYPE": "StandardStMan", "NAME": name})
            table.putcol(name, values)
    return spectral


def spectral_weight_requests(spectral, amplitude, bandpass):
    for name, correction, calibrate_weights in (
        ("amplitude", amplitude, False), ("bandpass", bandpass, True),
    ):
        for setting, average in (("channel", {"avgchannel": "2"}),
                                 ("time-channel", {"avgchannel": "2", "avgtime": "30"}),
                                 ("time", {"avgtime": "30"})):
            yield f"spectral-weight-{name}-{setting}", dict(
                vis=str(spectral), callib=library_entry(correction, calibrate_weights),
                xaxis="time", spw="0:0~1",
            ) | average


def compare(root, vis, tests, native):
    sources = {Path(options.get("vis", vis)) for _, options in tests}
    snapshots = {source: snapshot(source) for source in sources}
    defaults = dict(vis=str(vis), xaxis="time", yaxis="amp", ydatacolumn="corrected",
                    antenna="A0&A1", spw="0:0", correlation="XX", showgui=False,
                    expformat="txt", overwrite=True)
    points = 0
    cases = 0
    for name, options in tests:
        expected = root / f"{name}.casa.txt"
        actual = root / f"{name}.native.txt"
        args = defaults | options
        source = Path(args["vis"])
        before = snapshots[source]
        start = perf_counter()
        assert reference(**args, plotfile=str(expected)) is None
        casa_seconds = perf_counter() - start
        verify_source(source, before)
        start = perf_counter()
        assert native(**args, plotfile=str(actual)) is None
        native_seconds = perf_counter() - start
        count = compare_text(actual, expected)
        assert count > 0
        verify_source(source, before)
        print(f"{name}: {count} points, CASA {casa_seconds:.3f}s, casa-rs {native_seconds:.3f}s",
              flush=True)
        cases += 1
        points += count
    missing = root / "missing.callib"
    out = root / "missing.txt"
    for plot in (reference, native):
        try:
            plot(**defaults, callib=str(missing), plotfile=str(out))
        except RuntimeError as error:
            assert str(error) == "callib file does not exist", error
        else:
            raise AssertionError("PlotMS accepted a missing callibrary")
        assert not out.exists()
    print(f"{cases} exports, {points} points and the missing-file error matched.")


def disabled_selection(root, vis, amplitude, native):
    controls = dict(vis=str(vis), xaxis="time", yaxis="amp", ydatacolumn="corrected",
                    callib=library_entry(amplitude), showgui=False, expformat="txt")
    expected = root / "unselected.casa.txt"
    actual = root / "unselected.native.txt"
    assert reference(**controls, plotfile=str(expected)) is None
    before = snapshot(vis)
    assert native(**controls, selectdata=False, field="missing", antenna="wrong",
                  plotfile=str(actual)) is None
    verify_source(vis, before)
    # PlotMS groups the unfiltered MS by its visibility iterator. Compare its
    # coordinates by their complete source metadata when the order differs.
    def points(path):
        return sorted((tokens[2:], [float(value) for value in tokens[:2]])
                      for line in path.read_text().splitlines() if not line.startswith("#")
                      for tokens in [line.split()])

    expected_points = points(expected)
    actual_points = points(actual)
    assert [info for info, _ in actual_points] == [info for info, _ in expected_points]
    np.testing.assert_allclose([xy for _, xy in actual_points], [xy for _, xy in expected_points],
                               rtol=1.2e-5, atol=1e-7)
    print(f"Disabled selector fallback matched {len(expected_points)} unfiltered CASA points.")


def figures(root, vis, amplitude):
    from PIL import Image
    from casars import _plot_document, tasks

    controls = dict(vis=str(vis), xaxis="frequency", yaxis="amp", ydatacolumn="corrected",
                    antenna="A0&A1", spw="0:0", correlation="XX", showgui=False,
                    callib=library_entry(amplitude))
    text = root / "render-reference.txt"
    png = root / "corrected.png"
    assert reference(**controls, expformat="txt", plotfile=str(text)) is None
    values = np.array([[float(value) for value in row.split()[:2]]
                       for row in text.read_text().splitlines() if not row.startswith("#")])
    captured = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, axis = render(*args, **kwargs)
        captured.append((axis.get_xlabel(), axis.lines[0].get_xydata()))
        return figure, axis

    before = snapshot(vis)
    with patch.object(_plot_document, "render_document", record):
        result = tasks.run("plotms", **controls, plotfile=str(png), width=500, height=360, dpi=100)
    assert result.value is None
    assert {Path(product.path) for product in result.result.products if product.exists} == {png}
    [(label, coordinates)] = captured
    assert label == "Frequency (GHz)"
    np.testing.assert_allclose(coordinates, values, rtol=1.2e-5, atol=1e-7)
    with Image.open(png) as image:
        assert image.size == (500, 360)
    verify_source(vis, before)
    print(f"Rendered {len(values)} matching coordinates at 500 by 360 pixels.")


def compare_independent_ordinates(root, native, vis, amplitude, bandpass, flagged):
    from casars.tasks import TaskParameterError
    from plotms_ms_iterated_ordinates_oracle import compare_figures

    controls = dict(vis=str(vis), xaxis="channel", yaxis=["real", "real"],
                    ydatacolumn=["data", "corrected"], yaxislocation=["left", "right"],
                    antenna="A0&A1", spw="0:0~3", correlation="XX", showgui=False,
                    overwrite=True, callib=[library_entry(amplitude), library_entry(bandpass, True)],
                    exprange="all", title="Selected calibrated visibilities", xlabel="Calibration channel bin")
    binned = dict(avgchannel="2")
    cases = (
        ("callib-two-raw", {}, 96),
        ("callib-two-channel", binned, 48),
        ("callib-two-channel-scan", binned | dict(iteraxis="scan", gridrows=2), 48),
        ("callib-two-flagged-channel", binned | dict(
            callib=[library_entry(amplitude), library_entry(flagged, True)]), 48),
    )
    before = snapshot(vis)
    points = {}
    for name, changes, count in cases:
        options = controls | changes
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected), expformat="txt") is None
        rows = [line.split() for line in expected.read_text().splitlines() if not line.startswith("#")]
        assert len(rows) == count
        assert {point[3] for point in rows} == {"1", "2"}
        points[name] = rows
        assert native(**options, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching independent calibrated coordinates", flush=True)
        if name == "callib-two-channel-scan":
            assert [row[3] for row in rows] == ["1"] * 24 + ["2"] * 24
            values = np.array([point[:2] for point in rows], dtype=float)
            pages = [[(label, panel)] for label, panel in zip(("1", "2"), np.split(values, 2), strict=True)]
            compare_figures(root, native, "calibrated-ordinate-panels", options | dict(gridrows=1),
                            pages, ["Scan1", "Scan2_2"],
                            [("left", "Real", [0]), ("right", "Real:corrected", [1])])
    clear = points["callib-two-channel"]
    masked = points["callib-two-flagged-channel"]
    assert clear[:24] == masked[:24], "Calibration changed the stored ordinate"
    assert [row[2:] for row in clear] == [row[2:] for row in masked]
    assert sum(a[1] != b[1] for a, b in zip(clear[24:], masked[24:], strict=True)) == 12
    for name, options, message in (
        ("overlay", dict(showatm=True), "independent ordinate columns"),
        ("flagged-symbol", dict(customflaggedsymbol=[True, True],
                               flaggedsymbolshape=["circle", "circle"]), "displaying flagged points"),
    ):
        path = root / f"calibrated-ordinate-{name}.txt"
        try:
            native(**(controls | options), plotfile=str(path), expformat="txt")
        except TaskParameterError as error:
            assert message in str(error) and "#1032" in str(error)
        else:
            raise AssertionError(f"PlotMS accepted calibrated ordinates with {name}")
        assert not path.exists()
    verify_source(vis, before)


def compare_independent_order(root, native, vis, amplitude, bandpass):
    reordered = root / "ordinate-correlation.ms"
    copy_ms(vis, reordered)
    with opened(reordered, True) as table:
        rows = np.flatnonzero((table.getcol("ANTENNA1") == 0) & (table.getcol("ANTENNA2") == 1))
        first = int(rows[0])
        assert not table.getcell("FLAG", first).any()
        time = table.getcell("TIME", first)
        table.putcell("FLAG_ROW", first, True)
    before = snapshot(reordered)
    controls = dict(vis=str(reordered), xaxis="channel", yaxis=["real", "real"],
                    ydatacolumn=["data", "corrected"], antenna="A0&A1", spw="0:0~3",
                    correlation="XX,YY", coloraxis="corr", showgui=False, expformat="txt",
                    callib=[library_entry(amplitude), library_entry(bandpass, True)])
    for name, changes, count in (
        ("raw", {}, 192),
        ("channel", dict(avgchannel="2"), 96),
        ("scan", dict(avgchannel="2", iteraxis="scan", gridrows=2, exprange="all"), 96),
    ):
        options = controls | changes
        expected, actual = (root / f"ordinate-correlation-{name}.{engine}.txt"
                            for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        rows = [line.split() for line in expected.read_text().splitlines() if not line.startswith("#")]
        assert [row[12] for row in rows[:4]] == ["XX", "YY", "XX", "YY"]
        np.testing.assert_allclose([float(row[9]) for row in rows[:4]], time, rtol=0, atol=0.00051)
        print(f"ordinate-correlation-{name}: {count} matching ordered coordinates", flush=True)
    verify_source(reordered, before)


def compare_independent_spectra(root, native, vis):
    attach_syscal(vis, root / "ordinate-syscal.txt", spectrum=True)
    with opened(vis / "SYSCAL", True) as table:
        antennas = np.tile(np.arange(4, dtype=np.int32), table.nrows() // 4)
        table.putcol("ANTENNA_ID", antennas)
        spectrum = table.getcol("TSYS_SPECTRUM")
        spectrum[:, ::2, :] = 10
        spectrum[:, 1::2, :] = 50
        table.putcol("TSYS_SPECTRUM", spectrum)
    tsys = root / "ordinate-tsys.cal"
    casa.gencal(str(vis), str(tsys), "tsys")
    # Remove one complete bin on the selected baseline and polarization. The
    # surviving bins still have unequal sigma and weight spectra.
    with opened(vis, True) as table:
        rows = np.flatnonzero((table.getcol("ANTENNA1") == 0) & (table.getcol("ANTENNA2") == 1))
        first = int(rows[0])
        flags = table.getcell("FLAG", first)
        flags[0, 2:4] = True
        table.putcell("FLAG", first, flags)
    before = snapshot(vis)
    controls = dict(xaxis="channel", yaxis=["real", "real"],
                    ydatacolumn=["data", "corrected"], yaxislocation=["left", "right"],
                    antenna="A0&A1", spw="0:0~3", correlation="XX", avgchannel="2",
                    showgui=False, expformat="txt")
    cases = [("False", vis, False, (1.06802, 13.0989)), ("True", vis, True, (1.06802, 10.8068))]
    for column, corrected in (("WEIGHT_SPECTRUM", 31.0507), ("SIGMA_SPECTRUM", 12.2703)):
        partial = vis.with_name(f"without-{column.lower()}.ms")
        copy_ms(vis, partial)
        with opened(partial, True) as table:
            table.removecols([column])
        cases.append((f"without-{column.lower()}", partial, True, (1.04923, corrected)))
    paired_rows = []
    for name, source, calwt, first_bin in cases:
        start = snapshot(source)
        options = controls | dict(vis=str(source), callib=library_entry(tsys, calwt))
        stem = f"calibrated-ordinate-spectra-{name}"
        expected, actual = (root / f"{stem}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 46
        rows = [line.split() for line in expected.read_text().splitlines() if not line.startswith("#")]
        np.testing.assert_allclose([float(rows[0][1]), float(rows[23][1])],
                                   first_bin, rtol=1.2e-5)
        assert [row[2:] for row in rows[:23]] == [row[2:] for row in rows[23:]]
        paired_rows.append(rows)
        verify_source(source, start)
        print(f"{stem}: 46 matching coordinates with the flagged bin omitted", flush=True)
    assert paired_rows[0][:23] == paired_rows[1][:23], "calwt changed the stored ordinate"
    verify_source(vis, before)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True, help="Built calibrate executable")
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("ordinates", "existing"))
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_CALIBRATE_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native

    assert casa.version_string() == "6.7.6.14"
    vis, amplitude, bandpass, flagged, flagged_gain = fixture(root)
    # CASA can create temporary sorted MeasurementSets in its working directory.
    previous = Path.cwd()
    os.chdir(root)
    try:
        with plotms_launcher(args.app_run):
            if args.only != "existing":
                compare_independent_ordinates(root, native, vis, amplitude, bandpass, flagged)
                compare_independent_order(root, native, vis, amplitude, bandpass)
                spectral_dir = root / "ordinate-spectral"
                spectral_dir.mkdir()
                spectral = spectral_weight_fixture(spectral_dir, vis)
                compare_independent_spectra(root, native, spectral)
            if args.only != "ordinates":
                persisted = root / "persisted-correction.ms"
                copy_ms(vis, persisted)
                casa.clearcal(str(persisted))
                with opened(persisted, True) as table:
                    table.putcol("CORRECTED_DATA", 7 * table.getcol("DATA"))
                cases = list(requests(root, amplitude, bandpass, flagged, flagged_gain))
                spectral = spectral_weight_fixture(root, vis)
                cases.extend(spectral_weight_requests(spectral, amplitude, bandpass))
                cases.append(("preserve-existing-corrected-data", {
                    "vis": str(persisted), "callib": library_entry(amplitude),
                }))
                compare(root, vis, cases, native)
                figures(root, vis, amplitude)
                disabled_selection(root, vis, amplitude, native)
    finally:
        os.chdir(previous)


if __name__ == "__main__":
    main()
