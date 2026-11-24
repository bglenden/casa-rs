#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare channel averages of stored and virtual PlotMS ordinate columns."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
import numpy as np
from PIL import Image

from plotms_ms_baseline_channels_oracle import make_reversed_partial_spectrum
from plotms_ms_columns_oracle import compare_text, copy_ms, fixture, opened, plotms_launcher, reference, request, rows


def make_fixture(root):
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        for name, spectrum in (("WEIGHT_SPECTRUM", [1, 9]), ("SIGMA_SPECTRUM", [1, 0.5])):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            values = np.resize(np.asarray(spectrum, dtype=np.float32), shape[1])[None, :, None]
            table.putcol(name, np.broadcast_to(values, shape).copy())
        channels = (1 + 2 * np.arange(shape[1]))[None, :, None].astype(np.complex64)
        for exponent, name in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            table.putcol(name, np.broadcast_to(10**exponent * channels, shape).copy())
        table.putcol("FLAG", np.zeros(shape, dtype=bool))
    return vis


def virtual_fixtures(root, vis):
    virtual = root / "virtual.ms"
    copy_ms(vis, virtual)
    with opened(virtual, True) as table:
        table.removecols(["MODEL_DATA"])
    assert casa.setjy(str(virtual), field="FLUX", standard="manual", usescratch=False,
                      fluxdensity=[7.25, 0, 0, 0])

    mixed = root / "stored-and-virtual.ms"
    copy_ms(virtual, mixed)
    with opened(vis) as stored, opened(mixed, True) as table:
        model = stored.getcol("MODEL_DATA")
        table.addcols({"MODEL_DATA": stored.getcoldesc("MODEL_DATA")})
        table.putcol("MODEL_DATA", model)
    return virtual, mixed, model


def compare_sides(root, native, controls, *, expected_x="Channel"):
    from casars import _plot_document

    captured = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, primary = render(*args, **kwargs)
        sides = [(axis.yaxis.get_label_position(), axis.get_ylabel(),
                  [line.get_xydata() for line in axis.lines]) for axis in figure.axes]
        captured.append((primary.get_xlabel(), sides))
        return figure, primary

    options = controls | dict(expformat="png", width=500, height=360, dpi=100)
    expected, actual = (root / f"three-ordinates.{engine}.png" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    with patch.object(_plot_document, "render_document", record):
        assert native(**options, plotfile=str(actual)) is None
    [(xlabel, axes)] = captured
    assert xlabel == expected_x
    assert [(side, label) for side, label, _ in axes] == [
        ("left", "Real, Real:model"), ("right", "Real:corrected")
    ]
    selected = np.array([p[:2] for p in rows(root / "three-ordinates.casa.txt")], dtype=float)
    sections = np.split(selected, 3)
    for (_, _, layers), order in zip(axes, ((0, 2), (1,)), strict=True):
        for layer, index in zip(layers, order, strict=True):
            np.testing.assert_allclose(layer, sections[index], rtol=1.2e-5)
    for image in (expected, actual):
        with Image.open(image) as opened_image:
            assert opened_image.size == (500, 360)
    print(f"three ordinates: matched both sides and all {len(selected)} rendered coordinates", flush=True)


def compare(root, native):
    vis = make_fixture(root)
    masked, plain, row_flagged = (root / f"{name}.ms" for name in
                                 ("masked", "without-weight-spectrum", "row-flagged"))
    for target in (masked, plain, row_flagged):
        copy_ms(vis, target)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[:, 2, :] = True
        table.putcol("FLAG", flags)
    with opened(plain, True) as table:
        table.removecols(["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"])
    with opened(row_flagged, True) as table:
        table.putcell("FLAG_ROW", 0, True)
    reversed_rows = make_reversed_partial_spectrum(plain, root / "reverse-time-sparse-spectrum.ms")
    virtual, mixed, stored_model = virtual_fixtures(root, vis)

    base = request(vis) | dict(xaxis="channel", xdatacolumn="", spw="0:0~3", avgchannel="2",
                               yaxis=["real", "real", "real"], ydatacolumn=["data", "corrected", "model"],
                               yaxislocation=["left", "right", "left"])
    observed, corrected, model = (2.6, 6.6), (28, 68), (280, 680)
    cases = (
        ("three-ordinates", {}, (observed, corrected, model)),
        ("four-ordinates", dict(yaxis=["real", "real", "amp", "phase"],
                                ydatacolumn=["data", "corrected", "model", "model"],
                                yaxislocation=["left", "right", "left", "right"]),
         (observed, corrected, model, (0, 0))),
        ("third-corrected", dict(yaxis=["real", "phase", "real"],
                                 ydatacolumn=["data", "data", "corrected"]), (observed, (0, 0), corrected)),
        ("third-observed", dict(yaxis=["real", "amp", "real"],
                                ydatacolumn=["corrected", "model", "data"]), (corrected, model, observed)),
        ("independent-abscissa", dict(xaxis="real", xdatacolumn="corrected",
                                      yaxis=["real", "amp", "phase"],
                                      ydatacolumn=["data", "model", "corrected"]),
         (observed, model, (0, 0))),
        ("masked-channel", dict(vis=str(masked)), ((2.6, 7), (28, 70), (280, 700))),
        ("three-ordinates-ignore-row-flag", dict(vis=str(row_flagged)), (observed, corrected, model)),
        ("two-ordinates-ignore-row-flag", dict(vis=str(row_flagged), yaxis=["real", "real"],
                                              ydatacolumn=["data", "corrected"]), (observed, corrected)),
        ("uniform-channel", dict(vis=str(plain)), ((2, 6), (20, 60), (200, 600))),
        ("iterator-order-sparse-spectrum", dict(vis=str(reversed_rows)), ((5, 0), (20, 60), (200, 600))),
        ("third-virtual-model", dict(vis=str(virtual)), (observed, corrected, (7.25, 7.25))),
        ("virtual-overrides-stored-model", dict(vis=str(mixed)), (observed, corrected, (7.25, 7.25))),
        ("virtual-model-abscissa", dict(vis=str(mixed), xaxis="real", xdatacolumn="model",
                                        yaxis="channel", ydatacolumn="", yaxislocation="left"), ((0, 1),)),
    )
    real_x_values = {"corrected": corrected, "model": (7.25, 7.25)}
    for name, overrides, ordinates in cases:
        controls = base | overrides | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        indices = [0, 1] * (6 * len(ordinates))
        assert len(selected) == len(indices)
        assert [int(point[2]) for point in selected] == indices
        values = [coordinate for ordinate in ordinates for _ in range(6) for coordinate in ordinate]
        x_values = real_x_values.get(controls["xdatacolumn"], (0, 1)) * (6 * len(ordinates))
        np.testing.assert_allclose(np.array([p[:2] for p in selected], dtype=float),
                                   np.column_stack((x_values, values)), rtol=1.2e-5)
        if name == "iterator-order-sparse-spectrum":
            first_ordinate_times = [float(point[9]) for point in selected[:12]]
            assert first_ordinate_times == sorted(first_ordinate_times)
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching channel-averaged coordinates", flush=True)
    compare_model_provider(root, native, base | dict(vis=str(mixed), expformat="txt"))
    with opened(virtual) as table:
        assert "MODEL_DATA" not in table.colnames()
    with opened(mixed) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), stored_model)
    compare_sides(root, native, base)


def compare_model_provider(root, native, controls):
    from casars.tasks import TaskExecutionError, _runner

    output = root / "direct-provider.txt"
    # Exercise the standalone msexplore provider with the same resolved parameters.
    with patch.object(_runner, "_plotms_model_requested", return_value=False):
        try:
            native(**controls, plotfile=str(output))
        except TaskExecutionError as error:
            assert "use casars.tasks.plotms to predict virtual MeasurementSet models" in str(error), error
        else:
            raise AssertionError("the standalone provider exported a virtual model without a predictor")
    assert not output.exists()


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
        compare(root, native)


if __name__ == "__main__":
    main()
