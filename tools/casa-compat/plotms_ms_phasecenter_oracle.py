#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare fixed MeasurementSet phase-center shifts through public PlotMS."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import (
    casa, compare_figure, compare_text, copy_ms, fixture, opened, plotms_launcher, reference, request, rows,
    write_model,
)


PHASE_CENTER = "J2000 00h00m02.00s -29d59m40.00s"
UNFLAGGED_XX = {("1", "XX"), ("3", "XX")}


def compare_values(root, native, vis, shifted, name, axes, *, samples=UNFLAGGED_XX):
    controls = request(vis) | axes | dict(expformat="txt")
    original, expected, actual = (root / f"{name}.{engine}.txt"
                                  for engine in ("original", "casa", "native"))
    assert reference(**(controls | dict(transform=False, phasecenter=PHASE_CENTER)),
                     plotfile=str(original)) is None
    assert reference(**(controls | dict(vis=str(shifted))), plotfile=str(expected)) is None
    original_rows, expected_rows = rows(original), rows(expected)
    count = 6 * len(samples)
    assert len(original_rows) == len(expected_rows) == count
    assert [point[2:] for point in original_rows] == [point[2:] for point in expected_rows]
    assert {(point[2], point[12]) for point in expected_rows} == samples
    before, after = (np.array([point[:2] for point in selected], dtype=float)
                     for selected in (original_rows, expected_rows))
    changed = np.abs(after - before) > 1.2e-5 * np.maximum(np.abs(before), 1) + 1e-7
    assert np.all(np.any(changed, axis=0)), (name, before, after)

    assert native(**controls, transform=True, phasecenter=PHASE_CENTER, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == count
    inactive = root / f"{name}.inactive.txt"
    assert native(**controls, transform=False, phasecenter=PHASE_CENTER, plotfile=str(inactive)) is None
    assert compare_text(inactive, original) == count
    print(f"{name}: {count} shifted and {count} unchanged selected coordinates", flush=True)
    return expected


def marker_mask(path):
    with Image.open(path) as image:
        assert image.size == (500, 360)
        return np.all(np.asarray(image.convert("RGB")) == [0xdd, 0x22, 0x55], axis=-1)


def compare_shifted_figure(root, native, vis, shifted, controls, expected):
    image = request(vis) | controls | dict(transform=True, phasecenter=PHASE_CENTER,
                                           title="Shifted visibility", customsymbol=True,
                                           symbolshape="circle", symbolcolor="dd2255", symbolsize=5,
                                           symbolfill="fill", symboloutline=False,
                                           plotrange=[-45, -25, -180, 180])
    compare_figure(root, native, image, expected, ("Shifted visibility", "U (m)", "Phase (degrees)"))
    direct = marker_mask(root / "casa" / "u.png")
    assert np.any(direct) and np.any(marker_mask(root / "native" / "u.png"))
    for name, source in (("materialized", shifted), ("inactive", vis)):
        png = root / f"{name}.casa.png"
        assert reference(**(image | dict(vis=str(source), transform=False)), plotfile=str(png),
                         expformat="png", width=500, height=360, dpi=100) is None
        control_mask = marker_mask(png)
        assert np.any(control_mask)
        assert np.array_equal(direct, control_mask) == (name == "materialized"), name


def check_unreadable_model(root, native, vis, expected):
    from casars.tasks import TaskExecutionError

    work = root / "unreadable-model"
    work.mkdir()
    source = work / vis.name
    copy_ms(vis, source)
    with opened(source / "FIELD") as table:
        direction = table.getcell("PHASE_DIR", 1)[:, 0]
    with opened(source / "SPECTRAL_WINDOW") as table:
        frequency = table.getcell("CHAN_FREQ", 0)[0]
    image = work / "point.im"
    write_model(image, direction, frequency, 2.0, cell=5)
    assert casa.ft(vis=str(source), model=str(image), field="PHASE", usescratch=False) is None
    with opened(source / "SOURCE", True) as table:
        model_row = table.getkeyword(table.getkeyword("definedmodel_field_1"))
        record = table.getcell("SOURCE_MODEL", model_row)
        record["ft_0"]["container"]["diskimage"] = str(work / "missing.im")
        table.putcell("SOURCE_MODEL", model_row, record)
    controls = request(source) | dict(xaxis="real", yaxis="imag", xdatacolumn="model",
                                      expformat="txt", transform=True, phasecenter=PHASE_CENTER)
    physical = work / "physical.native.txt"
    assert native(**controls, plotfile=str(physical)) is None
    assert compare_text(physical, expected) == 12
    virtual = work / "virtual.native.txt"
    try:
        native(**(controls | dict(field="PHASE")), plotfile=str(virtual))
    except TaskExecutionError as error:
        assert "phasecenter with a virtual model is pending (#1032)" in str(error), error
    else:
        raise AssertionError("PlotMS evaluated an unsupported virtual visibility model")
    assert not virtual.exists()


def compare_unused_float(root, native, vis):
    from casars.tasks import TaskParameterError

    work = root / "mixed-data"
    work.mkdir()
    source = work / vis.name
    copy_ms(vis, source)
    with opened(source, True) as table:
        descriptor = table.getcoldesc("DATA")
        descriptor.update(name="FLOAT_DATA", valueType="float",
                          dataManagerType="StandardStMan", dataManagerGroup="FloatVis")
        table.addcols({"FLOAT_DATA": descriptor})
        table.putcol("FLOAT_DATA", table.getcol("DATA").real - 6)
    shifted = work / "shifted" / vis.name
    shifted.parent.mkdir()
    casa.phaseshift(vis=str(source), outputvis=str(shifted), phasecenter=PHASE_CENTER,
                    datacolumn="all")
    controls = dict(xaxis="real", yaxis="imag", xdatacolumn="model")
    compare_values(work, native, source, shifted, "unused-float", controls)
    output = work / "used-float.native.txt"
    try:
        native(**(request(source) | controls | dict(xdatacolumn="data")),
               transform=True, phasecenter=PHASE_CENTER, plotfile=str(output), expformat="txt")
    except TaskParameterError as error:
        assert "phasecenter with MeasurementSet FLOAT_DATA is pending (#1032)" in str(error), error
    else:
        raise AssertionError("PlotMS applied a complex rotation to FLOAT_DATA")
    assert not output.exists()


def check_refusals(root, native, vis):
    from casars.tasks import TaskParameterError

    base = request(vis) | dict(expformat="txt", transform=True, phasecenter=PHASE_CENTER)
    for name, options, diagnostic in (
        ("bad-value", dict(phasecenter="J2000 north west"), "Cannot interpret phasecenter"),
        ("other-frame", dict(phasecenter="AZELGEO 0deg 0deg"), "outside J2000 and Galactic"),
        ("averaging", dict(averagedata=True, avgchannel="3"), "unaveraged"),
    ):
        output = root / f"{name}.native.txt"
        try:
            native(**(base | options), plotfile=str(output))
        except TaskParameterError as error:
            assert diagnostic in str(error), error
        else:
            raise AssertionError(f"PlotMS accepted {name}")
        assert not output.exists()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("compositions",))
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    calibrate = args.calibrate_bin or args.native_bin.with_name("calibrate")
    os.environ["CASARS_CALIBRATE_BIN"] = str(calibrate.resolve())
    from casars.tasks import plotms as native
    from plotms_ms_phasecenter_compositions_oracle import compare_phasecenter_compositions

    with chdir(root), plotms_launcher(args.app_run):
        if args.only == "compositions":
            compare_phasecenter_compositions(root / "compositions", native, PHASE_CENTER)
            return
        vis, original_columns = fixture(root)
        with opened(vis, True) as table:
            row = np.arange(table.nrows())
            uvw = np.array([-30 - row / 16, 42 + row / 8, 4 + row / 32])
            table.putcol("UVW", uvw)
        shifted = root / "shifted" / vis.name
        shifted.parent.mkdir()
        # Direct PlotMS text export stalls after its selection header. The
        # phaseshift task uses the same tviphaseshift iterator and materializes
        # its coordinates into a separate MS readable by pinned PlotMS.
        casa.phaseshift(vis=str(vis), outputvis=str(shifted), phasecenter=PHASE_CENTER,
                        datacolumn="all")
        compare_values(root, native, vis, shifted, "independent", dict(xaxis="real", yaxis="imag"))
        compare_values(root, native, vis, shifted, "flagged",
                       dict(xaxis="real", yaxis="imag", customflaggedsymbol=True,
                            flaggedsymbolshape="diamond"), samples=UNFLAGGED_XX | {("2", "XX")})
        for axis in ("u", "v", "w"):
            controls = dict(xaxis=axis, yaxis="phase", xdatacolumn="", ydatacolumn="data")
            samples = UNFLAGGED_XX
            if axis == "u":
                controls["correlation"] = "XX,YY"
                samples = samples | {(channel, "YY") for channel in ("1", "2", "3")}
            expected = compare_values(root, native, vis, shifted, axis, controls, samples=samples)
            if axis == "u":
                compare_shifted_figure(root, native, vis, shifted, controls, expected)
        expected_model = compare_values(root, native, vis, shifted, "model",
                                        dict(xaxis="real", yaxis="imag", xdatacolumn="model"))
        check_unreadable_model(root, native, vis, expected_model)
        compare_unused_float(root, native, vis)
        check_refusals(root, native, vis)
        with opened(vis) as table:
            for name, values in (original_columns | {"UVW": uvw}).items():
                np.testing.assert_array_equal(table.getcol(name), values)
        compare_phasecenter_compositions(root / "compositions", native, PHASE_CENTER)


if __name__ == "__main__":
    main()
