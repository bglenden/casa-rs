# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired public PlotMS cases for antenna direction controls."""

from unittest.mock import patch

import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import compare_text, reference, rows


def compare_pointing_lists(work, native, vis):
    base = dict(vis=str(vis), xaxis="time", yaxis="ant-dec", antenna="A0&&&",
                showgui=False, overwrite=True)
    independent = base | dict(xaxis="ant-ra", xframe="j2000", xinterp="nearest",
                              yframe="galactic", yinterp="spline")
    for name, options in (
        ("one-layer", base | dict(yframe=["j2000"], yinterp=["nearest"])),
        ("empty-lists", independent | dict(yframe=[], yinterp=[], yaxislocation=[])),
        ("one-layer-shared-x-frame", independent | dict(yframe=["galactic"], yinterp=["spline"])),
        ("independent-scalar", independent),
    ):
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected), expformat="txt") is None
        assert native(**options, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 12
        print(f"{name}: 12 directly paired samples", flush=True)

    one = base | dict(yinterp=["nearest"], yaxislocation="right")
    one_text = work / "one-broadcast.casa.txt"
    assert reference(**one, plotfile=str(one_text), expformat="txt") is None
    image = work / "one-broadcast.casa.png"
    assert reference(**one, plotfile=str(image), expformat="png", width=500, height=360, dpi=100) is None
    with Image.open(image) as rendered:
        assert rendered.size == (500, 360)
    check_list_figure(native, work / "one-broadcast.native.png", one, [one_text],
                      "Time (MJD seconds)", [("right", "ICRS Declination (degrees)", [0])])

    parallel = independent | dict(yframe=["j2000", "galactic"], yinterp=["nearest", "spline"],
                                  yaxislocation=["left", "right"])
    parallel_labels = [("left", "J2000 Declination (degrees)", [0]),
                       ("right", "GALACTIC Latitude (degrees)", [1])]
    for name, options, scalars, xlabel, labels in (
        ("parallel", parallel, [("j2000", "nearest"), ("galactic", "spline")],
         "J2000 Right Ascension (degrees), GALACTIC Longitude (degrees)", parallel_labels),
        ("default-broadcast", base | dict(yinterp=["nearest", "spline"]),
         [("icrs", "nearest"), ("icrs", "spline")], "Time (MJD seconds)",
         [("left", "ICRS Declination (degrees)", [0, 1])]),
        ("scalar-broadcast", base | dict(yframe="b1950", yinterp=["nearest", "spline", "nearest"],
                                        yaxislocation="right"),
         [("b1950", "nearest"), ("b1950", "spline"), ("b1950", "nearest")], "Time (MJD seconds)",
         [("right", "B1950 Declination (degrees)", [0, 1, 2])]),
    ):
        check_scalar_layers(work, native, name, options, scalars, xlabel, labels)
    check_refusals(work, native, independent, parallel)


def check_scalar_layers(work, native, name, options, scalars, xlabel, labels):
    expected = []
    for index, (frame, interp) in enumerate(scalars):
        path = work / f"{name}-{index}.casa.txt"
        scalar = options | dict(yframe=frame, yinterp=interp, yaxislocation="left")
        if scalar["xaxis"] == "ant-ra":
            scalar |= dict(xframe=frame, xinterp=interp)
        assert reference(**scalar, plotfile=str(path), expformat="txt") is None
        assert len(rows(path)) == 12
        expected.append(path)
    # CASA's scalar exports supply each section including header, units and sample IDs.
    section_header = "# x y chan"
    sections = [path.read_text().split(section_header, 1) for path in expected]
    combined = work / f"{name}.scalar-reference.txt"
    combined.write_text(sections[0][0] + "".join(section_header + data for _, data in sections))
    actual = work / f"{name}.native.txt"
    assert native(**options, plotfile=str(actual), expformat="txt") is None
    count = compare_text(actual, combined)
    assert count == 12 * len(scalars)
    check_list_figure(native, work / f"{name}.native.png", options, expected, xlabel, labels)
    print(f"{name}: {count} ordered samples and artists against scalar CASA layers", flush=True)


def check_list_figure(native, output, options, expected, xlabel, labels):
    from casars import _plot_document

    render = _plot_document.render_document
    captured = []

    def record(*args, **kwargs):
        figure, axis = render(*args, **kwargs)
        assert axis.get_xlabel() == xlabel, axis.get_xlabel()
        for rendered, (side, label, indices) in zip(figure.axes, labels, strict=True):
            assert rendered.get_ylabel() == label, rendered.get_ylabel()
            assert rendered.yaxis.get_label_position() == side
            for artist, index in zip(rendered.lines, indices, strict=True):
                points = np.array([point[:2] for point in rows(expected[index])], dtype=float)
                actual = artist.get_xydata()
                np.testing.assert_allclose(actual[:, 0], points[:, 0], rtol=0,
                                           atol=0.00051 if options["xaxis"] == "time" else 2.1e-5)
                np.testing.assert_allclose(actual[:, 1], points[:, 1], rtol=0, atol=2.1e-5)
                for values, limits in zip(actual.T, (rendered.get_xlim(), rendered.get_ylim()), strict=True):
                    assert min(limits) <= min(values) <= max(values) <= max(limits)
                captured.append(len(points))
        return figure, axis

    with patch.object(_plot_document, "render_document", record):
        assert native(**options, plotfile=str(output), expformat="png", width=500, height=360, dpi=100) is None
    assert captured == [12] * len(expected)
    with Image.open(output) as rendered:
        assert rendered.size == (500, 360)


def check_refusals(work, native, independent, parallel):
    for name, overrides, reason in (
        ("x-frame-list", dict(xframe=["j2000", "galactic"]), "cStr"),
        ("x-interpolation-list", dict(xinterp=["nearest", "spline"]), "cStr"),
        ("x-empty-lists", dict(xframe=[], xinterp=[]), "cStr"),
        ("unequal-frame-length", dict(yframe=["j2000", "galactic"], yinterp=["nearest"]), "Length mismatch: yframe"),
        ("unequal-side-length", dict(yinterp=["nearest", "spline"], yaxislocation=["left"]), "Length mismatch: yaxislocation"),
        ("frame-only-list", dict(yframe=["j2000"]), "not yet implemented: yframe=list"),
    ):
        for engine, task in (("casa", reference), ("native", native)):
            output = work / f"{name}.{engine}.txt"
            try:
                task(**(independent | overrides), plotfile=str(output), expformat="txt")
            except (AssertionError, RuntimeError, TypeError, ValueError) as error:
                assert reason in str(error), (name, engine, error)
            else:
                raise AssertionError((name, engine, "expected refusal"))
            assert not output.exists()
        print(f"{name}: matched refusal with no output", flush=True)

    from grpc import RpcError, StatusCode

    for fmt in ("txt", "png"):
        output = work / f"parallel.casa.{fmt}"
        try:
            reference(**parallel, plotfile=str(output), expformat=fmt)
        except RpcError as error:
            assert error.code() == StatusCode.INVALID_ARGUMENT
            assert "plot parameters failed" in error.details()
        else:
            raise AssertionError(f"Expected casaplotms 2.9.1 to refuse parallel controls for {fmt}")
        assert not output.exists()
        print(f"parallel {fmt}: reproduced casaplotms multi-entry backend refusal", flush=True)
