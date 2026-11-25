# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired public PlotMS cases for antenna direction controls."""

from unittest.mock import patch

import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import compare_figure, compare_text, reference, rows


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

    for name, options, xlabel, side, ylabel in (
        ("one-broadcast", base | dict(yinterp=["nearest"], yaxislocation="right"),
         "Time (MJD seconds)", "right", "ICRS Declination (degrees)"),
        ("time-side-list", base | dict(xaxis="ant-ra", xframe="j2000", xinterp="nearest",
                                      yaxis="time", yaxislocation=["right"]),
         "J2000 Right Ascension (degrees)", "left", "Time (MJD seconds)"),
    ):
        check_direct_figure(work, native, name, options, xlabel, side, ylabel)

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


def check_direct_figure(work, native, name, options, xlabel, side, ylabel):
    text = work / f"{name}.casa.txt"
    assert reference(**options, plotfile=str(text), expformat="txt") is None
    image = work / f"{name}.casa.png"
    assert reference(**options, plotfile=str(image), expformat="png", width=500, height=360, dpi=100) is None
    with Image.open(image) as rendered:
        assert rendered.size == (500, 360)
    check_list_figure(native, work / f"{name}.native.png", options, [text],
                      xlabel, [(side, ylabel, [0])])
    print(f"{name}: 12 matching rendered coordinates, labels, dimensions and side", flush=True)


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
                for values, reference_values, limits, axis_name in zip(
                    actual.T, points.T, (rendered.get_xlim(), rendered.get_ylim()),
                    (options["xaxis"], options["yaxis"]), strict=True,
                ):
                    np.testing.assert_allclose(values, reference_values, rtol=0,
                                               atol=0.00051 if axis_name == "time" else 2.1e-5)
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
        ("one-item-ordinate-list", dict(yaxis=["ant-dec"], yinterp=["nearest", "spline"]),
         "currently not supported: multiple y-axes involving ant-ra or ant-dec"),
        ("empty-frame-entry", dict(yframe=[""], yinterp=["nearest"]), "invalid yframe value"),
        ("empty-interpolation-entry", dict(yinterp=[""]), "invalid yinterp value"),
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


def compare_pointing_partners(work, native, vis):
    base = dict(vis=str(vis), antenna='A0&&&;A0&A1', spw='0:1~3^2', correlation='YY',
                showgui=False, overwrite=True)
    for name, overrides, axis_labels in (
        ('longitude-channel', dict(xaxis='ant-ra', xframe='j2000', xinterp='nearest', yaxis='channel'),
         ('J2000 Right Ascension (degrees)', 'Channel')),
        ('latitude-real', dict(xaxis='ant-dec', xframe='galactic', xinterp='spline', yaxis='real', ydatacolumn='data'),
         ('GALACTIC Latitude (degrees)', 'Real')),
    ):
        options = base | overrides | dict(title=name)
        expected, actual = (work / f'{name}.{engine}.txt' for engine in ('casa', 'native'))
        assert reference(**options, plotfile=str(expected), expformat='txt') is None
        points = rows(expected)
        assert len(points) == 45
        assert {p[2] for p in points} == {'1', '3'}
        assert {p[12] for p in points} == {'YY'}
        assert sum(p[6] == '0' for p in points) == 21
        assert sum(p[6] == '1' for p in points) == 24
        assert native(**options, plotfile=str(actual), expformat='txt') is None
        assert compare_text(actual, expected) == 45
        compare_figure(work, native, options, expected, (name, *axis_labels))
        print(f'{name}: 45 ordered paired coordinates on two baselines', flush=True)

    for engine, task in (('casa', reference), ('native', native)):
        out = work / f'pointing-ordinate-list.{engine}.txt'
        try:
            task(**base, xaxis='time', yaxis=['ant-ra', 'ant-dec'], yaxislocation=['left', 'right'],
                 plotfile=str(out), expformat='txt')
        except (RuntimeError, ValueError) as error:
            assert 'currently not supported: multiple y-axes involving ant-ra or ant-dec' in str(error)
        else:
            raise AssertionError((engine, 'expected refusal for general list of pointing axes'))
        assert not out.exists()


def compare_pointing_averaging(work, native, vis):
    base = dict(vis=str(vis), antenna='A0&&&', spw='0', correlation='YY',
                showgui=False, overwrite=True)
    for name, controls, averaging, count, labels in (
        ('time-nearest', dict(xaxis='time', yaxis='ant-ra', yframe='j2000', yinterp='nearest'),
         dict(avgtime='20'), 12, ('Time (MJD seconds)', 'J2000 Right Ascension (degrees)')),
        ('time-spline', dict(xaxis='time', yaxis='ant-ra', yframe='galactic', yinterp='spline'),
         dict(avgtime='20'), 12, ('Time (MJD seconds)', 'GALACTIC Longitude (degrees)')),
        ('channel-latitude', dict(xaxis='channel', yaxis='ant-dec', yframe='j2000', yinterp='nearest'),
         dict(avgchannel='2'), 93, ('Channel', 'J2000 Declination (degrees)')),
    ):
        raw, expected, actual = (work / f'{name}.{suffix}.txt' for suffix in ('raw', 'casa', 'native'))
        assert reference(**base, **controls, averagedata=False, plotfile=str(raw), expformat='txt') is None
        options = base | controls | averaging | dict(averagedata=True, title=name)
        assert reference(**options, plotfile=str(expected), expformat='txt') is None
        assert len(rows(expected)) == count and rows(expected) == rows(raw)
        assert native(**options, plotfile=str(actual), expformat='txt') is None
        assert compare_text(actual, expected) == count
        compare_figure(work, native, options, expected, (name, *labels))
        print(f'{name}: {count} paired raw coordinates with the averaging request', flush=True)
    compare_pointing_scan_pages(work, native, base)


def compare_pointing_scan_pages(work, native, base):
    from casars import _plot_document
    from plotms_ms_layout_oracle import occupied_marker_cells

    options = base | dict(xaxis='time', yaxis='ant-ra', yframe='j2000', yinterp='nearest',
                          iteraxis='scan', gridrows=1, gridcols=2, exprange='all', title='Pointing',
                          customsymbol=True, symbolshape='square', symbolsize=6, symbolcolor='dd2255')
    expected, actual = (work / f'pointing-scans.{engine}.txt' for engine in ('casa', 'native'))
    assert reference(**options, plotfile=str(expected), expformat='txt') is None
    assert native(**options, plotfile=str(actual), expformat='txt') is None
    assert compare_text(actual, expected) == 12
    coordinates = [np.array([p[:2] for p in rows(expected) if p[3] == scan], dtype=float)
                   for scan in ('1', '2')]
    assert [len(panel) for panel in coordinates] == [6, 6]
    render = _plot_document.render_document
    captured = []

    def record(*args, **kwargs):
        figure, axes = render(*args, **kwargs)
        assert len(axes) == 2
        for panel, (axis, points) in enumerate(zip(axes, coordinates, strict=True)):
            assert axis.get_subplotspec().colspan.start == panel
            assert axis.get_title() == f'Pointing Scan: {panel + 1}'
            assert axis.get_xlabel() == 'Time (MJD seconds)'
            assert axis.get_ylabel() == 'J2000 Right Ascension (degrees)'
            actual = axis.lines[0].get_xydata()
            np.testing.assert_allclose(actual[:, 0], points[:, 0], atol=.00051, rtol=0)
            np.testing.assert_allclose(actual[:, 1], points[:, 1], atol=2.1e-5, rtol=0)
            captured.append(len(points))
        return figure, axes

    expected, actual = (work / f'scans-{engine}.png' for engine in ('casa', 'native'))
    images = options | dict(expformat='png', width=500, height=360, dpi=100)
    assert reference(**images, plotfile=str(expected)) is None
    with patch.object(_plot_document, 'render_document', record):
        assert native(**images, plotfile=str(actual)) is None
    assert captured == [6, 6]
    for path in (expected, actual):
        products = list(work.glob(path.stem + '*.png'))
        assert [product.name for product in products] == [path.stem + '_Scan1,2.png']
        assert occupied_marker_cells(products[0], (1, 2)) == {(0, 0), (0, 1)}
    print('scan-iteration: 12 paired coordinates and both occupied cells on the same 500 × 360 page', flush=True)
