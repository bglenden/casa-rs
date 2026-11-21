"""Pinned CASA parity and native interoperability for position–velocity strips."""

from pathlib import Path

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _ramp(curvature=0.):
    x, y, pol, chan = np.ogrid[:24, :26, :2, :5]
    return (x + 2*y + curvature*y*y + 40*pol + 100*chan).astype(np.float32)


def _read(image):
    coords = image.coordsys()
    try:
        return (
            image.getchunk(), image.getchunk(getmask=True), image.brightnessunit(),
            coords.axiscoordinatetypes(), coords.units(), coords.referencepixel()["numeric"],
            coords.increment()["numeric"], coords.toworld([2., 0., 2.], "n")["numeric"],
        )
    finally:
        coords.done()


def _compare(actual, expected):
    assert actual[0].dtype == expected[0].dtype == np.float64
    assert actual[1].dtype == expected[1].dtype == np.bool_
    assert actual[2:5] == expected[2:5]
    np.testing.assert_array_equal(actual[1], expected[1])
    for index in (0, 5, 6, 7):
        np.testing.assert_allclose(actual[index], expected[index], rtol=2e-4, atol=1e-6)


@pytest.mark.parametrize("geometry", [
    {"start": [4, 7], "end": [16, 7]},
    {"start": [4., 7.], "end": [16., 7.]},
    {"center": [10, 7], "length": 12, "pa": "90deg"},
    {"center": [10, 7], "length": "12arcmin", "pa": {"value": 90., "unit": "deg"}},
])
@pytest.mark.parametrize(("width", "rows"), [(1, [7]), (3, [6, 7, 8]), ("120arcsec", [6, 7, 8])])
def test_pv_geometry_and_width_agree_with_analytic_strip(casa, geometry, width, rows):
    results = []
    data = _ramp(.5)
    for api in (casa, tools):
        source = api.image()
        result = None
        try:
            assert source.fromarray(pixels=data)
            assert source.setbrightnessunit("K")
            result = source.pv(**geometry, width=width)
            assert result.isopen()
            assert not result.ispersistent()
            actual = _read(result)
            results.append(actual)
            assert actual[0].shape == (13, 2, 5)
            np.testing.assert_allclose(actual[0], data[4:17, rows].mean(axis=1), atol=2e-5)
            np.testing.assert_array_equal(source.getchunk(), data)
        finally:
            if result is not None:
                result.done()
            source.done()
    _compare(results[1], results[0])


def test_pv_diagonal_strip_and_native_writer_reader_matrix(casa, tmp_path):
    data = _ramp()
    mask = np.ones(data.shape, dtype=bool)
    mask[:6, :10, :, :] = False
    mask[13:17, 10:16, 1, 2] = False
    observed = {}
    for writer in (casa, tools):
        source = writer.image()
        product = None
        path = str(tmp_path / f"{writer.__name__}-pv.im")
        try:
            assert source.fromarray(pixels=data)
            assert source.setbrightnessunit("K")
            assert source.putregion(pixelmask=mask)
            assert source.sethistory(origin="test", history=["source image history"])
            product = source.pv(path, [4, 7], [16, 13], width=3)
            assert product.ispersistent()
            assert product.isopen()
            np.testing.assert_array_equal(source.getchunk(getmask=True), mask)
        finally:
            if product is not None:
                product.done()
            source.done()
        for reader in (casa, tools):
            image = reader.image()
            try:
                assert image.open(path)
                assert any("source image history" in row for row in image.history())
                observed[writer.__name__, reader.__name__] = _read(image)
            finally:
                image.done()
    expected = observed[casa.__name__, casa.__name__]
    assert expected[0].shape == (15, 2, 5)
    assert not expected[1].all()
    for result in observed.values():
        _compare(result, expected)


@pytest.mark.parametrize("region", [None, [], ""])
def test_pv_spectral_and_polarization_selection(casa, region):
    data = _ramp()
    for api in (casa, tools):
        source = api.image()
        product = None
        try:
            assert source.fromarray(pixels=data)
            product = source.pv(start=[4, 7], end=[16, 7], region=region, chans="1~3", stokes="Q")
            assert tuple(product.shape()) == (13, 1, 3)
            np.testing.assert_array_equal(product.getchunk(), data[4:17, 7, 1:2, 1:4])
        finally:
            if product is not None:
                product.done()
            source.done()


def test_pv_strided_channel_region_retains_sampled_wcs(casa):
    data = _ramp()
    region = casa.regionmanager().box([0, 0, 0, 0], [23, 25, 1, 4], [1, 1, 1, 2])
    results = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            product = source.pv(start=[4, 7], end=[16, 7], region=region)
            try:
                results.append(_read(product))
                np.testing.assert_array_equal(product.getchunk(), data[4:17, 7, :, ::2])
                assert results[-1][6][-1] == pytest.approx(2000.)
            finally:
                product.done()
        finally:
            source.done()
    _compare(results[1], results[0])


def test_pv_returns_a_detached_tool_when_requested_and_preserves_existing_output(casa, tmp_path):
    for api in (casa, tools):
        source = api.image()
        returned = api.image()
        assert not returned.pv().isopen()
        path = tmp_path / f"{api.__name__}.im"
        try:
            assert source.fromarray(pixels=_ramp())
            returned = source.pv(str(path), [4, 7], [16, 7], wantreturn=False)
            assert not returned.isopen()
            assert Path(path).is_dir()
            with pytest.raises(RuntimeError):
                source.pv(str(path), [4, 7], [16, 7], width=2, overwrite=True)
            assert returned.open(str(path))
            np.testing.assert_array_equal(returned.getchunk(), _ramp()[4:17, 7])
            assert source.isopen()
        finally:
            returned.done()
            source.done()


def test_pv_refuses_mixed_endpoints_and_direction_region(casa):
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=_ramp())
            for options in (
                {"start": [4, 7], "end": [16., 7.]},
                {"start": [4, 7], "end": [16, 7], "width": 3.},
                {"start": [4, 7], "end": [16, 7], "chans": "1~3"},
                {"start": [4, 7], "end": [16, 7], "region": None, "chans": "1;3"},
                {"start": [4, 7], "end": [16, 7], "region": "box[[2pix,3pix],[22pix,23pix]]"},
                {"start": [4, 7], "end": [16, 7], "center": [10, 7]},
            ):
                with pytest.raises(RuntimeError):
                    source.pv(**options)
                assert source.isopen()
        finally:
            source.done()


@pytest.mark.parametrize("framed", [False, True])
def test_sky_endpoint_strings_match_the_same_pixel_strip(casa, framed):
    records = []
    for api in (casa, tools):
        image = api.image()
        try:
            values = _ramp()
            image.fromarray(pixels=values)
            coordinates = image.coordsys()
            try:
                assert coordinates.setunits(["rad", "rad"], type="direction")
                positions = []
                for x in (4., 16.):
                    angles = coordinates.toworld([x, 7., 0., 0.], "n")["numeric"][:2]
                    pair = [f"{angle:.17g}rad" for angle in angles]
                    positions.append("J2000 " + " ".join(pair) if framed else pair)
            finally:
                coordinates.done()
            result = image.pv(start=positions[0], end=positions[1], unit="deg")
            try:
                records.append(_read(result))
                np.testing.assert_allclose(result.getchunk(), values[4:17, 7], atol=2e-5)
            finally:
                result.done()
        finally:
            image.done()
    _compare(records[1], records[0])


def test_pv_reverses_the_offset_pixels_when_the_endpoints_are_reversed():
    image = tools.image()
    try:
        values = _ramp()
        assert image.fromarray(pixels=values)
        result = image.pv(start=[16, 7], end=[4, 7])
        try:
            np.testing.assert_allclose(result.getchunk(), values[16:3:-1, 7], atol=2e-4)
        finally:
            result.done()
    finally:
        image.done()
