"""Maximum source measurements in sky images and spectral cubes."""

import logging

import numpy as np
import pytest

from casars import tools
from test_image_findsources import _compare, _components, _direction


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _gaussian(height, center, widths=(4., 5.)):
    x, y = np.ogrid[:24, :26]
    return (height * np.exp(
        -((x - center[0]) ** 2 / widths[0] + (y - center[1]) ** 2 / widths[1])
    )).astype(np.float32)


def _assert_position(image, component, pixel):
    coords = image.coordsys()
    try:
        expected = coords.toworld(pixel, "n")["numeric"][:2]
    finally:
        coords.done()
    actual = _direction(component)
    difference = (actual[0] - expected[0] + np.pi) % (2 * np.pi) - np.pi
    np.testing.assert_allclose([difference, actual[1] - expected[1]], 0, atol=6e-5)


def test_maxfit_component_records_from_both_native_writers(casa, tmp_path):
    bright = _gaussian(6., (7.3, 8.8))
    weak = _gaussian(3., (14.8, 16.2))
    cube = np.stack([bright, weak, bright * 2.], axis=-1)[:, :, None, :]
    mask = np.ones(cube.shape, dtype=bool)
    mask[:, :, :, 2] = False
    mask[:2, :, :, :] = False
    choices = [
        {},
        {"point": False, "width": 4},
        {"point": False, "width": 0},
        {"point": False, "width": -2},
        {"region": casa.regionmanager().box([9, 9, 0, 1], [23, 25, 0, 1])},
        {"region": casa.regionmanager().box([0, 0, 0, 0], [22, 24, 0, 1], [2, 2, 1, 1])},
    ]
    results = {}
    for writer in (casa, tools):
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        try:
            assert image.fromarray(path, cube)
            assert image.setbrightnessunit("mJy/beam")
            assert image.setrestoringbeam(major="2arcmin", minor="1arcmin", pa="21deg")
            assert image.putregion(pixelmask=mask)
        finally:
            image.done()
        for reader in (casa, tools):
            image = reader.image()
            assert image.open(path)
            try:
                before = image.history()
                results[writer.__name__, reader.__name__] = [
                    _components(image.maxfit(list=False, **options)) for options in choices
                ]
                np.testing.assert_array_equal(image.getchunk(), cube)
                np.testing.assert_array_equal(image.getchunk(getmask=True), mask)
                assert image.history() == before
            finally:
                image.done()
    expected = results[casa.__name__, casa.__name__]
    assert expected[0][0]["flux"]["value"][0] == pytest.approx(0.006, rel=0.08)
    assert expected[4][0]["flux"]["value"][0] == pytest.approx(0.003, rel=0.08)
    assert [components[0]["shape"]["type"] for components in expected] == [
        "Point", "Gaussian", "Point", "Gaussian", "Point", "Point",
    ]
    for records in results.values():
        for index, (actual, reference) in enumerate(zip(records, expected)):
            _compare(actual, reference, fitted=index == 1)


@pytest.mark.parametrize("negative_first", [True, False])
@pytest.mark.parametrize("point", [True, False])
def test_negfind_uses_peak_magnitude_across_channels(negative_first, point):
    positive_center, negative_center = (6.3, 7.2), (15.5, 17.1)
    positive = _gaussian(6., positive_center)
    negative = _gaussian(-12., negative_center) + _gaussian(2., (8.3, 9.2))
    planes = [negative, positive] if negative_first else [positive, negative]
    image = tools.image()
    try:
        image.fromarray(pixels=np.stack(planes, axis=-1)[:, :, None, :])
        image.setbrightnessunit("Jy/pixel")
        for negfind, center, sign in (
            (False, positive_center, 1.),
            (True, negative_center, -1.),
        ):
            component, = _components(image.maxfit(negfind=negfind, point=point, list=False))
            assert component["shape"]["type"] == ("Point" if point else "Gaussian")
            flux = component["flux"]["value"][0]
            expected = (6. if sign > 0 else 12.) * (1. if point else np.pi * np.sqrt(20.))
            assert flux == pytest.approx(sign * expected, rel=.08)
            channel = int(negative_first != negfind)
            _assert_position(image, component, [*center, 0, channel])
    finally:
        image.done()


def test_negative_peak_beats_weaker_positive_peak_in_one_plane():
    center = (7.3, 8.8)
    pixels = _gaussian(-12., center) + _gaussian(6., (15.3, 18.8))
    image = tools.image()
    try:
        image.fromarray(pixels=pixels)
        component, = _components(image.maxfit(negfind=True, list=False))
        assert component["flux"]["value"][0] < -11.
        _assert_position(image, component, center)
    finally:
        image.done()


def test_maxfit_listing_is_optional_and_preserves_attachment(caplog):
    image = tools.image()
    try:
        values = _gaussian(6., (7.3, 8.8))
        assert image.fromarray(pixels=values)
        with caplog.at_level(logging.INFO, logger="casars.tools.image"):
            assert image.maxfit(list=False)["nelements"] == 1
            assert not caplog.records
            assert image.maxfit()["nelements"] == 1
        assert len(caplog.records) == 6
        assert "Brightness" in caplog.messages[0] and "Jy" in caplog.messages[0]
        assert "Absolute pixel" in caplog.messages[4]
        np.testing.assert_array_equal(image.getchunk(), values)
    finally:
        image.done()


def test_maxfit_empty_and_masked_regions(casa):
    assert casa.image().maxfit(list=False) == tools.image().maxfit(list=False) == {}
    image = tools.image()
    try:
        values = _gaussian(6., (7.3, 8.8))
        assert image.fromarray(pixels=values)
        assert image.putregion(pixelmask=np.zeros(values.shape, dtype=bool))
        with pytest.raises(RuntimeError, match="no valid pixels"):
            image.maxfit()
        assert image.isopen()
        np.testing.assert_array_equal(image.getchunk(), values)
    finally:
        image.done()


@pytest.mark.parametrize("kind", ["complex", "double", "tiny", "linear", "multiple_beams"])
def test_maxfit_refuses_unsupported_images_without_detaching(casa, kind):
    for api in (casa, tools):
        image = api.image()
        try:
            values = _gaussian(6., (7.3, 8.8))
            if kind in ("complex", "double"):
                image.fromshape(shape=list(values.shape), type="c" if kind == "complex" else "d")
            elif kind == "tiny":
                image.fromarray(pixels=values[:4, :4])
            elif kind == "multiple_beams":
                image.fromarray(pixels=np.repeat(values[:, :, None, None], 2, axis=3))
                for channel, major in enumerate(("2arcmin", "3arcmin")):
                    image.setrestoringbeam(major=major, minor="1arcmin", pa="0deg",
                                           channel=channel, polarization=0)
            else:
                image.fromarray(pixels=values, linear=True)
            with pytest.raises(RuntimeError):
                image.maxfit(list=False)
            assert image.isopen()
        finally:
            image.done()
