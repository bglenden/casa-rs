"""Maximum source measurements in sky images and spectral cubes."""

import logging
from ast import literal_eval

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
        assert coords.units()[:2] == ["'", "'"]
        expected = np.deg2rad(coords.toworld(pixel, "n")["numeric"][:2] / 60)
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
    assert 0.005 < expected[0][0]["flux"]["value"][0] < 0.006
    assert 0.0025 < expected[4][0]["flux"]["value"][0] < 0.003
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
        assert -12. < component["flux"]["value"][0] < -10.
        _assert_position(image, component, center)
    finally:
        image.done()


@pytest.mark.parametrize("signed", [False, True])
@pytest.mark.parametrize("strongest_first", [False, True])
def test_provisional_peak_does_not_hide_a_stronger_adjacent_source(signed, strongest_first):
    strongest = (7., 13.) if strongest_first else (9., 13.)
    weaker = (9., 13.) if strongest_first else (7., 13.)
    sign = -1. if signed else 1.
    values = (
        _gaussian(sign * 12., strongest, (.27, .27))
        + _gaussian(6., weaker, (.27, .27))
    )
    image = tools.image()
    try:
        assert image.fromarray(pixels=values)
        component, = _components(image.maxfit(negfind=signed, list=False))
        assert 9. < sign * component["flux"]["value"][0] < 10.
        _assert_position(image, component, strongest)
    finally:
        image.done()


@pytest.mark.parametrize("signed", [False, True])
def test_raw_extremum_wins_even_if_a_broader_source_fits_higher(signed):
    center = (8., 7.)
    sign = -1. if signed else 1.
    values = _gaussian(sign * 12., center, (.27, .27)) + _gaussian(11., (17., 19.), (8., 8.))
    image = tools.image()
    try:
        assert image.fromarray(pixels=values)
        component, = _components(image.maxfit(negfind=signed, list=False))
        assert 9. < sign * component["flux"]["value"][0] < 10.
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


def test_maxfit_lists_region_local_pixels_and_world_units(casa, caplog):
    image = tools.image()
    region = casa.regionmanager().box([3, 4, 0, 1], [20, 22, 0, 1])
    try:
        values = _gaussian(6., (7.3, 8.8))
        image.fromarray(pixels=np.stack([values / 2, values], axis=-1)[:, :, None, :])
        with caplog.at_level(logging.INFO, logger="casars.tools.image"):
            result = image.maxfit(region=region)
        diagnostics = dict(message.split(" = ", 1) for message in caplog.messages if " = " in message)
        assert literal_eval(next(message.split(": ", 1)[1] for message in caplog.messages
                                 if message.startswith("World Axis Units"))) == ["'", "'"]
        pixel = literal_eval(diagnostics["Absolute pixel"])
        assert len(pixel) == 2
        np.testing.assert_allclose(pixel, [4.3, 4.8], atol=.2)
        component, = _components(result)
        np.testing.assert_allclose(
            literal_eval(diagnostics["Absolute world"]),
            np.rad2deg(_direction(component)) * 60,
            rtol=1e-8,
        )
    finally:
        image.done()


def test_maxfit_uses_the_selected_channel_beam(casa):
    records = []
    values = _gaussian(6., (7.3, 8.8))
    for api in (casa, tools):
        image = api.image()
        try:
            image.fromarray(pixels=np.repeat(values[:, :, None, None], 2, axis=3))
            image.setbrightnessunit("Jy/beam")
            for channel, major in enumerate(("2arcmin", "3arcmin")):
                image.setrestoringbeam(major=major, minor="1arcmin", pa="0deg",
                                       channel=channel, polarization=0)
            with pytest.raises(RuntimeError, match="multiple beams"):
                image.maxfit(list=False)
            result = []
            for channel in range(2):
                region = casa.regionmanager().box([0, 0, 0, channel], [23, 25, 0, channel])
                result.append(_components(image.maxfit(region=region, point=False, list=False)))
            records.append(result)
        finally:
            image.done()
    for actual, expected in zip(records[1], records[0]):
        _compare(actual, expected, fitted=True)
    flux = [record[0]["flux"]["value"][0] for record in records[1]]
    assert flux[0] / flux[1] == pytest.approx(1.5, rel=1e-4)


def test_maxfit_accepts_a_circle_and_rejects_disjoint_regions(casa):
    values = _gaussian(6., (7.3, 8.8)) + _gaussian(9., (16.2, 19.3))
    records = []
    for api in (casa, tools):
        image = api.image()
        try:
            image.fromarray(pixels=values)
            records.append(_components(image.maxfit(region="circle[[7pix, 9pix], 5pix]", list=False)))
            compound = "box[[0pix,1pix],[15pix,16pix]]\nbox[[12pix,13pix],[22pix,24pix]]"
            with pytest.raises(RuntimeError, match="Only a single"):
                image.maxfit(region=compound, list=False)
            np.testing.assert_array_equal(image.getchunk(), values)
        finally:
            image.done()
    _compare(records[1], records[0])


def test_maxfit_rejects_thin_direction_regions_across_channels(casa):
    image = tools.image()
    try:
        values = _gaussian(6., (7.3, 8.8))
        image.fromarray(pixels=np.broadcast_to(values[:, :, None, None], (24, 26, 1, 8)).copy())
        region = casa.regionmanager().box([0, 7, 0, 0], [23, 10, 0, 7])
        with pytest.raises(RuntimeError, match="No sources were found"):
            image.maxfit(region=region, list=False)
        assert image.isopen()
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
