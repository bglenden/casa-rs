"""Gaussian flux conversions against CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def compare(actual, expected):
    assert actual["unit"] == expected["unit"]
    np.testing.assert_allclose(actual["value"], expected["value"], rtol=2e-14, atol=0)


def test_detached_convertflux_is_empty():
    assert tools.image().convertflux() == {}


@pytest.mark.parametrize(
    "brightness, value, topeak",
    [
        ("", "3Jy", True),
        ("Jy/beam", "3mJy", True),
        ("mJy/beam", {"unit": "Jy", "value": 3}, True),
        ("Jy/beam", "3Jy/beam", False),
        ("Jy/beam.km/s", "3Jy.m/s", True),
        ("Jy/beam.km/s", "3Jy/beam.km/s", False),
        ("Jy/arcsec2", "3Jy", True),
        ("K", "3K.rad2", True),
        ("Jy/beam", "3K", False),
        ("Jy/beam", "3K.rad2", True),
        ("Jy/beam", "3kg", False),
    ],
)
def test_convertflux_uses_brightness_area_and_units(casa, brightness, value, topeak):
    actual, expected = tools.image(), casa.image()
    for image in [actual, expected]:
        assert image.fromshape(shape=[3, 4, 2, 3])
        assert image.setbrightnessunit(brightness)
        assert image.setrestoringbeam("4arcsec", "2arcsec", "30deg")
    compare(
        actual.convertflux(value, "4arcsec", "2arcsec", type="ignored", topeak=topeak),
        expected.convertflux(value, "4arcsec", "2arcsec", type="ignored", topeak=topeak),
    )
    for image in [actual, expected]:
        assert image.done()


def test_convertflux_without_beam_and_with_degenerate_gaussian(casa):
    actual, expected = tools.image(), casa.image()
    for image in [actual, expected]:
        assert image.fromshape(shape=[3, 4, 2, 3])
        assert image.setbrightnessunit("Jy/beam")
    for major, minor in [("4arcsec", "2arcsec"), ("4arcsec", "0arcsec")]:
        compare(
            actual.convertflux("3mJy", major, minor),
            expected.convertflux("3mJy", major, minor),
        )
    for image in [actual, expected]:
        assert image.done()


def test_convertflux_rejects_reversed_pixel_axes_in_the_fallback_beam(casa):
    actual, expected = tools.image(), casa.image()
    for image in [actual, expected]:
        assert image.fromshape(shape=[3, 4])
        assert image.setbrightnessunit("Jy/beam")
    cs = expected.coordsys()
    coordinates = cs.torecord()
    assert cs.done()
    coordinates["direction0"]["cdelt"] = np.array([-1.0, 2.0])
    for image in [actual, expected]:
        assert image.setcoordsys(coordinates)
        with pytest.raises(RuntimeError, match="Major axis must be greater or equal"):
            image.convertflux("3Jy", "4arcsec", "2arcsec")
        assert image.done()


def test_convertflux_uses_selected_native_or_fits_plane(casa, tmp_path):
    native = str(tmp_path / "source.im")
    fits = str(tmp_path / "source.fits")
    writer = casa.image()
    assert writer.fromshape(native, [3, 4, 2, 3])
    assert writer.setbrightnessunit("Jy/beam")
    assert writer.setrestoringbeam("4arcsec", "2arcsec", "20deg", channel=0, polarization=0)
    assert writer.setrestoringbeam("8arcsec", "3arcsec", "30deg", channel=1, polarization=1)
    assert writer.tofits(fits)
    assert writer.done()
    for path in [native, fits]:
        actual, expected = tools.image(), casa.image()
        for image in [actual, expected]:
            assert image.open(path)
        args = ("5Jy", "4arcsec", {"unit": "arcsec", "value": 2})
        for channel, pol in [(0, 1), (1, 1)]:
            compare(
                actual.convertflux(*args, channel=channel, polarization=pol),
                expected.convertflux(*args, channel=channel, polarization=pol),
            )
        for image in [actual, expected]:
            with pytest.raises(RuntimeError, match="AlwaysAssert"):
                image.convertflux(*args)
            assert image.done()


def test_convertflux_validates_image_kind_and_gaussian_axes(casa):
    for module in [tools, casa]:
        image = module.image()
        assert image.fromshape(shape=[3, 4], type="d")
        with pytest.raises(RuntimeError, match="only supports Float"):
            image.convertflux("3Jy", "4arcsec", "2arcsec")
        assert image.fromshape(shape=[3, 4], linear=True)
        with pytest.raises(RuntimeError, match="required coordinate Direction"):
            image.convertflux("3Jy", "4arcsec", "2arcsec")
        assert image.fromshape(shape=[3, 4])
        for major, minor, error in [
            (4, 2, "Major axis must have angular units"),
            ("4m", "2m", "Major axis must have angular units"),
            ("4arcsec", "-2arcsec", "Minor axis cannot be less than zero"),
            ("2arcsec", "4arcsec", "Major axis must be greater or equal"),
        ]:
            with pytest.raises(RuntimeError, match=error):
                image.convertflux("3Jy", major, minor)
        with pytest.raises(RuntimeError, match="Cannot convert units of Flux integral"):
            image.convertflux("3kg", "4arcsec", "2arcsec")
        assert image.done()
