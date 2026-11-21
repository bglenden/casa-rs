"""Pinned public Gaussian convolution, including its physical units and beam."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def pixel_gaussian(shape, center, major, minor, pa, sky):
    x, y = np.indices(shape, dtype=float)
    x -= center[0]
    y -= center[1]
    angle = np.deg2rad(pa)
    if sky:
        x, y = y, -x
    along = np.cos(angle) * x + np.sin(angle) * y
    across = -np.sin(angle) * x + np.cos(angle) * y
    model = np.exp(-4 * np.log(2) * ((along / major)**2 + (across / minor)**2))
    side = 2 * (int(5 * major / np.sqrt(8 * np.log(2)) + .5) + 1) + 1
    xp, yp = np.indices(shape)
    model[(abs(xp - center[0]) > side // 2) | (abs(yp - center[1]) > side // 2)] = 0
    return model


@pytest.mark.parametrize("linear,units,axes,angle,scale", [
    (False, "", [0, 1], 0, -1.),
    (False, "", [], 31, 1.5),
    (False, "", [1, 0], -45, -1.),
    (False, "Jy/pixel", [0, 1], 30, -1.),
    (False, "Jy/pixel", [1, 0], -30, 2.),
    (True, "", [0, 1], 45, -1.),
    (True, "", [1, 0], 30, 1.5),
])
def test_convolve2d_impulse_gaussian_and_units(casa, linear, units, axes, angle, scale):
    shape, center = (23, 25), (11, 12)
    signal = np.zeros(shape, dtype=np.float32)
    signal[center] = 2.
    gaussian = pixel_gaussian(shape, center, 4., 2., angle, not linear)
    norm = 1. if units else 1. / gaussian.sum()
    expected = 2. * (norm if scale <= 0 else scale) * gaussian
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=signal, linear=linear)
        if units:
            assert source.setbrightnessunit(units)
        result = source.convolve2d(axes=axes, major="4pix", minor="2pix", pa=f"{angle}deg", scale=scale)
        try:
            np.testing.assert_allclose(result.getchunk(), expected, rtol=2e-5, atol=4e-7)
            assert result.brightnessunit() == ("Jy/beam" if units else units)
            beam = result.restoringbeam()
            if units:
                assert beam["major"]["value"] == pytest.approx(4.)
                assert beam["minor"]["value"] == pytest.approx(2.)
                assert beam["positionangle"]["value"] == pytest.approx(angle)
            else:
                assert beam == {}
            assert result.getchunk(getmask=True).all()
            np.testing.assert_array_equal(source.getchunk(), signal)
        finally:
            result.done()
            source.done()


@pytest.mark.parametrize("kind", ["f", "d"])
def test_convolve2d_record_and_string_native_products(casa, kind, tmp_path):
    shape, center = (19, 21), (9, 10)
    signal = np.zeros(shape, dtype=float)
    signal[center] = 3.
    mask = np.ones(shape, dtype=bool)
    mask[8, 9] = False
    expected = 3. * pixel_gaussian(shape, center, 3., 2., 27., True)
    options = {
        "major": "",
        "minor": "",
        "pa": "",
        "beam": {
            "major": {"value": 180., "unit": "arcsec"},
            "minor": "120arcsec",
            "positionangle": {"value": 27., "unit": "deg"},
        },
    }
    for writer in (casa, tools):
        source = writer.image()
        assert source.fromarray(pixels=signal, type=kind)
        assert source.putregion(pixelmask=mask)
        path = str(tmp_path / f"{writer.__name__}-{kind}.im")
        output = source.convolve2d(path, scale=1., **options)
        output.done()
        source.done()
        for reader in (casa, tools):
            result = reader.image()
            assert result.open(path)
            try:
                np.testing.assert_allclose(result.getchunk(), expected, rtol=2e-5, atol=4e-7)
                np.testing.assert_array_equal(result.getchunk(getmask=True), mask)
                assert result.pixeltype() == ("float" if kind == "f" else "double")
                assert result.getchunk().dtype == np.float64
                assert any("Ran ia.convolve2d" == line for line in result.history())
            finally:
                result.done()


def test_convolve2d_requires_explicit_empty_widths_with_a_beam(casa):
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[11, 11])
        try:
            with pytest.raises(RuntimeError, match="may not be specified if beam is specified"):
                source.convolve2d(beam={"major": "3pix", "minor": "2pix", "pa": "0deg"})
        finally:
            source.done()
