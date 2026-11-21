"""Public image continuum fits and native line/model publication."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize("channels,order,model", [
    ([-1], 0, [16.25] * 8),
    ([0, 1, 2, 4, 5, 6, 7], 1, [2., 5., 8., 11., 14., 17., 20., 23.]),
    ([4, 0, 1, 1, 2], 1, [2., 5., 8., 11., 14., 17., 20., 23.]),
    ([0], 3, [2.] * 8),
    ([9], 0, [0.] * 8),
])
def test_continuum_uses_requested_fit_channels(casa, tmp_path, channels, order, model):
    pixels = np.float32([2., 5., 8., 41., 14., 17., 20., 23.])
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=pixels)
        path = str(tmp_path / f"model-{api.__name__}.im")
        residual = source.continuumsub(outcont=path, channels=channels, fitorder=order, pol="Q")
        fitted = api.image()
        assert fitted.open(path)
        try:
            np.testing.assert_allclose(fitted.getchunk(), model, atol=5e-6)
            np.testing.assert_allclose(residual.getchunk(), pixels - model, atol=5e-6)
            assert residual.getchunk(getmask=True).all()
            assert fitted.getchunk(getmask=True).all()
            assert residual.name() == "Temporary_Image"
            assert residual.history()[-1] == "Number valid             = 1"
            np.testing.assert_array_equal(source.getchunk(), pixels)
        finally:
            fitted.done()
            residual.done()
            source.done()


def test_native_continuum_pair_mask_units_and_history(casa, tmp_path):
    base = np.float32([2., 5., 8., 41., 14., 17., 20., 23.])
    pixels = np.broadcast_to(base, (2, 2, 2, 8)).copy()
    mask = np.ones_like(pixels, dtype=bool)
    mask[..., 3] = False
    mask[0, 0, 0] = False
    expected = np.broadcast_to(np.float32([2., 5., 8., 11., 14., 17., 20., 23.]), pixels.shape).copy()
    expected[0, 0, 0] = base
    expected_residual = pixels - expected
    expected_residual[0, 0, 0] = base
    for writer in (casa, tools):
        source = writer.image()
        assert source.fromarray(pixels=pixels)
        assert source.putregion(pixelmask=mask)
        assert source.setbrightnessunit("K")
        assert source.setrestoringbeam(major="3arcsec", minor="2arcsec", pa="20deg")
        assert source.setmiscinfo({"author": "synthetic test"})
        model = str(tmp_path / f"model-{writer.__name__}.im")
        line = str(tmp_path / f"line-{writer.__name__}.im")
        result = source.continuumsub(line, model, fitorder=1)
        result.done()
        source.done()
        for reader in (casa, tools):
            for path, expected_data in ((line, expected_residual), (model, expected)):
                image = reader.image()
                assert image.open(path)
                try:
                    np.testing.assert_allclose(image.getchunk(), expected_data, atol=5e-6)
                    np.testing.assert_array_equal(image.getchunk(getmask=True), mask)
                    assert image.brightnessunit() == "K"
                    assert image.restoringbeam()["major"]["value"] == pytest.approx(3.)
                    assert image.miscinfo()["author"] == "synthetic test"
                    assert image.history()[-7] == "Ran ia.continuumsub"
                    assert image.history()[-5:] == [
                        "Number of profiles       = 8",
                        "Number of fits attempted = 7",
                        "Number succeeded         = 7",
                        "Number converged         = 7",
                        "Number valid             = 7",
                    ]
                finally:
                    image.done()


@pytest.mark.parametrize("channels,values,valid", [
    ([], [np.nan] * 4, [False] * 4),
    ([0, 2, 3], [0., np.nan, 0., 0.], [True] * 4),
])
def test_continuum_nonfinite_fit_samples(casa, channels, values, valid):
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=np.float32([1., np.nan, 3., 4.]))
        result = source.continuumsub(outcont="", channels=channels, fitorder=1)
        try:
            np.testing.assert_allclose(result.getchunk(), values, atol=5e-6, equal_nan=True)
            np.testing.assert_array_equal(result.getchunk(getmask=True), valid)
        finally:
            result.done()
            source.done()


def test_continuum_channels_are_relative_to_selected_origin(casa, tmp_path):
    source_pixels = np.float32([2., 5., 8., 41., 14., 17., 20., 23.])
    manager = casa.regionmanager()
    region = manager.box([2], [6])
    manager.done()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=source_pixels)
        model = str(tmp_path / f"selection-{api.__name__}.im")
        result = source.continuumsub(outcont=model, channels=[0, 1, 2], region=region, fitorder=1)
        coords = result.coordsys()
        original = source.coordsys()
        try:
            np.testing.assert_allclose(result.getchunk(), [0., 33., 6., 9., 12.], atol=5e-6)
            np.testing.assert_allclose(coords.toworld([0.])["numeric"], original.toworld([2.])["numeric"])
        finally:
            original.done()
            coords.done()
            result.done()
            source.done()


def test_continuum_preflights_both_destinations_before_overwrite(tmp_path):
    source = tools.image()
    assert source.fromarray(pixels=np.float32([2., 5., 8., 11., 14.]))
    line = tmp_path / "line.im"
    retained = tools.image()
    assert retained.fromarray(str(line), np.float32([5., 7.]))
    retained.done()
    blocked = tmp_path / "not-an-image"
    blocked.write_text("keep this file")
    before = set(tmp_path.iterdir())
    try:
        with pytest.raises(RuntimeError, match="separate destinations"):
            source.continuumsub(str(line), str(line), overwrite=True)
        with pytest.raises(RuntimeError, match="not a true table"):
            source.continuumsub(str(line), str(blocked), overwrite=True)
        with pytest.raises(RuntimeError, match="There are not enough points"):
            source.continuumsub(str(line), str(tmp_path / "other.im"), fitorder=4, overwrite=True)
        assert set(tmp_path.iterdir()) == before
        assert blocked.read_text() == "keep this file"
        assert retained.open(str(line))
        np.testing.assert_array_equal(retained.getchunk(), [5., 7.])
    finally:
        retained.done()
        source.done()
