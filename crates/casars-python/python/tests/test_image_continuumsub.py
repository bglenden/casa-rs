"""Public image continuum fits and native line/model publication."""

import os
import subprocess
import sys

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
    # Isolate the pinned profile fitter after an invalid fit since its
    # native cleanup can abort when earlier fits ran in the same process.
    subprocess.run(
        [sys.executable, "-c", """
import sys
import numpy as np
from casatools import image
channels = [int(value) for value in sys.argv[1:]]
source = image()
assert source.fromarray(pixels=np.float32([1., np.nan, 3., 4.]))
result = source.continuumsub(outcont="", channels=channels, fitorder=1)
if channels:
    np.testing.assert_allclose(result.getchunk(), [0., np.nan, 0., 0.], atol=5e-6)
    assert result.getchunk(getmask=True).all()
else:
    assert np.isnan(result.getchunk()).all()
    assert not result.getchunk(getmask=True).any()
result.done()
source.done()
""", *map(str, channels)],
        env=dict(os.environ, OMP_NUM_THREADS="1"), check=True, capture_output=True,
    )
    source = tools.image()
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
        with pytest.raises(RuntimeError, match="not a true table"):
            source.continuumsub(str(blocked), str(line), overwrite=True)
        with pytest.raises(RuntimeError, match="There are not enough points"):
            source.continuumsub(str(line), str(tmp_path / "other.im"), fitorder=4, overwrite=True)
        assert set(tmp_path.iterdir()) == before
        assert blocked.read_text() == "keep this file"
        assert retained.open(str(line))
        np.testing.assert_array_equal(retained.getchunk(), [5., 7.])
    finally:
        retained.done()
        source.done()

def test_continuum_fits_each_quadratic_profile_independently(casa, tmp_path):
    x = np.arange(9, dtype=np.float32)
    offsets = np.arange(6, dtype=np.float32).reshape(2, 3, 1)
    model = (offsets + 1.) + (offsets - 2.) * x + .5 * x**2
    signal = np.zeros_like(model)
    signal[0, 2, 3] = 21.
    signal[1, 1, 7] = 37.
    observed = model + signal
    for api in (casa, tools):
        original = api.image()
        assert original.fromarray(pixels=observed)
        dst = str(tmp_path / f"quad-{api.__name__}.im")
        result = original.continuumsub(outcont=dst, channels=[0, 1, 2, 4, 5, 6, 8], fitorder=2)
        fitted = api.image()
        assert fitted.open(dst)
        try:
            np.testing.assert_allclose(fitted.getchunk(), model, atol=5e-6)
            np.testing.assert_allclose(result.getchunk(), signal, atol=5e-6)
        finally:
            fitted.done()
            result.done()
            original.done()


@pytest.mark.parametrize("channels,order,expected", [
    ([1], 1, [2., 4., 6., 8., 10., 12.]),
    ([1], 3, [1., 4., 15., 40., 85., 156.]),
    ([1, 2], 2, [2.6285714, 4., 6., 8.6285714, 11.8857143, 15.7714286]),
])
def test_continuum_underdetermined_fit_keeps_casa_scaling(casa, tmp_path, channels, order, expected):
    pixels = np.float32([2., 4., 6., 8., 10., 12.])
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=pixels)
        dst = str(tmp_path / f"under-{api.__name__}.im")
        result = source.continuumsub(outcont=dst, channels=channels, fitorder=order)
        model = api.image()
        assert model.open(dst)
        try:
            np.testing.assert_allclose(model.getchunk(), expected, atol=3e-6, rtol=2e-6)
            np.testing.assert_allclose(result.getchunk(), pixels - expected, atol=5e-6, rtol=2e-6)
        finally:
            model.done()
            result.done()
            source.done()


def test_continuum_full_rank_fit_uses_the_selected_abscissa(casa, tmp_path):
    x = np.arange(8192, dtype=np.float64)
    values = np.float32(x**4)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        dst = str(tmp_path / f"narrow-{api.__name__}.im")
        result = source.continuumsub(outcont=dst, channels=[0, 1, 2, 3, 4], fitorder=4)
        model = api.image()
        assert model.open(dst)
        try:
            np.testing.assert_allclose(model.getchunk()[[0, 1, 2, 3, 4, 100, 8191]],
                                       values[[0, 1, 2, 3, 4, 100, 8191]], atol=5e-5, rtol=2e-5)
            np.testing.assert_allclose(result.getchunk()[:5], 0., atol=5e-5)
        finally:
            model.done()
            result.done()
            source.done()


def test_high_order_continuum_with_one_channel_has_no_dense_matrix():
    source = tools.image()
    pixels = np.linspace(2., 8., 32769, dtype=np.float32)
    assert source.fromarray(pixels=pixels)
    product = source.continuumsub(outcont="", channels=[0], fitorder=32767)
    try:
        np.testing.assert_allclose(product.getchunk(), pixels - 2., atol=5e-7)
    finally:
        product.done()
        source.done()


def test_continuum_rejects_nested_output_entries_even_through_symlinks(tmp_path):
    source = tools.image()
    assert source.fromarray(pixels=np.float32([1., 2., 3., 4.]))
    outer = tmp_path / "line.im"
    other = tmp_path / "model.im"
    holding = tools.image()
    for path, values in ((outer, [4., 9.]), (other, [8., 10.])):
        assert holding.fromarray(str(path), np.float32(values))
        holding.done()
    child = outer / "child"
    child.symlink_to(other, target_is_directory=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    linked_dir = outer / "linked-dir"
    linked_dir.symlink_to(outside, target_is_directory=True)
    try:
        for companion in (child, linked_dir / "nested.im"):
            with pytest.raises(RuntimeError, match="separate destinations"):
                source.continuumsub(str(outer), str(companion), overwrite=True)
        assert child.is_symlink() and linked_dir.is_symlink()
        assert list(outside.iterdir()) == []
        for path, values in ((outer, [4., 9.]), (other, [8., 10.])):
            assert holding.open(str(path))
            np.testing.assert_array_equal(holding.getchunk(), values)
            holding.done()
    finally:
        source.done()
        holding.done()
