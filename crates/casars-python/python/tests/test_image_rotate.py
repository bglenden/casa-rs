"""Pinned CASA parity and native image interop for rotation."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _ramp():
    x, y = np.ogrid[:24, :26]
    return (x + 2*y).astype(np.float32)


def _plane_rotation(angle):
    cosine, sine = np.cos(angle), np.sin(angle)
    return np.array([[cosine, -sine], [sine, cosine]])


@pytest.mark.parametrize("linear", [False, True])
@pytest.mark.parametrize("argument,angle", [
    ("35deg", np.deg2rad(35)),
    ({"value": 35, "unit": "deg"}, np.deg2rad(35)),
    (.25, .25),
    ({"value": 1., "unit": "m"}, 1.),
])
def test_rotate_supported_angle_forms_and_analytic_sampling(casa, linear, argument, angle):
    ideal = _ramp()
    products = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=list(ideal.shape), linear=linear)
            assert source.putchunk(ideal)
            coords = source.coordsys()
            ref = coords.referencepixel()["numeric"]
            coords.done()
            product = source.rotate(pa=argument, method="linear", decimate=1)
            try:
                coords = product.coordsys()
                rotation = _plane_rotation(angle)
                np.testing.assert_allclose(
                    coords.lineartransform(type="linear" if linear else "direction"), rotation,
                    atol=2e-14,
                )
                np.testing.assert_array_equal(coords.referencepixel()["numeric"], ref)
                world_sample = np.array([10., 12.])
                px, py = ref + rotation.T @ (world_sample - ref)
                values, valid = product.getchunk(), product.getchunk(getmask=True)
                np.testing.assert_allclose(values[10, 12], px + 2*py, rtol=2e-6)
                products.append((values, valid))
                coords.done()
            finally:
                product.done()
            np.testing.assert_array_equal(source.getchunk(), ideal)
        finally:
            source.done()
    assert products[0][0].shape == products[1][0].shape == ideal.shape
    np.testing.assert_array_equal(products[1][1], products[0][1])
    np.testing.assert_allclose(products[1][0], products[0][0], rtol=2e-5, atol=2e-5)


def test_rotate_shape_defaults_mask_and_native_writer_readers(casa, tmp_path):
    expected = _ramp()[..., None, None] + 10*np.arange(7, dtype=np.float32)
    selected = np.ones(expected.shape, dtype=bool)
    selected[2:4, 8:11, :, :] = False
    products = {}
    for writer in (casa, tools):
        source = writer.image()
        path = str(tmp_path / f"{writer.__name__}-rotation.im")
        try:
            assert source.fromarray(pixels=expected)
            assert source.putregion(pixelmask=selected)
            assert source.setbrightnessunit("K")
            assert source.sethistory(origin="test", history=["rotation input"])
            default = source.rotate()
            try:
                np.testing.assert_array_equal(default.getchunk(), expected)
                np.testing.assert_array_equal(default.getchunk(getmask=True), selected)
            finally:
                default.done()
            saved = source.rotate(path, pa="35deg", replicate=True, dropdeg=True)
            try:
                assert saved.ispersistent()
            finally:
                saved.done()
        finally:
            source.done()
        for reader in (casa, tools):
            image = reader.image()
            try:
                assert image.open(path)
                assert any("rotation input" in line for line in image.history())
                assert image.brightnessunit() == "K"
                coords = image.coordsys()
                try:
                    np.testing.assert_allclose(coords.lineartransform(type="direction"),
                                               _plane_rotation(np.deg2rad(35)), atol=2e-14)
                finally:
                    coords.done()
                products[writer.__name__, reader.__name__] = image.getchunk(), image.getchunk(getmask=True)
            finally:
                image.done()
    for pixels, valid in products.values():
        np.testing.assert_array_equal(valid, selected[:, :, 0, :])
        np.testing.assert_array_equal(pixels[valid], expected[:, :, 0, :][valid])


def test_rotate_refusals_preserve_existing_named_image(casa, tmp_path):
    data = _ramp()
    for api in (casa, tools):
        source = api.image()
        prior = api.image()
        path = str(tmp_path / f"{api.__name__}-before.im")
        try:
            assert source.fromarray(pixels=data)
            assert prior.fromarray(path, pixels=data+1)
            prior.done()
            for arguments in (
                {"pa": [1., 2.]}, {"pa": "something invalid"},
                {"pa": "35deg", "decimate": 9}, {"pa": "35deg", "method": "sinc"},
            ):
                with pytest.raises(RuntimeError):
                    source.rotate(path, overwrite=True, **arguments)
                assert source.isopen()
            assert prior.open(path)
            np.testing.assert_array_equal(prior.getchunk(), data+1)
        finally:
            prior.done()
            source.done()


def test_rotate_right_angle_keeps_the_actual_sky_overlap():
    source = tools.image()
    data = _ramp()
    x, y = np.ogrid[:24, :26]
    original_x, original_y = y - 1, 25 - x
    valid = np.broadcast_to((original_x >= 0) & (original_x < 24), data.shape)
    expected = np.where(valid, original_x + 2*original_y, 0)
    try:
        assert source.fromarray(pixels=data)
        result = source.rotate(pa="90deg", method="nearest")
        try:
            np.testing.assert_array_equal(result.getchunk(getmask=True), valid)
            np.testing.assert_array_equal(result.getchunk(), expected)
        finally:
            result.done()
    finally:
        source.done()
