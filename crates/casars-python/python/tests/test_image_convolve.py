"""Pinned CASA and independent finite-domain image convolution references."""

from itertools import product
import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def centered_convolution(pixels, kernel, scale=-1, valid=None):
    if valid is not None:
        pixels = np.where(valid, pixels, 0)
    kernel = np.asarray(kernel, dtype=pixels.dtype)
    weights = kernel / np.sum(kernel) if scale <= 0 else kernel * scale
    weights = weights.reshape(weights.shape + (1,) * (pixels.ndim - kernel.ndim))
    result = np.zeros(pixels.shape, dtype=np.float64)
    for index in product(*(range(length) for length in weights.shape)):
        shift = [i - width // 2 for i, width in zip(index, weights.shape)]
        if any(abs(delta) >= length for delta, length in zip(shift, pixels.shape)):
            continue
        source = tuple(slice(max(-delta, 0), n - max(delta, 0))
                       for delta, n in zip(shift, pixels.shape))
        destination = tuple(slice(max(delta, 0), n - max(-delta, 0))
                            for delta, n in zip(shift, pixels.shape))
        result[destination] += weights[index] * pixels[source]
    return result


@pytest.mark.parametrize("shape,kernel_shape,scale", [
    ((5,), (3,), -1.),
    ((5,), (4,), 1.),
    ((5,), (13,), 0.),
    ((4, 7), (4,), 1.5),
    ((1, 7), (4, 6), -1.),
    ((4, 1), (1, 7), 1.),
    ((3, 4, 5), (2, 3, 2), -1.),
    ((37, 41, 7), (11, 20), 1.),
    ((37, 41, 7), (13, 9, 3), -1.),
])
def test_convolve_centered_finite_support(casa, shape, kernel_shape, scale):
    rng = np.random.default_rng(12)
    values = rng.normal(10, 2, shape).astype(np.float32)
    kernel = rng.uniform(-1, 5, kernel_shape).astype(np.float32)
    valid = rng.random(shape) > .17
    expected = centered_convolution(values, kernel, scale=scale, valid=valid)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        assert source.putregion(pixelmask=valid)
        result = source.convolve(kernel=kernel, scale=scale)
        try:
            assert result.pixeltype() == "float"
            assert result.getchunk().dtype == np.float64
            np.testing.assert_allclose(result.getchunk(), expected, rtol=2e-6, atol=4e-4)
            np.testing.assert_array_equal(result.getchunk(getmask=True), valid)
            np.testing.assert_array_equal(source.getchunk(), values)
        finally:
            result.done()
            source.done()


@pytest.mark.parametrize("kind,dtype", [("f", np.float32), ("d", np.float64)])
def test_convolve_native_kernel_and_products(casa, tmp_path, kind, dtype):
    a = np.arange(1, 1 + 6 * 3 * 5, dtype=dtype).reshape(6, 3, 5)
    weights = np.array([1., 2., 4., -1.], dtype=dtype)
    validity = (a != 24) & (a != 46)
    manager = casa.regionmanager()
    region = manager.box([1, 0, 0], [5, 2, 4], [2, 1, 2])
    manager.done()
    selection = np.s_[1:6:2, :, ::2]
    expected = centered_convolution(a[selection], weights, valid=validity[selection])
    for writer in (casa, tools):
        source = writer.image()
        kernel = writer.image()
        kernel_path = str(tmp_path / f"{writer.__name__}-{kind}-kernel.im")
        product_path = str(tmp_path / f"{writer.__name__}-{kind}-convolved.im")
        assert source.fromarray(pixels=a, type=kind)
        assert source.putregion(pixelmask=validity)
        if kind == "f":
            assert source.setbrightnessunit("Jy/beam")
            assert source.setmiscinfo({"label": "synthetic"})
            assert source.setrestoringbeam(major="3arcsec", minor="2arcsec", pa="20deg")
            assert source.sethistory(origin="analytic", history=["original provenance"])
        assert kernel.fromarray(kernel_path, weights, type=kind)
        assert kernel.putregion(pixelmask=np.array([True, False, True, False]))
        kernel.done()
        output = source.convolve(product_path, kernel_path, region=region)
        if kind == "f":
            coords = source.coordsys()
            expected_world = coords.toworld([3., 1., 2.])["numeric"]
            coords.done()
        output.done()
        source.done()
        for reader in (casa, tools):
            restored = reader.image()
            assert restored.open(product_path)
            try:
                assert restored.pixeltype() == ("float" if kind == "f" else "double")
                assert restored.getchunk().dtype == np.float64
                tol = 2e-5 if kind == "f" else 1e-12
                np.testing.assert_allclose(restored.getchunk(), expected, rtol=tol, atol=tol)
                np.testing.assert_array_equal(restored.getchunk(getmask=True), validity[selection])
                if kind == "f":
                    coords = restored.coordsys()
                    try:
                        np.testing.assert_allclose(coords.toworld([1., 1., 1.])["numeric"], expected_world)
                    finally:
                        coords.done()
                    assert restored.restoringbeam() == {}
                    assert restored.brightnessunit() == "Jy/beam"
                    assert restored.miscinfo()["label"] == "synthetic"
                    assert restored.history()[-4] == "original provenance"
                    assert restored.history()[-3] == "Ran ia.convolve"
                    assert "Degenerate axes added to the kernel" in restored.history()[-1]
            finally:
                restored.done()
