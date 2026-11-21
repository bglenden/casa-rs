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
            for channel in range(5):
                assert source.setrestoringbeam(
                    major=f"{3+channel}arcsec", minor="2arcsec", pa="20deg", channel=channel,
                )
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


def test_convolve_contiguous_and_strided_kernel_arguments(casa):
    values = np.arange(1, 13, dtype=np.float32).reshape(4, 3)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            for kernel in (
                [[1, 2], [3, 4]],
                np.arange(1, 5, dtype=np.int64)[::-1],
                np.ones((3, 1), dtype=np.float32, order="F"),
            ):
                output = source.convolve(kernel=kernel, scale=1.)
                try:
                    np.testing.assert_allclose(
                        output.getchunk(), centered_convolution(values, kernel, scale=1.),
                        rtol=2e-6, atol=2e-5,
                    )
                finally:
                    output.done()
        finally:
            source.done()


def test_convolve_stretches_a_mask_and_keeps_the_source_unchanged(casa, tmp_path):
    original = np.arange(1, 73, dtype=np.float32).reshape(4, 3, 6)
    original_mask = original != 27
    spatial = np.ones_like(original[..., 0])
    spatial[1, 1] = 0
    spatial[3, 0] = 0
    valid = original_mask & (spatial[..., None] != 0)
    for api in (casa, tools):
        source = api.image()
        mask = api.image()
        path = str(tmp_path / f"{api.__name__}-stretch.im")
        assert mask.fromarray(path, spatial)
        mask.done()
        assert source.fromarray(pixels=original)
        assert source.putregion(pixelmask=original_mask)
        expression = f"'{path}' > 0"
        try:
            with pytest.raises(RuntimeError, match="shape.*different"):
                source.convolve(kernel=[1, 2, 4], mask=expression)
            output = source.convolve(kernel=[1, 2, 4], mask=expression, stretch=True)
            try:
                np.testing.assert_allclose(
                    output.getchunk(), centered_convolution(original, [1, 2, 4], valid=valid),
                    rtol=2e-6, atol=2e-5,
                )
                np.testing.assert_array_equal(output.getchunk(getmask=True), valid)
                assert output.maskhandler("get") == ["mask0"]
                np.testing.assert_array_equal(source.getchunk(), original)
                np.testing.assert_array_equal(source.getchunk(getmask=True), original_mask)
            finally:
                output.done()
        finally:
            source.done()


@pytest.mark.parametrize("kernel,pattern", [
    ([], "kernel is not understood"),
    (np.array([], dtype=np.float64), "Kernel array cannot be empty"),
    (np.array([1, 2], dtype=np.float16), "kernel is not understood"),
    (np.array([[1, 2]]), "more axes than the image"),
    (True, "kernel is not understood"),
    (1, "kernel is not understood"),
])
def test_convolve_kernel_diagnostics(casa, kernel, pattern):
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=np.float32([1., 2., 3., 4.]))
        try:
            with pytest.raises(RuntimeError, match=pattern):
                source.convolve(kernel=kernel)
        finally:
            source.done()


@pytest.mark.parametrize("shape,kernel,expect_nan", [
    ((5,), [1, -1], True),
    ((5, 1), [1, 0, -1], True),
    ((1, 4), [1, -1], False),
])
def test_convolve_zero_sum_normalization(casa, shape, kernel, expect_nan):
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=np.ones(shape, np.float32))
        output = source.convolve(kernel=kernel)
        try:
            predicate = np.isnan if expect_nan else np.isneginf
            assert predicate(output.getchunk()).all()
            assert output.getchunk(getmask=True).all()
        finally:
            output.done()
            source.done()


def test_convolve_cannot_replace_an_input_or_a_foreign_file(tmp_path):
    source_path = str(tmp_path / "source.im")
    kernel_path = str(tmp_path / "kernel.im")
    foreign = tmp_path / "foreign"
    foreign.write_text("preserve me")
    source = tools.image()
    kernel = tools.image()
    assert source.fromarray(source_path, np.float32([1., 2., 3.]))
    assert kernel.fromarray(kernel_path, np.float32([3., 1.]))
    kernel.done()
    try:
        for target in (source_path, kernel_path):
            with pytest.raises(RuntimeError):
                source.convolve(target, kernel_path, overwrite=True)
        with pytest.raises(RuntimeError, match="not a true table"):
            source.convolve(str(foreign), [1, 2], overwrite=True)
        np.testing.assert_array_equal(source.getchunk(), [1., 2., 3.])
        assert kernel.open(kernel_path)
        np.testing.assert_array_equal(kernel.getchunk(), [3., 1.])
        assert foreign.read_text() == "preserve me"
        assert {path.name for path in tmp_path.iterdir()} == {"source.im", "kernel.im", "foreign"}
        result = source.convolve(kernel=source_path, scale=1.)
        try:
            np.testing.assert_array_equal(result.getchunk(), [4., 10., 12.])
        finally:
            result.done()
    finally:
        kernel.done()
        source.done()


@pytest.mark.parametrize("length,width", [(13, 3), (262147, 63)])
def test_convolve_valid_nan_spans_only_its_active_plane(casa, length, width):
    values = np.ones((length, 3), dtype=np.float32)
    values[length - 3, 0] = np.nan
    values[length // 2, 1] = np.nan
    valid = np.ones_like(values, dtype=bool)
    valid[length // 2, 1] = False
    weights = np.ones(width, dtype=np.float32) / width
    clean = np.where(valid, values, 0.)
    expected = np.stack([
        np.convolve(clean[:, lane], weights, mode="same") for lane in (1, 2)
    ], axis=1)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        assert source.putregion(pixelmask=valid)
        product = source.convolve(kernel=weights, scale=1.)
        try:
            actual = product.getchunk()
            assert np.isnan(actual[:, 0]).all()
            np.testing.assert_allclose(actual[:, 1:], expected, rtol=4e-6, atol=4e-7)
            np.testing.assert_array_equal(product.getchunk(getmask=True), valid)
        finally:
            product.done()
            source.done()


def test_convolve_kernel_and_source_tile_seams(casa):
    length, width = 550001, 32769
    impulse = 262143
    signal = np.zeros(length)
    signal[impulse] = 3.
    weights = np.linspace(-2., 7., width)
    start = impulse - width // 2
    stop = start + width
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=signal, type="d")
        result = source.convolve(kernel=weights, scale=1.)
        try:
            actual = result.getchunk()
            np.testing.assert_allclose(actual[start:stop], 3. * weights, rtol=1e-11, atol=1e-11)
            np.testing.assert_allclose(actual[:start], 0., atol=1e-11)
            np.testing.assert_allclose(actual[stop:], 0., atol=1e-11)
        finally:
            result.done()
            source.done()


def test_convolve_valid_infinity_has_deterministic_nan_output_on_an_active_axis():
    source = tools.image()
    values = np.array([[1., 1.], [1., 1.], [np.inf, 2.], [1., 1.]], dtype=np.float32)
    assert source.fromarray(pixels=values)
    result = source.convolve(kernel=[1., 0., 0.], scale=1.)
    try:
        assert np.isnan(result.getchunk()[:, 0]).all()
        np.testing.assert_array_equal(result.getchunk()[:, 1], [1., 2., 1., 0.])
        assert result.getchunk(getmask=True).all()
    finally:
        result.done()
        source.done()
