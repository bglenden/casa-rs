"""Read CASA image region pixels and masks through the public tool."""

import os

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


def test_sparse_getchunk_reads_a_casa_image_with_nine_axes(casa, tmp_path):
    path = str(tmp_path / "nine-axes.im")
    pixels = np.arange(41, dtype=np.float32).reshape((41,) + (1,) * 8)
    source = casa.image()
    try:
        assert source.fromarray(path, pixels=pixels, linear=True)
    finally:
        source.done()

    for backend in (casa, tools):
        restored = backend.image()
        try:
            assert restored.open(path)
            result = restored.getchunk(inc=[20] + [1] * 8)
            np.testing.assert_array_equal(result, pixels[::20].astype(np.float64), strict=True)
        finally:
            restored.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_lazy_getregion_reads_and_locks_its_native_operand(casa, tmp_path):
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    for backend in (casa, tools):
        source = backend.image()
        path = str(tmp_path / f"{backend.__name__}.im")
        assert source.fromarray(path, pixels)
        assert source.done()
        result = source.imagecalc(pixels=f'"{path}" + 1')
        assert result.unlock()
        np.testing.assert_array_equal(result.haslock(), [False, False])
        np.testing.assert_array_equal(result.getregion(), pixels + 1)
        np.testing.assert_array_equal(result.haslock(), [True, False])
        assert result.done()


@pytest.mark.parametrize("kind", ["f", "d", "c", "cd"])
def test_getregion_whole_or_strided_typed_pixels(casa, kind):
    values = np.arange(5 * 4 * 2, dtype=np.float64).reshape(5, 4, 2)
    if kind in {"c", "cd"}:
        values = values + 1j * values[::-1]
    rust, oracle = tools.image(), casa.image()
    region = casa.regionmanager().box([1, 0, 0], [4, 3, 1], [2, 2, 1])
    for image in (rust, oracle):
        _assert_value(image.getregion(), np.empty(0, dtype=np.bool_))
        assert image.fromshape(shape=[5, 4, 2], type=kind)
        assert image.putchunk(values)
    _assert_value(rust.getregion(), values)
    _assert_value(rust.getregion(region=region), values[1:5:2, 0:4:2, :])
    for axes, drop in (([-1], False), ([0], False), ([1, 2], True)):
        for getmask in (False, True):
            actual = rust.getregion(region, axes, "", False, drop, getmask)
            if kind == "cd" and axes != [-1]:
                # Pinned CASA aborts in ImageCollapser on these DComplex axes.
                expected = values[1:5:2, 0:4:2, :].mean(axis=tuple(axes), keepdims=not drop)
                if getmask:
                    expected = np.ones(expected.shape, dtype=np.bool_)
            else:
                expected = oracle.getregion(region=region, axes=axes, dropdeg=drop, getmask=getmask)
            _assert_value(actual, expected)
    _assert_value(rust.getregion(region=[True], mask=[False], list=True), oracle.getregion())


def test_getregion_combines_expression_and_native_default_masks(casa, tmp_path):
    path = str(tmp_path / "masked.im")
    source = tools.image()
    values = np.arange(5 * 4 * 2, dtype=np.float64).reshape(5, 4, 2)
    assert source.fromarray(path, values)
    assert source.calcmask(f'"{path}" < 31', name="valid")
    assert source.done()
    rust, oracle = tools.image(), casa.image()
    assert rust.open(path) and oracle.open(path)
    assert rust.unlock() and oracle.unlock()
    region = casa.regionmanager().box([1, 0, 0], [4, 3, 1], [2, 2, 1])
    for axes in ([-1], [0], [0, 1]):
        for getmask in (False, True):
            arguments = dict(region=region, axes=axes, mask=f'"{path}" > 12', getmask=getmask)
            _assert_value(rust.getregion(**arguments), oracle.getregion(**arguments))
    _assert_value(rust.haslock(), oracle.haslock())
    expected = oracle.getregion(getmask=True, mask="F")
    _assert_value(rust.getregion(getmask=True, mask="F"), expected)
    _assert_value(expected, np.zeros(values.shape, dtype=np.bool_))
    assert rust.done() and oracle.done()


def test_getregion_stretches_singleton_mask_axis(casa, tmp_path):
    other = str(tmp_path / "match.im")
    source = tools.image()
    mask_values = np.arange(20, dtype=np.float64).reshape(5, 4, 1)
    assert source.fromarray(other, mask_values)
    assert source.done()
    region = casa.regionmanager().box([1, 0, 0], [4, 3, 1], [2, 2, 1])
    expression = f'"{other}" > 7.5'
    for make in (tools.image, casa.image):
        tool = make()
        assert tool.fromshape(shape=[5, 4, 2])
        with pytest.raises(RuntimeError):
            tool.getregion(region, mask=expression, getmask=True)
        actual = tool.getregion(region, mask=expression, getmask=True, stretch=True)
        expected = np.broadcast_to(mask_values[1:5:2, 0:4:2, :] > 7.5, (2, 2, 2))
        _assert_value(actual, expected)


@pytest.mark.parametrize(
    ("source_shape", "target_shape", "linear", "stretch", "expected_error"),
    [
        ((4, 4), (4, 4), True, False, "a region axis is unknown"),
        ((4, 4), (4, 4), True, True, "a region axis is unknown"),
        ((4, 4, 1), (4, 4), False, True, "Unable to extend mask"),
        ((4, 4), (4, 4, 2), False, False, "mask should not be extended"),
        ((4, 4), (4, 4, 2), False, True, None),
        ((4, 4, 1), (4, 4, 2, 3), False, True, None),
    ],
)
def test_getregion_requires_matching_mask_axes(
    casa, tmp_path, source_shape, target_shape, linear, stretch, expected_error
):
    path = str(tmp_path / "mask.im")
    source = tools.image()
    values = np.arange(np.prod(source_shape), dtype=np.float64).reshape(source_shape)
    assert source.fromarray(path, values, linear=linear)
    assert source.done()
    for make in (tools.image, casa.image):
        image = make()
        assert image.fromshape(shape=list(target_shape))
        kwargs = dict(mask=f'"{path}" > 3', getmask=True, stretch=stretch)
        if expected_error:
            with pytest.raises(RuntimeError, match=expected_error):
                image.getregion(**kwargs)
        else:
            expanded_shape = (4, 4) + (1,) * (len(target_shape) - 2)
            expected = np.broadcast_to((values > 3).reshape(expanded_shape), target_shape)
            _assert_value(image.getregion(**kwargs), expected)
        assert image.done()


@pytest.mark.parametrize("getmask", [False, True])
def test_getregion_rejects_fully_masked_collapse(casa, getmask):
    for make in (tools.image, casa.image):
        image = make()
        assert image.fromshape(shape=[4, 3, 2])
        for axes in ([0], [0, 1, 2]):
            with pytest.raises(RuntimeError, match="All selected pixels are masked"):
                image.getregion(axes=axes, getmask=getmask, mask="F")
        _assert_value(
            image.getregion(axes=[0, 1, 2], getmask=True),
            np.ones((1, 1, 1), dtype=np.bool_),
        )
        assert image.done()


def test_getregion_refuses_scalar_mask_and_region():
    image = tools.image()
    assert image.fromshape(shape=[3, 3])
    with pytest.raises(RuntimeError, match="Unsupported type for region bool"):
        image.getregion(region=False)
    with pytest.raises(RuntimeError, match="Mask is not understood"):
        image.getregion(mask=True)
    with pytest.raises(RuntimeError, match="invalid defined axes"):
        image.getregion(axes=[99])


def test_getregion_collapses_linear_pixels_along_only_the_requested_axes():
    image = tools.image()
    values = np.arange(40, dtype=np.float64).reshape(5, 4, 2)
    assert image.fromarray(pixels=values, linear=True)
    _assert_value(image.getregion(axes=[0]), values.mean(axis=0, keepdims=True))
