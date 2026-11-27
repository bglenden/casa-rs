"""Public histogram reductions against CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _data():
    return np.array([[-4., 1., 2.5], [6.125, -2.5, .5], [1., 2., 7.],
                     [3., -2., -1.], [4., 5., 6.]], dtype=np.float64)


def _profile(api, pixels, mask=None, *, typecode="f", **options):
    im = api.image()
    try:
        assert im.histograms() == {}
        assert im.fromarray(pixels=pixels, type=typecode)
        if mask is not None:
            assert im.putregion(pixelmask=mask)
        return im.histograms(**options)
    finally:
        im.done()


def _compare(actual, reference):
    assert actual.keys() == reference.keys() == {"values", "counts", "mean", "sigma"}
    for key in actual:
        assert actual[key].dtype == reference[key].dtype == np.float64
        assert actual[key].shape == reference[key].shape
        np.testing.assert_allclose(actual[key], reference[key], rtol=2e-7, atol=2e-7)


@pytest.mark.parametrize("typecode", ["f", "d"])
@pytest.mark.parametrize("axes,bins", [([-1], 4), (0, 4), ([1], 1), ([0, 1], 9)])
def test_histogram_axes_and_sample_statistics(casa, typecode, axes, bins):
    pixels = _data()
    native = _profile(tools, pixels, typecode=typecode, axes=axes, nbins=bins)
    pinned = _profile(casa, pixels, typecode=typecode, axes=axes, nbins=bins)
    _compare(native, pinned)
    reduced = None if axes in ([-1], [0, 1]) else axes if isinstance(axes, int) else axes[0]
    np.testing.assert_allclose(native["mean"], pixels.mean(axis=reduced), atol=2e-7)
    np.testing.assert_allclose(native["sigma"], pixels.std(axis=reduced, ddof=1), atol=2e-7)
    if reduced is None:
        np.testing.assert_array_equal(native["counts"].sum(), pixels.size)
    else:
        np.testing.assert_array_equal(native["counts"].sum(axis=0), pixels.shape[reduced])


@pytest.mark.parametrize("range_args", [[], [-1.], [-3.1], [2.4, -.3], [-11., 18.]])
@pytest.mark.parametrize("cumu,log", [(False, False), (True, False), (False, True), (True, True)])
def test_inclusion_trimming_and_count_forms(casa, range_args, cumu, log):
    pixels = _data()
    mask = np.ones(pixels.shape, dtype=bool)
    mask[3, 2] = False
    arguments = dict(nbins=4, includepix=range_args, cumu=cumu, log=log)
    actual = _profile(tools, pixels, mask, **arguments)
    reference = _profile(casa, pixels, mask, **arguments)
    _compare(actual, reference)
    data = pixels[mask]
    if range_args not in ([], [-1.]):
        limits = (-abs(range_args[0]), abs(range_args[0])) if len(range_args) == 1 else sorted(range_args)
        data = data[(data >= limits[0]) & (data <= limits[1])]
    else:
        limits = [data.min(), data.max()]
    compare = np.histogram(data, bins=4, range=tuple(limits))[0].astype(float)
    if cumu:
        compare = np.cumsum(compare)
    if log:
        compare = np.log10(np.maximum(compare, 1.))
    np.testing.assert_allclose(actual["counts"], compare, atol=1e-7)


@pytest.mark.parametrize("typecode", ["f", "d"])
def test_constant_and_one_sample_bins(casa, typecode):
    pixels = np.broadcast_to(np.array([2., 300., 4.]), (5, 3)).copy()
    mask = np.ones(pixels.shape, dtype=bool)
    mask[1:, 1] = False
    args = dict(typecode=typecode, axes=0, nbins=4)
    out = _profile(tools, pixels, mask, **args)
    _compare(out, _profile(casa, pixels, mask, **args))
    np.testing.assert_array_equal(out["counts"][0], [5., 1., 5.])
    assert not out["counts"][1:].any()
    np.testing.assert_array_equal(out["sigma"], 0.)


def test_native_image_masks_and_region_writer_reader_matrix(casa, tmp_path):
    pixels = _data()
    region_manager = casa.regionmanager()
    region = region_manager.box([0, 0], [4, 2], [2, 1])
    region_manager.done()
    source_data = pixels[::2]
    for writer in (casa, tools):
        prefix = tmp_path / f"{writer.__name__}.im"
        selected = writer.image()
        assert selected.fromarray(str(prefix), pixels)
        assert selected.done()
        for reader in (casa, tools):
            image = reader.image()
            assert image.open(str(prefix))
            try:
                result = image.histograms(
                    0, region, f'"{prefix}">-3', 4,
                )
                expected = source_data.copy()
                expected[expected < -3] = np.nan
                np.testing.assert_allclose(result["mean"], np.nanmean(expected, axis=0), atol=2e-7)
                np.testing.assert_array_equal(result["counts"].sum(axis=0), np.isfinite(expected).sum(axis=0))
            finally:
                image.done()


@pytest.mark.parametrize("arguments,reason", [
    ({"nbins": 0}, "number of bins"),
    ({"axes": [2]}, "cursor axes"),
    ({"axes": [-1, 0]}, "nonnegative"),
    ({"axes": []}, "nonnegative"),
    ({"includepix": [1., 2., 3.]}, "inclusion range"),
    ({"includepix": [42., 99.]}, "Failed to compute statistics"),
])
def test_invalid_histogram_preserves_image(casa, arguments, reason):
    pixels = _data()
    for api in (casa, tools):
        image = api.image()
        assert image.fromarray(pixels=pixels)
        try:
            with pytest.raises(RuntimeError, match=reason):
                image.histograms(**arguments)
            np.testing.assert_array_equal(image.getchunk(), pixels)
        finally:
            image.done()


def test_missing_pixels_in_a_slice_refuse_histograms(casa):
    data = _data()
    mask = np.ones(data.shape, dtype=bool)
    mask[:, 1] = False
    for api in (casa, tools):
        with pytest.raises(RuntimeError, match="Failed to compute statistics"):
            _profile(api, data, mask, axes=0)


def test_duplicate_axes_are_refused_without_losing_the_attachment():
    image = tools.image()
    pixels = _data()
    assert image.fromarray(pixels=pixels)
    try:
        with pytest.raises(RuntimeError, match="distinct and increasing"):
            image.histograms(axes=[1, 1])
        np.testing.assert_array_equal(image.getchunk(), pixels)
    finally:
        image.done()


def test_expression_mask_stretch_uses_original_group_geometry(casa, tmp_path):
    data = np.arange(5 * 3 * 4, dtype=np.float32).reshape(5, 3, 1, 4)
    eligible = np.arange(15).reshape(5, 3, 1, 1) % 3 != 0
    mask_path = tmp_path / "sky.im"
    mask = casa.image()
    assert mask.fromarray(str(mask_path), eligible.astype(np.float32))
    assert mask.done()
    expression = f'"{mask_path}">0'
    outputs = []
    for api in (casa, tools):
        image = api.image()
        assert image.fromarray(pixels=data)
        try:
            with pytest.raises(RuntimeError):
                image.histograms(axes=[0, 1, 2], mask=expression)
            outputs.append(image.histograms(axes=[0, 1, 2], mask=expression, nbins=4, stretch=True))
        finally:
            image.done()
    native, pinned = outputs[::-1]
    _compare(native, pinned)
    np.testing.assert_allclose(native["mean"], data[eligible[..., 0]].mean(axis=0))
    np.testing.assert_array_equal(native["counts"].sum(axis=0), eligible.sum())


def test_complex_attachment_is_preserved_after_histogram_refusal(casa):
    data = (_data() + 1j * _data() / 2).astype(np.complex64)
    for api in (casa, tools):
        image = api.image()
        assert image.fromarray(pixels=data)
        try:
            with pytest.raises(RuntimeError, match="only supports real-valued images"):
                image.histograms()
            assert image.pixeltype() == "complex"
            np.testing.assert_array_equal(image.getchunk(), data)
        finally:
            image.done()
