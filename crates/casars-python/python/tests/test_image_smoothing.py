"""Public boxcar and Hanning kernels, validity, and source grids."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def smooth_expected(data, mask, name, width, decimate):
    clean = np.where(mask, data, 0)
    count = data.size
    if name == "hanning":
        result = np.convolve(clean, [.25, .5, .25], mode="same")
        result[0] = (clean[0] + clean[1]) / 2.
        result[-1] = (clean[-2] + clean[-1]) / 2.
        start, factor, bins = 1, 2, (count - 1) // 2
    else:
        result = np.lib.stride_tricks.sliding_window_view(clean, width).mean(axis=-1)
        start, factor = 0, width
        bins = count // width if decimate == "copy" else len(result) // width
    valid = mask[:len(result)]
    if decimate is None:
        return result, valid, 0, 1
    indices = start + np.arange(bins) * factor
    if decimate == "copy":
        step = 1 if name == "boxcar" and bins == 1 else factor
        return result[indices], valid[indices], start, step
    groups = indices[:, None] + np.arange(factor)
    counts = valid[groups].sum(axis=1)
    if factor > 1 and np.any(counts == 0):
        return None, None, start, factor
    sample_values = np.where(valid[groups], result[groups], 0)
    return sample_values.sum(axis=1) / np.maximum(counts, 1), counts != 0, start, factor


@pytest.mark.parametrize("method,width,decimate", [
    ("boxcar", 2, None),
    ("boxcar", 3, "copy"),
    ("boxcar", 2, "mean"),
    ("boxcar", 4, "mean"),
    ("boxcar", 1, "mean"),
    ("hanning", None, None),
    ("hanning", None, "copy"),
    ("hanning", None, "mean"),
])
@pytest.mark.parametrize("masked", [False, True])
def test_smoothing_lines_have_analytic_values_and_mask(casa, tmp_path, method, width, decimate, masked):
    data = np.arange(9, dtype=np.float32)**2
    mask = (data > 4) & (data != 25) if masked else np.ones_like(data, dtype=bool)
    values, validity, start, step = smooth_expected(data, mask, method, width, decimate)
    options = {"drop": decimate is not None, "dmethod": decimate or "ignored"}
    if width is not None:
        options["width"] = width
    for api in (tools, casa):
        source = api.image()
        infile = str(tmp_path / f"line-{api.__name__}.im")
        assert source.fromarray(infile, data)
        if masked:
            assert source.calcmask(f'"{infile}" > 4 && "{infile}" != 25')
        try:
            if values is None:
                with pytest.raises(RuntimeError, match="All selected pixels are masked"):
                    getattr(source, method)(**options)
            else:
                product = getattr(source, method)(**options)
                try:
                    np.testing.assert_allclose(product.getchunk(), values, rtol=2e-6)
                    np.testing.assert_array_equal(product.getchunk(getmask=True), validity)
                    coords = product.coordsys()
                    try:
                        np.testing.assert_allclose(coords.referencepixel()["numeric"], [(4. - start) / step])
                        np.testing.assert_allclose(coords.increment()["numeric"], [1000. * step])
                    finally:
                        coords.done()
                    assert product.history()[-4] == f"Ran ia.{method}"
                finally:
                    product.done()
            np.testing.assert_array_equal(source.getchunk(), data)
            np.testing.assert_array_equal(source.getchunk(getmask=True), mask)
        finally:
            source.done()


@pytest.mark.parametrize("method,width", [("boxcar", 2), ("hanning", None)])
def test_native_smoothing_preserves_complex_values_and_metadata(casa, tmp_path, method, width):
    real = np.arange(1, 28, dtype=np.float32).reshape(3, 9)**2
    pixels = np.complex64(real * (1. + 2.j))
    mask = np.ones(9, dtype=bool)
    expected = np.stack([smooth_expected(row, mask, method, width, "copy")[0] for row in pixels])
    options = {"axis": 1}
    if width is not None:
        options["width"] = width
    for writer in (tools, casa):
        source = writer.image()
        assert source.fromarray(pixels=pixels)
        assert source.setbrightnessunit("Jy/beam")
        assert source.setrestoringbeam(major="3arcsec", minor="2arcsec", pa="20deg")
        path = str(tmp_path / f"smoothed-{writer.__name__}.im")
        result = getattr(source, method)(outfile=path, **options)
        result.done()
        source.done()
        for reader in (tools, casa):
            reopened = reader.image()
            assert reopened.open(path)
            try:
                np.testing.assert_array_equal(reopened.getchunk(), expected)
                assert reopened.getchunk(getmask=True).all()
                assert reopened.brightnessunit() == "Jy/beam"
                assert reopened.restoringbeam()["major"]["value"] == pytest.approx(3.)
                assert reopened.pixeltype() == "Complex"
            finally:
                reopened.done()


def test_strided_region_hanning_preserves_sample_positions(casa):
    data = np.arange(5*4*2*7, dtype=np.float32).reshape(5, 4, 2, 7)
    before = data[1:4, 1:4, :, ::2]
    expected = np.stack((.25 * before[..., 0] + .5 * before[..., 1] + .25 * before[..., 2],), axis=-1)
    region = casa.regionmanager()
    try:
        choose = region.box([1, 1, 0, 0], [3, 3, 1, 6], [1, 1, 1, 2])
    finally:
        region.done()
    for api in (tools, casa):
        original = api.image()
        assert original.fromarray(pixels=data)
        result = original.hanning(region=choose)
        original_coords = original.coordsys()
        result_coords = result.coordsys()
        try:
            np.testing.assert_array_equal(result.getchunk(), expected)
            np.testing.assert_allclose(
                result_coords.toworld([1, 1, 1, 0])["numeric"],
                original_coords.toworld([2, 2, 1, 2])["numeric"],
                rtol=1e-11, atol=1e-11,
            )
        finally:
            result_coords.done()
            original_coords.done()
            result.done()
            original.done()


def test_smoothing_validation_preserves_existing_native_output(casa, tmp_path):
    data = np.arange(16, dtype=np.float32).reshape(4, 4)
    for api in (tools, casa):
        original = api.image()
        result = api.image()
        assert original.fromarray(pixels=data, linear=True)
        path = str(tmp_path / f"destination-{api.__name__}.im")
        assert result.fromarray(path, data + 100.)
        result.done()
        try:
            for method, args, diagnostic in [
                ("hanning", {}, "image has no spectral coordinate"),
                ("hanning", {"axis": 0, "dmethod": "wrong"}, "Value of dmethod"),
                ("boxcar", {"axis": 0, "width": 0}, "Boxcar width must be positive"),
                ("boxcar", {"axis": 1, "width": 5}, "must have at least 5 pixels"),
            ]:
                with pytest.raises(RuntimeError, match=diagnostic):
                    getattr(original, method)(outfile=path, overwrite=True, **args)
            assert result.open(path)
            np.testing.assert_array_equal(result.getchunk(), data + 100.)
            np.testing.assert_array_equal(original.getchunk(), data)
        finally:
            result.done()
            original.done()


def test_analytic_two_sample_boxcar_keeps_the_single_plane_reference():
    image = tools.image()
    assert image.fromarray(pixels=np.array([1., 5.], dtype=np.float32))
    output = image.boxcar()
    coords = output.coordsys()
    try:
        np.testing.assert_array_equal(output.getchunk(), [3.])
        assert coords.referencepixel()["numeric"].tolist() == [1.]
        assert coords.increment()["numeric"].tolist() == [1000.]
    finally:
        coords.done()
        output.done()
        image.done()


@pytest.mark.parametrize("method,shape,count", [
    ("boxcar", [6, 7, 2], 56),
    ("hanning", [7, 7, 2], 58),
])
def test_circle_selection_keeps_smoothing_validity(casa, method, shape, count):
    data = np.arange(8 * 9 * 2, dtype=np.float32).reshape(8, 9, 2)
    cases = []
    for api in (tools, casa):
        source = api.image()
        assert source.fromarray(pixels=data)
        result = getattr(source, method)(region="circle[[4pix,4pix],3pix]", drop=False, axis=0)
        try:
            assert result.shape().tolist() == shape
            valid = result.getchunk(getmask=True)
            assert int(valid.sum()) == count
            cases.append((result.getchunk(), valid))
        finally:
            result.done()
            source.done()
    np.testing.assert_allclose(cases[0][0], cases[1][0])
    np.testing.assert_array_equal(cases[0][1], cases[1][1])
