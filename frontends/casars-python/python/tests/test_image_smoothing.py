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
                        center = 500. * (width - 1) if width is not None else 0.
                        np.testing.assert_allclose(
                            coords.toworld([0.])["numeric"],
                            [1.415e9 + 1000. * (start - 4.) + center],
                            rtol=0, atol=1e-6,
                        )
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
@pytest.mark.parametrize("complex_pixels", [False, True])
def test_native_smoothing_preserves_pixels_and_metadata(casa, tmp_path, method, width, complex_pixels):
    real = np.arange(1, 28, dtype=np.float32).reshape(3, 9)**2
    pixels = np.complex64(real * (1. + 2.j)) if complex_pixels else real
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
                assert reopened.pixeltype() == ("complex" if complex_pixels else "float")
            finally:
                reopened.done()

            log = reader.table()
            assert log.open(path + "/logtable")
            try:
                origins = {
                    "boxcar": ("::image::boxcar", "::ImageBoxcarSmoother::_reportOldNewImageShapes"),
                    "hanning": ("::image::_hanning", "::HanningSmoother::_reportOldNewImageShapes"),
                }[method]
                assert log.getcol("LOCATION")[-4:].tolist() == [origins[0]] * 2 + [origins[1]] * 2
            finally:
                log.done()


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

@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("method,decimation,offset,stride,expected,world,refpix,refval,increment", [
    ("boxcar", False, 0, 1, [.5, 1.5, 2.5, 3.5, 4.5], [10.7, 11.7, 17.7, 21.7, 71.7], 0., 10.7, 15.4),
    ("boxcar", True, 0, 1, [.5, 2.5, 4.5], [10.7, 12.7, 24.7], 0., 10.7, 30.8),
    ("boxcar", True, 1, 2, [2.], [33.8], -.5, 18.4, 30.8),
    ("hanning", True, 0, 1, [1., 3.], [18.4, 20.4], -.5, 3., 30.8),
    ("hanning", True, 1, 1, [2., 4.], [33.8, 35.8], -1., 3., 30.8),
])
def test_smoothing_tabular_lookup_grid(
    casa, tmp_path, persistent, method, decimation, offset, stride, expected, world, refpix, refval, increment,
):
    manager = casa.regionmanager()
    selection = manager.box([offset], [5], [stride])
    manager.done()
    for api in (tools, casa):
        coordinate = api.coordsys()
        assert coordinate.newcoordsys(tabular=True)
        assert coordinate.settabular(pixel=[0, 1, 2, 3, 4, 5], world=[3, 4, 10, 14, 64, 80])
        image = api.image()
        assert image.fromarray(pixels=np.arange(6, dtype=np.float32), csys=coordinate.torecord())
        coordinate.done()
        path = str(tmp_path / f"tabular-{api.__name__}.im") if persistent else ""
        output = getattr(image, method)(axis=0, drop=decimation, region=selection, outfile=path)
        coords = output.coordsys()
        try:
            np.testing.assert_array_equal(output.getchunk(), expected)
            np.testing.assert_allclose(coords.referencepixel()["numeric"], [refpix], atol=1e-12)
            np.testing.assert_allclose(coords.referencevalue()["numeric"], [refval], atol=1e-12)
            np.testing.assert_allclose(coords.increment()["numeric"], [increment], atol=1e-12)
            np.testing.assert_allclose(
                [coords.toworld([p])["numeric"][0] for p in range(len(expected))],
                world, rtol=0., atol=1e-10,
            )
        finally:
            coords.done()
            output.done()
            image.done()

        if persistent:
            for reader in (tools, casa):
                reader_table = reader.table()
                assert reader_table.open(path)
                try:
                    native = reader_table.getkeyword("coords")["tabular0"]
                    np.testing.assert_allclose(native["crpix"], [refpix], atol=1e-12)
                    np.testing.assert_allclose(native["crval"], [refval], atol=1e-12)
                finally:
                    reader_table.done()
                reopened = reader.image()
                assert reopened.open(path)
                restored = reopened.coordsys()
                try:
                    np.testing.assert_allclose(restored.referencepixel()["numeric"], [0.], atol=1e-12)
                    np.testing.assert_allclose(restored.referencevalue()["numeric"], [world[0]], atol=1e-12)
                    np.testing.assert_allclose(
                        [restored.toworld([p])["numeric"][0] for p in range(len(expected))],
                        world, rtol=0., atol=1e-10,
                    )
                finally:
                    restored.done()
                    reopened.done()


@pytest.mark.parametrize("method,labels,expected", [
    ("boxcar", ["I", "Q", "U"], [.5, 2.5]),
    ("hanning", ["Q", "U"], [1.]),
])
def test_smoothing_native_stokes_grid(casa, tmp_path, method, labels, expected):
    for writer in (tools, casa):
        coord = writer.coordsys()
        assert coord.newcoordsys(direction=True, stokes="I Q U V")
        source = writer.image()
        data = np.broadcast_to(np.arange(4, dtype=np.float32), (4, 5, 4)).copy()
        assert source.fromarray(pixels=data, csys=coord.torecord())
        coord.done()
        path = str(tmp_path / f"{writer.__name__}.im")
        output = getattr(source, method)(axis=2, outfile=path)
        output.done()
        source.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            coordinate = image.coordsys()
            try:
                assert coordinate.stokes() == labels
                np.testing.assert_array_equal(image.getchunk(), np.broadcast_to(expected, (4, 5, len(expected))))
            finally:
                coordinate.done()
                image.done()


def test_boxcar_uses_the_implicit_decimator_region(casa):
    manager = casa.regionmanager()
    region = manager.box([0, 0, 0], [3, 4, 5])
    manager.done()
    data = np.arange(120, dtype=np.float32).reshape((4, 5, 6))
    for api in (tools, casa):
        coord = api.coordsys()
        assert coord.newcoordsys(linear=3)
        source = api.image()
        assert source.fromarray(pixels=data, csys=coord.torecord())
        coord.done()
        product = source.boxcar(axis=2, region=region)
        try:
            np.testing.assert_array_equal(product.getchunk(), [[[.5, 2.5, 4.5]]])
            with pytest.raises(RuntimeError, match="cursorShape"):
                source.boxcar(axis=0, region=region)
            with pytest.raises(RuntimeError, match="shape has to be > 0"):
                source.boxcar(axis=0, region=region, dmethod="mean")
        finally:
            product.done()
            source.done()


@pytest.mark.parametrize("method,width,decimation", [
    ("hanning", None, None),
    ("hanning", None, "mean"),
    ("boxcar", 3, None),
    ("boxcar", 3, "mean"),
    ("boxcar", 262148, "copy"),
    ("boxcar", 262148, "mean"),
])
def test_smoothing_windows_cross_bounded_reads(method, width, decimation):
    length = 2 * width + 11 if width and width > 262144 else 262183
    data = np.float32(np.arange(length) % 7)
    mask = data != 2.
    options = {"axis": 0, "drop": decimation is not None, "dmethod": decimation or "copy"}
    if width is not None:
        options["width"] = width
    # For a wide boxcar, use prefix sums so the oracle is linear in the number of pixels.
    if width and width > 262144:
        packed = np.where(mask, data, 0.)
        prefix = np.r_[0., np.cumsum(packed, dtype=np.float64)]
        window = (prefix[width:] - prefix[:-width]) / width
        if decimation == "copy":
            starts = np.arange(length // width) * width
            expected = window[starts]
            validity = mask[starts]
        else:
            bins = len(window) // width
            grouped_mask = mask[:bins * width].reshape(bins, width)
            groups = window[:bins * width].reshape(bins, width)
            expected = np.sum(groups * grouped_mask, axis=1) / grouped_mask.sum(axis=1)
            validity = grouped_mask.any(axis=1)
    else:
        expected, validity, *_ = smooth_expected(data, mask, method, width, decimation)
    source = tools.image()
    assert source.fromarray(pixels=data)
    assert source.putregion(pixelmask=mask)
    result = getattr(source, method)(**options)
    try:
        np.testing.assert_allclose(result.getchunk(), expected, rtol=2e-6)
        np.testing.assert_array_equal(result.getchunk(getmask=True), validity)
    finally:
        result.done()
        source.done()


def test_masked_mean_uses_validity_across_orthogonal_tiles(casa):
    pixels = np.broadcast_to(np.arange(8, dtype=np.float32), (2, 8)).copy()
    valid = np.zeros((2, 8), dtype=bool)
    valid[0, 1:3] = True
    valid[0, 5:7] = True
    valid[1, 3:5] = True
    for api in (tools, casa):
        source = api.image()
        assert source.fromarray(pixels=pixels)
        assert source.putregion(pixelmask=valid)
        result = source.hanning(axis=1, dmethod="mean")
        try:
            np.testing.assert_array_equal(result.getchunk(getmask=True), [[True, False, True], [False, True, False]])
        finally:
            result.done()
            source.done()
