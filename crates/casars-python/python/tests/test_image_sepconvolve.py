"""Pinned CASA parity and native writer–reader coverage for image.sepconvolve."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _impulse(shape):
    data = np.zeros(shape, dtype=np.float32)
    data[tuple(np.array(shape)//2)] = 1.
    return data


def _gaussian(coordinates, first, length, width, scale=-1.):
    sigma = width/np.sqrt(8*np.log(2))
    n = max(length, 2*(int(5*sigma + .5) + 1))
    peak = 1/(np.sqrt(2*np.pi)*sigma) if scale < 0 else scale
    return peak*np.exp(-.5*((coordinates-first-(n % 2)/2)/sigma)**2)


@pytest.mark.parametrize("width", [[2], np.array([2.]), np.array(2.), ["2 pix"], "2arcmin",
                                  ["2pix 3pix"], ["2pix,3pix"]])
@pytest.mark.parametrize("gain", [-1., 0., 2.])
def test_sepconvolve_gaussian_admitted_widths_and_normalization(casa, width, gain):
    data = _impulse((13, 15))
    reference = _gaussian(np.arange(13), 6, 13, 2, gain)
    expected = np.zeros_like(data)
    expected[:, 7] = reference
    for api in (casa, tools):
        source = api.image()
        output = None
        try:
            assert source.fromarray(pixels=data)
            output = source.sepconvolve(widths=width, scale=gain)
            result = output.getchunk()
            assert result.dtype == source.getchunk().dtype
            np.testing.assert_allclose(result, expected, atol=3e-7)
            np.testing.assert_array_equal(output.getchunk(getmask=True), np.ones_like(data, dtype=bool))
        finally:
            if output is not None:
                output.done()
            source.done()


@pytest.mark.parametrize("length", [13, 14, 513])
@pytest.mark.parametrize("scale", [-1., 1.3])
def test_sepconvolve_hanning_uses_the_full_profile(casa, length, scale):
    data = _impulse((length, 6))
    middle = (length-1)//2
    radius = (length+1)//2
    phase = (radius-np.abs(np.arange(length)-middle))*2*np.pi/(length+1)
    normalizer = 1/(length+1) if scale < 0 else .5
    expected = np.asarray(normalizer*(1-np.cos(phase)), dtype=np.float32)
    if scale >= 0:
        expected *= np.float32(scale)
    for api in (casa, tools):
        source = api.image()
        result = None
        try:
            assert source.fromarray(pixels=data)
            result = source.sepconvolve(types=["Hanning"], widths=[3], scale=scale)
            # CASA leaves the trailing even-length kernel coefficient uninitialized.
            # All assigned coefficients, and the native zero padding, are checked.
            limit = length if length % 2 or api is tools else length-1
            # CASA's single-precision FFT leaves noise below 7e-7 on the
            # amplitude-one, wide reference kernel.
            tolerance = 7e-7 if api is casa else 2e-8
            np.testing.assert_allclose(result.getchunk()[:limit, 3], expected[:limit], atol=tolerance)
            np.testing.assert_array_equal(result.getchunk(getmask=True)[:, 3], np.ones(length, dtype=bool))
        finally:
            if result is not None:
                result.done()
            source.done()


@pytest.mark.parametrize("widths", [[2, 4], ["2 arcmin", "4 arcmin"], "2pix,4pix",
                                   np.array([[2.], [4.]])])
@pytest.mark.parametrize("scale", [-1., 1.2])
def test_sepconvolve_combines_kernels_in_axis_order(casa, widths, scale):
    data = _impulse((13, 15))
    expected = np.zeros_like(data)
    expected[4:8, :] = (.25 if scale < 0 else scale)*_gaussian(np.arange(15), 7, 15, 2, scale)
    for api in (casa, tools):
        source = api.image()
        result = None
        try:
            assert source.fromarray(pixels=data)
            result = source.sepconvolve(widths=widths, types=["Gauss", "b"], axes=[1, 0], scale=scale)
            np.testing.assert_allclose(result.getchunk(), expected, atol=3e-7)
        finally:
            if result is not None:
                result.done()
            source.done()


def test_sepconvolve_honors_a_single_explicit_axis():
    data = _impulse((13, 15))
    expected = np.zeros_like(data)
    expected[6, 5:9] = .25
    source = tools.image()
    result = None
    try:
        assert source.fromarray(pixels=data)
        result = source.sepconvolve(widths=[4], types=["boxcar"], axes=[1])
        np.testing.assert_array_equal(result.getchunk(), expected)
    finally:
        if result is not None:
            result.done()
        source.done()


@pytest.mark.parametrize("axes", [[1.9, .1], np.array([1., 0.]), [True, False]])
def test_sepconvolve_accepts_numeric_axis_vectors(casa, axes):
    data = _impulse((13, 15))
    expected = np.zeros_like(data)
    expected[4:8, 6:8] = 1/8
    for api in (casa, tools):
        source = api.image()
        result = None
        try:
            assert source.fromarray(pixels=data)
            result = source.sepconvolve(axes=axes, types=["b", "b"], widths=[2,4])
            np.testing.assert_allclose(result.getchunk(), expected, atol=3e-7)
        finally:
            if result is not None:
                result.done()
            source.done()


def test_sepconvolve_rejects_a_tuple_axis_argument_before_publication(casa, tmp_path):
    data = _impulse((13, 15))
    for api, exception in ((casa, SystemError), (tools, TypeError)):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            with pytest.raises(exception):
                source.sepconvolve(axes=(0,1), widths=[2,4])
        finally:
            source.done()
    source, existing = tools.image(), tools.image()
    name = str(tmp_path/"prior.im")
    try:
        assert source.fromarray(pixels=data)
        assert existing.fromarray(name, pixels=data+7)
        existing.done()
        with pytest.raises(TypeError):
            source.sepconvolve(name, axes=(0,1), widths=[2,4], overwrite=True)
        assert existing.open(name)
        np.testing.assert_array_equal(existing.getchunk(), data+7)
    finally:
        source.done()
        existing.done()


def test_sepconvolve_region_raw_mask_values_and_native_interoperability(casa, tmp_path):
    data = np.full((13, 15, 1, 5), 2., dtype=np.float32)
    original_valid = np.ones(data.shape, dtype=bool)
    original_valid[7, 8, 0, 2] = False
    manager = casa.regionmanager()
    region = manager.box([2, 3, 0, 1], [11, 13, 0, 3])
    manager.done()
    cropped = data[2:12, 3:14, :, 1:4]
    valid = original_valid[2:12, 3:14, :, 1:4]
    selected = np.where(valid, cropped, 0.)
    expected = np.zeros_like(selected)
    # The boxcar of width 3 begins an extra pixel to the left in an
    # even-length profile. The cropped x and y lengths are 10 and 11.
    for ox in (-2, -1, 0):
        for oy in (-1, 0, 1):
            x = slice(max(0, ox), min(10, 10+ox))
            y = slice(max(0, oy), min(11, 11+oy))
            sx = slice(max(0, -ox), min(10, 10-ox))
            sy = slice(max(0, -oy), min(11, 11-oy))
            expected[x, y] += selected[sx, sy]/9.
    for writer in (casa, tools):
        source = writer.image()
        product_path = str(tmp_path/f"{writer.__name__}.im")
        result = None
        try:
            assert source.fromarray(pixels=data)
            assert source.putregion(pixelmask=original_valid)
            assert source.setbrightnessunit("K")
            assert source.setmiscinfo({"observer": "SEP fixture"})
            assert source.sethistory(origin="fixture", history=["original sepconvolve input"])
            for channel in range(5):
                assert source.setrestoringbeam(major=f"{channel+2}arcmin", minor="1arcmin",
                                               pa="4deg", channel=channel, polarization=0)
            result = source.sepconvolve(product_path, widths=[3, 3], types=["b", "b"], region=region)
            assert result.ispersistent()
            np.testing.assert_array_equal(source.getchunk(), data)
        finally:
            if result is not None:
                result.done()
            source.done()
        for reader in (casa, tools):
            saved = reader.image()
            coords = None
            try:
                assert saved.open(product_path)
                assert saved.brightnessunit() == "K"
                assert saved.miscinfo()["observer"] == "SEP fixture"
                history = saved.history()
                assert any("original sepconvolve input" in line for line in history)
                assert any("ia.sepconvolve" in line for line in history)
                coords = saved.coordsys()
                np.testing.assert_array_equal(coords.referencepixel()["numeric"], [4., 4., 0., 1.])
                np.testing.assert_array_equal(saved.getchunk(getmask=True), valid)
                np.testing.assert_allclose(saved.getchunk(), expected, atol=4e-7)
                for channel in range(3):
                    beam = saved.restoringbeam(channel=channel, polarization=0)
                    assert beam["major"] == {"value": pytest.approx(channel+3.), "unit": "arcmin"}
            finally:
                if coords is not None:
                    coords.done()
                saved.done()


def test_sepconvolve_invalid_calls_preserve_named_output(casa, tmp_path):
    data = _impulse((13, 15))
    for api in (casa, tools):
        source, prior = api.image(), api.image()
        name = str(tmp_path/f"{api.__name__}-prior.im")
        try:
            assert source.fromarray(pixels=data)
            assert prior.fromarray(name, pixels=data+4.)
            prior.done()
            for width in (None, [], 2., {"value": 2., "unit": "pix"},
                          np.array([True]), np.array([2.], dtype=np.float16),
                          np.array([2], dtype=object)):
                detached = source.sepconvolve(name, widths=width, overwrite=True)
                assert not detached.isopen()
                detached.done()
            for options in (
                {"widths": [2, 3], "axes": [0, 0]},
                {"widths": [2, 3], "types": ["gauss"]},
                {"widths": [2], "types": ["triangle"]},
                {"widths": ["2m"]},
                {"widths": "2 pix"},
                {"widths": [2, "3 pix"]},
            ):
                with pytest.raises(RuntimeError):
                    source.sepconvolve(name, overwrite=True, **options)
            assert prior.open(name)
            np.testing.assert_array_equal(prior.getchunk(), data+4.)
        finally:
            prior.done()
            source.done()


@pytest.mark.parametrize("type_argument,is_boxcar", [
    (None, False), ([], False), (("boxcar",), False),
    (np.array(["boxcar"]), False), (["boxcar"], True), ("boxcar", True),
])
def test_sepconvolve_kernel_label_containers_follow_casa(casa, type_argument, is_boxcar):
    data = _impulse((13, 15))
    expected = .5 if is_boxcar else _gaussian(6, 6, 13, 2)
    for api in (casa, tools):
        source = api.image()
        output = None
        try:
            assert source.fromarray(pixels=data)
            output = source.sepconvolve(widths=[2], types=type_argument)
            assert output.getchunk()[6, 7] == pytest.approx(expected, abs=2e-7)
        finally:
            if output is not None:
                output.done()
            source.done()


def test_sepconvolve_flattens_numeric_widths_in_casa_axis_order(casa):
    shape = (11, 13, 7, 9)
    data = _impulse(shape)
    widths = np.asarray([[2., 3.], [4., 5.]])
    expected = 1.
    for axis, (length, width) in enumerate(zip(shape, widths.ravel(order="F"))):
        aligned = [1] * len(shape)
        aligned[axis] = length
        expected = expected*_gaussian(np.arange(length), length//2, length, width).reshape(aligned)
    for api in (casa, tools):
        source = api.image()
        output = None
        try:
            assert source.fromshape(shape=list(shape), linear=True)
            assert source.putchunk(data)
            output = source.sepconvolve(widths=widths)
            np.testing.assert_allclose(output.getchunk(), expected, atol=3e-7)
        finally:
            if output is not None:
                output.done()
            source.done()


def test_sepconvolve_stretches_an_external_mask_without_masking_intermediate_passes(casa, tmp_path):
    data = np.ones((13, 15, 1, 5), dtype=np.float32)
    selected = np.broadcast_to(np.arange(13)[:, None, None, None] >= 6, (13, 15, 1, 1))
    valid = np.broadcast_to(selected, data.shape)
    for api in (casa, tools):
        source, mask_image = api.image(), api.image()
        name = str(tmp_path/f"{api.__name__}-mask.im")
        result = None
        try:
            assert source.fromarray(pixels=data)
            assert mask_image.fromarray(name, pixels=selected.astype(np.float32))
            mask_image.done()
            options = {"types": ["b", "b"], "widths": [3, 3], "mask": f'"{name}" > 0.5'}
            with pytest.raises(RuntimeError):
                source.sepconvolve(**options)
            result = source.sepconvolve(stretch=True, **options)
            np.testing.assert_array_equal(result.getchunk(getmask=True), valid)
            pixels = result.getchunk()
            np.testing.assert_allclose(pixels[5:8, 7, 0, :], [[1/3]*5, [2/3]*5, [1]*5], atol=3e-7)
            np.testing.assert_array_equal(source.getchunk(), data)
            assert mask_image.open(name)
            np.testing.assert_array_equal(mask_image.getchunk(), selected)
        finally:
            if result is not None:
                result.done()
            mask_image.done()
            source.done()


@pytest.mark.parametrize("kind", ["c", "d", "cd"])
def test_sepconvolve_rejects_other_pixel_types(casa, kind):
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=[13, 15], type=kind)
            with pytest.raises(RuntimeError):
                source.sepconvolve(widths=[2])
            assert source.isopen()
        finally:
            source.done()


@pytest.mark.parametrize("width", [0., -2.])
def test_sepconvolve_rejects_undefined_gaussian_kernels(width):
    source = tools.image()
    try:
        assert source.fromarray(pixels=_impulse((13, 15)))
        with pytest.raises(RuntimeError, match="positive"):
            source.sepconvolve(widths=[width])
        assert source.isopen()
    finally:
        source.done()


@pytest.mark.parametrize("sample", [np.nan, np.inf])
def test_sepconvolve_nonfinite_samples_poison_only_their_profile(casa, sample):
    data = np.ones((13, 5), dtype=np.float32)
    data[5, 2] = sample
    for api in (casa, tools):
        source = api.image()
        result = None
        try:
            assert source.fromarray(pixels=data)
            result = source.sepconvolve(widths=[2])
            pixels = result.getchunk()
            assert np.isnan(pixels[:, 2]).all()
            assert np.isfinite(pixels[:, [0, 1, 3, 4]]).all()
            assert result.getchunk(getmask=True).all()
        finally:
            if result is not None:
                result.done()
            source.done()


def test_sepconvolve_long_profile_keeps_original_samples_across_chunk_edges():
    data = np.zeros((262200, 2), dtype=np.float32)
    locations = [262103, 262112, 262119, 262125, 262139]
    values = [1., -.5, 3., 2., 5.]
    data[locations, 0] = values
    data[-1, 1] = np.nan
    expected = sum(
        value*_gaussian(np.arange(data.shape[0]), point, data.shape[0], 2)
        for point, value in zip(locations, values)
    )
    source = tools.image()
    product = None
    try:
        assert source.fromarray(pixels=data)
        product = source.sepconvolve(widths=[2])
        pixels = product.getchunk()
        np.testing.assert_allclose(pixels[:, 0], expected, atol=1e-7, rtol=3e-7)
        assert np.isnan(pixels[:, 1]).all()
        assert product.getchunk(getmask=True).all()
    finally:
        if product is not None:
            product.done()
        source.done()
