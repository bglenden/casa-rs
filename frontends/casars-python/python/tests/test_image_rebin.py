"""Public image binning against CASA 6.7.6.14 and independent bin means."""

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def _means(pixels, valid, factors, crop=False):
    shape = tuple(size // factor if crop else (size + factor - 1) // factor
                  for size, factor in zip(pixels.shape, factors))
    result = np.zeros(shape, dtype=pixels.dtype)
    mask = np.zeros(shape, dtype=bool)
    for index in np.ndindex(shape):
        block = tuple(slice(i * width, (i + 1) * width) for i, width in zip(index, factors))
        samples = pixels[block][valid[block]]
        if samples.size:
            result[index] = samples.mean()
            mask[index] = True
    return result, mask


@pytest.mark.parametrize("crop,dropdeg", [(False, False), (False, True), (True, False), (True, True)])
def test_rebin_partial_bins_and_degenerate_axes(casa, crop, dropdeg):
    shape = (5, 4, 2)
    values = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
    valid = np.ones(shape, dtype=bool)
    valid[0:2, :3, 0] = False
    valid[3, 2, 1] = False
    expected, mask = _means(values, valid, (2, 3, 1), crop)
    if dropdeg:
        expected, mask = expected.squeeze(), mask.squeeze()
    for backend in (tools, casa):
        source = backend.image()
        assert source.fromarray(pixels=values)
        assert source.putregion(pixelmask=valid)
        product = source.rebin("", np.array([2, 3], dtype=np.int32), crop=crop, dropdeg=dropdeg)
        np.testing.assert_array_equal(product.getchunk(), expected)
        np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
        coords = product.coordsys()
        reference = [0.75, 1 / 3, 0.0]
        increments = [-2, 3, 1]
        if crop and dropdeg:
            del reference[1]
            del increments[1]
        np.testing.assert_allclose(coords.referencepixel()["numeric"], reference)
        np.testing.assert_allclose(coords.increment()["numeric"], increments)
        assert product.done() and source.done()


@pytest.mark.parametrize("complex_pixels", [False, True])
def test_rebin_preserves_native_pixels_mask_and_metadata(casa, tmp_path, complex_pixels):
    values = np.arange(63, dtype=np.float32).reshape(9, 7)
    if complex_pixels:
        values = (values + 2j * values).astype(np.complex64)
    valid = np.ones(values.shape, dtype=bool)
    valid[1:4, 1:3] = False
    valid[6, 3] = False
    region = casa.regionmanager().box([1, 1], [7, 5])
    selected = np.s_[1:8, 1:6]
    expected, mask = _means(values[selected], valid[selected], (3, 2))
    for index, writer in enumerate((tools, casa)):
        source = writer.image()
        assert source.fromarray(pixels=values)
        assert source.putregion(pixelmask=valid)
        assert source.setbrightnessunit("Jy/beam")
        assert source.setmiscinfo({"object": "Rebin field"})
        assert source.setrestoringbeam("0.2arcmin", "1arcsec", "2rad")
        assert source.sethistory("fixture", ["source history"])
        world = source.coordsys().toworld([5, 3.5])["numeric"]
        filename = str(tmp_path / f"product-{index}.im")
        product = source.rebin(filename, [3, 2], region=region)
        assert product.done() and source.done()
        for reader in (casa, tools):
            product = reader.image()
            assert product.open(filename)
            np.testing.assert_array_equal(product.getchunk(), expected)
            np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
            assert product.maskhandler("get") == ["mask0"]
            assert product.maskhandler("default") == ["mask0"]
            assert product.miscinfo()["object"] == "Rebin field"
            assert product.brightnessunit() == "Jy/beam"
            assert product.restoringbeam()["positionangle"] == {"unit": "rad", "value": 2.0}
            coords = product.coordsys()
            np.testing.assert_allclose(coords.referencepixel()["numeric"], [2 / 3, 0.75])
            np.testing.assert_allclose(coords.toworld([1, 1])["numeric"], world)
            history = product.history()
            assert "source history" in history
            assert any("Ran ia.rebin" in row for row in history)
            assert product.done()


def test_rebin_explicit_strided_region_controls_pixels_and_wcs(casa):
    values = np.arange(8 * 6, dtype=np.float32).reshape(8, 6)
    selection = casa.regionmanager().box([1, 0], [7, 4], [2, 2])
    selected = values[1:8:2, :5:2]
    expected, _ = _means(selected, np.ones_like(selected, dtype=bool), (2, 2))
    for backend in (tools, casa):
        source = backend.image()
        assert source.fromarray(pixels=values)
        product = source.rebin("", [2, 2], region=selection, mask=[])
        np.testing.assert_array_equal(product.getchunk(), expected)
        coords = product.coordsys()
        np.testing.assert_allclose(coords.increment()["numeric"], [-4, 4])
        np.testing.assert_allclose(
            coords.toworld([0, 0])["numeric"], source.coordsys().toworld([2, 1])["numeric"]
        )
        assert product.done() and source.done()


def test_rebin_fits_blank_and_stretch(casa, tmp_path):
    source_path = tmp_path / "pixels.fits"
    source_path.write_bytes(_hdu(True, [1, -999, 3, 4]))
    mask_path = str(tmp_path / "mask.im")
    mask = casa.image()
    assert mask.fromarray(mask_path, np.array([[0], [1]], dtype=np.float32))
    assert mask.done()
    for backend in (tools, casa):
        source = backend.image()
        assert source.open(str(source_path))
        expression = f'"{mask_path}" > 0'
        with pytest.raises(RuntimeError, match="mask should not be extended"):
            source.rebin("", [1, 2], mask=expression)
        product = source.rebin("", [1, 2], mask=expression, stretch=True)
        np.testing.assert_array_equal(product.getchunk(), [[0], [4]])
        np.testing.assert_array_equal(product.getchunk(getmask=True), [[False], [True]])
        assert product.maskhandler("get") == ["mask0"]
        assert product.maskhandler("default") == ["mask0"]
        assert product.done() and source.done()


@pytest.mark.parametrize("mask", [False, True])
def test_rebin_sums_in_native_precision_and_axis_order(casa, mask):
    values = np.array([[1e8, -1e8], [1, 0]], dtype=np.float32)
    for backend in (tools, casa):
        source = backend.image()
        assert source.fromarray(pixels=values)
        if mask:
            assert source.putregion(pixelmask=np.array([[True, True], [True, False]]))
        result = source.rebin("", [2, 2])
        np.testing.assert_array_equal(result.getchunk(), [[0]])
        assert result.done() and source.done()


def test_rebin_complex_mean_does_not_overflow_in_scalar_division(casa):
    values = np.full((2, 3), 4e37 + 3e37j, dtype=np.complex64)
    for backend in (tools, casa):
        source = backend.image()
        assert source.fromarray(pixels=values)
        result = source.rebin("", [2, 3])
        np.testing.assert_allclose(result.getchunk(), values[:1, :1], rtol=2e-7)
        assert result.done() and source.done()


@pytest.mark.parametrize("kind", ["coupled", "sampled"])
def test_rebin_coordinate_parameters_match_casa(casa, kind):
    if kind == "coupled":
        cs = casa.coordsys().newcoordsys(linear=2)
        assert cs.setlineartransform("linear", np.array([[2.0, 0.5], [0.6, 3.0]]))
        shape, factors = [7, 5], [3, 2]
        expected = [10.25, 53 / 6]
    else:
        cs = casa.coordsys().newcoordsys(direction=True, spectral=True)
        frequency = np.array([1, 1.1, 1.4, 2.1, 2.2, 2.5, 2.6]) * 1e9
        assert cs.setspectral(frequencies={"value": frequency, "unit": "Hz"})
        shape, factors = [4, 4, 7], [1, 1, 3]
        expected = [21599, 1, 1.5666666666666667e9]
    outputs = []
    for backend in (casa, tools):
        source = backend.image()
        assert source.fromshape(shape=shape, csys=cs.torecord())
        product = source.rebin("", factors)
        coords = product.coordsys()
        np.testing.assert_allclose(coords.toworld(np.ones(len(shape)))["numeric"], expected)
        coordinate = "linear" if kind == "coupled" else "spectral"
        outputs.append((coords.increment(), coords.referencepixel(), coords.lineartransform(coordinate)))
        assert product.done() and source.done()
    for left, right in zip(*outputs):
        _assert_value(left, right)


@pytest.mark.parametrize(
    "limits,crop,expected",
    [([0, 6], False, [1.2666666666666667e9, 1.5666666666666667e9, 2.4666666666666667e9]),
     ([1, 5], True, [1.6e9]), ([3, 3], False, [2.1e9])],
)
def test_rebin_sampled_frequencies_survive_native_reopen(casa, tmp_path, limits, crop, expected):
    cs = casa.coordsys().newcoordsys(direction=True, spectral=True)
    frequency = np.array([1, 1.1, 1.4, 2.1, 2.2, 2.5, 2.6]) * 1e9
    assert cs.setspectral(frequencies={"value": frequency, "unit": "Hz"})
    region = casa.regionmanager().box([0, 0, limits[0]], [3, 3, limits[1]])
    for index, writer in enumerate((casa, tools)):
        source = writer.image()
        assert source.fromshape(shape=[4, 4, 7], csys=cs.torecord())
        path = str(tmp_path / f"sampled-{index}.im")
        result = source.rebin(path, [1, 1, 3], region=region, crop=crop)
        assert result.coordsys().referencepixel()["numeric"][-1] == pytest.approx(-1 / 3)
        assert result.coordsys([2]).referencepixel()["numeric"][0] == pytest.approx(-1 / 3)
        for channel, value in enumerate(expected):
            assert result.toworld([0, 0, channel])["numeric"][-1] == pytest.approx(value)
        sibling = writer.image()
        assert sibling.open(path)
        assert sibling.coordsys().referencepixel()["numeric"][-1] == pytest.approx(0)
        assert sibling.done()
        assert result.coordsys().referencepixel()["numeric"][-1] == pytest.approx(-1 / 3)
        assert result.done() and source.done()
        for reader in (casa, tools):
            reopened = reader.image()
            assert reopened.open(path)
            reference = reopened.coordsys().referencepixel()["numeric"][-1]
            assert reference == pytest.approx(0)
            stored = reopened.coordsys().torecord()["spectral1"]["tabular"]
            np.testing.assert_allclose(stored["crpix"], [0])
            np.testing.assert_allclose(stored["crval"], expected[:1])
            for channel, value in enumerate(expected):
                assert reopened.toworld([0, 0, channel])["numeric"][-1] == pytest.approx(value)
            assert reopened.done()


def test_rebin_selected_tabular_axis_keeps_live_reference(casa):
    for api in (casa, tools):
        system, source = api.coordsys(), api.image()
        product = full = selected = None
        try:
            assert system.newcoordsys(tabular=True)
            assert system.settabular(pixel=np.arange(7.), world=[3., 4., 10., 14., 64., 80., 90.])
            assert source.fromarray(pixels=np.arange(7, dtype=np.float32), csys=system.torecord())
            product = source.rebin("", [3])
            np.testing.assert_array_equal(product.getchunk(), [1., 4., 6.])
            full, selected = product.coordsys(), product.coordsys([0])
            for coordinate in (full, selected):
                assert coordinate.referencepixel()["numeric"][0] == pytest.approx(-1 / 3)
                assert coordinate.toworld([1.])["numeric"][0] == pytest.approx(20.5)
        finally:
            for tool in (selected, full, product, source, system):
                if tool is not None:
                    tool.done()


@pytest.mark.parametrize("form", ["inline", "file"])
@pytest.mark.parametrize("case", ["single", "off_image", "off_plane", "range", "corr"])
def test_rebin_crtf_admission_classifies_regions_after_clipping(casa, tmp_path, form, case):
    box = "box[[2pix,3pix],[5pix,6pix]]"
    text = {
        "single": box,
        "off_image": box + "\nbox[[100pix,100pix],[110pix,111pix]]",
        "off_plane": box + "\ncircle[[8pix,8pix], 3pix], range=[7pix,8pix]",
        "range": box + ", range=[0pix,4pix]",
        "corr": box + ", corr=[I]",
    }[case]
    if form == "file":
        path = tmp_path / "section.crtf"
        path.write_text(f"#CRTFv0\n{text}\n")
        region = str(path)
    else:
        region = text
    pixels = np.arange(15 * 17 * 5, dtype=np.float32).reshape(15, 17, 1, 5)
    expected = pixels[2:6, 3:7].reshape(2, 2, 2, 2, 1, 5).mean(axis=(1, 3))
    for api in (casa, tools):
        image, product = api.image(), None
        try:
            assert image.fromarray(pixels=pixels)
            if case in {"range", "corr"}:
                with pytest.raises(RuntimeError, match="Only a single n-dimensional rectangular region"):
                    image.rebin("", [2, 2, 1, 1], region=region)
            else:
                product = image.rebin("", [2, 2, 1, 1], region=region)
                np.testing.assert_array_equal(product.getchunk(), expected)
                np.testing.assert_array_equal(product.getchunk(getmask=True), True)
            np.testing.assert_array_equal(image.getchunk(), pixels)
        finally:
            if product is not None:
                product.done()
            image.done()


def test_rebin_selects_multiple_beams_and_rejects_spectral_rebin(casa):
    selection = casa.regionmanager().box([0, 0, 1, 1], [4, 3, 1, 3])
    for backend in (casa, tools):
        source = backend.image()
        assert source.fromshape(shape=[5, 4, 2, 4])
        for channel in range(4):
            for stokes in range(2):
                assert source.setrestoringbeam(
                    f"{channel + stokes + 2}arcsec", "1arcsec", "2rad",
                    channel=channel, polarization=stokes,
                )
        with pytest.raises(RuntimeError, match="spectral axis cannot be rebinned"):
            source.rebin("", [1, 1, 1, 2])
        product = source.rebin("", [2, 2], region=selection)
        np.testing.assert_array_equal(product.shape(), [3, 2, 1, 3])
        for index in range(3):
            original = source.restoringbeam(channel=index + 1, polarization=1)
            assert product.restoringbeam(channel=index, polarization=0) == original
        assert product.done() and source.done()


def test_rebin_validation_and_overwrite(casa, tmp_path):
    for index, backend in enumerate((casa, tools)):
        target = tmp_path / f"output-{index}.im"
        source = backend.image()
        with pytest.raises(RuntimeError, match="Unable to create image"):
            source.rebin("", 2)
        assert source.fromshape(shape=[5, 4, 2])
        for factors, kwargs, diagnostic in (
            ([], {}, "factor vector cannot be empty"),
            ([1], {}, "All rebinning factors are 1"),
            ([-1, 2], {}, "factors must be positive"),
            ([0, 2], {}, "factors must be positive"),
            ([2, 1, 1, 1], {}, "Factor vector length"),
            ([2, 1, 2], {}, "polarization axis cannot be rebinned"),
            ([6, 2], {"crop": True}, "must be <= ?trc"),
        ):
            with pytest.raises(RuntimeError, match=diagnostic):
                source.rebin(str(target), factors, **kwargs)
            assert not target.exists()
        product = source.rebin(str(target), 2)
        np.testing.assert_array_equal(product.shape(), [3, 4, 2])
        assert product.done()
        with pytest.raises(RuntimeError, match="All rebinning factors are 1"):
            source.rebin(str(target), [1], overwrite=True)
        assert product.open(str(target))
        np.testing.assert_array_equal(product.shape(), [3, 4, 2])
        assert product.done()
        with pytest.raises(RuntimeError, match="exists|already a file"):
            source.rebin(str(target), [3, 2])
        product = source.rebin(str(target), [3, 2], overwrite=True)
        np.testing.assert_array_equal(product.shape(), [2, 2, 2])
        assert product.done() and source.done()
        for kind, diagnostic in (("d", "double precision"), ("cd", r"complex<double> precision")):
            assert source.fromshape(shape=[4, 4], type=kind)
            with pytest.raises(RuntimeError, match=diagnostic):
                source.rebin("", 2)
            assert source.done()


def test_rebin_one_bin_larger_than_the_read_tile():
    shape = (513, 513)
    values = np.arange(np.prod(shape), dtype=np.float32).reshape(shape) % 11
    valid = values != 3
    expected, mask = _means(values, valid, shape)
    source = tools.image()
    assert source.fromarray(pixels=values)
    assert source.putregion(pixelmask=valid)
    product = source.rebin("", shape)
    np.testing.assert_array_equal(product.getchunk(), expected)
    np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
    assert product.done() and source.done()


def test_rebin_drops_an_all_valid_output_mask(casa):
    for backend in (casa, tools):
        source = backend.image()
        assert source.fromshape(shape=[4, 4])
        assert source.putregion(pixelmask=np.eye(4, dtype=bool) | np.fliplr(np.eye(4, dtype=bool)))
        product = source.rebin("", [2, 2])
        assert product.getchunk(getmask=True).all()
        assert product.maskhandler("get") == ["T"]
        assert product.maskhandler("default") == [""]
        assert product.done() and source.done()


def test_rebin_accepts_tuple_factors_without_a_post_write_error(casa, tmp_path):
    source = casa.image()
    assert source.fromshape(shape=[4, 4])
    oracle = tmp_path / "oracle.im"
    with pytest.raises(SystemError):
        source.rebin(str(oracle), (2, 2))
    assert oracle.is_dir()
    assert source.done()

    source = tools.image()
    assert source.fromshape(shape=[4, 4])
    product = source.rebin(str(tmp_path / "product.im"), (2, 2))
    np.testing.assert_array_equal(product.shape(), [2, 2])
    assert product.done() and source.done()
