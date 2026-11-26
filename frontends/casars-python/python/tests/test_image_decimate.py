"""Axis decimation through the public image tool and native image format."""

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


@pytest.mark.parametrize("method", ["cOpY", "mean"])
def test_decimate_masked_pixels_and_tail_against_independent_reference(casa, method):
    values = np.arange(28, dtype=np.float32).reshape(7, 4)
    valid = np.ones_like(values, dtype=bool)
    valid[0, :2] = False
    valid[2:4, 0] = False
    valid[5, 3] = False
    if method.lower() == "copy":
        expected = values[::2]
        expected_valid = valid[::2]
    else:
        expected = np.array([[4, 5, 4, 5], [0, 11, 12, 13], [18, 19, 20, 19]])
        expected_valid = np.ones((3, 4), dtype=bool)
        expected_valid[1, 0] = False
    for backend in (casa, tools):
        source = backend.image()
        assert source.fromarray(pixels=values)
        assert source.putregion(pixelmask=valid)
        output = source.decimate(axis=0, factor=2, method=method)
        np.testing.assert_array_equal(output.getchunk(), expected)
        np.testing.assert_array_equal(output.getchunk(getmask=True), expected_valid)
        assert output.maskhandler("get") == ["T"]
        assert output.maskhandler("default") == [""]
        np.testing.assert_allclose(output.coordsys().referencepixel()["numeric"], [1.5, 2])
        np.testing.assert_allclose(output.coordsys().increment()["numeric"], [-2, 1])
        assert "Decimated axis 0" in output.history()[-3]
        assert output.done() and source.done()


def test_decimate_preserves_complex_science_and_native_metadata(casa, tmp_path):
    data = np.arange(42, dtype=np.float32).reshape(7, 6)
    values = (data + 2j * data).astype(np.complex64)
    selected = values[1:6, 1:5]
    expected = (selected[:, 0::2] + selected[:, 1::2]) / 2
    region = casa.regionmanager().box([1, 1], [5, 4])
    for index, writer in enumerate((tools, casa)):
        source = writer.image()
        assert source.fromarray(pixels=values)
        assert source.setbrightnessunit("Jy/beam")
        assert source.setmiscinfo({"target": "source"})
        assert source.setrestoringbeam("0.2arcmin", "1arcsec", "2rad")
        destination = str(tmp_path / f"decimate-{index}.im")
        output = source.decimate(destination, 1, 2, "M", region)
        np.testing.assert_array_equal(output.getchunk(), expected)
        assert output.done() and source.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(destination)
            np.testing.assert_array_equal(image.getchunk(), expected)
            assert image.getchunk(getmask=True).all()
            assert image.brightnessunit() == "Jy/beam"
            assert image.miscinfo()["target"] == "source"
            assert image.restoringbeam()["positionangle"] == {"unit": "rad", "value": 2.0}
            cs = image.coordsys()
            np.testing.assert_allclose(cs.referencepixel()["numeric"], [2.0, 1.0])
            np.testing.assert_allclose(cs.increment()["numeric"], [-1, 2])
            assert image.done()


def test_decimate_protects_destinations_and_rejects_invalid_domains(casa, tmp_path):
    for index, backend in enumerate((casa, tools)):
        source = backend.image()
        assert source.fromshape(shape=[7, 5])
        target = tmp_path / f"product-{index}.im"
        for keywords, diagnostic in (
            ({"factor": -1}, "factor cannot be negative"),
            ({"factor": 0}, "factor must be positive"),
            ({"factor": 8}, "factor cannot be greater"),
            ({"axis": -1}, "axis cannot be negative"),
            ({"axis": 2}, "less than the number of axes"),
            ({"method": "sum"}, "Unsupported decimation method"),
        ):
            with pytest.raises(RuntimeError, match=diagnostic):
                source.decimate(str(target), **keywords)
            assert not target.exists()
        mask = np.ones((7, 5), dtype=bool)
        mask[2:4, :] = False
        assert source.putregion(pixelmask=mask)
        with pytest.raises(RuntimeError, match="All selected pixels are masked"):
            source.decimate(str(target), factor=2, method="mean")
        assert not target.exists()
        no_op = source.decimate()
        _assert_value(no_op.getchunk(getmask=True), mask)
        assert no_op.done() and source.done()
        assert source.fromshape(shape=[4, 4], type="d")
        with pytest.raises(RuntimeError, match="double precision pixel values"):
            source.decimate(factor=2)
        assert source.done()


def test_decimate_rejects_multiple_beams(casa):
    for backend in (casa, tools):
        source = backend.image()
        assert source.fromshape(shape=[4, 5, 2, 6])
        assert source.setrestoringbeam("3arcsec", "1arcsec", "2rad", channel=0, polarization=0)
        with pytest.raises(RuntimeError, match="does not support images with multiple beams"):
            source.decimate(factor=2)
        assert source.done()


@pytest.mark.parametrize("method", ["set", "putregion", "calcmask", "clear"])
def test_decimate_anonymous_pixel_mask_can_be_edited(casa, method):
    for backend in (casa, tools):
        source = backend.image()
        assert source.fromshape(shape=[4, 4])
        mask = np.ones((4, 4), dtype=bool)
        mask[0, 0] = False
        assert source.putregion(pixelmask=mask)
        product = source.decimate(factor=2)
        assert not product.getchunk(getmask=True)[0, 0]
        if method == "set":
            assert product.set(pixelmask=1)
        elif method == "putregion":
            assert product.putregion(pixelmask=np.ones((2, 4), dtype=bool))
        elif method == "calcmask":
            assert product.calcmask("T", "named", asdefault=False)
            assert not product.getchunk(getmask=True)[0, 0]
            product.maskhandler("set", ["named"])
            assert product.getchunk(getmask=True).all()
            product.maskhandler("delete", ["named"])
        else:
            product.maskhandler("set", [])
        assert product.getchunk(getmask=True).all()
        assert product.maskhandler("get") == ["T"]
        assert product.maskhandler("default") == [""]
        assert product.done() and source.done()


@pytest.mark.parametrize("complex_pixels", [False, True])
@pytest.mark.parametrize("samples,expected", [([1e10, 1.0, -1e10], 1 / 3), ([2e38, 2e38], 2e38)])
def test_decimate_mean_accumulates_before_rounding(casa, complex_pixels, samples, expected):
    pixels = np.broadcast_to(np.array(samples, dtype=np.float32)[:, None], (len(samples), 2)).copy()
    if complex_pixels:
        pixels = pixels + 1j * pixels
        expected = np.complex64(complex(expected, expected))
    else:
        expected = np.float32(expected)
    for backend in (casa, tools):
        image = backend.image()
        assert image.fromarray(pixels=pixels)
        result = image.decimate(factor=len(samples), method="mean")
        np.testing.assert_array_equal(result.getchunk(), [[expected, expected]])
        assert result.done() and image.done()


def test_decimate_direct_fits_with_stretched_mask(casa, tmp_path):
    path = tmp_path / "pixels.fits"
    path.write_bytes(_hdu(True, [1, -999, 3, 4]))
    mask_path = tmp_path / "mask.im"
    mask = casa.image()
    assert mask.fromarray(str(mask_path), np.array([[0], [1]], dtype=np.float32))
    assert mask.done()
    for index, backend in enumerate((casa, tools)):
        source = backend.image()
        assert source.open(str(path))
        expression = f'"{mask_path}" > 0'
        with pytest.raises(RuntimeError, match="mask should not be extended"):
            source.decimate(axis=1, factor=2, method="mean", mask=expression)
        destination = str(tmp_path / f"decimated-{index}.im")
        output = source.decimate(destination, axis=1, factor=2, method="mean", mask=expression, stretch=True)
        np.testing.assert_array_equal(output.getchunk(), [[0.0], [4.0]])
        np.testing.assert_array_equal(output.getchunk(getmask=True), [[False], [True]])
        assert output.maskhandler("get") == ["mask0"]
        assert source.done() and output.done()
        for reader in (casa, tools):
            result = reader.image()
            assert result.open(destination)
            np.testing.assert_array_equal(result.getchunk(getmask=True), [[False], [True]])
            np.testing.assert_array_equal(result.getchunk(), [[0.0], [4.0]])
            assert result.done()


def test_decimate_non_direction_coordinates_honor_implicit_and_explicit_regions(casa):
    shape = (4, 5, 6)
    values = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
    region = casa.regionmanager().box([0, 0, 0], [3, 4, 5])
    for backend in (casa, tools):
        source = backend.image()
        cs = backend.coordsys().newcoordsys(linear=3)
        assert source.fromarray(pixels=values, csys=cs.torecord())
        implicit = source.decimate(factor=2, axis=2)
        np.testing.assert_array_equal(implicit.getchunk(), values[:1, :1, ::2])
        explicit = source.decimate(factor=2, region=region)
        np.testing.assert_array_equal(explicit.getchunk(), values[::2])
        with pytest.raises(RuntimeError, match="cursorShape <=0 or > latticeShape"):
            source.decimate(factor=2)
        assert implicit.done() and explicit.done() and source.done()


def test_decimate_two_linear_axes_with_spectral_keeps_the_full_implicit_region(casa):
    values = np.arange(4 * 4 * 4, dtype=np.float32).reshape(4, 4, 4)
    for backend in (casa, tools):
        source = backend.image()
        cs = backend.coordsys().newcoordsys(spectral=True, linear=2)
        assert source.fromarray(pixels=values, csys=cs.torecord())
        result = source.decimate(axis=2, factor=2)
        np.testing.assert_array_equal(result.getchunk(), values[:, :, ::2])
        assert result.done() and source.done()


def test_decimate_stokes_axes_remain_usable_across_two_calls(casa):
    values = np.arange(2 * 2 * 4 * 5, dtype=np.float32).reshape(2, 2, 4, 5)
    for backend in (casa, tools):
        image = backend.image()
        assert image.fromarray(pixels=values)
        polarizations = image.decimate(axis=2, factor=2)
        assert polarizations.coordsys().stokes() == ["I", "Q", "U", "V"]
        result = polarizations.decimate(axis=3, factor=2)
        assert result.coordsys().stokes() == ["I", "Q"]
        np.testing.assert_array_equal(result.getchunk(), values[:, :, ::2, ::2])
        assert image.done() and polarizations.done() and result.done()


@pytest.mark.parametrize("start,stride,world", [(1, 1, [18.25, 20.25]), (0, 2, [3, 7])])
def test_decimate_tabular_lookup_values_in_native_output(casa, tmp_path, start, stride, world):
    values = np.arange(5, dtype=np.float32)
    cs = casa.coordsys().newcoordsys(tabular=True)
    assert cs.settabular(pixel=[0, 1, 2, 3, 4], world=[3, 4, 10, 14, 64])
    region = casa.regionmanager().box([start], [4], [stride])
    for index, backend in enumerate((tools, casa)):
        image = backend.image()
        assert image.fromarray(pixels=values, csys=cs.torecord())
        path = str(tmp_path / f"table-{index}.im")
        result = image.decimate(path, factor=2, region=region)
        assert image.done() and result.done()
        for reader in (casa, tools):
            reopened = reader.image()
            assert reopened.open(path)
            for pixel, expected in enumerate(world):
                np.testing.assert_allclose(reopened.toworld([float(pixel)])["numeric"], [expected])
            np.testing.assert_array_equal(reopened.getchunk(), values[start::2 * stride])
            assert reopened.done()
