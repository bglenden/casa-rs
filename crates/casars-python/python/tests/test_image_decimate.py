"""Axis decimation through the public image tool and native image format."""

import numpy as np
import pytest

from casars import tools
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
