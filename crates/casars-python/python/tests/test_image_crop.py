"""Crop masked image edges through the public CASA image tool."""

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu
from test_image_workflow import _assert_value


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def test_crop_selected_axes_with_two_way_native_readback(casa, tmp_path):
    shape = [7, 6]
    values = np.arange(42, dtype=np.float32).reshape(shape)
    mask = np.zeros(shape, dtype=bool)
    mask[1:6, 1:5] = True
    output_coordinates = []
    for index, (writer, reader) in enumerate(((casa, tools), (tools, casa))):
        source = str(tmp_path / f"source-{index}.im")
        target = str(tmp_path / f"product-{index}.im")
        parent = writer.image()
        assert parent.fromarray(source, values)
        assert parent.putregion(pixelmask=mask)
        assert parent.setbrightnessunit("Jy/pixel")
        assert parent.setmiscinfo({"observer_note": "crop test"})
        assert parent.setrestoringbeam("3arcsec", "2arcsec", "15deg")
        child = parent.crop(outfile=target, axes=[0, 0])
        assert parent.done()
        _assert_value(child.getchunk(), values[1:6, :].astype(np.float64))
        _assert_value(child.getchunk(getmask=True), mask[1:6, :])
        assert child.brightnessunit() == "Jy/pixel"
        assert child.miscinfo() == {"observer_note": "crop test"}
        _assert_value(child.restoringbeam()["major"], {"value": 3.0, "unit": "arcsec"})
        history = child.history()
        assert history[-4] == "Ran ia.crop"
        assert history[-2:] == [
            f"Original {source} size => [7, 6]",
            f"New {target} size => [5, 6]",
        ]
        assert child.done()
        other = reader.image()
        assert other.open(target)
        _assert_value(other.getchunk(), values[1:6, :].astype(np.float64))
        _assert_value(other.getchunk(getmask=True), mask[1:6, :])
        coordinates = other.coordsys()
        _assert_value(coordinates.referencepixel()["numeric"], np.array([2.0, 3.0]))
        output_coordinates.append(coordinates.torecord())
        assert coordinates.done() and other.done()
    _assert_value(*output_coordinates)


def test_crop_combines_spatial_spectral_and_stokes_selection(casa):
    shape = [7, 6, 4, 5]
    values = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
    valid = np.zeros(shape, dtype=bool)
    valid[1:6, 1:5] = True
    expected = values[1:6, 1:5, :2, 1:4].astype(np.float64)
    for backend in (casa, tools):
        parent = backend.image()
        assert parent.fromarray(pixels=values)
        assert parent.putregion(pixelmask=valid)
        with pytest.raises(RuntimeError, match="box and regionPtr"):
            parent.crop(box="0,0,6,5")
        with pytest.raises(RuntimeError, match="regionPtr and regionName, chans"):
            parent.crop(chans="1~3")
        child = parent.crop(box="0,0,6,5", chans="1~3", stokes="IQ", region=[])
        _assert_value(child.getchunk(), expected)
        assert child.getchunk(getmask=True).all()
        assert child.done() and parent.done()


def test_crop_fits_region_with_expression_and_detached_result(casa, tmp_path):
    path = tmp_path / "source.fits"
    content = _hdu(True, [1, -999, 3, 4])
    path.write_bytes(content)
    rg = casa.regionmanager()
    region = rg.box([0, 0], [1, 1])
    for index, backend in enumerate((casa, tools)):
        output = str(tmp_path / f"output-{index}.im")
        parent = backend.image()
        assert parent.open(str(path))
        child = parent.crop(
            outfile=output, axes=[0], region=region, mask=f'"{path}" > 2', wantreturn=False,
        )
        assert not child.isopen() and child.done() and parent.done()
        assert child.open(output)
        _assert_value(child.getchunk(), np.array([[1, 3], [np.nan, 4]], dtype=np.float64))
        _assert_value(child.getchunk(getmask=True), np.array([[False, True], [False, True]]))
        assert child.done()
    assert path.read_bytes() == content


def test_crop_rejects_invalid_axes_and_unfindable_valid_planes(casa, tmp_path):
    for index, backend in enumerate((casa, tools)):
        image = backend.image()
        assert image.fromshape(shape=[7, 6])
        with pytest.raises(RuntimeError, match="All axes values"):
            image.crop(axes=[-1])
        with pytest.raises(RuntimeError, match="Illegal axis number"):
            image.crop(axes=[2])
        for rows, message in (
            (slice(0, 0), "completely masked"),
            (slice(0, 2), "minFound && maxFound"),
        ):
            mask = np.zeros((7, 6), dtype=bool)
            mask[rows, 1:5] = True
            assert image.putregion(pixelmask=mask)
            path = tmp_path / f"failed-{index}.im"
            with pytest.raises(RuntimeError, match=message):
                image.crop(outfile=str(path), axes=[0])
            assert not path.exists()
            _assert_value(image.getchunk(getmask=True), mask)
        assert image.done()


def test_crop_stretches_mask_and_records_shape_history_with_history_disabled(casa, tmp_path):
    mask_path = str(tmp_path / "mask.im")
    writer = casa.image()
    assert writer.fromarray(mask_path, np.array([[0], [1], [1], [1], [1], [1], [0]], dtype=np.float32))
    assert writer.done()
    expression = f'"{mask_path}" > 0'
    for backend in (casa, tools):
        source = backend.image()
        assert source.fromshape(shape=[7, 6])
        source.dohistory(False)
        with pytest.raises(RuntimeError, match="mask should not be extended"):
            source.crop(mask=expression)
        cropped = source.crop(mask=expression, stretch=True)
        assert cropped.shape().tolist() == [5, 6]
        assert cropped.getchunk(getmask=True).all()
        assert cropped.history()[-2:] == [
            "Original Temporary_Image size => [7, 6]",
            "New  size => [5, 6]",
        ]
        assert cropped.done() and source.done()


def test_crop_rejects_complex_images(casa):
    for backend in (casa, tools):
        source = backend.image()
        assert source.fromshape(shape=[3, 4], type="c")
        with pytest.raises(RuntimeError, match="only supports Float"):
            source.crop()
        assert source.done()
