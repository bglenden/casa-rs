"""CASA geometry, mask updates, and native readback for image.putregion."""

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize("usemask", [False, True])
def test_putregion_writes_new_validity_before_using_the_mask(casa, tmp_path, usemask):
    values = np.arange(20, dtype=np.float32).reshape(4, 5)
    valid = np.array([[1, 0, 1], [0, 1, 0]], dtype=np.int32)
    selected = np.s_[1:3, 1:4]
    region = casa.regionmanager().box([1, 1], [2, 3])
    expected_pixels = values.copy()
    expected_pixels[selected] = np.where(valid, 55, values[selected]) if usemask else 55
    expected_mask = np.ones(values.shape, dtype=bool)
    expected_mask[selected] = valid
    histories = []
    for backend in (casa, tools):
        path = str(tmp_path / f"{backend.__name__}.im")
        writer = backend.image()
        assert writer.fromarray(path, values)
        assert writer.putregion(
            np.full(valid.shape, 55, dtype=np.float32), valid, region,
            usemask=usemask, locking=False,
        )
        histories.append(writer.history()[-2:])
        assert writer.done()
        for reader_module in (casa, tools):
            reader = reader_module.image()
            assert reader.open(path)
            np.testing.assert_array_equal(reader.getchunk(), expected_pixels)
            np.testing.assert_array_equal(reader.getchunk(getmask=True), expected_mask)
            assert reader.done()
    assert histories[0] == histories[1]


@pytest.mark.parametrize("replicate", [False, True])
def test_putregion_missing_axes_and_strided_writes(casa, replicate):
    region = casa.regionmanager().box([0, 1], [4, 3], [2, 1])
    expected = np.zeros((5, 4), dtype=np.float32)
    if replicate:
        expected[::2, 1:] = [[8], [9], [10]]
    else:
        expected[:3, 1] = [8, 9, 10]
    for backend in (casa, tools):
        image = backend.image()
        assert image.fromshape(shape=[5, 4])
        assert image.putregion([8, 9, 10], region=region, replicate=replicate)
        np.testing.assert_array_equal(image.getchunk(), expected)
        assert image.done()


@pytest.mark.parametrize("pixel_type,dtype", [
    ("f", np.float32), ("d", np.float64), ("c", np.complex64), ("cd", np.complex128),
])
def test_putregion_validates_shape_before_creating_mask(casa, pixel_type, dtype):
    values = np.arange(6, dtype=dtype).reshape(2, 3)
    if np.issubdtype(dtype, np.complexfloating):
        values += 2j
    for backend in (casa, tools):
        image = backend.image()
        assert image.fromshape(shape=list(values.shape), type=pixel_type)
        assert image.putregion(values)
        before = image.maskhandler("default")
        with pytest.raises(RuntimeError, match="do not match|does not match|different shapes"):
            image.putregion(values, pixelmask=np.ones((3, 2), dtype=bool))
        assert image.maskhandler("default") == before
        pixels = image.getchunk()
        assert pixels.dtype == (
            np.complex128 if np.issubdtype(dtype, np.complexfloating) else np.float64
        )
        np.testing.assert_array_equal(pixels, values)
        assert image.done()


def test_putregion_mask_only_controls_later_default_masking(casa):
    values = np.arange(12, dtype=np.float32).reshape(3, 4)
    region = casa.regionmanager().box([1, 1], [2, 3])
    valid = np.ones(values.shape, dtype=bool)
    valid[1:, 1:] = [[True, False, True], [False, True, False]]
    for backend in (casa, tools):
        image = backend.image()
        assert image.fromarray(pixels=values)
        assert image.putregion(pixelmask=valid[1:, 1:], region=region)
        np.testing.assert_array_equal(image.getchunk(), values)
        assert image.putregion(np.full(values.shape, 4, dtype=np.float32))
        np.testing.assert_array_equal(image.getchunk(), np.where(valid, 4, values))
        np.testing.assert_array_equal(image.getchunk(getmask=True), valid)
        assert image.done()


def test_putregion_rejects_read_only_fits_without_modifying_the_file(tmp_path):
    path = tmp_path / "image.fits"
    original = _hdu(True, [1, 2, 3, 4])
    path.write_bytes(original)
    image = tools.image()
    assert image.open(str(path))
    with pytest.raises(RuntimeError, match="not writable"):
        image.putregion(np.full((2, 2), 9, dtype=np.float32))
    assert path.read_bytes() == original
    np.testing.assert_array_equal(image.getchunk(), [[1, 3], [2, 4]])
    assert image.done()


def test_putregion_rejects_bad_arrays_without_taking_a_native_write_lock(casa, tmp_path):
    for index, backend in enumerate((casa, tools)):
        path = str(tmp_path / f"edit-{index}.im")
        image = backend.image()
        assert image.fromshape(path, [2, 2])
        assert image.unlock()
        with pytest.raises(RuntimeError, match="must specify at least"):
            image.putregion()
        np.testing.assert_array_equal(image.haslock(), [False, False])
        with pytest.raises(RuntimeError, match="different shapes"):
            image.putregion(np.ones((2, 2)), np.ones((2, 1), dtype=bool))
        np.testing.assert_array_equal(image.haslock(), [False, False])
        np.testing.assert_array_equal(image.getchunk(), np.zeros((2, 2)))
        assert image.done()


def test_putregion_writes_many_strided_mask_blocks_to_one_native_table(casa, tmp_path):
    shape = [512, 512]
    region = casa.regionmanager().box([0, 1], [510, 511], [2, 2])
    x, y = np.indices((256, 256))
    selected = (x + y) % 3 == 0
    expected = np.ones(shape, dtype=bool)
    expected[::2, 1::2] = selected
    for index, (writer, reader) in enumerate(((casa, tools), (tools, casa))):
        path = str(tmp_path / f"strided-{index}.im")
        image = writer.image()
        assert image.fromshape(path, shape)
        assert image.putregion(pixelmask=selected, region=region, replicate=True)
        assert image.done()
        reopened = reader.image()
        assert reopened.open(path)
        np.testing.assert_array_equal(reopened.getchunk(getmask=True), expected)
        np.testing.assert_array_equal(reopened.getchunk(), np.zeros(shape))
        assert reopened.done()


def test_putregion_uses_the_bounding_box_of_native_compound_regions(casa):
    shape = [10, 12, 5]
    rg = casa.regionmanager()
    oracle = casa.image()
    assert oracle.fromshape(shape=shape)
    coordinates = oracle.coordsys().torecord()
    regions = {
        "0": rg.fromtext("box [[1pix, 2pix], [4pix, 5pix]]", shape, coordinates),
        "1": rg.fromtext("box [[3pix, 4pix], [7pix, 9pix]]", shape, coordinates),
    }
    assert oracle.done()
    for region, target in (
        (rg.intersection(regions), np.s_[3:5, 4:6, :]),
        (rg.makeunion(regions), np.s_[1:8, 2:10, :]),
    ):
        expected = np.zeros(shape, dtype=np.float32)
        expected[target] = 7
        for backend in (casa, tools):
            image = backend.image()
            assert image.fromshape(shape=shape)
            assert image.putregion(expected[target], region=region)
            np.testing.assert_array_equal(image.getchunk(), expected)
            assert image.done()
