"""Native image interoperability for signed coordinate-axis maps."""

from pathlib import Path
import shutil

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.mark.parametrize("fixture", ["swapped", "interleaved"])
def test_native_mapped_image_write_and_read_in_both_implementations(tmp_path, fixture):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    data_dir = Path(__file__).resolve().parents[3] / "casa-images/tests/data/casa-6.7.6/axis_maps"
    source_path = tmp_path / "source.image"
    shutil.copytree(data_dir / f"{fixture}.image", source_path)
    source = casa.image()
    assert source.open(str(source_path))
    coordinates = source.coordsys()
    record = coordinates.torecord()
    shape = tuple(source.shape())
    pixels = source.getchunk()
    point = [float(length - 1) for length in shape]
    world = source.toworld(point, "nqs")
    expected_names = coordinates.names()
    assert source.done()
    valid = np.ones(shape, dtype=bool, order="F")
    valid[(0,) * len(shape)] = False

    for index, backend in enumerate((tools, casa)):
        path = str(tmp_path / f"written-{index}.image")
        writer = backend.image()
        assert writer.fromarray(path, pixels)
        assert writer.setcoordsys(record)
        assert writer.putregion(pixelmask=valid)
        assert writer.done()
        for reader_backend in (tools, casa):
            reader = reader_backend.image()
            assert reader.open(path)
            assert tuple(reader.shape()) == shape
            np.testing.assert_array_equal(reader.getchunk(), pixels)
            np.testing.assert_array_equal(reader.getchunk(getmask=True), valid)
            reopened = reader.coordsys()
            assert reopened.names() == expected_names
            actual = reopened.torecord()
            for key, value in record.items():
                if key.startswith(("pixelmap", "worldmap", "pixelreplace", "worldreplace")):
                    np.testing.assert_array_equal(actual[key], value, err_msg=key)
            _assert_value(reader.toworld(point, "nqs"), world)
            np.testing.assert_allclose(reader.topixel(world)["numeric"], point, atol=5e-7)
            assert reader.done()


@pytest.mark.parametrize("operation", ["decimate", "rebin", "copy"])
def test_image_operations_use_the_mapped_frequency_pixel(tmp_path, operation):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    cs = casa.coordsys().newcoordsys(direction=True, stokes="I", spectral=True)
    assert cs.setreferencepixel([1.0, 2.0, 0.0, 3.0])
    assert cs.setreferencevalue([1.1, 0.3, 1.0, 1.4e9])
    assert cs.setincrement([-1e-4, 1e-4, 1.0, 2e6])
    assert cs.transpose([0, 3, 1, 2])
    record = cs.torecord()
    if operation == "copy":
        record["worldmap0"] = [1, 2]
        record["worldmap1"] = [3]
        record["worldmap2"] = [0]
    path = str(tmp_path / "channels.image")
    data = np.arange(120, dtype=np.float32).reshape((5, 6, 4, 1), order="F")
    writer = casa.image()
    assert writer.fromarray(path, data, csys=record)
    assert writer.done()

    products = []
    for backend in (tools, casa):
        image = backend.image()
        assert image.open(path)
        if operation == "decimate":
            product = image.decimate(axis=1, factor=2, method="copy")
        elif operation == "rebin":
            product = image.rebin("", [1, 2, 1, 1])
        else:
            region = casa.regionmanager().box([0, 1, 0, 0], [4, 4, 3, 0])
            product = image.newimagefromimage(infile=path, region=region, dropdeg=True)
        products.append((image, product))

    (_, left), (_, right) = products
    _assert_value(left.getchunk(), right.getchunk())
    _assert_value(left.coordsys().increment(), right.coordsys().increment())
    _assert_value(left.coordsys().referencepixel(), right.coordsys().referencepixel())
    point = [3.0, 1.0, 2.0] if operation == "copy" else [3.0, 1.0, 2.0, 0.0]
    _assert_value(left.toworld(point), right.toworld(point))
    if operation == "copy":
        np.testing.assert_array_equal(left.coordsys().axesmap(), [2, 1, 0])
        assert left.toworld(point)["numeric"][1] == 1.398e9
        np.testing.assert_array_equal(left.getchunk(), data[:, 1:5, :, 0])
    elif operation == "decimate":
        np.testing.assert_array_equal(left.getchunk(), data[:, ::2, :, :])
    else:
        np.testing.assert_array_equal(left.getchunk(), (data[:, ::2] + data[:, 1::2]) / 2)
    for image, product in products:
        assert product.done() and image.done()


@pytest.mark.parametrize("writer", ["casa-rs", "CASA"])
def test_mapped_fits_round_trip_keeps_stokes_and_frequency_pixel_axes(tmp_path, writer):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    cs = casa.coordsys().newcoordsys(stokes="I,Q", spectral=True)
    assert cs.setreferencepixel([0.0, 0.0])
    assert cs.setreferencevalue([1.0, 1.4e9])
    assert cs.setincrement([1.0, 2e6])
    record = cs.torecord()
    record["worldmap0"] = [1]
    record["worldmap1"] = [0]
    source = (casa if writer == "CASA" else tools).image()
    data = np.array([[1, 2], [3, 4]], dtype=np.float32)
    assert source.fromarray(pixels=data, csys=record)
    fits = str(tmp_path / "mapped.fits")
    assert source.tofits(fits, stokeslast=False)
    restored = tools.image()
    assert restored.fromfits(infile=fits)
    np.testing.assert_array_equal(restored.getchunk(), data)
    np.testing.assert_array_equal(restored.coordsys().axesmap(), [1, 0])
    for pixel, world in (
        ([0.0, 0.0], [1.4e9, 1.0]),
        ([1.0, 0.0], [1.4e9, 2.0]),
        ([0.0, 1.0], [1.402e9, 1.0]),
    ):
        np.testing.assert_allclose(restored.toworld(pixel)["numeric"], world)
        np.testing.assert_allclose(restored.topixel(world)["numeric"], pixel)
    assert restored.done() and source.done()


def test_copy_with_a_coupled_hidden_local_pixel(tmp_path):
    casa = pytest.importorskip("casatools")
    cs = casa.coordsys().newcoordsys(spectral=True, linear=2)
    record = cs.torecord()
    record["linear1"]["pc"] = np.array([[1.0, 1.0], [1.0, 2.0]])
    record["linear1"]["crval"] = np.array([3.0, 17.0])
    record["linear1"]["cdelt"] = np.array([1.0, 1.0])
    for domain in ("pixel", "world"):
        record[f"{domain}map1"] = np.array([-1, 1], dtype=np.int32)
    record["pixelreplace1"] = np.array([2.0, 0.0])
    record["worldreplace1"] = np.array([3.0, 0.0])
    path = str(tmp_path / "coupled.image")
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    writer = casa.image()
    assert writer.fromarray(path, pixels, csys=record)
    expected = writer.toworld([0.0, 1.0])
    assert expected["numeric"][1] == 21.0
    assert writer.done()

    product = tools.image().newimagefromimage(infile=path)
    np.testing.assert_array_equal(product.getchunk(), pixels)
    _assert_value(product.toworld([0.0, 1.0]), expected)
    np.testing.assert_array_equal(product.coordsys().torecord()["pixelmap1"], [-1, 1])
    assert product.done()
