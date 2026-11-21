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
