"""Public world-reference and increment edits against pinned CASA."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def test_world_edits_convert_declared_units_and_keep_casa_fallbacks(casa):
    for backend in (casa, tools):
        cs = backend.coordsys()
        assert cs.newcoordsys(direction=True, spectral=True, linear=2)
        assert cs.setreferencevalue([11.0, 22.0, 1.5e9, 3.0, 4.0])
        assert cs.setincrement(value=[7.0], type="linear")
        np.testing.assert_allclose(cs.increment()["numeric"], [-1, 1, 1000, 7, 4])
        assert cs.setincrement(value=["-2arcsec", "3arcsec"], type="direction")
        np.testing.assert_allclose(cs.increment(type="direction")["numeric"], [-2 / 60, 3 / 60])
        assert cs.setreferencevalue({"numeric": [1.42e9]}, "spectral")
        np.testing.assert_allclose(cs.referencevalue()["numeric"], [11, 22, 1.42e9, 3, 4])
        assert cs.setincrement([], "linear")
        np.testing.assert_allclose(cs.increment(type="linear")["numeric"], [11, 22])
        assert cs.setreferencevalue(["1deg", "2deg"], "direction")
        np.testing.assert_allclose(cs.referencevalue(type="direction")["numeric"], [60, 120])
        assert cs.done()


def test_world_edits_preserve_native_records_and_transform(casa, tmp_path):
    pixels = np.array([[1.5, 3.5], [2.0, 1.0]])
    expected = np.array([[4.0, 5.0], [10.0, 12.0]])
    for writer in (casa, tools):
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [4, 5], linear=True)
        cs = image.coordsys()
        assert cs.setreferencepixel([1.5, 2.0])
        assert cs.setreferencevalue([4.0, 10.0])
        assert cs.setincrement([0.5, -2.0])
        expected_reference = cs.referencevalue()["numeric"]
        assert image.setcoordsys(cs.torecord())
        assert cs.done() and image.done()
        for peer in (casa, tools):
            reopened = peer.image()
            assert reopened.open(path)
            coordinates = reopened.coordsys()
            np.testing.assert_allclose(coordinates.referencevalue()["numeric"], expected_reference)
            np.testing.assert_allclose(coordinates.toworldmany(pixels)["numeric"], expected)
            assert coordinates.done() and reopened.done()


def test_rejected_world_edits_preserve_the_coordinate_record():
    cs = tools.coordsys()
    assert cs.newcoordsys(linear=2)
    assert cs.setreferencevalue([11.0, 22.0])
    with pytest.raises(RuntimeError, match="increment cannot be zero"):
        cs.setincrement([3.0, 0.0])
    np.testing.assert_array_equal(cs.increment()["numeric"], [1.0, 1.0])
    with pytest.raises(RuntimeError, match="relative"):
        cs.setreferencevalue({"numeric": [4.0], "ar_type": "relative"})
    np.testing.assert_array_equal(cs.referencevalue()["numeric"], [11.0, 22.0])
    point = np.array([[1.0], [2.0]])
    np.testing.assert_array_equal(cs.toworldmany(point)["numeric"], [[12.0], [24.0]])
    assert cs.done()
