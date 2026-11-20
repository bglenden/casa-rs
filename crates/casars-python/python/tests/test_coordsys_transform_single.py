"""The scalar pixel/world conversion APIs through both Python bindings."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize("format", ["n", "nq", "s", "m", "NQSM", ""])
def test_single_world_records_at_mixed_coordinates(casa, format):
    parameters = dict(direction=True, spectral=True, stokes=["I", "Q"], linear=2)
    rust = tools.coordsys().newcoordsys(**parameters)
    oracle = casa.coordsys().newcoordsys(**parameters)
    value = np.array([3.5, 2.25, 1, 4.0, 5.0, -6.0])
    actual = rust.toworld(value=value, format=format)
    expected = oracle.toworld(value=value, format=format)
    _assert_value(actual, expected)
    if format.lower().startswith("n"):
        np.testing.assert_allclose(rust.topixel(actual)["numeric"], value, atol=1e-7)


@pytest.mark.parametrize(
    "pixel,expected",
    [
        ([], [110, 190]),
        (np.array(6.0), [112, 190]),
        ([6], [112, 190]),
        ([6, 4, 2], [112, 184]),
        ({"numeric": [6.0, 4.0], "ar_type": "relative", "pw_type": "world"}, [112, 184]),
    ],
)
def test_single_transforms_are_affine_with_defaults_and_truncation(casa, pixel, expected):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(linear=2)
        assert cs.setreferencepixel([5.0, 2.0])
        assert cs.setreferencevalue([110.0, 190.0])
        assert cs.setincrement([2.0, -3.0])
        world = cs.toworld(pixel)
        assert world["pw_type"] == "world" and world["ar_type"] == "absolute"
        np.testing.assert_allclose(world["numeric"], expected)
        value = cs.topixel(value={"numeric": [112.0, 184.0], "pw_type": "pixel"})
        assert value["pw_type"] == "pixel" and value["ar_type"] == "absolute"
        np.testing.assert_allclose(value["numeric"], [6, 4])
        np.testing.assert_allclose(cs.topixel()["numeric"], [5, 2])
        ns = cs.toworld([6.0, 4.0], "nqsm")
        for field in ("quantity", "measure", "string"):
            np.testing.assert_allclose(cs.topixel({field: ns[field]})["numeric"], [6, 4])


def test_single_transforms_reject_invalid_input_without_editing_state(casa):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(linear=2)
        for value in (2, 2.0, [True], np.array([True])):
            with pytest.raises(RuntimeError, match="unsupported data type for pixel"):
                cs.toworld(value)
        with pytest.raises(RuntimeError, match="unsupported record type for pixel"):
            cs.toworld({})
        with pytest.raises(RuntimeError, match="incorrect data type"):
            cs.toworld({"numeric": [2, 3]})
        with pytest.raises(RuntimeError, match="Unrecognized format for world coordinate"):
            cs.topixel(2)
        with pytest.raises(RuntimeError, match="value is relative"):
            cs.topixel({"numeric": [2.0, 3.0], "ar_type": "relative"})
        np.testing.assert_array_equal(cs.referencepixel()["numeric"], [0, 0])


def test_public_single_transforms_use_the_native_record_in_all_four_directions(casa, tmp_path):
    for i, writer in enumerate((tools, casa)):
        cs = writer.coordsys().newcoordsys(spectral=True, linear=1)
        assert cs.setreferencepixel([3.0, 2.0])
        assert cs.setreferencevalue([1.42e9, 9.0])
        assert cs.setincrement([1000.0, -2.0])
        image = writer.image()
        path = str(tmp_path / f"coordinate-{i}.im")
        assert image.fromshape(path, [9, 5], csys=cs.torecord())
        assert image.done()
        for reader in (tools, casa):
            reopened = reader.image()
            assert reopened.open(path)
            cs = reopened.coordsys()
            world = cs.toworld([4.0, 3.0])
            np.testing.assert_allclose(world["numeric"], [1.420001e9, 7.0], rtol=0, atol=1e-7)
            np.testing.assert_allclose(cs.topixel(world)["numeric"], [4, 3], rtol=0, atol=1e-7)
            assert reopened.done()
