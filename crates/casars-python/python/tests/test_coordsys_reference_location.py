"""Reference anchoring through the public coordinate tool."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize(
    "pixels,world,mask,expected_world",
    [
        ([2, 3], None, None, [0, 0]),
        (np.array([2.8, 3.7]), [8.0], None, [8, 0]),
        (["2", "3"], {"numeric": [8.0, 9.0]}, [False], [8, 9]),
        ([2, 3], [8.0, 9.0, 10.0], np.array([True, True]), [8, 9]),
        ([2, 3], {"quantity": {"*1": {"value": 8.0, "unit": "km"},
                               "*2": {"value": 9.0, "unit": "km"}}}, None, [8, 9]),
    ],
)
def test_reference_location_forms_and_affine_values(casa, pixels, world, mask, expected_world):
    for backend in (casa, tools):
        cs = backend.coordsys().newcoordsys(linear=2)
        assert cs.setincrement([2.0, -3.0])
        kwargs = {name: value for name, value in [("world", world), ("mask", mask)] if value is not None}
        assert cs.setreferencelocation(pixels, **kwargs)
        np.testing.assert_allclose(cs.referencepixel()["numeric"], [2, 3])
        np.testing.assert_allclose(cs.referencevalue()["numeric"], expected_world)
        output = cs.toworldmany(np.array([[4.0], [5.0]]))["numeric"][:, 0]
        np.testing.assert_allclose(output, np.asarray(expected_world) + [4, -6])


def test_reference_location_default_and_invalid_lengths(casa):
    for backend in (casa, tools):
        cs = backend.coordsys().newcoordsys(linear=2)
        assert not cs.setreferencelocation()
        assert not cs.setreferencelocation(world=[9.0, 12.0])
        assert not cs.setreferencelocation([2, 3], [9.0, 12.0], [True])
        assert not cs.setreferencelocation([2, 3], [9.0, 12.0], [])
        assert not cs.setreferencelocation([], [9.0, 12.0])
        np.testing.assert_array_equal(cs.referencepixel()["numeric"], [0, 0])
        np.testing.assert_array_equal(cs.referencevalue()["numeric"], [0, 0])
        cs = backend.coordsys().newcoordsys(linear=1)
        assert cs.setreferencelocation(world=4.0)
        np.testing.assert_array_equal(cs.referencepixel()["numeric"], [-1])
        np.testing.assert_array_equal(cs.referencevalue()["numeric"], [4])


def test_reference_location_keeps_masked_axes_and_stokes():
    cs = tools.coordsys().newcoordsys(linear=2)
    assert cs.setreferencelocation([2, 3], [8.0, 9.0], [True, False])
    np.testing.assert_array_equal(cs.referencepixel()["numeric"], [2, 0])
    np.testing.assert_array_equal(cs.referencevalue()["numeric"], [8, 0])
    assert cs.setreferencelocation([5, 6], [11.0, 12.0], [False, False])
    np.testing.assert_array_equal(cs.referencepixel()["numeric"], [2, 0])
    np.testing.assert_array_equal(cs.referencevalue()["numeric"], [8, 0])

    cs = tools.coordsys().newcoordsys(direction=True, stokes=["I", "Q"])
    assert cs.setreferencelocation([2, 3, 1], [0.1, 0.2, 2.0])
    np.testing.assert_allclose(cs.referencepixel()["numeric"], [2, 3, 0])
    np.testing.assert_allclose(cs.referencevalue()["numeric"], [0.1, 0.2, 1])
    assert cs.stokes() == ["I", "Q"]


def test_reference_location_persists_analytic_affine_transform(casa, tmp_path):
    for index, backend in enumerate((tools, casa)):
        cs = backend.coordsys().newcoordsys(linear=2)
        assert cs.setincrement([2.0, -3.0])
        assert cs.setreferencelocation([2, 3], [8.0, 9.0])
        path = str(tmp_path / f"reference-{index}.im")
        source = backend.image()
        assert source.fromshape(path, [10, 10], csys=cs.torecord())
        assert source.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            cs = image.coordsys()
            np.testing.assert_allclose(cs.toworldmany(np.array([[4.0], [5.0]]))["numeric"][:, 0], [12, 3])
            np.testing.assert_allclose(cs.topixelmany(np.array([[12.0], [3.0]]))["numeric"][:, 0], [4, 5])
            assert image.done()
