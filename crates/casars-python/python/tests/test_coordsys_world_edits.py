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
    for index, writer in enumerate((casa, tools)):
        path = str(tmp_path / f"world-edit-{index}.im")
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


def test_stokes_world_edits_keep_the_discrete_codes(casa):
    for backend in (casa, tools):
        cs = backend.coordsys()
        assert cs.newcoordsys(stokes=["I", "Q", "U", "V"])
        assert cs.setreferencevalue(["U"], "stokes")
        assert cs.setincrement([0.0], "stokes")
        np.testing.assert_array_equal(cs.referencevalue()["numeric"], [1])
        np.testing.assert_array_equal(cs.increment()["numeric"], [1])
        assert list(cs.stokes()) == ["I", "Q", "U", "V"]
        assert cs.done()


@pytest.mark.parametrize(
    "value,expected",
    [
        ("7km", [7, 1]),
        (["7km"], [7, 22]),
        ({"string": ["7km"]}, [7, 22]),
        ({"measure": {}}, [11, 22]),
        ([], [11, 22]),
        (np.array([], dtype=np.float64), [1, 1]),
        (np.array([], dtype=np.int32), [1, 1]),
    ],
)
def test_increment_fallback_depends_on_the_pinned_input_form(casa, value, expected):
    for backend in (casa, tools):
        cs = backend.coordsys()
        assert cs.newcoordsys(linear=2)
        assert cs.setreferencevalue([11.0, 22.0])
        assert cs.setincrement(value)
        np.testing.assert_array_equal(cs.increment()["numeric"], expected)
        assert cs.done()


def test_selected_string_vector_and_measure_follow_different_casa_axis_rules(casa):
    for backend in (casa, tools):
        cs = backend.coordsys()
        assert cs.newcoordsys(direction=True, spectral=True, linear=2)
        assert cs.setreferencevalue([11.0, 22.0, 1.5e9, 3.0, 4.0])
        assert cs.setreferencevalue("2GHz", "spectral")
        with pytest.raises(RuntimeError, match="string formatted world direction"):
            cs.setreferencevalue(["2GHz"], "spectral")
        np.testing.assert_allclose(cs.referencevalue(type="spectral")["numeric"], [2e9], rtol=2e-16, atol=0)
        assert cs.setincrement({"measure": {}}, "linear")
        np.testing.assert_array_equal(cs.increment(type="linear")["numeric"], [3, 4])
        assert cs.done()


def test_invalid_stokes_measure_keeps_the_whole_transform(casa):
    for backend in (casa, tools):
        cs = backend.coordsys()
        assert cs.newcoordsys(stokes=["I", "Q"], linear=2)
        expected_reference = cs.referencevalue()["numeric"].copy()
        expected_increment = cs.increment()["numeric"].copy()
        measure = {
            "measure": {
                "stokes": "XX",
                "linear": {"*1": {"value": 8.0, "unit": "km"}, "*2": {"value": 9.0, "unit": "km"}},
            },
        }
        for method in (cs.setreferencevalue, cs.setincrement):
            with pytest.raises(RuntimeError, match="Stokes"):
                method(measure)
            np.testing.assert_array_equal(cs.referencevalue()["numeric"], expected_reference)
            np.testing.assert_array_equal(cs.increment()["numeric"], expected_increment)
        assert cs.done()


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
