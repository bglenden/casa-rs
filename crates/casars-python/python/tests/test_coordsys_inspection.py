"""CASA oracle coverage for coordinate queries on native image attachments."""

from pathlib import Path

import numpy as np
import pytest

from casars import tools


def assert_equal(actual, expected):
    assert type(actual) is type(expected)
    if isinstance(expected, np.ndarray):
        assert actual.dtype == expected.dtype
        np.testing.assert_array_equal(actual, expected)
    elif isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            assert_equal(actual[key], expected[key])
    else:
        assert actual == expected


QUERY_CALLS = [
    ("axesmap", (), {}),
    ("axesmap", (False,), {}),
    ("axiscoordinatetypes", (), {}),
    ("axiscoordinatetypes", (), {"world": False}),
    ("coordinatetype", (), {}),
    ("coordinatetype", (2,), {}),
    ("coordinatetype", (), {"which": -3}),
    ("findaxis", (), {}),
    ("findaxis", (False, 1), {}),
    ("findaxis", (), {"world": True, "axis": 3}),
    ("findaxisbyname", ("ra",), {}),
    ("findaxisbyname", ("RIGHT ASCENSION", False), {}),
    ("findaxisbyname", (), {"axisname": "sp", "allowfriendlyname": True}),
    ("findaxisbyname", ("freq", False), {}),
    ("findcoordinate", (), {}),
    ("findcoordinate", ("Linear",), {}),
    ("findcoordinate", ("St", -1), {}),
    ("findcoordinate", (), {"type": "SP", "which": 1}),
    ("names", (), {}),
    ("names", ("D",), {}),
    ("names", ("missing",), {}),
    ("naxes", (), {}),
    ("naxes", (False,), {}),
    ("ncoordinates", (), {}),
    ("observer", (), {}),
    ("stokes", (), {}),
    ("telescope", (), {}),
    ("units", (), {}),
    ("units", ("DI",), {}),
    ("units", ("Sp",), {}),
    ("units", ("Lin",), {}),
]


@pytest.mark.parametrize("method,args,kwargs", QUERY_CALLS)
def test_native_image_coordinates_match_pinned_casa(native_coordinates, method, args, kwargs):
    actual, expected = native_coordinates
    assert_equal(getattr(actual, method)(*args, **kwargs), getattr(expected, method)(*args, **kwargs))


@pytest.fixture(scope="module")
def native_coordinates(tmp_path_factory):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    path = str(tmp_path_factory.mktemp("coordsys") / "cube.im")
    writer = casa.image()
    assert writer.fromshape(path, [4, 7, 3, 12])
    assert writer.done()
    expected = casa.image()
    actual = tools.image()
    assert expected.open(path)
    expected_cs = expected.coordsys()
    assert expected.done()
    assert actual.open(path)
    actual_cs = actual.coordsys()
    assert actual.done()
    return actual_cs, expected_cs


@pytest.mark.parametrize(
    "method,args,message",
    [
        ("coordinatetype", (7,), "There are only 3 coordinates available"),
        ("findaxis", (False, -1), "Value of axis cannot be negative"),
        ("findaxis", (True, 7), "axisInCoordinateSystem < nWorldAxes"),
        ("findaxis", (False, 7), "axisInCoordinateSystem < nPixelAxes"),
        ("findaxisbyname", ("RA", False), "No axis matches requested axis ra"),
        ("findaxisbyname", ("",), "Multiple axes"),
    ],
)
def test_coordinate_query_errors(native_coordinates, method, args, message):
    for coordinate_tool in native_coordinates:
        with pytest.raises(RuntimeError, match=message):
            getattr(coordinate_tool, method)(*args)


def test_empty_coordinates_and_parent_name_lifecycle():
    actual = tools.coordsys()
    assert actual.naxes() == actual.ncoordinates() == 0
    assert actual.names() == actual.units() == actual.axiscoordinatetypes() == []
    assert_equal(actual.axesmap(), np.array([], dtype=np.int64))
    assert not actual.findcoordinate()["return"]
    assert actual.parentname() == "unknown"
    assert actual.setparentname(imagename="observation.image")
    assert actual.parentname() == "observation.image"
    with pytest.raises(RuntimeError, match="CoordinateSystem is empty"):
        actual.coordinatetype()
    with pytest.raises(RuntimeError, match="No coordinate of type Stokes"):
        actual.stokes()
    assert actual.done()
    assert actual.parentname() == ""
    assert actual.naxes() == 0
    assert actual.parentname() == "unknown"
    assert actual.telescope() == "ALMA"
    assert actual.observer() == "Karl Jansky"


def test_coordinate_copy_does_not_share_edits_or_parent_name(native_coordinates):
    for original in native_coordinates:
        snapshot = original.copy()
        assert snapshot.setparentname("source")
        result = snapshot.copy()
        assert type(result) is type(original)
        assert snapshot.parentname() == "source"
        assert result.parentname() == "unknown"
        assert result.setreferencepixel([1.5, 2.5], type="direction")
        expected_original = original.referencepixel("direction")["numeric"]
        np.testing.assert_array_equal(snapshot.referencepixel("direction")["numeric"], expected_original)
        np.testing.assert_array_equal(result.referencepixel("direction")["numeric"], [1.5, 2.5])
        assert snapshot.done()
        assert result.naxes() == 4


def test_coordinate_order_follows_casa_record_creation_order(tmp_path: Path):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    original = casa.image()
    assert original.fromshape("", [4, 7, 3, 12])
    path = str(tmp_path / "transposed.im")
    result = original.transpose(outfile=path, order="2301")
    expected = result.coordsys()
    assert result.done()
    assert original.done()
    reopened = tools.image()
    assert reopened.open(path)
    actual = reopened.coordsys()
    assert reopened.done()
    assert_equal(actual.coordinatetype(), expected.coordinatetype())
    assert_equal(actual.axiscoordinatetypes(), expected.axiscoordinatetypes())
    assert_equal(actual.findcoordinate("Direction"), expected.findcoordinate("Direction"))
    assert_equal(actual.findaxis(False, 0), expected.findaxis(False, 0))
    assert_equal(actual.names(), expected.names())
    assert_equal(actual.units(), expected.units())
