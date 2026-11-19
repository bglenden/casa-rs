"""Observation metadata edits against CASA 6.7.6.14."""

import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


@pytest.mark.parametrize("value", ["A. Observer", "José Jansky", ""])
def test_setobserver_edits_the_natively_persisted_label(casa, value):
    for make in (tools.coordsys, casa.coordsys):
        coordinate = make().newcoordsys(direction=True)
        assert coordinate.setobserver(value=value)
        assert coordinate.observer() == value
        assert coordinate.torecord()["observer"] == value
        with pytest.raises(TypeError, match="must be a string"):
            coordinate.setobserver(3)
        assert coordinate.observer() == value


def test_telescope_name_lookup_and_unknown_name_preserve_the_last_position(casa):
    actual = tools.coordsys()
    expected = casa.coordsys()
    for name in ("ATCA", "ATC", "alma", "Unknown telescope", "", "VLA", "EVLA"):
        result = expected.settelescope(value=name)
        assert actual.settelescope(name) is result
        assert actual.telescope() == expected.telescope() == name
        _assert_value(actual.torecord()["telescopeposition"], expected.torecord()["telescopeposition"])
    for coordinate in (actual, expected):
        with pytest.raises(TypeError, match="must be a string"):
            coordinate.settelescope(None)
        assert coordinate.telescope() == "EVLA"


def test_observer_and_telescope_persist_in_native_image(casa, tmp_path):
    path = str(tmp_path / "observation.im")
    coordinate = tools.coordsys().newcoordsys(direction=True)
    assert coordinate.settelescope("VLA")
    assert coordinate.setobserver("P. Tester")
    writer = tools.image()
    assert writer.fromshape(path, [3, 4], csys=coordinate.torecord())
    assert writer.done()

    reader = casa.image()
    assert reader.open(path)
    readback = reader.coordsys()
    assert readback.telescope() == "VLA"
    assert readback.observer() == "P. Tester"
    _assert_value(readback.torecord()["telescopeposition"], coordinate.torecord()["telescopeposition"])
    assert reader.done()


def test_unknown_telescope_keeps_the_last_known_position():
    coordinate = tools.coordsys().newcoordsys(direction=True)
    assert coordinate.settelescope("ALMA")
    assert coordinate.setobserver("P. Tester")
    saved = coordinate.torecord()
    assert not coordinate.settelescope("Unknown telescope")
    record = coordinate.torecord()
    assert record["telescope"] == "Unknown telescope"
    assert record["observer"] == "P. Tester"
    _assert_value(record["telescopeposition"], saved["telescopeposition"])
