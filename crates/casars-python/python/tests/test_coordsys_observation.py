"""Observation metadata edits against CASA 6.7.6.14."""

import numpy as np
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


def epoch(value, reference="UTC", unit="d", offset=None):
    record = {"type": "epoch", "refer": reference, "m0": {"value": value, "unit": unit}}
    if offset is not None:
        record["offset"] = offset
    return record


@pytest.mark.parametrize("record", [
    epoch(24 * 60001, unit="h"), epoch(60000.0, "TAI"), epoch(60001.0, "TT"),
    epoch(60000.5, "R_UTC"), epoch(-0.25, "mystery"), epoch(np.array([60000.0, 60001.0])),
    epoch(0.25, "TAI", offset=epoch(1.0, offset=epoch(60000.0))),
    epoch(np.array([60000.0], dtype=np.longdouble)),
    epoch(np.array([60000], dtype=np.longlong)),
    epoch(np.array([60000], dtype=np.ulonglong)),
    epoch(np.asarray(60000.0)),
    epoch([True, 2.5]), epoch(5_184_000_000, unit="s"),
    {"type": "epoch", "refer": "UTC"},
])
def test_setepoch_and_native_record_round_trip(casa, record):
    rust = tools.coordsys().newcoordsys(linear=2)
    oracle = casa.coordsys().newcoordsys(linear=2)
    assert rust.setepoch(value=record) and oracle.setepoch(value=record)
    expected = oracle.epoch()
    _assert_value(rust.epoch(), expected)
    saved = rust.torecord()
    _assert_value(saved["obsdate"], expected)
    for create in (tools.coordsys, casa.coordsys):
        restored = create()
        assert restored.fromrecord(saved)
        _assert_value(restored.epoch(), expected)


def test_failed_setepoch_preserves_observation_date(casa):
    for create in (tools.coordsys, casa.coordsys):
        cs = create()
        assert cs.setepoch(epoch(60100.0))
        saved = cs.epoch()
        for invalid in (
            {}, epoch(20.0, unit="m"), casa.measures().direction(),
            epoch(np.int64(60000)), epoch(np.float32(20.0)),
            epoch(np.array([20.0], dtype=np.float16)),
        ):
            assert cs.setepoch(invalid) is False
        with pytest.raises(RuntimeError, match="Cannot convert to QuantumVectorDouble"):
            cs.setepoch(epoch([60000, 60001]))
        with pytest.raises(RuntimeError, match="Cannot convert to QuantumVectorDouble"):
            cs.setepoch(epoch(np.array([], dtype="l")))
        with pytest.raises(RuntimeError, match="Cannot convert to QuantumVectorDouble"):
            cs.setepoch(epoch([3, False]))
        with pytest.raises(RuntimeError, match="Cannot convert to QuantumVectorDouble"):
            cs.setepoch(epoch(0.5, offset=epoch([60000, 60001])))
        with pytest.raises(RuntimeError, match="Wrong QuantumHolder to convert asQuantumVectorDouble"):
            cs.setepoch(epoch(1 + 0j))
        with pytest.raises(TypeError, match="is not a dictionary"):
            cs.setepoch(None)
        _assert_value(cs.epoch(), saved)


def test_empty_epoch_vector_leaves_date_unchanged():
    cs = tools.coordsys()
    assert cs.setepoch(epoch(60100.0))
    assert not cs.setepoch(epoch(np.array([], dtype=np.float64)))
    _assert_value(cs.epoch(), epoch(60100.0))


def test_byteswapped_epoch_vector_matches_the_native_value():
    cs = tools.coordsys()
    value = np.array([20.0], dtype=np.dtype(np.float64).newbyteorder())
    assert cs.setepoch(epoch(value))
    assert cs.epoch()["m0"]["value"] == 20.0


@pytest.mark.parametrize("raw_offset, days", [
    ({**epoch(60000.0), "m1": {"value": 12.0, "unit": "h"}}, 60000.5),
    (epoch(np.array([60000.0, 60001.0])), 60000.0),
    ({"type": "epoch", "refer": "UTC"}, 0.0),
])
def test_restoring_a_raw_epoch_offset_uses_all_components(casa, raw_offset, days):
    record = tools.coordsys().newcoordsys(linear=2).torecord()
    record["obsdate"] = epoch(60000.000033, offset=raw_offset)
    for make in (tools.coordsys, casa.coordsys):
        restored = make()
        assert restored.fromrecord(record)
        result = restored.epoch()
        assert result["m0"]["value"] == 60000.000033
        assert result["offset"]["m0"]["value"] == days


def test_epoch_offset_persists_in_native_image(casa, tmp_path):
    path = str(tmp_path / "epoch.im")
    cs = tools.coordsys().newcoordsys(direction=True)
    assert cs.setepoch(epoch(0.125, "R_TAI", offset=epoch(60000.0)))
    writer = tools.image()
    assert writer.fromshape(path, [3, 4], csys=cs.torecord())
    assert writer.done()
    for create in (tools.image, casa.image):
        image = create()
        assert image.open(path)
        _assert_value(image.coordsys().epoch(), cs.epoch())
        assert image.done()
