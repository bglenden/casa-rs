"""Direction and frequency conversion layers on public coordinate systems."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def galactic_coordinates(api, name):
    cs = api.coordsys().newcoordsys(direction=True)
    assert cs.setunits(["rad", "rad"])
    assert cs.setconversiontype(direction=name)
    assert cs.getconversiontype("direction") == "GALACTIC"
    assert cs.getconversiontype("direction", False) == "J2000"
    return cs


def test_direction_conversion_matches_independent_galactic_rotation():
    cs = galactic_coordinates(tools, b"gal")
    # MeasData::J2000toGAL uses the FK5-derived Galactic pole.
    matrix = np.array([
        [-0.0548777621, -0.8734369591, -0.4838350026],
        [0.4941083214, -0.4448308610, 0.7469822433],
        [-0.8676666398, -0.1980741871, 0.4559837919],
    ])
    direction = matrix[:, 0] / np.linalg.norm(matrix[:, 0])
    x, y, z = direction
    expected = [np.arctan2(y, x), np.arctan2(z, np.hypot(x, y))]
    reference = cs.referencepixel()["numeric"]
    actual = cs.toworld(reference)["numeric"]
    np.testing.assert_allclose(actual, expected, atol=2.e-10, rtol=0.0)
    # The rounded matrix is orthogonal only to its published precision.
    # At RA=Dec=0, SIN projection uses the last two direction cosines.
    back = matrix.T @ direction
    expected_pixel = reference + (back / np.linalg.norm(back))[1:] / cs.increment()["numeric"]
    np.testing.assert_allclose(cs.topixel(actual)["numeric"], expected_pixel, atol=2.e-11)
    np.testing.assert_allclose(cs.referencevalue()["numeric"], [0.0, 0.0])


def test_direct_and_context_dependent_direction_frames(casa):
    left = galactic_coordinates(tools, "galactic")
    right = galactic_coordinates(casa, b"GAL")
    for point in ([0., 0.], [2.5, 4.5]):
        np.testing.assert_allclose(left.toworld(point)["numeric"], right.toworld(point)["numeric"], atol=5.e-10)
    for cs in (left, right):
        assert cs.setconversiontype("AZEL")
        assert cs.getconversiontype("direction") == "AZEL"
        with pytest.raises(RuntimeError):
            cs.toworld([0., 0.])
        assert cs.setconversiontype("J2000")
        np.testing.assert_allclose(cs.toworld([0., 0.])["numeric"], [0., 0.], atol=1.e-12)


def test_legacy_lsr_record_is_restored_and_written_as_lsrk(casa, tmp_path):
    reference = casa.coordsys().newcoordsys(direction=True, spectral=True)
    record = reference.torecord()
    axis = "spectral1"
    assert record[axis]["system"] == "LSRK"
    record[axis]["system"] = "LSR"
    record[axis]["velUnit"] = "m/s"
    pixel = [1.0, 2.0, 3.5]
    frequency = reference.toworld(pixel)["numeric"][2]
    for writer in (tools, casa):
        coordinates = writer.coordsys()
        assert coordinates.fromrecord(record)
        restored = coordinates.torecord()
        assert restored[axis]["system"] == "LSRK"
        assert restored[axis]["velUnit"] == "m/s"
        assert coordinates.referencecode("spectral")[0] == "LSRK"
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [5, 5, 6], restored)
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                reopened = image.coordsys().torecord()[axis]
                assert reopened["system"] == "LSRK"
                assert reopened["velUnit"] == "m/s"
                actual = image.toworld(pixel)["numeric"][2]
                assert actual == pytest.approx(frequency, rel=0.0, abs=1.e-6)
            finally:
                image.done()


@pytest.mark.parametrize("frame, velocity_unit", [("LSRK", "m/s"), ("BARY", "")])
def test_legacy_lsr_default_velocity_unit_and_conversion_context(casa, frame, velocity_unit):
    reference = casa.coordsys().newcoordsys(direction=True, spectral=True)
    record = reference.torecord()
    spectral = record["spectral1"]
    spectral["system"] = "LSR"
    spectral["prefVelType"] = 1
    spectral["prefVelUnit"] = velocity_unit
    del spectral["velType"], spectral["velUnit"]
    conversion = spectral["conversion"]
    unset_epoch = conversion["epoch"]
    epoch = {"type": "epoch", "refer": "UTC", "m0": {"value": 57890.0, "unit": "d"}}
    conversion.update(system=frame, epoch=epoch)
    for api in (casa, tools):
        coordinates = api.coordsys()
        assert coordinates.fromrecord(record)
        restored = coordinates.torecord()["spectral1"]
        assert restored["velType"] == 1
        assert restored["velUnit"] == (velocity_unit or "km/s")
        assert restored["conversion"]["system"] == frame
        assert restored["conversion"]["epoch"] == (unset_epoch if frame == "LSRK" else epoch)


def test_spectral_conversion_uses_observation_context_and_survives_native_io(casa, tmp_path, caplog):
    base = casa.coordsys().newcoordsys(direction=True, spectral=True)
    assert base.setunits(["rad", "rad", "Hz"])
    assert base.settelescope("EVLA")
    assert base.setepoch({"type": "epoch", "refer": "UTC", "m0": {"value": 58850.0, "unit": "d"}})
    record = base.torecord()
    axis = base.findcoordinate("spectral")["world"][0]
    pixel = [0.0, 0.0, 3.25]
    native = base.toworld(pixel)["numeric"][axis]
    expected_tool = casa.coordsys()
    assert expected_tool.fromrecord(record)
    assert expected_tool.setconversiontype(spectral="BARY")
    expected = expected_tool.toworld(pixel)["numeric"]
    assert abs(expected[axis] - native) > 100.0
    for writer in (tools, casa):
        cs = writer.coordsys()
        assert cs.fromrecord(record)
        assert cs.setconversiontype(spectral="bar")
        assert cs.getconversiontype("spectral", False) == "LSRK"
        assert cs.getconversiontype("spectral") == "BARY"
        _assert_value(cs.torecord()["spectral1"]["conversion"], expected_tool.torecord()["spectral1"]["conversion"])
        np.testing.assert_allclose(cs.toworld(pixel)["numeric"], expected, atol=0.01, rtol=0.0)
        np.testing.assert_allclose(cs.topixel(expected)["numeric"], pixel, atol=1.e-5)
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [5, 5, 6], cs.torecord())
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                assert coords.getconversiontype("spectral") == "BARY"
                np.testing.assert_allclose(coords.toworld(pixel)["numeric"], expected, atol=0.01, rtol=0.0)
            finally:
                image.done()
        assert cs.setconversiontype(spectral="REST")
        assert cs.getconversiontype("spectral") == "BARY"
        if writer is tools:
            assert "requires a source radial velocity" in caplog.text
        assert cs.setconversiontype(spectral="LSRK")
        np.testing.assert_allclose(cs.toworld(pixel)["numeric"][axis], native, atol=1.e-6)


def test_bad_frames_and_missing_spectral_context_have_the_casa_error_classes(casa):
    for api in (tools, casa):
        empty = api.coordsys()
        assert empty.setconversiontype("invalid", "invalid")
        spectral = api.coordsys().newcoordsys(spectral=True)
        with pytest.raises(RuntimeError, match="No DirectionCoordinate"):
            spectral.setconversiontype(spectral="LSRK")
        cs = api.coordsys().newcoordsys(direction=True, spectral=True)
        for value in (None, 13):
            with pytest.raises(TypeError, match="_direction must be a string"):
                cs.setconversiontype(direction=value)
        with pytest.raises(RuntimeError, match="Invalid direction reference system"):
            cs.setconversiontype(direction="invalid")
        with pytest.raises(RuntimeError, match="Transformations to/from frame"):
            cs.setconversiontype(spectral="Undefined")
        assert cs.settelescope("Unlisted observatory") is False
        before = cs.torecord()
        with pytest.raises(RuntimeError, match="Cannot find observatory"):
            cs.setconversiontype(spectral="BARY")
        _assert_value(cs.torecord(), before)


def test_combined_failure_preserves_both_layers(casa):
    for api, direction in ((tools, "J2000"), (casa, "GALACTIC")):
        cs = api.coordsys().newcoordsys(direction=True, spectral=True)
        with pytest.raises(RuntimeError, match="invalid frequency system"):
            cs.setconversiontype(direction="GAL", spectral="invalid")
        assert cs.getconversiontype("direction") == direction
        assert cs.getconversiontype("spectral") == "LSRK"
