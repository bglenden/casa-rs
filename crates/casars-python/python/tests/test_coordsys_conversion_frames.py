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
    # The first column of the IAU equatorial-to-Galactic rotation at J2000.
    x, y, z = -0.0548755604, 0.4941094279, -0.8676661490
    expected = [np.arctan2(y, x), np.arcsin(z)]
    reference = cs.referencepixel()["numeric"]
    actual = cs.toworld(reference)["numeric"]
    np.testing.assert_allclose(actual, expected, atol=2.e-7)
    np.testing.assert_allclose(cs.topixel(actual)["numeric"], reference, atol=1.e-8)
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


def test_spectral_conversion_uses_observation_context_and_survives_native_io(casa, tmp_path):
    base = casa.coordsys().newcoordsys(direction=True, spectral=True)
    assert base.setunits(["rad", "rad", "Hz"])
    assert base.settelescope("EVLA")
    assert base.setepoch(casa.measures().epoch("UTC", "58850d"))
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
        assert cs.settelescope("Unlisted observatory")
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
