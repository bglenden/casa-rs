"""World reference values and increments through the public coordinate tool."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


@pytest.mark.parametrize("method", ["increment", "referencevalue"])
def test_world_reference_records(casa, method):
    config = dict(direction=True, spectral=True, stokes="I Q", linear=2, tabular=True)
    ours = tools.coordsys().newcoordsys(**config)
    oracle = casa.coordsys().newcoordsys(**config)
    for fmt, kind in (
        ("n", ""),
        ("nqs", ""),
        ("qsm", "linear"),
        ("nqsm", "spectral"),
        ("NQSM", "direction"),
        ("nqsm", "stokes"),
        ("nqsm", "tabular"),
        ("?", ""),
        ("m", ""),
    ):
        _assert_value(getattr(ours, method)(fmt, kind), getattr(oracle, method)(fmt, kind))


def test_world_reference_direction_quantities_and_numeric_order():
    cs = tools.coordsys().newcoordsys(direction=True, spectral=True, stokes="I Q")
    reference = cs.referencevalue("nq")
    increment = cs.increment("nq")
    _assert_value(reference["numeric"], np.array([0.0, 0.0, 1.0, 1.415e9]))
    _assert_value(increment["numeric"], np.array([-1.0, 1.0, 1.0, 1000.0]))
    assert increment["quantity"]["*1"]["unit"] == "'"


@pytest.mark.parametrize("frame", ["J2000", "GALACTIC", "ECLIPTIC", "AZEL", "HADEC", "SUPERGAL"])
def test_world_reference_formats_native_direction_frames(casa, frame):
    oracle = casa.coordsys().newcoordsys(direction=True)
    assert oracle.setdirection(refcode=frame)
    assert oracle.setreferencevalue([0.5, -0.25], "direction")
    ours = tools.coordsys()
    assert ours.fromrecord(oracle.torecord())
    _assert_value(ours.referencevalue("nqsm", "direction"), oracle.referencevalue("nqsm", "direction"))
    _assert_value(ours.increment("nqs", "direction"), oracle.increment("nqs", "direction"))


def test_world_reference_missing_coordinate_and_zero_rest_frequency(casa):
    oracle = casa.coordsys().newcoordsys(spectral=True)
    assert oracle.setrestfrequency(0.0)
    ours = tools.coordsys()
    assert ours.fromrecord(oracle.torecord())
    for tool in (ours, oracle):
        assert tool.increment(type="linear") == {}
        with pytest.raises(RuntimeError):
            tool.referencevalue(type="linear")
        with pytest.raises(RuntimeError, match="velocity is NaN"):
            tool.referencevalue("m", "spectral")


@pytest.mark.parametrize(
    "frame,longitude,latitude",
    [("J2000", 270.0, -10.0), ("ITRF", 270.0, -10.0), ("HADEC", 180.0, -10.0), ("J2000", -0.0, -0.0)],
)
def test_world_reference_canonical_direction_angles(casa, frame, longitude, latitude):
    oracle = casa.coordsys().newcoordsys(direction=True)
    assert oracle.setdirection(refcode=frame)
    assert oracle.setunits(["deg", "deg"], "direction")
    assert oracle.setreferencevalue([longitude, latitude], "direction")
    ours = tools.coordsys()
    assert ours.fromrecord(oracle.torecord())
    _assert_value(ours.referencevalue("nsm"), oracle.referencevalue("nsm"))


def test_world_reference_formats_scientific_hour_angle(casa):
    oracle = casa.coordsys().newcoordsys(linear=2)
    assert oracle.addcoordinate(direction=True)
    assert oracle.setdirection(refcode="HADEC")
    assert oracle.setunits(["deg", "deg"], "direction")
    assert oracle.setreferencevalue([270.0, -10.0], "direction")
    ours = tools.coordsys()
    assert ours.fromrecord(oracle.torecord())
    _assert_value(ours.referencevalue("s", "direction"), oracle.referencevalue("s", "direction"))


@pytest.mark.parametrize("setting", ["frequency", "velocity", "wavelength", "air-wavelength"])
def test_world_reference_spectral_display_units(casa, setting):
    oracle = casa.coordsys().newcoordsys(spectral=True)
    if setting == "frequency":
        assert oracle.setunits(["GHz"])
        assert oracle.setreferencevalue([115.2712018])
    record = oracle.torecord()
    if setting == "velocity":
        record["spectral0"]["formatUnit"] = "km/s"
    elif setting in ("wavelength", "air-wavelength"):
        record["spectral0"]["formatUnit"] = "mm"
        if setting == "air-wavelength":
            record["spectral0"]["nativeType"] = 5
    ours = tools.coordsys()
    assert oracle.fromrecord(record)
    assert ours.fromrecord(record)
    _assert_value(ours.referencevalue("nqs"), oracle.referencevalue("nqs"))
    _assert_value(ours.increment("nqs"), oracle.increment("nqs"))


@pytest.mark.parametrize("setting", ["undefined", "negative-frequency"])
def test_world_reference_preserves_spectral_measures(casa, setting):
    oracle = casa.coordsys().newcoordsys(spectral=True)
    if setting == "negative-frequency":
        assert oracle.setreferencevalue([-1.415e9])
    record = oracle.torecord()
    if setting == "undefined":
        record["spectral0"]["system"] = "Undefined"
        record["spectral0"]["conversion"]["system"] = "Undefined"
    ours = tools.coordsys()
    assert oracle.fromrecord(record)
    assert ours.fromrecord(record)
    _assert_value(ours.referencevalue("nm"), oracle.referencevalue("nm"))
