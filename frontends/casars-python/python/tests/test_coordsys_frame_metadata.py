"""Read coordinate frame and transform metadata through the public tools."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


def mixed_coordinate(make):
    return make().newcoordsys(True, True, "I Q", 2, True)


def test_projection_names_and_parameter_counts_match_casa(casa):
    actual = mixed_coordinate(tools.coordsys)
    expected = mixed_coordinate(casa.coordsys)
    _assert_value(actual.projection(), expected.projection())
    _assert_value(actual.projection(type="aLl"), expected.projection("aLl"))
    for code in expected.projection("all")["types"].tolist() + ["GLS", "tan-extra"]:
        _assert_value(actual.projection(code), expected.projection(code))
    for tool in (actual, expected):
        for code in ("T", "NCP", "not-a-projection"):
            with pytest.raises(RuntimeError, match="unknown projection"):
                tool.projection(code)


@pytest.mark.parametrize("name", ["", "Di", "d", "SP", "spectral", "Lin", "unknown"])
@pytest.mark.parametrize("list_all", [False, True])
def test_reference_codes_match_casa(casa, name, list_all):
    actual = mixed_coordinate(tools.coordsys)
    expected = mixed_coordinate(casa.coordsys)
    _assert_value(actual.referencecode(type=name, list=list_all), expected.referencecode(name, list_all))


def test_reference_and_conversion_frames_can_differ(casa):
    initial = mixed_coordinate(casa.coordsys)
    record = initial.torecord()
    record["direction0"]["conversionSystem"] = "GALACTIC"
    record["spectral2"]["conversion"]["system"] = "BARY"
    for make in (tools.coordsys, casa.coordsys):
        coordinate = make()
        assert coordinate.fromrecord(record)
        assert coordinate.referencecode() == ["J2000", "", "LSRK", "", ""]
        assert coordinate.getconversiontype() == ""
        assert coordinate.conversiontype() == "GALACTIC"
        assert coordinate.getconversiontype(type="direction", showconversion=False) == "J2000"
        assert coordinate.getconversiontype("spectral", True) == "BARY"
        assert coordinate.getconversiontype("spectral", False) == "LSRK"
        assert coordinate.conversiontype("linear") == ""


def test_linear_transforms_and_rest_frequency_match_native_records(casa):
    initial = mixed_coordinate(casa.coordsys)
    record = initial.torecord()
    coupled = np.array([[2.0, 3.0], [4.0, 5.0]])
    record["direction0"]["pc"] = coupled
    record["linear3"]["pc"] = coupled
    record["spectral2"]["wcs"]["pc"] = 3.5
    record["spectral2"]["restfreq"] = 1.5e9
    record["spectral2"]["restfreqs"] = [1.2e9, 1.5e9, 1.6e9]
    actual = tools.coordsys()
    expected = casa.coordsys()
    assert actual.fromrecord(record) and expected.fromrecord(record)
    for name in ("D", "SP", "ST", "linear", "tab", "unrecognized"):
        _assert_value(actual.lineartransform(type=name), expected.lineartransform(name))
    _assert_value(actual.restfrequency(), expected.restfrequency())


def test_linear_transform_keeps_local_axes_when_an_independent_axis_was_removed(casa):
    record = casa.coordsys().newcoordsys(linear=2).torecord()
    record["worldmap0"] = [0, -2]
    record["pixelmap0"] = [0, -2]
    actual = tools.coordsys()
    expected = casa.coordsys()
    assert actual.fromrecord(record) and expected.fromrecord(record)
    assert actual.naxes() == expected.naxes() == 1
    _assert_value(actual.lineartransform("linear"), expected.lineartransform("linear"))


def test_frame_metadata_on_an_empty_system():
    tool = tools.coordsys()
    assert tool.referencecode() == []
    assert tool.referencecode("linear") == [""]
    assert tool.getconversiontype("direction") == ""
    assert tool.conversiontype() == ""
    assert tool.referencecode("di", list=True)[0] == "J2000"
    for method, args, message in [
        ("lineartransform", ("",), "specify the coordinate type"),
        ("lineartransform", ("linear",), "which < nCoordinates"),
        ("projection", ("all",), "No coordinate of type Direction"),
        ("referencecode", ("spectral",), "No coordinate of type Spectral"),
        ("restfrequency", (), "No coordinate of type Spectral"),
    ]:
        with pytest.raises(RuntimeError, match=message):
            getattr(tool, method)(*args)
