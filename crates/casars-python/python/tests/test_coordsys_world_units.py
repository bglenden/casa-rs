"""Unit conversions and relabeling through the public coordinate tool."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def test_units_convert_the_whole_system_or_the_requested_index(casa):
    for backend in (casa, tools):
        cs = backend.coordsys()
        assert cs.newcoordsys(direction=True, spectral=True, linear=2)
        assert cs.setunits(["GHz"], type="linear", which=1)
        assert list(cs.units()) == ["'", "'", "GHz", "km", "km"]
        assert cs.setunits(["deg", "deg", "MHz", "m", "m"], overwrite=True, which=9)
        assert list(cs.units()) == ["deg", "deg", "MHz", "m", "m"]
        np.testing.assert_allclose(cs.increment()["numeric"], [-1.0/60, 1.0/60, 0.001, 1000, 1000])
        assert cs.done()


def test_overwrite_relabels_only_the_selected_linear_coordinate(casa):
    for backend in (casa, tools):
        cs = backend.coordsys()
        assert cs.newcoordsys(spectral=True, linear=2)
        before = cs.increment()["numeric"].copy()
        assert cs.setunits("Jy,K", "linear", overwrite=True)
        assert list(cs.units()) == ["Hz", "Jy", "K"]
        assert cs.setunits(["myunit", "whatever"], "linear", True)
        assert list(cs.units()) == ["Hz", "myunit", "whatever"]
        assert cs.setunits(cs.units())
        np.testing.assert_array_equal(cs.increment()["numeric"], before)
        with pytest.raises(RuntimeError, match="length nWorldAxes"):
            cs.setunits(["m"], overwrite=True)
        assert list(cs.units()) == ["Hz", "myunit", "whatever"]
        assert cs.done()


def test_overwrite_relabels_the_tabular_unit(casa):
    for backend in (casa, tools):
        cs = backend.coordsys()
        assert cs.newcoordsys(tabular=True)
        before = cs.increment()["numeric"].copy()
        assert cs.setunits("Jy", type="tabular", overwrite=True)
        assert list(cs.units()) == ["Jy"]
        np.testing.assert_array_equal(cs.increment()["numeric"], before)
        assert cs.done()


def test_failed_mixed_unit_edit_preserves_the_transform():
    cs = tools.coordsys()
    assert cs.newcoordsys(direction=True, spectral=True, linear=2)
    before = cs.increment()["numeric"].copy()
    with pytest.raises(RuntimeError, match="Units are not compatible dimensionally"):
        cs.setunits(["deg", "deg", "m", "m", "m"])
    assert list(cs.units()) == ["'", "'", "Hz", "km", "km"]
    np.testing.assert_array_equal(cs.increment()["numeric"], before)
    assert cs.done()


def test_setunits_preserves_the_affine_transform_across_native_readback(casa, tmp_path):
    for index, (writer, reader) in enumerate(((casa, tools), (tools, casa))):
        path = str(tmp_path / f"units-{index}.im")
        image = writer.image()
        assert image.fromshape(path, [4, 5], linear=True)
        cs = image.coordsys()
        assert cs.setunits(["km", "km"], "linear", overwrite=True)
        assert cs.setreferencepixel([0.0, 0.0])
        assert cs.setreferencevalue([1.0, 2.0])
        assert cs.setincrement([0.25, 0.5])
        assert cs.setunits(["m,m"], type="linear")
        assert image.setcoordsys(cs.torecord())
        assert cs.done() and image.done()
        reopened = reader.image()
        assert reopened.open(path)
        cs = reopened.coordsys()
        assert list(cs.units()) == ["m", "m"]
        np.testing.assert_allclose(cs.toworldmany(np.array([[1.0], [2.0]]))["numeric"], [[1250], [3000]])
        assert cs.done() and reopened.done()
