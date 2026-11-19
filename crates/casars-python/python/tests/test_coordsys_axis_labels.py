"""Public axis labels and polarization edits against pinned CASA."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


def test_editing_all_and_selected_world_axis_names(casa):
    kwargs = dict(direction=True, spectral=True, stokes="I,Q", linear=2, tabular=True)
    rust = tools.coordsys().newcoordsys(**kwargs)
    oracle = casa.coordsys().newcoordsys(**kwargs)
    rust.setparentname("image.im")
    for value, coordinate in [
        (["Longitude", "Latitude", "Pol", "Freq", "A", "B", "Distance"], ""),
        ("First, Second", "direction"), ("West North", "D"),
        (["", ""], "linear"), ("", "spectral"), (["distance"], "tabular"),
    ]:
        assert rust.setnames(value=value, type=coordinate)
        assert oracle.setnames(value, coordinate)
        assert rust.names() == oracle.names()
    assert rust.parentname() == "image.im"
    for fn in (rust.setnames, oracle.setnames):
        with pytest.raises(RuntimeError, match="Supplied axis names vector must be of length 2"):
            fn(("ignored", "tuple"), "direction")
        with pytest.raises(RuntimeError, match="names vector must be of length nWorldAxes"):
            fn(["partial"])
        with pytest.raises(TypeError, match="list _value must contain strings"):
            fn([1, 2], "direction")
    assert rust.names() == oracle.names()


def test_axis_names_follow_visible_world_order_after_transpose(casa):
    oracle = casa.coordsys().newcoordsys(direction=True, spectral=True, stokes="I,Q")
    assert oracle.transpose([2, 3, 0, 1])
    rust = tools.coordsys()
    assert rust.fromrecord(oracle.torecord())
    names = ["Pol", "Radio Frequency", "Longitude", "Latitude"]
    assert oracle.setnames(names) and rust.setnames(names)
    assert rust.names() == oracle.names()
    for field in ("direction0", "stokes1", "spectral2"):
        _assert_value(rust.torecord()[field], oracle.torecord()[field])


def test_renaming_the_visible_axis_preserves_the_removed_label(casa):
    record = tools.coordsys().newcoordsys(linear=2).torecord()
    record["worldmap0"] = record["pixelmap0"] = np.asarray([0, -2], dtype=np.int32)
    for make in (tools.coordsys, casa.coordsys):
        tool = make()
        assert tool.fromrecord(record)
        assert tool.setnames(["active"])
        assert tool.names() == ["active"]
        assert tool.torecord()["linear0"]["axes"].tolist() == ["active", "LinAxis1"]
        with pytest.raises(RuntimeError, match="must be of length 2"):
            tool.setnames(["active"], type="linear")
    tool = tools.coordsys()
    assert tool.fromrecord(record)
    with pytest.raises(RuntimeError, match="removed coordinate axis"):
        tool.setnames(["changed", "removed"], type="linear")
    assert tool.names() == ["LinAxis0"]


@pytest.mark.parametrize("input_names", ["Q,U", "i q", ["XX", "YY"], ["RX", "RCircular"], "IQUV", "", ["", "V"]])
def test_setstokes_updates_values_and_preserves_the_axis_label(casa, input_names):
    rust = tools.coordsys().newcoordsys(stokes="I,Q")
    oracle = casa.coordsys().newcoordsys(stokes="I,Q")
    for coordinate in (rust, oracle):
        assert coordinate.setnames("Polarization", "stokes")
        assert coordinate.setstokes(stokes=input_names)
        assert coordinate.names() == ["Polarization"]
        with pytest.raises(RuntimeError, match="same Stokes more than once"):
            coordinate.setstokes(["Q", "q"])
        with pytest.raises(RuntimeError, match="did not specify any new Stokes"):
            coordinate.setstokes([])
    assert rust.stokes() == oracle.stokes()
    _assert_value(rust.torecord()["stokes0"], oracle.torecord()["stokes0"])


def test_edited_axis_labels_round_trip_through_a_native_image(casa, tmp_path):
    path = str(tmp_path / "labeled.im")
    cs = tools.coordsys().newcoordsys(direction=True, stokes="I,Q")
    assert cs.setnames(["L", "B", "Polarization"])
    assert cs.setstokes("RR,LL")
    writer = tools.image()
    assert writer.fromarray(path, np.zeros((2, 2, 2)), csys=cs.torecord())
    assert writer.done()
    reader = casa.image()
    assert reader.open(path)
    assert reader.coordsys().names() == cs.names()
    assert reader.coordsys().stokes() == ["RR", "LL"]
    assert reader.done()
