"""Public coordinate records with nonconsecutive and independent axis maps."""

from pathlib import Path
import shutil

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


FIXTURES = Path(__file__).resolve().parents[3] / "casa-images/tests/data/casa-6.7.6/axis_maps"


def independent_world_order(record):
    record["worldmap0"] = np.array([0, 2], dtype=np.int32)
    record["worldmap1"] = np.array([1], dtype=np.int32)
    return record


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def read_coordinates(casa, tmp_path, fixture):
    path = tmp_path / "source.image"
    shutil.copytree(FIXTURES / f"{fixture}.image", path)
    source = casa.image()
    try:
        assert source.open(str(path))
        return source.coordsys().torecord()
    finally:
        assert source.done()


@pytest.mark.parametrize("fixture", ["swapped", "interleaved", "independent"])
def test_mapped_record_queries_and_transforms(casa, tmp_path, fixture):
    record = read_coordinates(casa, tmp_path, "swapped" if fixture == "independent" else fixture)
    if fixture == "independent":
        record = independent_world_order(record)
    left, right = (api.coordsys() for api in (tools, casa))
    assert left.fromrecord(record) and right.fromrecord(record)
    for name in ("names", "units", "coordinatetype", "referencepixel", "findcoordinate"):
        _assert_value(getattr(left, name)(), getattr(right, name)(), name)
    for name in ("referencevalue", "increment"):
        _assert_value(getattr(left, name)("nq"), getattr(right, name)("nq"), name)
    for side in (False, True):
        for name in ("axesmap", "axiscoordinatetypes"):
            _assert_value(getattr(left, name)(side), getattr(right, name)(side), name)
        for axis in range(right.naxes(side)):
            _assert_value(left.findaxis(side, axis), right.findaxis(side, axis), "findaxis")

    delta = np.array([0.25, 0.1, 0.3, 0.2])
    delta[right.findcoordinate("Stokes")["pixel"]] = 0.0
    point = right.referencepixel()["numeric"] + delta
    matrix = np.column_stack((right.referencepixel()["numeric"], point))
    expected_world = right.toworld(point, "nq")
    _assert_value(left.toworld(point, "nq"), expected_world, "toworld")
    _assert_value(left.toworld(point, "sm"), right.toworld(point, "sm"), "toworld")
    _assert_value(left.topixel(expected_world), right.topixel(expected_world), "topixel")
    np.testing.assert_allclose(left.topixel(expected_world)["numeric"], point, atol=5e-7)
    expected_many = right.toworldmany(matrix)
    _assert_value(left.toworldmany(matrix), expected_many, "toworldmany")
    worlds = expected_many["numeric"]
    _assert_value(left.topixelmany(worlds), right.topixelmany(worlds), "topixelmany")
    for value, is_world in ((point, False), (expected_world, True)):
        offsets = right.torel(value, is_world)
        _assert_value(left.torel(value, is_world), offsets, "torel")
        _assert_value(left.toabs(offsets, is_world), right.toabs(offsets, is_world), "toabs")
    for value, is_world in ((matrix, False), (worlds, True)):
        offsets = right.torelmany(value, is_world)
        _assert_value(left.torelmany(value, is_world), offsets, "torelmany")
        _assert_value(left.toabsmany(offsets, is_world), right.toabsmany(offsets, is_world), "toabsmany")
    if fixture == "independent":
        np.testing.assert_array_equal(left.axesmap(True), [2, 0, 1, 3])
        np.testing.assert_array_equal(left.axesmap(False), [1, 2, 0, 3])
        assert left.units("direction") == ["rad", "rad"]
        assert left.units("stokes") == [""]
        assert right.units("direction") == ["", "rad"]
        assert right.units("stokes") == ["rad"]


def test_independent_axis_record_edits_and_cross_readback(casa, tmp_path):
    record = independent_world_order(read_coordinates(casa, tmp_path, "swapped"))
    saved = []
    for api in (tools, casa):
        cs = api.coordsys()
        assert cs.fromrecord(record)
        assert cs.setnames(["longitude", "polarization", "latitude", "frequency"])
        assert cs.setunits(["deg", "", "rad", "MHz"])
        assert cs.setreferencepixel([2.0, 3.0, 0.0, 4.0])
        reference = cs.referencevalue()["numeric"]
        reference[[0, 2, 3]] += [0.5, 0.125, 1.5]
        assert cs.setreferencevalue(reference)
        delta = cs.increment()["numeric"]
        delta[[0, 2, 3]] *= [1.1, 1.2, 1.3]
        assert cs.setincrement(delta)
        saved.append(cs.torecord())

    reference = casa.coordsys()
    assert reference.fromrecord(saved[1])
    for raw in saved:
        for api in (tools, casa):
            reopened = api.coordsys()
            assert reopened.fromrecord(raw)
            for name in ("axesmap", "names", "units", "referencevalue", "referencepixel", "increment"):
                _assert_value(getattr(reopened, name)(), getattr(reference, name)(), name)
            point = [2.25, 2.75, 0.0, 4.5]
            _assert_value(reopened.toworld(point), reference.toworld(point), "toworld")
            np.testing.assert_array_equal(raw["pixelmap0"], [1, 0])
            np.testing.assert_array_equal(raw["worldmap0"], [0, 2])


def test_world_axis_without_pixel_remains_editable():
    source = tools.coordsys().newcoordsys(direction=True, spectral=True)
    record = source.torecord()
    record["pixelmap0"] = np.array([-1, 0], dtype=np.int32)
    record["pixelmap1"] = np.array([1], dtype=np.int32)
    target = tools.coordsys()
    assert target.fromrecord(record)
    assert target.naxes(False) == 2
    assert target.naxes(True) == 3
    np.testing.assert_array_equal(target.axesmap(), [1, 2])
    np.testing.assert_array_equal(target.axesmap(False), [-1, 0, 1])
    assert target.setnames(["longitude", "latitude", "frequency"])
    assert target.names() == ["longitude", "latitude", "frequency"]
    assert target.setreferencepixel([2.0, 3.0])
    np.testing.assert_array_equal(target.referencepixel()["numeric"], [2.0, 3.0])
    assert target.setreferencevalue([0.4, 0.3, 1.42e9])
    np.testing.assert_allclose(target.referencevalue()["numeric"], [0.4, 0.3, 1.42e9])


def hidden_linear_record(api, axis=-1):
    source = api.coordsys().newcoordsys(linear=2)
    assert source.setreferencevalue([3.0, 17.0])
    assert source.setreferencepixel([0.0, 0.0])
    assert source.setincrement([1.0, 1.0])
    record = source.torecord()
    mask = [-1, 0] if axis == -1 else [0, -2]
    for domain in ("pixel", "world"):
        record[f"{domain}map0"] = np.array(mask, dtype=np.int32)
    record["worldreplace0"] = np.array([3.0, 17.0])
    record["pixelreplace0"] = np.array([2.0, 2.0])
    return record


def test_reference_measures_keep_hidden_physical_components(casa):
    record = hidden_linear_record(casa)
    record["linear0"]["pc"] = np.array([[1.0, 1.0], [1.0, 2.0]])
    actual, expected = (api.coordsys() for api in (tools, casa))
    assert actual.fromrecord(record) and expected.fromrecord(record)
    reference = expected.referencevalue("m")
    _assert_value(actual.referencevalue("m"), reference, "linear measure")
    _assert_value(actual.referencevalue("n", "linear"), expected.referencevalue("n", "linear"))
    # The inverse uses the fixed hidden world value of 3 and the visible
    # reference value of 17, so both local pixel offsets are zero.
    np.testing.assert_array_equal(actual.topixel(reference)["numeric"], [0.0])
    assert actual.setreferencevalue([99.0], "linear")
    np.testing.assert_array_equal(actual.referencevalue()["numeric"], [99.0])
    update = actual.referencevalue("m")
    update["measure"]["linear"]["*2"]["value"] = 21.0
    assert actual.setreferencevalue(update, "linear")
    np.testing.assert_array_equal(actual.referencevalue()["numeric"], [21.0])
    np.testing.assert_array_equal(actual.torecord()["linear0"]["crval"], [3.0, 21.0])


def test_reference_direction_measure_includes_removed_world_component(casa):
    source = casa.coordsys().newcoordsys(direction=True)
    assert source.setunits(["rad", "rad"], "direction")
    assert source.setreferencevalue([0.9, 0.3])
    record = source.torecord()
    record["worldmap0"] = np.array([-1, 0], dtype=np.int32)
    actual, expected = (api.coordsys() for api in (tools, casa))
    assert actual.fromrecord(record) and expected.fromrecord(record)
    _assert_value(actual.referencevalue("m"), expected.referencevalue("m"), "direction measure")


def test_matrix_edit_promotes_a_coupled_slice_without_resetting_other_coordinates():
    record = hidden_linear_record(tools, axis=-2)
    target = tools.coordsys()
    assert target.fromrecord(record)
    assert target.addcoordinate(tabular=True)
    record = target.torecord()
    for domain in ("pixel", "world"):
        record[f"{domain}map0"] = np.array([1, -2], dtype=np.int32)
        record[f"{domain}map1"] = np.array([0], dtype=np.int32)
    record["tabular1"]["pixelvalues"] = np.array([0.0, 2.0, 5.0])
    record["tabular1"]["worldvalues"] = np.array([10.0, 14.0, 30.0])
    assert target.fromrecord(record)
    tabular = np.array([[2.0]])
    assert target.setlineartransform("tabular", tabular)
    point = [1.0, 3.0]
    before = target.toworld(point)["numeric"]
    matrix = np.array([[1.0, 1.0], [1.0, 2.0]])
    assert target.setlineartransform("linear", matrix)
    np.testing.assert_array_equal(target.lineartransform("tabular"), tabular)
    after = target.toworld(point)["numeric"]
    # The hidden pixel supplies the second matrix column with value 2.
    np.testing.assert_array_equal(before, [14.0, 6.0])
    np.testing.assert_array_equal(after, [14.0, 8.0])
    reopened = tools.coordsys()
    assert reopened.fromrecord(target.torecord())
    np.testing.assert_array_equal(reopened.toworld(point)["numeric"], after)
