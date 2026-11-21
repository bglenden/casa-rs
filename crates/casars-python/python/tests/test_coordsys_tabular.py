"""Public tabular samples, live edits, and native CASA interoperability."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize(
    ("pixels", "worlds", "sample", "expected"),
    [
        ([0, 2, 5], [10, 14, 30], [-1, 1, 3.5, 8], [8, 12, 22, 46]),
        ([5, 2, 0], [10, 14, 30], [-1, 1, 3.5, 8], [38, 22, 12, 6]),
        ([5, 2, 0], [30, 14, 10], [-1, 1, 3.5, 8], [8, 12, 22, 46]),
    ],
)
def test_interpolation_and_extrapolation(pixels, worlds, sample, expected):
    cs = tools.coordsys().newcoordsys(tabular=True)
    assert cs.settabular(pixels, worlds)
    np.testing.assert_allclose(cs.toworldmany([sample])["numeric"], [expected])
    np.testing.assert_allclose(cs.topixelmany(np.array([expected], dtype=float))["numeric"], [sample])


def test_selection_uses_creation_order_and_reuses_live_world_samples(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(linear=1, tabular=True)
        assert cs.addcoordinate(tabular=True)
        assert cs.setnames(["Distance", "First", "Second"])
        assert cs.setunits(["km", "m", "cm"])
        assert cs.settabular([0, 2, 5], [10, 14, 30])
        assert cs.settabular(np.array([[0, 3, 9]]), ["5.0", "8.0", "16.0"], np.int32(1))
        assert cs.transpose([2, 0, 1])
        assert cs.setlineartransform("tabular", [[2.0]])
        assert cs.settabular(pixel=[0, 4, 10], which=0)
        assert cs.settabular(world=[5, 17, 29], which=1)
        np.testing.assert_allclose(cs.toworld([6, 2, 7])["numeric"], [23, 2, 34])
        np.testing.assert_array_equal(cs.names(), ["Second", "Distance", "First"])
        np.testing.assert_array_equal(cs.units(), ["cm", "km", "m"])
        np.testing.assert_array_equal(cs.lineartransform("tabular"), [[1.0]])


def test_identity_matrix_keeps_live_reference_edits_until_samples_are_replaced(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(tabular=True)
        assert cs.settabular([0, 2, 5], [10, 14, 30])
        assert cs.setreferencepixel([1.0])
        np.testing.assert_array_equal(cs.referencepixel()["numeric"], [1.0])
        assert cs.setreferencevalue([15.0])
        assert cs.settabular(pixel=[0, 2, 5])
        np.testing.assert_allclose(cs.torecord()["tabular0"]["worldvalues"], [11, 15, 31])


@pytest.mark.parametrize(("factor", "samples"), [(1.0, [12, 16, 32]), (2.0, [8, 16, 48])])
def test_reference_location_applies_both_live_edits_before_replacing_samples(casa, factor, samples):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(tabular=True)
        assert cs.settabular([0, 2, 5], [10, 14, 30])
        assert cs.setlineartransform("tabular", [[factor]])
        assert cs.setreferencelocation([1.0], [16.0])
        assert cs.settabular(pixel=[0, 2, 5])
        np.testing.assert_allclose(cs.torecord()["tabular0"]["worldvalues"], samples)


def test_inverse_detects_repeated_abscissae_in_corrected_pixel_units(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(tabular=True)
        assert cs.settabular([0.0, 1.0], [0.0, 1e-301])
        np.testing.assert_allclose(cs.topixel([5e-302])["numeric"], [0.5])
        assert cs.settabular([0.0, 1.0, 2.0], [0.0, 1e-15, 1.0])
        with pytest.raises(RuntimeError, match="repeated x values"):
            cs.topixel([5e-16])


@pytest.mark.parametrize("both_supplied", [True, False])
def test_constant_mapping_live_edits_and_public_restore(casa, both_supplied):
    records = []
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(tabular=True)
        assert cs.settabular(2.0, [7.5])
        assert cs.setreferencepixel([5.0])
        assert cs.setreferencevalue([22.0])
        assert cs.setincrement([3.0])
        assert cs.setlineartransform("tabular", [[2.0]])
        np.testing.assert_allclose(cs.toworld([100.0])["numeric"], [4.0])
        np.testing.assert_allclose(cs.topixel([91.0])["numeric"], [2.0])
        records.append(cs.torecord())
        if both_supplied:
            assert cs.settabular([3.0], [8.0])
            np.testing.assert_allclose(cs.toworld([100.0])["numeric"], [8.0])
        else:
            assert cs.settabular(pixel=[3.0])
            np.testing.assert_allclose(cs.toworld([100.0])["numeric"], [4.0])
        np.testing.assert_allclose(cs.topixel([91.0])["numeric"], [3.0])
        np.testing.assert_allclose(cs.increment()["numeric"], [0.0])
    _assert_value(records[0]["tabular0"], records[1]["tabular0"])
    for api in (tools, casa):
        for record in records:
            cs = api.coordsys()
            assert cs.fromrecord(record)
            np.testing.assert_allclose(cs.referencepixel()["numeric"], [2.0])
            np.testing.assert_allclose(cs.increment()["numeric"], [0.0])
            np.testing.assert_allclose(cs.toworld([100.0])["numeric"], [4.0])
            np.testing.assert_allclose(cs.topixel([91.0])["numeric"], [2.0])


def test_first_interval_can_change_direction(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(tabular=True)
        assert cs.settabular([0, 2, 5], [10, 7, 30])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [8.5])
        np.testing.assert_allclose(cs.topixel([10.0])["numeric"], [2.0 + 9.0 / 23.0])
        assert cs.settabular([2, 0, 5], [1, 2, 3])
        np.testing.assert_allclose(cs.toworld([2.0])["numeric"], [2.4])
        np.testing.assert_allclose(cs.torecord()["tabular0"]["worldvalues"], [2.4, 2.0, 3.0])


@pytest.mark.parametrize(
    ("pixels", "worlds", "expected"),
    [([0, 2, 5], [10, 7, 30], 26.0), ([2, 0, 5], [1, 2, 3], -7.4)],
)
def test_reference_edits_keep_the_original_interpolation_intervals(casa, pixels, worlds, expected):
    tables = []
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(tabular=True)
        assert cs.settabular(pixels, worlds)
        assert cs.setlineartransform("tabular", [[2.0]])
        assert cs.setreferencevalue([15.0])
        assert cs.setreferencepixel([1.0])
        assert cs.setincrement([-4.0])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [expected])
        tables.append(cs.torecord()["tabular0"])
    _assert_value(tables[0], tables[1])


def test_replace_samples_on_a_hidden_pixel_axis(casa):
    for api in (tools, casa):
        original = api.coordsys().newcoordsys(linear=1, tabular=True)
        assert original.settabular([0, 2, 5], [10, 14, 30])
        record = original.torecord()
        record["pixelmap1"] = np.array([-2], dtype=np.int32)
        record["pixelreplace1"] = np.array([1.5])
        cs = api.coordsys()
        assert cs.fromrecord(record)
        assert cs.settabular(world=[16, 30, 76])
        changed = cs.torecord()
        np.testing.assert_equal(changed["pixelmap1"], [-2])
        np.testing.assert_equal(changed["pixelreplace1"], [1.5])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [1.0, 26.5])


def test_default_and_bad_arguments_leave_original_coordinate(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(linear=1)
        assert cs.settabular(which=6) is False
        with pytest.raises(RuntimeError, match="could not be found"):
            cs.settabular([0, 1], [1, 2])
        assert cs.addcoordinate(tabular=True)
        with pytest.raises(RuntimeError, match="same length"):
            cs.settabular(pixel=[0, 1])
        assert cs.settabular([0, 1, 3], [1, 2, 5])
        before = cs.torecord()
        assert cs.settabular([-1], [], -2) is False
        for values, error, phrase in (
            (([0, 1], [2], 0), RuntimeError, "same length"),
            (([0, 0], [2, 3], 0), RuntimeError, "first and last pixel values"),
            (([0, 1], [2, 2], 0), RuntimeError, "start and end values"),
            (([0, 2, 1], [2, 3, 4], 0), RuntimeError, "monotonically"),
            (([0, 1], [2, 3], -1), RuntimeError, "must be >= 0"),
            (([0, 1], [2, 3], None), RuntimeError, "must be >= 0"),
            (([0, 1], [2, 3], 1), RuntimeError, "could not be found"),
            (([0, 1], [2, 3], 0.9), TypeError, "must be an integer"),
        ):
            with pytest.raises(error, match=phrase):
                cs.settabular(*values)
        _assert_value(cs.torecord(), before)


def test_unusable_samples_preserve_the_prior_mapping():
    cs = tools.coordsys().newcoordsys(tabular=True)
    assert cs.settabular([0, 2, 5], [10, 14, 30])
    before = cs.torecord()
    with pytest.raises(RuntimeError, match="samples must be finite"):
        cs.settabular([0, 1], [2, np.inf])
    with pytest.raises(RuntimeError, match="repeated pixel values"):
        cs.settabular([0, 0, 2], [5, 6, 7])
    with pytest.raises(RuntimeError, match="repeated pixel values"):
        cs.settabular([0., 1e-30, 1.], [0., 1., 2.])
    _assert_value(cs.torecord(), before)


def test_tuple_binding_error_preserves_samples(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(tabular=True)
        assert cs.settabular([0, 1, 3], [1, 2, 5])
        before = cs.torecord()
        with pytest.raises(SystemError, match="with an exception set"):
            cs.settabular((0, 1), [2, 3])
        if api is tools:
            _assert_value(cs.torecord(), before)
        else:
            np.testing.assert_array_equal(cs.torecord()["tabular0"]["worldvalues"], [2, 3])


@pytest.mark.parametrize("single", [False, True])
def test_native_interoperability_between_writers_and_readers(casa, tmp_path, single):
    for writer in (tools, casa):
        cs = writer.coordsys().newcoordsys(tabular=True)
        pixels, worlds = ([2.0], [7.5]) if single else ([0, 2, 5], [10, 14, 30])
        assert cs.settabular(pixels, worlds)
        path = str(tmp_path / (writer.__name__ + ".im"))
        image = writer.image()
        assert image.fromshape(path, [6], cs.torecord())
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                np.testing.assert_allclose(coords.toworld([3.5])["numeric"], [7.5 if single else 22.0])
                np.testing.assert_allclose(coords.topixel([22.0])["numeric"], [2.0 if single else 3.5])
                stored = coords.torecord()["tabular0"]
                np.testing.assert_allclose(stored["pixelvalues"], pixels)
                np.testing.assert_allclose(stored["worldvalues"], worlds)
            finally:
                image.done()
