"""Coordinate replacement with independent axis maps and native products."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def replace_swapped_spectral_axis(api):
    source = api.coordsys().newcoordsys(linear=1, tabular=True)
    assert source.settabular([0, 2, 5], [10, 14, 30])
    assert source.transpose([1, 0])
    record = source.torecord()
    target = api.coordsys().newcoordsys(spectral=True, linear=1)
    assert target.transpose([1, 0])
    assert target.setobserver("Original observer")
    masks = target.torecord()
    assert target.replace(csys=record, whichin=np.int64(1), whichout=0)
    assert target.observer() == "Original observer"
    _assert_value(source.torecord(), record)
    stored = target.torecord()
    assert "spectral0" not in stored
    for key in ("worldmap0", "pixelmap0", "worldmap1", "pixelmap1"):
        _assert_value(stored[key], masks[key])
    return target


def test_replacement_indexes_creation_order_after_transpose():
    target = replace_swapped_spectral_axis(tools)
    np.testing.assert_allclose(target.toworld([3.0, 2.0])["numeric"], [3.0, 14.0])
    np.testing.assert_allclose(target.topixel([3.0, 14.0])["numeric"], [3.0, 2.0])


def test_replacement_uses_the_full_hidden_local_coordinate(casa):
    for api in (tools, casa):
        original = api.coordsys().newcoordsys(linear=2)
        record = original.torecord()
        record["pixelmap0"] = np.array([0, -2], dtype=np.int32)
        record["worldmap0"] = np.array([0, -2], dtype=np.int32)
        record["pixelreplace0"] = np.array([0.0, 7.0])
        record["worldreplace0"] = np.array([0.0, 25.0])
        target = api.coordsys()
        assert target.fromrecord(record)

        source = api.coordsys().newcoordsys(linear=2)
        assert source.setunits(["m", "m"])
        assert source.setreferencepixel([2.0, 3.0])
        assert source.setreferencevalue([100.0, 200.0])
        assert source.setincrement([3.0, 4.0])
        assert source.setlineartransform("linear", [[1.0, 2.0], [3.0, 5.0]])
        assert target.replace(source.torecord(), 0, 0)
        updated = target.torecord()
        np.testing.assert_array_equal(updated["pixelmap0"], [0, -2])
        _assert_value(updated["pixelreplace0"], record["pixelreplace0"])
        np.testing.assert_allclose(updated["worldreplace0"], [0.0, 25000.0])
        np.testing.assert_allclose(target.toworld([4.0])["numeric"], [142.0])
        np.testing.assert_allclose(target.topixel([142.0])["numeric"], [18532.0])


def test_replacement_preserves_other_live_coordinate_edits(casa):
    for api in (tools, casa):
        target = api.coordsys().newcoordsys(linear=1, tabular=True)
        assert target.settabular([0, 2, 5], [10, 14, 30])
        assert target.setlineartransform("tabular", [[2.0]])
        assert target.setreferencepixel([1.0], "tabular")
        before = target.torecord()["tabular1"]
        source = api.coordsys().newcoordsys(stokes="Q,U")
        assert target.replace(source.torecord(), 0, 0)
        _assert_value(target.torecord()["tabular1"], before)
        np.testing.assert_allclose(target.toworld([1.0, 1.0])["numeric"], [3.0, 6.0])


def test_bad_records_indices_and_dimensions_are_atomic(casa):
    for api in (tools, casa):
        target = api.coordsys().newcoordsys(spectral=True)
        source = api.coordsys().newcoordsys(linear=1).torecord()
        wider = api.coordsys().newcoordsys(linear=2).torecord()
        before = target.torecord()
        for args, error, message in (
            ((None, 0, 0), TypeError, "not a dictionary"),
            (({}, 0, 0), RuntimeError, "Illegal index 0 for input coordinate"),
            ((source, 1, 0), RuntimeError, "Illegal index 1 for input coordinate"),
            ((source, 0, -1), RuntimeError, "Illegal index -1 for output coordinate"),
            ((source, None, 0), RuntimeError, "Illegal index -1 for input coordinate"),
            ((source, 0.5, 0), TypeError, "_whichin must be an integer"),
            ((source, 0, "0"), TypeError, "_whichout must be an integer"),
            ((wider, 0, 0), RuntimeError, "Coordinates must have the same number of axes"),
        ):
            with pytest.raises(error, match=message):
                target.replace(*args)
            _assert_value(target.torecord(), before)


def test_replacement_survives_all_native_writer_reader_pairs(casa, tmp_path):
    for writer in (tools, casa):
        target = replace_swapped_spectral_axis(writer)
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [6, 6], target.torecord())
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                np.testing.assert_allclose(coords.toworld([3.5, 3.5])["numeric"], [3.5, 22.0])
                np.testing.assert_allclose(coords.topixel([3.5, 22.0])["numeric"], [3.5, 3.5])
            finally:
                image.done()
