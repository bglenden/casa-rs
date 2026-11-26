"""Native records preserve a CoordinateSystem as a coordinate inside another."""

from copy import deepcopy

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.fixture
def system_record(casa):
    seed = casa.coordsys().newcoordsys(linear=2)
    try:
        assert seed.setreferencepixel([5.0, 8.0])
        assert seed.setreferencevalue([100.0, 30.0])
        assert seed.setincrement([2.0, -3.0])
        assert seed.setepoch({"type": "epoch", "refer": "UTC", "m0": {"value": 59000.0, "unit": "d"}})
        nested = seed.torecord()
    finally:
        seed.done()
    nested["linear0"]["units"] = ["m", "s"]
    return nested_system_record(nested, [90.0, 54.0])


def nested_system_record(nested, world_at_origin):
    rank = len(world_at_origin)
    return {
        "coordsys0": nested,
        "telescope": nested["telescope"],
        "observer": nested["observer"],
        "obsdate": nested["obsdate"],
        "pointingcenter": nested["pointingcenter"],
        "worldmap0": np.arange(rank, dtype=np.int32),
        "pixelmap0": np.arange(rank, dtype=np.int32),
        "worldreplace0": np.array(world_at_origin, dtype=np.float64),
        "pixelreplace0": np.zeros(rank, dtype=np.float64),
    }


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_setcoordsys_rejects_invalid_increment_on_a_hidden_nested_world_axis(backend, request):
    api = tools if backend == "casars" else request.getfixturevalue("casa")
    coordinate = api.coordsys().newcoordsys(linear=2)
    image = api.image()
    try:
        assert coordinate.setincrement([3., 5.])
        nested = coordinate.torecord()
        nested["worldmap0"] = np.array([0, -2], dtype=np.int32)
        good = nested_system_record(nested, [0.])
        good["pixelmap0"] = np.array([0, 1], dtype=np.int32)
        good["pixelreplace0"] = np.zeros(2, dtype=np.float64)
        invalid = deepcopy(good)
        invalid["coordsys0"]["linear0"]["cdelt"] = [3., 0.]
        assert image.fromshape(shape=[4, 3], csys=good)
        with pytest.raises(RuntimeError, match="Linear transformation matrix is singular"):
            image.setcoordsys(invalid)
        assert image.isopen()
        np.testing.assert_array_equal(image.toworld([2., 1.])["numeric"], [6.])
        attached = image.coordsys()
        try:
            np.testing.assert_array_equal(
                attached.torecord()["coordsys0"]["linear0"]["cdelt"], [3., 5.]
            )
        finally:
            attached.done()
    finally:
        image.done()
        coordinate.done()


@pytest.mark.parametrize("permuted", [False, True])
def test_nested_system_scalar_and_batch_world_transforms(casa, system_record, permuted):
    order = np.array([1, 0] if permuted else [0, 1])
    system_record["pixelmap0"] = order.astype(np.int32)
    system_record["worldmap0"] = order.astype(np.int32)
    pixels = np.array([[6.0, 3.0], [7.0, 10.0]])[order]
    world = np.array([[102.0, 96.0], [33.0, 24.0]])[order]
    for api in (casa, tools):
        cs = api.coordsys()
        cloned = None
        try:
            assert cs.fromrecord(system_record)
            cloned = cs.copy()
            assert cs.ncoordinates() == 1
            assert cs.naxes() == 2
            assert list(cs.coordinatetype()) == ["System"]
            assert not cs.findcoordinate("linear")["return"]
            np.testing.assert_array_equal(cs.names(), np.array(["LinAxis0", "LinAxis1"])[order])
            np.testing.assert_array_equal(cs.units(), np.array(["m", "s"])[order])
            np.testing.assert_array_equal(cs.toworld(pixels[:, 0])["numeric"], world[:, 0])
            np.testing.assert_array_equal(cs.topixel(world[:, 0])["numeric"], pixels[:, 0])
            np.testing.assert_array_equal(cs.toworldmany(pixels)["numeric"], world)
            np.testing.assert_array_equal(cs.topixelmany(world)["numeric"], pixels)
            relative = cs.torel(cs.toworld(pixels[:, 0]))
            np.testing.assert_array_equal(relative["numeric"], np.array([2.0, 3.0])[order])
            np.testing.assert_array_equal(cs.toabs(relative)["numeric"], world[:, 0])
            with pytest.raises(RuntimeError, match="Cannot handle Coordinates of type System"):
                cs.toworld(pixels[:, 0], format="m")
            assert "System" in cs.summary(list=False)[0]
            saved = cs.torecord()
            assert "linear0" not in saved
            assert "linear0" in saved["coordsys0"]
            np.testing.assert_array_equal(saved["pixelmap0"], order)
            np.testing.assert_array_equal(cloned.toworldmany(pixels)["numeric"], world)
            shifted = cs.subimage([2.0, 1.0], [6, 9])
            original_reference = np.array([5.0, 8.0])
            np.testing.assert_array_equal(
                shifted["coordsys0"]["linear0"]["crpix"], original_reference - np.array([2, 1])[order],
            )
        finally:
            if cloned is not None:
                cloned.done()
            cs.done()


def test_nested_edits_and_records_survive_all_native_writer_reader_pairs(casa, system_record, tmp_path):
    pixels = np.arange(12 * 13, dtype=np.float32).reshape(12, 13)
    for writer in (casa, tools):
        cs = writer.coordsys()
        image = writer.image()
        dest = str(tmp_path / f"{writer.__name__}-nested.im")
        try:
            assert cs.fromrecord(system_record)
            assert cs.setunits(["cm", "ms"])
            np.testing.assert_array_equal(cs.toworld([6, 7])["numeric"], [10200.0, 33000.0])
            assert cs.setreferencevalue([2.0, 3.0])
            assert cs.setnames(["X", "Y"])
            record = cs.torecord()
            assert image.fromarray(dest, pixels, csys=record)
        finally:
            cs.done()
            image.done()
        for reader in (casa, tools):
            image = reader.image()
            read = None
            try:
                assert image.open(dest)
                np.testing.assert_array_equal(image.getchunk(), pixels)
                read = image.coordsys()
                assert list(read.coordinatetype()) == ["System"]
                stored = read.torecord()["coordsys0"]["linear0"]
                np.testing.assert_array_equal(stored["axes"], ["X", "Y"])
                np.testing.assert_array_equal(stored["units"], ["cm", "ms"])
                np.testing.assert_array_equal(stored["crval"], [2.0, 3.0])
                np.testing.assert_array_equal(read.toworld([6, 7])["numeric"], [202.0, 3003.0])
            finally:
                if read is not None:
                    read.done()
                image.done()


@pytest.mark.parametrize("hide_longitude", [False, True])
def test_direction_and_frequency_in_a_nested_system_use_declared_units(casa, hide_longitude):
    seed = casa.coordsys().newcoordsys(direction=True, spectral=True)
    try:
        assert seed.setunits(["deg", "deg", "Hz"])
        assert seed.setreferencevalue([20.0, 30.0, 1.4e9])
        assert seed.setreferencepixel([4.0, 5.0, 8.0])
        assert seed.setincrement([-0.25, 0.125, 1000.0])
        original = nested_system_record(seed.torecord(), seed.toworld([0, 0, 0])["numeric"])
    finally:
        seed.done()

    points = np.array([[4.0, 5.0, 8.0], [4.0 if hide_longitude else 6.0, 4.0, 7.0]]).T
    east = np.deg2rad(-0.25 * (points[0] - 4.0))
    north = np.deg2rad(0.125 * (points[1] - 5.0))
    forward = np.sqrt(1.0 - east**2 - north**2)
    latitude = np.deg2rad(30.0)
    expected = np.array([
        20.0 + np.rad2deg(np.arctan2(east, forward * np.cos(latitude) - north * np.sin(latitude))),
        np.rad2deg(np.arcsin(forward * np.sin(latitude) + north * np.cos(latitude))),
        1.4e9 + (points[2] - 8.0) * 1000.0,
    ])
    first_axis = int(hide_longitude)
    if hide_longitude:
        original["pixelmap0"] = np.array([-1, 0, 1], dtype=np.int32)
        original["worldmap0"] = np.array([-1, 0, 1], dtype=np.int32)
        original["pixelreplace0"][0] = 4.0
        original["worldreplace0"][0] = 20.0
        points = points[1:]
        expected = expected[1:]
    for api in (casa, tools):
        cs = api.coordsys()
        try:
            assert cs.fromrecord(original)
            np.testing.assert_array_equal(cs.units(), ["deg", "deg", "Hz"][first_axis:])
            np.testing.assert_allclose(cs.referencevalue()["numeric"], [20.0, 30.0, 1.4e9][first_axis:])
            np.testing.assert_allclose(cs.increment()["numeric"], [-0.25, 0.125, 1000.0][first_axis:])
            np.testing.assert_allclose(cs.toworldmany(points)["numeric"], expected, atol=1e-8, rtol=0)
            np.testing.assert_allclose(cs.topixelmany(expected)["numeric"], points, atol=1e-8, rtol=0)
            np.testing.assert_allclose(cs.toworld(points[:, 1])["numeric"], expected[:, 1], atol=1e-8, rtol=0)
            np.testing.assert_allclose(cs.topixel(expected[:, 1])["numeric"], points[:, 1], atol=1e-8, rtol=0)
            if not hide_longitude:
                np.testing.assert_array_equal(cs.increment("s")["string"], [
                    "-9.00000000e+02 arcsec", "4.50000000e+02 arcsec", "1.00000000e+03 Hz",
                ])
                rows = cs.summary(list=False)[0].splitlines()[-3:]
                assert rows[0].endswith("20     4.00 -9.000000e+02 arcsec")
                assert rows[1].endswith("30     5.00  4.500000e+02 arcsec")
                assert cs.setreferencevalue([40.0, 50.0, 1.4e9])
                assert cs.setincrement([-0.5, 0.25, 500.0])
                np.testing.assert_allclose(cs.referencevalue()["numeric"], [40.0, 50.0, 1.4e9])
                np.testing.assert_allclose(cs.increment()["numeric"], [-0.5, 0.25, 500.0])
                assert cs.setreferencelocation(pixel=[6, 7, 8], world=[42.0, 52.0, 1.4e9])
                np.testing.assert_allclose(cs.referencevalue()["numeric"], [42.0, 52.0, 1.4e9])
                np.testing.assert_array_equal(cs.referencepixel()["numeric"], [6.0, 7.0, 8.0])
                image = api.image()
                try:
                    assert image.fromshape(shape=[12, 13, 15], csys=cs.torecord())
                    report = image.summary(list=False)
                    assert list(report["axisunits"]) == ["deg", "deg", "Hz"]
                    np.testing.assert_allclose(report["refval"], [42.0, 52.0, 1.4e9])
                    np.testing.assert_allclose(report["incr"], [-0.5, 0.25, 500.0])
                finally:
                    image.done()
        finally:
            cs.done()


def test_fourier_axes_remain_in_the_nested_system(casa, system_record, tmp_path):
    pixels = np.zeros((12, 13), dtype=np.float32)
    pixels[6, 6] = 1.0
    for api in (casa, tools):
        image = api.image()
        result = api.image()
        coords = None
        try:
            assert image.fromarray(pixels=pixels, csys=system_record)
            name = str(tmp_path / f"{api.__name__}-fourier.im")
            assert image.fft(real=name, axes=[0, 1])
            assert result.open(name)
            np.testing.assert_allclose(result.getchunk(), 1.0, atol=2e-7, rtol=0)
            coords = result.coordsys()
            assert list(coords.coordinatetype()) == ["System"]
            assert list(coords.units()) == ["1/m", "Hz"]
            np.testing.assert_allclose(coords.increment()["numeric"], [1.0 / 24.0, -1.0 / 39.0])
            np.testing.assert_allclose(coords.toworld([6.0, 6.0])["numeric"], [0.0, 0.0])
        finally:
            if coords is not None:
                coords.done()
            result.done()
            image.done()


def test_mixed_conversion_of_a_nested_system_raises_a_python_error(system_record):
    # CASA 6.7.6.14 terminates the process for mixed conversion of this record.
    cs = tools.coordsys()
    try:
        assert cs.fromrecord(system_record)
        with pytest.raises(RuntimeError, match="nested coordinate system"):
            cs.convert([6.0, 7.0], unitsin=["pix", "pix"], unitsout=["cm", "ms"])
        with pytest.raises(RuntimeError, match="nested coordinate system"):
            cs.convertmany([[6.0], [7.0]], unitsin=["pix", "pix"], unitsout=["cm", "ms"])
        np.testing.assert_array_equal(cs.toworld([6.0, 7.0])["numeric"], [102.0, 33.0])
    finally:
        cs.done()


def test_replacing_a_coordinate_with_a_nested_system_retains_its_definition(casa, system_record):
    for api in (casa, tools):
        output = api.coordsys().newcoordsys(linear=2)
        reopened = api.coordsys()
        try:
            assert output.replace(system_record, 0, 0)
            assert list(output.coordinatetype()) == ["System"]
            saved = output.torecord()
            assert "coordsys0" in saved
            assert "linear0" in saved["coordsys0"]
            assert reopened.fromrecord(saved)
            np.testing.assert_array_equal(reopened.toworld([6.0, 7.0])["numeric"], [102.0, 33.0])
        finally:
            reopened.done()
            output.done()


@pytest.mark.parametrize("field", ["pixelreplace0", "worldreplace0"])
def test_rejected_nested_record_preserves_previous_state(casa, system_record, field):
    malformed = deepcopy(system_record)
    malformed["coordsys0"].pop(field)
    for api in (casa, tools):
        cs = api.coordsys()
        try:
            assert cs.fromrecord(system_record)
            with pytest.raises(RuntimeError):
                cs.fromrecord(malformed)
            np.testing.assert_array_equal(cs.toworld([6.0, 7.0])["numeric"], [102.0, 33.0])
        finally:
            cs.done()


def test_nested_stokes_labels_format_without_losing_the_system_type(casa):
    seed = casa.coordsys().newcoordsys(stokes=["I", "Q", "U", "V"])
    try:
        record = nested_system_record(seed.torecord(), [1.0])
    finally:
        seed.done()
    for api in (casa, tools):
        cs = api.coordsys()
        try:
            assert cs.fromrecord(record)
            assert list(cs.toworld([2.0], "s")["string"]) == ["U"]
            assert list(cs.increment("s")["string"]) == ["I"]
            assert cs.summary(list=False)[0].splitlines()[-1].split() == [
                "0", "0", "System", "Stokes", "I", "0.00", "I",
            ]
        finally:
            cs.done()


def test_summary_rejects_a_nested_system_with_unequal_axis_counts_without_aborting(system_record):
    system_record["coordsys0"]["worldmap0"] = np.array([-1, 0], dtype=np.int32)
    system_record["worldmap0"] = np.array([0], dtype=np.int32)
    system_record["worldreplace0"] = np.array([54.0])
    cs = tools.coordsys()
    try:
        assert cs.fromrecord(system_record)
        with pytest.raises(RuntimeError, match="unequal pixel and world axes"):
            cs.summary(list=False)
        np.testing.assert_array_equal(cs.toworld([6.0, 7.0])["numeric"], [33.0])
    finally:
        cs.done()
