"""Coordinate-only subimages, signed axis placement, and native readback."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def test_fractional_linear_shift_is_independent_of_the_output_shape():
    cs = tools.coordsys().newcoordsys(linear=2)
    assert cs.setreferencepixel([3.0, -1.0])
    assert cs.setreferencevalue([20.0, 45.0])
    assert cs.setincrement([3.0, -7.0])
    start = [1.50000001, -2.25]
    before = cs.torecord()
    record = cs.subimage(start, [-1, 0])
    np.testing.assert_allclose(record["linear0"]["crpix"], [1.5, 1.25])
    cropped = tools.coordsys()
    assert cropped.fromrecord(record)
    np.testing.assert_allclose(cropped.toworld([3.5, 2.25])["numeric"], [26.0, 38.0])
    _assert_value(cs.torecord(), before)


def test_interleaved_axes_stokes_and_fractional_origin_match_casa(casa):
    records = []
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(direction=True, stokes="I,Q,U,V", linear=2)
        assert cs.transpose([1, 2, 4, 0, 3])
        before = cs.torecord()
        # Dec, Stokes, Linear1, RA, Linear0. The Stokes origin rounds to 1.
        starts = [1.75, 1.2, -1.5, 0.25, 1.5]
        point = np.array([1.0, 0.0, 2.0, 2.0, 3.0])
        shift = np.array([1.75, 1.0, -1.5, 0.25, 1.5])
        result = cs.subimage(np.array([starts]), newshape=[4, 2, 4, 4, 4])
        new = api.coordsys()
        assert new.fromrecord(result)
        _assert_value(new.stokes(), ["Q", "U"])
        np.testing.assert_allclose(new.toworld(point)["numeric"], cs.toworld(point + shift)["numeric"])
        np.testing.assert_array_equal(result["pixelmap0"], [3, 0])
        np.testing.assert_array_equal(result["worldreplace1"], before["worldreplace1"])
        _assert_value(cs.torecord(), before)
        records.append(result)
    for field in ("direction0", "stokes1", "linear2", "pixelmap0", "worldmap0"):
        _assert_value(records[0][field], records[1][field])


@pytest.mark.parametrize("length, expected", [([], [1020., 1055., 1105., 1155.]), ([1], [1020.])])
@pytest.mark.parametrize(("unit", "right_scale"), [("Hz", 1.e6), ("MHz", 1.0)])
def test_subsampled_spectral_tables_keep_native_frames_and_physical_hz(casa, length, expected, unit, right_scale):
    source = casa.coordsys().newcoordsys(spectral=True)
    frequencies = np.array([1.e9, 1.01e9, 1.03e9, 1.08e9])
    assert source.setspectral(refcode="LSRK", frequencies={"value": frequencies, "unit": "Hz"})
    assert source.setunits([unit])
    record = source.torecord()
    record["worldreplace0"] = np.array([1020.0 * right_scale])
    assert source.fromrecord(record)
    current = tools.coordsys()
    assert current.fromrecord(source.torecord())
    left_record = current.subimage([1.5], length)
    right_record = source.subimage([1.5], length)
    left, right = (returned["spectral0"] for returned in (left_record, right_record))
    assert left["unit"] == right["unit"] == "Hz"
    assert left["system"] == right["system"] == "LSRK"
    expected = np.array(expected)
    np.testing.assert_allclose(right["tabular"]["worldvalues"], expected * right_scale)
    np.testing.assert_allclose(left["tabular"]["worldvalues"], expected * 1.e6)
    np.testing.assert_allclose(left["restfreq"], right["restfreq"] * (1.e6 / right_scale))
    np.testing.assert_allclose(left_record["worldreplace0"], [1020.e6])
    np.testing.assert_allclose(left_record["worldreplace0"], right_record["worldreplace0"])
    with pytest.raises(RuntimeError, match="illegal table"):
        current.subimage([1.5], [-1])


def test_sampled_spectral_section_keeps_an_undefined_frame(casa):
    source = casa.coordsys().newcoordsys(spectral=True)
    frequencies = np.array([1.e9, 1.01e9, 1.03e9])
    assert source.setspectral(frequencies={"value": frequencies, "unit": "Hz"})
    record = source.torecord()
    record["spectral0"]["system"] = "Undefined"
    record["spectral0"]["conversion"]["system"] = "Undefined"
    for api in (tools, casa):
        cs = api.coordsys()
        assert cs.fromrecord(record)
        section = cs.subimage([0.5], [2])["spectral0"]
        assert section["system"] == "Undefined"
        assert section["conversion"]["system"] == "Undefined"
        np.testing.assert_allclose(section["tabular"]["worldvalues"], [1005.e6, 1020.e6])


def test_spectral_extrapolation_rejects_unusable_new_tables(casa):
    source = casa.coordsys().newcoordsys(spectral=True)
    frequencies = np.array([10.0, 9.0, 11.0, 13.0])
    assert source.setspectral(frequencies={"value": frequencies, "unit": "Hz"})
    for api in (tools, casa):
        cs = api.coordsys()
        assert cs.fromrecord(source.torecord())
        before = cs.torecord()
        with pytest.raises(RuntimeError, match="start and end values"):
            cs.subimage([-1.0], [4])
        with pytest.raises(RuntimeError, match="monotonically"):
            cs.subimage([-1.0], [5])
        _assert_value(cs.torecord(), before)


def test_independent_pixel_and_world_order_keeps_increments_on_their_own_axes(casa):
    source = casa.coordsys().newcoordsys(spectral=True, linear=1)
    frequencies = np.array([10.0, 11.0, 13.0, 18.0])
    assert source.setspectral(frequencies={"value": frequencies, "unit": "Hz"})
    assert source.setincrement([2.0], "linear")
    record = source.torecord()
    record["worldmap0"] = np.array([1], dtype=np.int32)
    record["worldmap1"] = np.array([0], dtype=np.int32)
    for api, linear_step, spectral_step in ((tools, 2.0, 5.0), (casa, 5.0, 8.0 / 3.0)):
        cs = api.coordsys()
        assert cs.fromrecord(record)
        section = cs.subimage([2.0, 0.0], [2, 4])
        np.testing.assert_array_equal(section["worldmap0"], [1])
        np.testing.assert_array_equal(section["pixelmap0"], [0])
        np.testing.assert_allclose(section["linear1"]["cdelt"], [linear_step])
        np.testing.assert_allclose(section["spectral0"]["tabular"]["worldvalues"], [13.0, 13.0 + spectral_step])


def test_numeric_zero_dimensional_arrays_are_one_element_origins(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(linear=1)
        for origin, pixel in ((np.array(1.5), -1.5), (np.array(2, dtype=np.int16), -2.0)):
            np.testing.assert_array_equal(cs.subimage(origin)["linear0"]["crpix"], [pixel])
        with pytest.raises(RuntimeError, match="not a vector of pixel positions"):
            cs.subimage(np.array([], dtype=np.float16))
        with pytest.raises(RuntimeError, match="originShift.nelements"):
            cs.subimage(np.array([], dtype=float))
    if np.little_endian:
        origin = np.array(1.5, dtype=">f8")
        for api, pixel in ((tools, -1.5), (casa, 0.0)):
            cs = api.coordsys().newcoordsys(linear=1)
            np.testing.assert_array_equal(cs.subimage(origin)["linear0"]["crpix"], [pixel])


def test_rejected_arguments_and_stokes_extents(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(stokes="I,Q,U,V")
        before = cs.torecord()
        for origin in (None, [], 1.0, [True]):
            with pytest.raises(RuntimeError, match="not a vector of pixel positions"):
                cs.subimage(origin)
        with pytest.raises(RuntimeError, match="originShift.nelements"):
            cs.subimage([0.0, 1.0])
        with pytest.raises(RuntimeError, match="nShape==0"):
            cs.subimage([0.0], [1, 2])
        with pytest.raises(RuntimeError, match="Illegal origin shift"):
            cs.subimage([-1.75])
        with pytest.raises(RuntimeError, match="New shape is invalid"):
            cs.subimage([3.0], [2])
        with pytest.raises(TypeError, match="_newshape must not be a string"):
            cs.subimage([0.0], "2")
        with pytest.raises(SystemError, match="exception set"):
            cs.subimage([0.0], (2,))
        np.testing.assert_array_equal(cs.subimage([-0.75], [0])["stokes0"]["stokes"], ["I", "Q", "U", "V"])
        _assert_value(cs.torecord(), before)


def test_tabular_coordinate_section_survives_native_readback(casa, tmp_path):
    for writer in (tools, casa):
        cs = writer.coordsys().newcoordsys(tabular=True)
        assert cs.settabular([0, 2, 5], [10, 14, 30])
        section = cs.subimage([1.5], [3])
        table = section["tabular0"]
        np.testing.assert_allclose(table["crpix"], [-1.5])
        np.testing.assert_allclose(table["pixelvalues"], [0, 2, 5])
        np.testing.assert_allclose(table["worldvalues"], [16, 20, 36])
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [3], section)
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                np.testing.assert_allclose(coords.toworld([1.0])["numeric"], [18.0])
            finally:
                image.done()


def test_sampled_spectral_section_keeps_conversion_across_native_readback(casa, tmp_path):
    source = casa.coordsys().newcoordsys(direction=True, spectral=True)
    frequencies = np.array([1.e9, 1.01e9, 1.03e9, 1.08e9])
    assert source.setspectral(refcode="LSRK", frequencies={"value": frequencies, "unit": "Hz"})
    assert source.setconversiontype(spectral="BARY")
    native = source.torecord()
    scale = source.toworld([0.0, 0.0, 0.0])["numeric"][2] / frequencies[0]
    for writer in (tools, casa):
        coords = writer.coordsys()
        assert coords.fromrecord(native)
        record = coords.subimage([0.0, 0.0, 1.5], [2, 2, 4])
        assert record["spectral1"]["system"] == "LSRK"
        assert record["spectral1"]["conversion"]["system"] == "BARY"
        np.testing.assert_allclose(record["spectral1"]["tabular"]["worldvalues"], np.array([1020., 1055., 1105., 1155.]) * 1.e6)
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [2, 2, 4], record)
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                assert coords.getconversiontype("spectral", True) == "BARY"
                world = coords.toworld([0.0, 0.0, 0.5])["numeric"]
                np.testing.assert_allclose(world, [0.0, 0.0, 1037.5e6 * scale], rtol=1.e-11)
                np.testing.assert_allclose(coords.topixel(world)["numeric"], [0.0, 0.0, 0.5], atol=1.e-8)
            finally:
                image.done()
