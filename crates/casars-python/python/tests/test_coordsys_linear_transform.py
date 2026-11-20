"""Live and persisted coordinate matrix edits with a pinned CASA oracle."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _linear(backend):
    cs = backend.coordsys().newcoordsys(linear=2)
    assert cs.setreferencevalue([110.0, 190.0], "linear")
    assert cs.setreferencepixel([2.0, 3.0], "linear")
    assert cs.setincrement([2.0, 4.0], "linear")
    return cs


def test_coupled_linear_matrix_matches_an_independent_affine_calculation(casa):
    matrix = np.array([[1.0, 2.0], [3.0, 5.0]])
    pixels = np.array([[3.0, 1.0], [5.0, 4.0]])
    expected = np.array([[110.0], [190.0]]) + np.array([[2.0], [4.0]]) * (matrix.T @ (pixels - [[2.0], [3.0]]))
    for backend in (tools, casa):
        cs = _linear(backend)
        for values in (matrix.tolist(), matrix.astype(np.int32), matrix[::-1][::-1]):
            assert cs.setlineartransform(type="linear", value=values)
            np.testing.assert_array_equal(cs.lineartransform("linear"), matrix)
            np.testing.assert_allclose(cs.toworldmany(pixels)["numeric"], expected)
            np.testing.assert_allclose(cs.topixelmany(expected)["numeric"], pixels)
        assert cs.setlineartransform("linear", [[1.0]])
        np.testing.assert_array_equal(cs.lineartransform("linear"), matrix)
        assert cs.setlineartransform("linear", np.array([], dtype=float))
        np.testing.assert_array_equal(cs.lineartransform("linear"), matrix)
        with pytest.raises(RuntimeError, match="not a double or integer array"):
            cs.setlineartransform("linear", [])
        with pytest.raises(RuntimeError, match="not a double or integer array"):
            cs.setlineartransform("linear", 2.0)
        with pytest.raises(RuntimeError, match="value array is empty"):
            cs.setlineartransform("linear", np.array(2.0))
        np.testing.assert_array_equal(cs.lineartransform("linear"), matrix)


def test_direction_matrix_normalizes_columns_with_signed_increment_scales(casa):
    matrix = np.array([[-1.0, 0.5], [0.2, -1.0]])
    scale = np.copysign(np.linalg.norm(matrix, axis=0), np.diag(matrix))
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(direction=True)
        assert cs.setunits(["deg", "deg"], "direction")
        assert cs.setreferencevalue([40.0, 23.0], "direction")
        assert cs.setreferencepixel([3.0, 5.0], "direction")
        pixel = np.array([4.0, 7.0])
        increment = cs.increment()["numeric"]
        expected = cs.toworld([3.0, 5.0] + matrix.T @ (pixel - [3.0, 5.0]))["numeric"]
        assert cs.setlineartransform("direction", matrix)
        np.testing.assert_allclose(cs.lineartransform("direction"), matrix / scale)
        np.testing.assert_allclose(cs.increment()["numeric"], increment * scale)
        np.testing.assert_allclose(cs.toworld(pixel)["numeric"], expected, atol=1e-12, rtol=0.0)
        np.testing.assert_allclose(cs.topixel(expected)["numeric"], pixel, atol=1e-9, rtol=0.0)
        with pytest.raises(RuntimeError, match="Linear transformation matrix is singular"):
            cs.setlineartransform("direction", [[0.0, 2.0], [1.0, 0.0]])
        np.testing.assert_allclose(cs.lineartransform("direction"), matrix / scale)


def test_spectral_pc_is_separate_from_increment_and_stokes_stays_fixed(casa):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(spectral=True, stokes="I,Q")
        spectral = int(cs.findcoordinate("spectral")["pixel"][0])
        pixel = np.ones(2)
        initial = cs.toworld(pixel)["numeric"]
        increment = cs.increment(type="spectral")["numeric"]
        assert cs.setlineartransform("spectral", [-2.0])
        assert cs.setlineartransform("stokes", [[4.0]])
        np.testing.assert_array_equal(cs.lineartransform("stokes"), [[1.0]])
        np.testing.assert_array_equal(cs.lineartransform("spectral"), [[-2.0]])
        np.testing.assert_array_equal(cs.increment(type="spectral")["numeric"], increment)
        expected = initial.copy()
        expected[spectral] -= 3 * increment[0]
        np.testing.assert_allclose(cs.toworld(pixel)["numeric"], expected, atol=1e-7, rtol=0.0)
        with pytest.raises(RuntimeError, match="Linear transformation matrix is singular"):
            cs.setlineartransform("spectral", [[0.0]])
        np.testing.assert_allclose(cs.toworld(pixel)["numeric"], expected, atol=1e-7, rtol=0.0)
        restored = backend.coordsys()
        assert restored.fromrecord(cs.torecord())
        np.testing.assert_array_equal(restored.lineartransform("spectral"), [[-2.0]])
        np.testing.assert_allclose(restored.topixel(expected)["numeric"], pixel)


@pytest.mark.parametrize("sampled", [False, True])
def test_tabular_pc_live_and_record_restore_semantics(casa, sampled):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(tabular=True)
        if sampled:
            record = cs.torecord()
            record["tabular0"]["pixelvalues"] = np.array([0.0, 2.0, 5.0])
            record["tabular0"]["worldvalues"] = np.array([10.0, 14.0, 30.0])
            assert cs.fromrecord(record)
        assert cs.setlineartransform("tabular", [[2.0]])
        np.testing.assert_array_equal(cs.lineartransform("tabular"), [[2.0]])
        world = cs.toworld([1.0])["numeric"]
        np.testing.assert_array_equal(world, [14.0 if sampled else 2.0])
        np.testing.assert_array_equal(cs.topixel(world)["numeric"], [1.0])
        recorded = cs.torecord()
        np.testing.assert_array_equal(recorded["tabular0"]["pc"], [[2.0]])
        if sampled:
            np.testing.assert_array_equal(recorded["tabular0"]["worldvalues"], [10.0, 18.0, 50.0])
        restored = backend.coordsys()
        assert restored.fromrecord(recorded)
        np.testing.assert_array_equal(restored.lineartransform("tabular"), [[1.0]])
        np.testing.assert_array_equal(restored.torecord()["tabular0"]["pc"], [[1.0]])
        np.testing.assert_allclose(restored.torecord()["tabular0"]["cdelt"], [8.0 if sampled else 1.0])
        np.testing.assert_array_equal(restored.toworld([1.0])["numeric"], [14.0 if sampled else 1.0])


@pytest.mark.parametrize("sampled", [False, True])
@pytest.mark.parametrize("field", ["name", "increment", "referencepixel", "referencevalue"])
def test_later_tabular_edits_preserve_the_live_matrix(casa, sampled, field):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(tabular=True)
        if sampled:
            record = cs.torecord()
            record["tabular0"]["pixelvalues"] = np.array([0.0, 2.0, 5.0])
            record["tabular0"]["worldvalues"] = np.array([10.0, 14.0, 30.0])
            assert cs.fromrecord(record)
        assert cs.setlineartransform("tabular", [[2.0]])
        if field == "name":
            assert cs.setnames(["TabularSample"], "tabular")
            expected = 14.0 if sampled else 2.0
        elif field == "increment":
            assert cs.setincrement([7.0], "tabular")
            expected = 17.0 if sampled else 14.0
        elif field == "referencepixel":
            assert cs.setreferencepixel([2.0], "tabular")
            expected = -2.0
        else:
            assert cs.setreferencevalue([9.0], "tabular")
            expected = 13.0 if sampled else 11.0
        np.testing.assert_array_equal(cs.lineartransform("tabular"), [[2.0]])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [expected])


@pytest.mark.parametrize("kind", ["linear", "direction"])
def test_singular_matrices_keep_forward_transforms_and_report_undefined_inverses(casa, kind):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(**{kind: 2 if kind == "linear" else True})
        assert cs.setlineartransform(kind, [[1.0, 2.0], [2.0, 4.0]])
        world = cs.toworld([1.0, 1.0])["numeric"]
        assert np.isfinite(world).all()
        assert np.isnan(cs.topixel(world)["numeric"]).all()
        assert np.isnan(cs.topixelmany(world[:, None])["numeric"]).all()


def test_small_valid_matrices_are_invertible_and_zero_wcs_rows_are_rejected(casa):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(linear=2)
        matrix = np.eye(2) * 1e-16
        assert cs.setlineartransform("linear", matrix)
        pixel = np.array([1.0, 2.0])
        world = cs.toworld(pixel)["numeric"]
        np.testing.assert_allclose(cs.topixel(world)["numeric"], pixel)
        with pytest.raises(RuntimeError, match="Linear transformation matrix is singular"):
            cs.setlineartransform("linear", [[0.0, 0.0], [0.0, 1.0]])
        np.testing.assert_array_equal(cs.lineartransform("linear"), matrix)


def test_setter_and_getter_select_the_same_record_coordinate_with_permuted_axes(casa):
    original = casa.coordsys().newcoordsys(linear=1)
    assert original.addcoordinate(linear=1)
    record = original.torecord()
    for prefix in ("pixelmap", "worldmap"):
        record[prefix + "0"] = np.array([1], dtype=np.int32)
        record[prefix + "1"] = np.array([0], dtype=np.int32)
    for backend in (tools, casa):
        cs = backend.coordsys()
        assert cs.fromrecord(record)
        assert cs.setlineartransform("linear", [[3.0]])
        np.testing.assert_array_equal(cs.lineartransform("linear"), [[3.0]])
        np.testing.assert_array_equal(cs.toworld([1.0, 2.0])["numeric"], [1.0, 6.0])
        np.testing.assert_array_equal(cs.torecord()["linear0"]["pc"], [[3.0]])


def test_linear_coordinate_matrix_after_native_write_and_reopen(casa, tmp_path):
    matrix = np.array([[1.0, 2.0], [3.0, 5.0]])
    for index, writer in enumerate((tools, casa)):
        path = str(tmp_path / f"matrix-{index}.im")
        cs = _linear(writer)
        assert cs.setlineartransform("linear", matrix)
        image = writer.image()
        assert image.fromshape(path, [6, 7], csys=cs.torecord())
        assert image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            cs = image.coordsys()
            np.testing.assert_array_equal(cs.lineartransform("linear"), matrix)
            np.testing.assert_array_equal(cs.toworld([3.0, 5.0])["numeric"], [124.0, 238.0])
            assert image.done()


def test_invalid_matrix_type_and_rank_do_not_edit_a_coordinate():
    cs = _linear(tools)
    with pytest.raises(RuntimeError, match="two dimensions"):
        cs.setlineartransform("linear", np.ones((2, 2, 2)))
    with pytest.raises(RuntimeError, match="finite values"):
        cs.setlineartransform("linear", [[1.0, np.inf], [0.0, 1.0]])
    np.testing.assert_array_equal(cs.lineartransform("linear"), np.eye(2))
    standalone = tools.coordsys().newcoordsys(tabular=True)
    record = standalone.torecord()
    record["tabular0"]["pixelvalues"] = np.array([0.0, 2.0, 5.0])
    record["tabular0"]["worldvalues"] = np.array([10.0, 14.0, 30.0])
    assert standalone.fromrecord(record)
    for value in (0.0, 1e-20, 1e308):
        with pytest.raises(RuntimeError, match="invalid tabular samples"):
            standalone.setlineartransform("tabular", [[value]])
    np.testing.assert_array_equal(standalone.lineartransform("tabular"), [[1.0]])


def test_non_native_byte_order_is_respected_on_matrix_input():
    cs = _linear(tools)
    matrix = np.array([[1.0, 2.0], [3.0, 5.0]], dtype=np.dtype("float64").newbyteorder("S"))
    assert cs.setlineartransform("linear", matrix)
    np.testing.assert_array_equal(cs.lineartransform("linear"), matrix)
    np.testing.assert_array_equal(cs.toworld([3.0, 5.0])["numeric"], [124.0, 238.0])


def test_sampled_spectral_transform_is_folded_into_persisted_samples(casa):
    source = casa.coordsys().newcoordsys(spectral=True)
    assert source.setspectral(frequencies={"value": np.array([1e9, 1.2e9, 1.5e9]), "unit": "Hz"})
    sample = source.torecord()
    for backend in (tools, casa):
        cs = backend.coordsys()
        assert cs.fromrecord(sample)
        assert cs.setlineartransform("spectral", [[-2.0]])
        np.testing.assert_array_equal(cs.lineartransform("spectral"), [[-2.0]])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [6e8])
        np.testing.assert_allclose(cs.topixel([6e8])["numeric"], [1.0])
        record = cs.torecord()
        np.testing.assert_array_equal(record["spectral0"]["tabular"]["pc"], [[-2.0]])
        np.testing.assert_allclose(record["spectral0"]["tabular"]["worldvalues"], [1e9, 6e8, 0.0])
        assert cs.fromrecord(record)
        np.testing.assert_array_equal(cs.lineartransform("spectral"), [[1.0]])
        np.testing.assert_array_equal(cs.torecord()["spectral0"]["tabular"]["pc"], [[1.0]])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [6e8])


def test_later_spectral_edits_keep_the_live_sampled_matrix(casa):
    source = casa.coordsys().newcoordsys(spectral=True)
    assert source.setspectral(frequencies={"value": np.array([1e9, 1.2e9, 1.5e9]), "unit": "Hz"})
    sample = source.torecord()
    for backend in (tools, casa):
        cs = backend.coordsys()
        assert cs.fromrecord(sample)
        assert cs.setlineartransform("spectral", [[2.0]])
        assert cs.setnames(["CustomFrequency"], "spectral")
        assert cs.setrestfrequency(2e9)
        np.testing.assert_array_equal(cs.lineartransform("spectral"), [[2.0]])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [1.4e9])
        assert cs.setincrement([7.0], "spectral")
        np.testing.assert_array_equal(cs.lineartransform("spectral"), [[2.0]])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [1e9 + 11.2], atol=1e-7, rtol=0.0)


def test_live_sampled_spectral_reference_pixel_keeps_its_world_anchor(casa):
    source = casa.coordsys().newcoordsys(spectral=True)
    assert source.setspectral(frequencies={"value": np.array([1e9, 1.2e9, 1.5e9]), "unit": "Hz"})
    sample = source.torecord()
    for backend in (tools, casa):
        cs = backend.coordsys()
        assert cs.fromrecord(sample)
        assert cs.setlineartransform("spectral", [[2.0]])
        assert cs.setreferencepixel([2.0], "spectral")
        np.testing.assert_array_equal(cs.referencepixel()["numeric"], [2.0])
        np.testing.assert_allclose(cs.referencevalue()["numeric"], [1e9])
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [4e8])
        np.testing.assert_allclose(cs.torecord()["spectral0"]["tabular"]["worldvalues"], [0, 4e8, 1e9])


def test_sampled_spectral_record_keeps_the_scale_of_nested_units(casa):
    source = casa.coordsys().newcoordsys(spectral=True)
    assert source.setspectral(frequencies={"value": np.array([1e9, 1.2e9, 1.5e9]), "unit": "Hz"})
    record = source.torecord()
    spectral = record["spectral0"]
    spectral["unit"] = "GHz"
    spectral["restfreq"] /= 1e9
    spectral["restfreqs"] /= 1e9
    table = spectral["tabular"]
    table["units"] = np.array(["MHz"])
    for field in ("crval", "cdelt", "worldvalues"):
        table[field] /= 1e6
    cs = tools.coordsys()
    assert cs.fromrecord(record)
    assert cs.setlineartransform("spectral", [[2.0]])
    np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [1.4])
    assert cs.fromrecord(cs.torecord())
    np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [1.4])
