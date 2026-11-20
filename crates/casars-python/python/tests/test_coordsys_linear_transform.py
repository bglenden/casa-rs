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
        with pytest.raises(RuntimeError, match="value array is empty"):
            cs.setlineartransform("linear", 2.0)
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
        np.testing.assert_array_equal(restored.toworld([1.0])["numeric"], [14.0 if sampled else 1.0])


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
    with pytest.raises(RuntimeError, match="invalid tabular samples"):
        standalone.setlineartransform("tabular", [[0.0]])
    np.testing.assert_array_equal(standalone.lineartransform("tabular"), [[1.0]])


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
        np.testing.assert_allclose(cs.toworld([1.0])["numeric"], [6e8])
