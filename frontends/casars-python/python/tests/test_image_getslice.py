"""Line sampling of public image planes against CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def lattice(shape=(7, 8, 3)):
    x, y, z = np.indices(shape)
    return x + 10 * y + 100 * z


def image(api, pixels, kind="f", path="", masked=False):
    tool = api.image()
    assert tool.fromshape(path, list(pixels.shape), type=kind)
    assert tool.putchunk(pixels)
    if masked:
        mask = np.ones(pixels.shape, dtype=bool)
        mask[2, 3, 1] = False
        assert tool.putregion(pixelmask=mask)
    return tool


def assert_record(actual, expected, atol=2e-6):
    dtypes = {"axes": np.int64, "mask": np.bool_, "pixel": expected["pixel"].dtype}
    for key in ("distance", "xpos", "ypos"):
        dtypes[key] = np.float64
    assert set(actual) == set(expected) == set(dtypes)
    for key, dtype in dtypes.items():
        assert actual[key].dtype == dtype
        assert actual[key].shape == expected[key].shape
        if key in ("axes", "mask"):
            np.testing.assert_array_equal(actual[key], expected[key])
        else:
            np.testing.assert_allclose(actual[key], expected[key], atol=atol, rtol=0)


def test_polyline_spacing_and_distances_are_independent(casa):
    points = np.array([[0, 0], [1.75, 0], [3.5, 0], [4, 1.25], [4, 3]])
    expected = {
        "axes": np.array([0, 1]),
        "xpos": points[:, 0],
        "ypos": points[:, 1],
        "distance": np.r_[0, np.cumsum(np.linalg.norm(np.diff(points, axis=0), axis=1))],
        "mask": np.ones(5, dtype=bool),
        "pixel": 100 + points[:, 0] + 10 * points[:, 1],
    }
    for api in (tools, casa):
        tool = image(api, lattice().astype(np.float32))
        try:
            actual = tool.getslice(
                np.array([0, 4, 4], dtype=np.int32), ["0", "0", "3"],
                coord=[-22, -30, 1], npts=5,
            )
            assert_record(actual, expected)
            default = tool.getslice([0, 4, 4], [0, 0, 3])
            np.testing.assert_array_equal(default["distance"], np.arange(8))
            np.testing.assert_array_equal(default["pixel"], [0, 1, 2, 3, 4, 14, 24, 34])
            nested = tool.getslice([[0, 4, 4]], [[0], [0], [3]])
            assert_record(nested, default)
            one = tool.getslice([0.25, 0.5], [0.1, 0.3])
            np.testing.assert_allclose(one["pixel"], [1.25], atol=1e-7)
            np.testing.assert_array_equal(one["distance"], [0.0])
        finally:
            tool.done()


@pytest.mark.parametrize("method", ["nearest", "lanczos", "Cubic"])
@pytest.mark.parametrize(
    "kind,dtype", [("f", np.float32), ("d", np.float64), ("c", np.complex64), ("cd", np.complex128)]
)
def test_slice_pixel_types_and_interpolators(casa, method, kind, dtype):
    x, y, z = np.indices((7, 8, 3))
    data = np.asarray(x**2 + 2 * y**2 + x * y + 100 * z, dtype=dtype)
    if np.issubdtype(dtype, np.complexfloating):
        data.imag = -2 * data.real
    options = dict(npts=np.int32(3), coord=[0, 0, -1], method=method)
    values = []
    for api in (tools, casa):
        tool = image(api, data, kind)
        try:
            values.append(tool.getslice([1.25, 3.25], [1.75, 3.75], **options))
        finally:
            tool.done()
    assert_record(*values, atol=3e-5)
    result = values[0]
    np.testing.assert_array_equal(result["mask"], [True] * 3)
    np.testing.assert_array_equal(result["xpos"], [1.25, 2.25, 3.25])
    px, py = result["xpos"], result["ypos"]
    if method == "nearest":
        px = np.floor(px + 0.5)
        py = np.floor(py + 0.5)
    expected = px**2 + 2 * py**2 + px * py + 200.0
    if method == "lanczos":
        # CASA selects linear interpolation from the first letter. A linear
        # interpolant of x² adds t(1−t) to the quadratic at fraction t.
        expected += 3 * 0.25 * 0.75
    if np.issubdtype(dtype, np.complexfloating):
        expected = expected - 2j * expected
    np.testing.assert_allclose(result["pixel"], expected, atol=3e-5, rtol=0)


def test_mask_and_all_native_writer_reader_combinations(casa, tmp_path):
    for writer in (tools, casa):
        path = str(tmp_path / f"{writer.__name__}.im")
        tool = image(writer, lattice().astype(np.float32), path=path, masked=True)
        assert tool.done()
        for reader in (tools, casa):
            tool = reader.image()
            assert tool.open(path)
            try:
                nearest = tool.getslice([0, 4], [3, 3], coord=[0, 0, 1], npts=9, method=b"Near")
                np.testing.assert_array_equal(nearest["pixel"], [130, 131, 131, 0, 0, 133, 133, 134, 134])
                np.testing.assert_array_equal(nearest["mask"], [1, 1, 1, 0, 0, 1, 1, 1, 1])
                linear = tool.getslice([0, 4], [3, 3], coord=[0, 0, 1], npts=9)
                np.testing.assert_array_equal(linear["pixel"], [130, 130.5, 0, 0, 0, 0, 133, 133.5, 134])
                np.testing.assert_array_equal(linear["mask"], [1, 1, 0, 0, 0, 0, 1, 1, 1])
                mapped = tool.getslice([0, 6], [1, 2], axes=[0, 2, 1], coord=[0, 2, 0], npts=3)
                np.testing.assert_array_equal(mapped["axes"], [0, 2])
                np.testing.assert_array_equal(mapped["pixel"], [120, 173, 226])
                reversed_axes = tool.getslice([0, 2], [0, 1], axes=[2, 0], coord=[0, 2, 0], npts=3)
                np.testing.assert_array_equal(reversed_axes["axes"], [2, 0])
                np.testing.assert_array_equal(reversed_axes["pixel"], [20, 71, 122])
            finally:
                tool.done()


def test_errors_and_detached_behavior(casa):
    for api in (tools, casa):
        assert api.image().getslice([0, 2], [0, 2]) == {}
        tool = image(api, lattice().astype(np.float32))
        try:
            with pytest.raises(RuntimeError, match=r"x.nelements\(\) == y.nelements\(\)"):
                tool.getslice([0], [0, 2])
            with pytest.raises(RuntimeError, match="coord must be of length"):
                tool.getslice([0, 2], [0, 2], coord=[])
            with pytest.raises(RuntimeError, match="end of curve falls outside"):
                tool.getslice([0, 8], [0, 2])
            with pytest.raises(RuntimeError, match="Illegal interpolation method"):
                tool.getslice([0, 2], [0, 2], method="z")
            with pytest.raises(TypeError, match="argument _axes must not be a string"):
                tool.getslice([0, 2], [0, 2], axes="01")
            with pytest.raises(SystemError):
                tool.getslice((0, 2), [0, 2])
            with pytest.raises(RuntimeError, match="np == npoints"):
                tool.getslice([1, 1], [1, 1], npts=3)
        finally:
            tool.done()


def test_fits_attachment_preserves_the_selected_plane_mask(casa, tmp_path):
    path = str(tmp_path / "masked.fits")
    source = image(casa, lattice().astype(np.float32), masked=True)
    try:
        assert source.tofits(outfile=path)
    finally:
        source.done()
    results = []
    for api in (tools, casa):
        tool = api.image()
        assert tool.open(path)
        try:
            results.append(tool.getslice([0, 4], [3, 3], coord=[0, 0, 1], npts=9, method="nearest"))
        finally:
            tool.done()
    assert_record(*results)
    np.testing.assert_array_equal(results[0]["pixel"], [130, 131, 131, 0, 0, 133, 133, 134, 134])
    np.testing.assert_array_equal(results[0]["mask"], [1, 1, 1, 0, 0, 1, 1, 1, 1])


def test_invalid_indices_raise_a_python_error():
    tool = image(tools, lattice().astype(np.float32))
    try:
        with pytest.raises(RuntimeError, match="axis1"):
            tool.getslice([0, 2], [0, 2], axes=[0])
        with pytest.raises(RuntimeError, match="coordinate on axis 2"):
            tool.getslice([0, 2], [0, 2], coord=[0, 0, 100])
        with pytest.raises(TypeError, match="npts"):
            tool.getslice([0, 2], [0, 2], npts=None)
        np.testing.assert_allclose(tool.getslice([0, 2], [0, 2], npts=3)["pixel"], [0, 11, 22])
    finally:
        tool.done()
