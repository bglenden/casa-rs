"""Pinned image decomposition, native input interoperability and analytic fits."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def test_decompose_inclusive_threshold_and_exclusive_bounds(casa):
    values = np.zeros((13, 15, 1, 1), dtype=np.float32)
    values[1, 2, 0, 0] = 7.
    values[7, 7, 0, 0] = 9.
    values[8, 8, 0, 0] = 1.
    values[11, 12, 0, 0] = 10.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            with pytest.raises(RuntimeError, match="specify a nonnegative threshold"):
                source.decompose()
            result = source.decompose(threshold=1., simple=True, fit=False)
            np.testing.assert_array_equal(result["blc"], [[1, 2], [7, 7], [11, 12]])
            np.testing.assert_array_equal(result["trc"], [[2, 3], [9, 9], [12, 13]])
            assert result["components"].shape == (0, 0)
            assert result["components"].dtype == np.float64
            assert result["blc"].dtype == result["trc"].dtype == np.int64
            axial = source.decompose(threshold=1., simple=True, fit=False, naxis=1)
            np.testing.assert_array_equal(axial["blc"], [[1, 2], [7, 7], [8, 8], [11, 12]])
            assert all(a.shape == (0, 0) for a in source.decompose(threshold=11., simple=True, fit=False).values())
        finally:
            source.done()


def test_decompose_native_inputs_preserve_region_grid(casa, tmp_path):
    values = np.zeros((9, 11), dtype=np.float32)
    values[1, 2] = 7.
    values[3, 4] = 1.
    values[5, 6] = 4.
    region_tool = casa.regionmanager()
    region = region_tool.box([2, 3], [8, 10])
    region_tool.done()
    for writer in (casa, tools):
        path = str(tmp_path / f"{writer.__name__}-islands.im")
        source = writer.image()
        assert source.fromarray(path, values, linear=True)
        source.done()
        for reader in (casa, tools):
            image = reader.image()
            assert image.open(path)
            try:
                result = image.decompose(region, simple=True, threshold=1., fit=False)
                np.testing.assert_array_equal(result["blc"], [[1, 1], [3, 3]])
                np.testing.assert_array_equal(result["trc"], [[2, 2], [4, 4]])
                np.testing.assert_array_equal(image.getchunk(), values)
            finally:
                image.done()


@pytest.mark.parametrize("simple", [False, True])
def test_decompose_fits_analytic_gaussians(casa, simple):
    x, y = np.indices((42, 41), dtype=np.float64)
    values = (7 * np.exp(-((x - 10.3)**2 / 10 + (y - 14.7)**2 / 5))
              + 4 * np.exp(-((x - 28.3)**2 / 8 + (y - 30.7)**2 / 13))).astype(np.float32)
    expected = np.array([
        [7., -10.7, -5.3, 2 * np.sqrt(10 * np.log(2)) - 21, np.sqrt(.5)],
        [4., 7.3, 10.7, 2 * np.sqrt(13 * np.log(2)) - 21, np.sqrt(8 / 13)],
    ])
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            result = source.decompose(threshold=.7, simple=simple, fit=True)
            components = result["components"]
            np.testing.assert_allclose(components[:, :5], expected, rtol=2e-4, atol=2e-4)
            np.testing.assert_allclose(np.cos(2 * components[:, 5]), [-1., 1.], atol=2e-6)
            np.testing.assert_array_equal(result["blc"], [[6, 12], [25, 26]])
            np.testing.assert_array_equal(result["trc"], [[16, 19], [33, 36]])
        finally:
            source.done()


@pytest.mark.parametrize("fit", [False, True])
def test_decompose_cube_moments_and_world_widths(casa, fit):
    x, y, z = np.indices((12, 13, 11), dtype=np.float64)
    values = (7 * np.exp(-((x - 7.3)**2 / 10 + (y - 5.7)**2 / 5 + (z - 4.2)**2 / 4))).astype(np.float32)
    if fit:
        expected = [7., 1.3, -.3, -.8, 2 * np.sqrt(10 * np.log(2)) - 6,
                    2 * np.sqrt(5 * np.log(2)) - 6, 2 * np.sqrt(4 * np.log(2)) - 5, 0., 0.]
    else:
        sample = np.where(values >= .7, values.astype(float), 0.)
        total = sample.sum()
        center = [(sample * axis).sum() / total for axis in (x, y, z)]
        d = [axis - center[i] for i, axis in enumerate((x, y, z))]
        covariance = np.array([[(sample * a * b).sum() / total for b in d] for a in d])
        xx, yy, zz = np.diag(covariance)
        theta = np.arctan(2 * covariance[0, 1] / (xx - yy)) / 2
        phi = -np.arctan(2 * covariance[0, 2]
                         / ((xx - zz) * np.cos(theta) + (yy - zz) * np.sin(theta))) / 2
        reference = np.array([6., 6., 5.])
        expected = [values.max(), *(center - reference),
                    *(2.84 * np.sqrt(np.diag(covariance)) - reference), theta, phi]
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            result = source.decompose(threshold=.7, fit=fit)
            np.testing.assert_array_equal(result["blc"], [[3, 3, 2]])
            np.testing.assert_array_equal(result["trc"], [[12, 10, 8]])
            np.testing.assert_allclose(result["components"][0], expected, rtol=3e-4, atol=3e-4)
        finally:
            source.done()


def test_decompose_honors_default_and_expression_masks(tmp_path):
    values = np.zeros((9, 11), dtype=np.float32)
    values[1, 2] = 7.
    values[3, 4] = 1.
    values[5, 6] = 4.
    source = tools.image()
    operand = tools.image()
    path = str(tmp_path / "mask.im")
    assert operand.fromarray(path, np.float32(values >= 4), linear=True)
    operand.done()
    assert source.fromarray(pixels=values, linear=True)
    assert source.putregion(pixelmask=values != 7.)
    try:
        simple = source.decompose(simple=True, threshold=1., fit=False)
        np.testing.assert_array_equal(simple["blc"], [[3, 4], [5, 6]])
        intersected = source.decompose(simple=True, threshold=1., fit=False, mask=f"'{path}' > 0")
        np.testing.assert_array_equal(intersected["blc"], [[5, 6]])
        np.testing.assert_array_equal(source.getchunk(getmask=True), values != 7.)
    finally:
        source.done()


def test_decompose_no_detections_does_not_fit_the_background():
    source = tools.image()
    assert source.fromshape(shape=[11, 13])
    try:
        for simple in (False, True):
            result = source.decompose(simple=simple, fit=True, threshold=.7)
            assert all(a.shape == (0, 0) for a in result.values())
    finally:
        source.done()


def test_decompose_contour_range_controls_blended_sources(casa):
    x, y = np.indices((25, 25), dtype=float)
    values = (7 * np.exp(-((x - 9.3)**2 / 7 + (y - 11.7)**2 / 4))
              + 4 * np.exp(-((x - 15.4)**2 / 6 + (y - 12.1)**2 / 4))).astype(np.float32)
    expected = [
        [7., -2.7, -.3, 2 * np.sqrt(7 * np.log(2)) - 12., np.sqrt(4/7)],
        [4., 3.4, .1, 2 * np.sqrt(6 * np.log(2)) - 12., np.sqrt(4/6)],
    ]
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            original = source.decompose(threshold=.8, fit=False, minrange=1)
            np.testing.assert_array_equal(original["blc"], [[6, 9], [13, 10]])
            np.testing.assert_array_equal(original["trc"], [[14, 15], [19, 15]])
            np.testing.assert_allclose(original["components"][:, 1:3],
                                       [[-2.5908165, -.3041191], [3.3486948, .1218748]], atol=3e-5)
            merged = source.decompose(threshold=.8, fit=False, minrange=4)
            np.testing.assert_array_equal(merged["blc"], [[6, 9]])
            np.testing.assert_array_equal(merged["trc"], [[19, 15]])
            assert merged["components"].shape == (1, 6)
            fitted = source.decompose(threshold=.8, fit=True, maxrms=1e-4, maxretry=0)
            np.testing.assert_allclose(fitted["components"][:, :5], expected, rtol=4e-4, atol=4e-4)
        finally:
            source.done()


def test_decompose_bounds_merge_runs_across_tiles_and_cube_faces():
    values = np.zeros((550003, 3, 3), np.float32)
    values[49149:49155, 0, 0] = 10.
    values[49155:49158, 1, 0] = 7.  # Connects only across an edge.
    values[49158:49162, 2, 1] = 3.  # Connects only across a corner.
    values[500000:500002, 1, 1] = 4.
    # Use an analytic law here so very large synthetic CASA images do not
    # make the pinned regression suite duplicate a separate resource probe.
    source = tools.image()
    assert source.fromarray(pixels=values, linear=True)
    try:
        edges = source.decompose(simple=True, threshold=1., fit=False, naxis=2)
        np.testing.assert_array_equal(edges["blc"], [[49149, 0, 0], [49158, 2, 1], [500000, 1, 1]])
        np.testing.assert_array_equal(edges["trc"], [[49158, 2, 1], [49162, 3, 2], [500002, 2, 2]])
        corners = source.decompose(simple=True, threshold=1., fit=False, naxis=3)
        np.testing.assert_array_equal(corners["blc"], [[49149, 0, 0], [500000, 1, 1]])
        np.testing.assert_array_equal(corners["trc"], [[49162, 3, 2], [500002, 2, 2]])
    finally:
        source.done()
