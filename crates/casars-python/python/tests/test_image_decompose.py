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
            result = source.decompose(threshold=.7, simple=simple, fit=True, maxrms=1e-4, maxretry=0)
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
            result = source.decompose(threshold=.7, fit=fit, maxrms=1e-4, maxretry=0)
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


def test_decompose_reports_fitted_sky_centers_in_the_stored_units(casa):
    x, y = np.indices((17, 19), dtype=float)
    values = (6 * np.exp(-((x - 11.3)**2 / 11 + (y - 13.1)**2 / 3))).astype(np.float32)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        coords = source.coordsys()
        expected = coords.toworld([11.3, 13.1])["numeric"]
        coords.done()
        try:
            result = source.decompose(threshold=.7, fit=True, maxrms=1e-4, maxretry=0)["components"][0]
            assert result[0] == pytest.approx(6., rel=2e-4)
            np.testing.assert_allclose(result[1:3], expected, rtol=1e-6)
            assert result[3] == pytest.approx(2.47745776, abs=6e-5)
            assert result[4] == pytest.approx(np.sqrt(3 / 11), rel=2e-4)
        finally:
            source.done()


@pytest.mark.parametrize("simple", [False, True])
def test_decompose_fits_a_tilted_gaussian_with_casa_position_angle(casa, simple):
    x, y = np.indices((31, 33), dtype=float)
    x -= 13.3
    y -= 14.6
    angle = .43
    along = -np.sin(angle) * x + np.cos(angle) * y
    across = np.cos(angle) * x + np.sin(angle) * y
    values = (8 * np.exp(-4 * np.log(2) * ((along / 5.)**2 + (across / 3.)**2))).astype(np.float32)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            row = source.decompose(threshold=.5, simple=simple, fit=True, maxrms=1e-5, maxretry=0)["components"][0]
            np.testing.assert_allclose(row, [8., -1.7, -1.4, -10., .6, angle], rtol=3e-4, atol=3e-4)
        finally:
            source.done()


def test_decompose_simple_width_uses_valid_wings_below_the_detection_threshold(casa):
    values = np.zeros((13, 15), dtype=np.float32)
    values[7, 7] = 8.
    values[7, [6, 8]] = 4.
    values[7, [5, 9]] = 1.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            result = source.decompose(threshold=6., simple=True, maxrms=1e-4, maxretry=0)
            np.testing.assert_array_equal(result["blc"], [[7, 7]])
            np.testing.assert_array_equal(result["trc"], [[8, 8]])
            np.testing.assert_allclose(result["components"], [[8., 1., 0., 5/3 - 6, .45, 0.]], atol=5e-6)
        finally:
            source.done()


def test_decompose_brightness_units_do_not_change_fitted_geometry():
    x, y = np.indices((31, 33), dtype=float)
    values = (7 * np.exp(-((x - 10.3)**2 / 10 + (y - 14.7)**2 / 5))).astype(np.float32)
    source = tools.image()
    try:
        for scale in (1., 1e-6, 1e6):
            assert source.fromarray(pixels=values * np.float32(scale), linear=True)
            result = source.decompose(threshold=.7 * scale, simple=True, maxrms=1e-5 * scale, maxretry=0)
            row = result["components"][0]
            row[0] /= scale
            np.testing.assert_allclose(row, [7., -4.7, -1.3, 2 * np.sqrt(10 * np.log(2)) - 15,
                                             np.sqrt(.5), np.pi / 2], rtol=2e-4, atol=2e-4)
    finally:
        source.done()


def test_decompose_extended_contour_stops_at_the_casa_search_radius(casa):
    values = np.zeros((552, 5), dtype=np.float32)
    values[1:551, 1:4] = 1.
    values[5, 2], values[7, 2] = 7., 6.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            result = source.decompose(threshold=1., ncontour=2, fit=False)
            np.testing.assert_array_equal(result["blc"], [[1, 1], [7, 1]])
            np.testing.assert_array_equal(result["trc"], [[7, 4], [257, 4]])
        finally:
            source.done()


def test_decompose_fits_crowded_islands_together(casa):
    x, y = np.indices((211, 23), dtype=float)
    index = np.arange(33, dtype=float)
    center_x = 8.3 + 6 * index
    center_x[-1] -= .6
    center_y = 10.3 + .1 * (index % 3)
    peaks = 7. + .25 * (index % 4)
    fwhm_x = 4.1 + .25 * (index % 3)
    fwhm_y = 2.8 + .1 * (index % 4)
    values = sum(peak * np.exp(-4 * np.log(2) * (((x - cx) / wx)**2 + ((y - cy) / wy)**2))
                 for peak, cx, cy, wx, wy in zip(peaks, center_x, center_y, fwhm_x, fwhm_y))
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values.astype(np.float32), linear=True)
        try:
            result = source.decompose(threshold=.3, simple=True, maxrms=1e-4, maxretry=0)
            assert result["blc"].shape == (1, 2)
            expected = np.column_stack((peaks, center_x - 105, center_y - 11, fwhm_x - 105, fwhm_y / fwhm_x))
            np.testing.assert_allclose(result["components"][:, :5], expected, rtol=3e-4, atol=3e-4)
            np.testing.assert_allclose(result["components"][:, 5], np.pi / 2, atol=3e-4)
        finally:
            source.done()


@pytest.mark.parametrize("simple", [False, True])
def test_decompose_retains_starting_estimates_after_one_iteration(casa, simple):
    values = np.zeros((13, 15), dtype=np.float32)
    values[7, 7], values[8, 7], values[8, 8] = 9, 3, 1
    expected = [9., 1., 0., -5.25, 4/3, 0.] if simple else [
        9., 1.30769253, .07692289, -5., np.sqrt(3), .32175052,
    ]
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            result = source.decompose(threshold=.7, simple=simple, maxiter=1, maxretry=0)
            np.testing.assert_allclose(result["components"][0], expected, rtol=5e-5, atol=5e-6)
        finally:
            source.done()


@pytest.mark.parametrize("simple", [False, True])
def test_decompose_sparse_fit_reconstructs_all_detected_samples(casa, simple):
    points = np.array([[7., 7.], [8., 7.], [8., 8.]])
    intensities = [9., 3., 1.]
    values = np.zeros((13, 15), dtype=np.float32)
    values[tuple(points.astype(int).T)] = intensities
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            peak, x, y, width, ratio, angle = source.decompose(
                threshold=.7, simple=simple, maxrms=1e-4, maxretry=0,
            )["components"][0]
            # The three data points constrain fewer than six parameters.
            # Compare the predictions at each detected sample.
            dx, dy = (points - [x + 6., y + 7.]).T
            along = (-np.sin(angle) * dx + np.cos(angle) * dy) / (width + 6.)
            across = (np.cos(angle) * dx + np.sin(angle) * dy) / ((width + 6.) * ratio)
            predicted = peak * np.exp(-4 * np.log(2) * (along**2 + across**2))
            np.testing.assert_allclose(predicted, intensities, rtol=0., atol=1e-5)
        finally:
            source.done()


@pytest.mark.parametrize("simple, width", [(False, -5.), (True, -5.25)])
def test_decompose_default_fit_for_a_single_point(casa, simple, width):
    values = np.zeros((13, 15), dtype=np.float32)
    values[7, 7] = 9.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, linear=True)
        try:
            result = source.decompose(threshold=.7, simple=simple)
            np.testing.assert_array_equal(result["components"], [[9., 1., 0., width, 1., 0.]])
        finally:
            source.done()
