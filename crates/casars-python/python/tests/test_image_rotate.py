"""Pinned CASA parity and native image interop for rotation."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _ramp():
    x, y = np.ogrid[:24, :26]
    return (x + 2*y).astype(np.float32)


def _plane_rotation(angle):
    cosine, sine = np.cos(angle), np.sin(angle)
    return np.array([[cosine, -sine], [sine, cosine]])


@pytest.mark.parametrize("linear", [False, True])
@pytest.mark.parametrize("argument,angle", [
    ("35deg", np.deg2rad(35)),
    ({"value": 35, "unit": "deg"}, np.deg2rad(35)),
    (.25, .25),
    ({"value": 1., "unit": "m"}, 1.),
])
def test_rotate_supported_angle_forms_and_analytic_sampling(casa, linear, argument, angle):
    ideal = _ramp()
    products = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=list(ideal.shape), linear=linear)
            assert source.putchunk(ideal)
            coords = source.coordsys()
            ref = coords.referencepixel()["numeric"]
            coords.done()
            product = source.rotate(pa=argument, method="linear", decimate=1)
            try:
                coords = product.coordsys()
                rotation = _plane_rotation(angle)
                np.testing.assert_allclose(
                    coords.lineartransform(type="linear" if linear else "direction"), rotation,
                    atol=2e-14,
                )
                np.testing.assert_array_equal(coords.referencepixel()["numeric"], ref)
                world_sample = np.array([10., 12.])
                px, py = ref + rotation.T @ (world_sample - ref)
                values, valid = product.getchunk(), product.getchunk(getmask=True)
                np.testing.assert_allclose(values[10, 12], px + 2*py, rtol=2e-6)
                products.append((values, valid))
                coords.done()
            finally:
                product.done()
            np.testing.assert_array_equal(source.getchunk(), ideal)
        finally:
            source.done()
    assert products[0][0].shape == products[1][0].shape == ideal.shape
    np.testing.assert_array_equal(products[1][1], products[0][1])
    np.testing.assert_allclose(products[1][0], products[0][0], rtol=2e-5, atol=2e-5)


def test_rotate_shape_defaults_mask_and_native_writer_readers(casa, tmp_path):
    expected = _ramp()[..., None, None] + 10*np.arange(7, dtype=np.float32)
    selected = np.ones(expected.shape, dtype=bool)
    selected[2:4, 8:11, :, :] = False
    products = {}
    for writer in (casa, tools):
        source = writer.image()
        path = str(tmp_path / f"{writer.__name__}-rotation.im")
        try:
            assert source.fromarray(pixels=expected)
            assert source.putregion(pixelmask=selected)
            assert source.setbrightnessunit("K")
            assert source.sethistory(origin="test", history=["rotation input"])
            default = source.rotate()
            try:
                np.testing.assert_array_equal(default.getchunk(), expected)
                np.testing.assert_array_equal(default.getchunk(getmask=True), selected)
            finally:
                default.done()
            saved = source.rotate(path, pa="35deg", replicate=True, dropdeg=True)
            try:
                assert saved.ispersistent()
            finally:
                saved.done()
        finally:
            source.done()
        for reader in (casa, tools):
            image = reader.image()
            try:
                assert image.open(path)
                assert any("rotation input" in line for line in image.history())
                assert image.brightnessunit() == "K"
                coords = image.coordsys()
                try:
                    np.testing.assert_allclose(coords.lineartransform(type="direction"),
                                               _plane_rotation(np.deg2rad(35)), atol=2e-14)
                finally:
                    coords.done()
                products[writer.__name__, reader.__name__] = image.getchunk(), image.getchunk(getmask=True)
            finally:
                image.done()
    # Replication sets the sample positions. Cubic interpolation still
    # uses its four-by-four input mask stencil at those positions.
    cubic_mask = np.ones(expected.shape[:2] + (expected.shape[-1],), dtype=bool)
    cubic_mask[1:5, 6:12, :] = False
    for pixels, valid in products.values():
        np.testing.assert_array_equal(valid, cubic_mask)
        np.testing.assert_array_equal(pixels[valid], expected[:, :, 0, :][valid])


def test_rotate_refusals_preserve_existing_named_image(casa, tmp_path):
    data = _ramp()
    for api in (casa, tools):
        source = api.image()
        prior = api.image()
        path = str(tmp_path / f"{api.__name__}-before.im")
        try:
            assert source.fromarray(pixels=data)
            assert prior.fromarray(path, pixels=data+1)
            prior.done()
            for arguments in (
                {"pa": [1., 2.]}, {"pa": "something invalid"},
                {"pa": "35deg", "decimate": 9}, {"pa": "35deg", "method": "sinc"},
            ):
                with pytest.raises(RuntimeError):
                    source.rotate(path, overwrite=True, **arguments)
                assert source.isopen()
            assert prior.open(path)
            np.testing.assert_array_equal(prior.getchunk(), data+1)
        finally:
            prior.done()
            source.done()


def test_rotate_right_angle_keeps_the_actual_sky_overlap():
    source = tools.image()
    data = _ramp()
    x, y = np.ogrid[:24, :26]
    original_x, original_y = y - 1, 25 - x
    valid = np.broadcast_to((original_x >= 0) & (original_x < 24), data.shape)
    expected = np.where(valid, original_x + 2*original_y, 0)
    try:
        assert source.fromarray(pixels=data)
        result = source.rotate(pa="90deg", method="nearest")
        try:
            np.testing.assert_array_equal(result.getchunk(getmask=True), valid)
            np.testing.assert_array_equal(result.getchunk(), expected)
        finally:
            result.done()
    finally:
        source.done()


@pytest.mark.parametrize("method", ["nearest", "linear", "cubic"])
def test_rotate_half_turn_excludes_pixels_outside_the_exact_source_grid(method):
    source = tools.image()
    x, y = np.ogrid[:24, :26]
    valid = (x > 0) & (y > 0)
    expected = np.where(valid, (24 - x) + 2 * (26 - y), 0)
    try:
        assert source.fromarray(pixels=_ramp())
        result = source.rotate(pa="180deg", method=method)
        try:
            np.testing.assert_array_equal(result.getchunk(getmask=True), valid)
            np.testing.assert_array_equal(result.getchunk(), expected)
        finally:
            result.done()
    finally:
        source.done()


@pytest.mark.parametrize("angle", [120, 180, 240])
def test_obtuse_rotation_preserves_direction_transform(casa, angle):
    products = []
    for api in (casa, tools):
        source = api.image()
        product = coords = None
        try:
            assert source.fromarray(pixels=_ramp())
            product = source.rotate(pa=f"{angle}deg", replicate=True)
            coords = product.coordsys()
            increments = coords.increment()["numeric"]
            matrix = coords.lineartransform(type="direction")
            # PC and CDELT can change signs together. Check the complete
            # transform against the requested rotation.
            np.testing.assert_allclose(
                np.diag(increments) @ matrix,
                np.diag([-1., 1.]) @ _plane_rotation(np.deg2rad(angle)), atol=2e-14,
            )
            products.append((increments, matrix, product.getchunk(), product.getchunk(getmask=True)))
        finally:
            if coords is not None:
                coords.done()
            if product is not None:
                product.done()
            source.done()
    native, reference = products[1], products[0]
    np.testing.assert_allclose(native[0], reference[0], atol=2e-14)
    np.testing.assert_allclose(native[1], reference[1], atol=2e-14)
    np.testing.assert_allclose(native[2], reference[2], atol=2e-6)
    np.testing.assert_array_equal(native[3], reference[3])


@pytest.mark.parametrize("linear", [False, True])
def test_rotate_validates_rank_before_expanding_default_shape(casa, linear):
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=[24, 26], linear=linear)
            for shape in ([-2], [-1, -1, -1], [-1, 2, 3, 4]):
                with pytest.raises(RuntimeError):
                    source.rotate(pa="35deg", shape=shape)
            for shape in ([], [-1], [-2, 3]):
                product = source.rotate(pa="35deg", shape=shape)
                try:
                    np.testing.assert_array_equal(product.shape(), [24, 26])
                finally:
                    product.done()
        finally:
            source.done()


def test_rotate_dropdeg_maps_spectral_axis_and_beams_after_transpose():
    source = tools.image()
    data = _ramp()[..., None, None] + 100*np.arange(7, dtype=np.float32)
    mapped = product = coords = None
    try:
        assert source.fromarray(pixels=data)
        for channel in range(7):
            assert source.setrestoringbeam(major=f"{2+channel}arcmin", minor="1arcmin",
                                           pa="5deg", channel=channel, polarization=0)
        mapped = source.transpose(order="2301")
        product = mapped.rotate(pa="35deg", dropdeg=True, replicate=True, method="nearest")
        np.testing.assert_array_equal(product.shape(), [7, 24, 26])
        np.testing.assert_array_equal(product.getchunk(), data[:, :, 0, :].transpose(2, 0, 1))
        coords = product.coordsys()
        assert coords.axiscoordinatetypes() == ["Spectral", "Direction", "Direction"]
        for channel in range(7):
            beam = product.restoringbeam(channel=channel, polarization=0)
            assert beam["major"] == {"unit": "arcmin", "value": pytest.approx(2+channel)}
    finally:
        if coords is not None:
            coords.done()
        if product is not None:
            product.done()
        if mapped is not None:
            mapped.done()
        source.done()


@pytest.mark.parametrize("linear", [False, True])
def test_rotate_singleton_plane_matches_interpolation_admission(casa, linear):
    profiles = np.arange(26, dtype=np.float32)[None, :]
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=[1, 26], linear=linear)
            assert source.putchunk(profiles)
            for shape, pa in (([1, 26], "35deg"), ([24, 26], "35deg"), ([24, 26], "0deg")):
                with pytest.raises(RuntimeError):
                    source.rotate(pa=pa, method="nearest", shape=shape)
            product = source.rotate(pa="0deg", method="nearest")
            try:
                np.testing.assert_array_equal(product.getchunk(), profiles)
            finally:
                product.done()
        finally:
            source.done()


def test_rotate_rejects_resized_linear_singletons(casa):
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=[24, 26], linear=True)
            with pytest.raises(RuntimeError):
                source.rotate(pa="35deg", shape=[1, 26])
            source.done()
            assert source.fromshape(shape=[1, 1], linear=True)
            assert source.putchunk(np.array([[5.]], dtype=np.float32))
            with pytest.raises(RuntimeError):
                source.rotate(pa="35deg", shape=[24, 26], method="nearest")
        finally:
            source.done()


def test_rotate_keeps_the_only_pixel_in_a_linear_plane():
    source = tools.image()
    product = coords = None
    try:
        assert source.fromshape(shape=[1, 1], linear=True)
        assert source.putchunk(np.array([[5.]], dtype=np.float32))
        product = source.rotate(pa="35deg", method="linear")
        np.testing.assert_array_equal(product.getchunk(), [[5.]])
        np.testing.assert_array_equal(product.getchunk(getmask=True), [[True]])
        coords = product.coordsys()
        np.testing.assert_array_equal(coords.lineartransform(type="linear"), np.eye(2))
    finally:
        if coords is not None:
            coords.done()
        if product is not None:
            product.done()
        source.done()


def test_rotate_checks_pv_restriction_after_dropping_singleton_spectral_axis(casa):
    data = _ramp()[None, ...]
    for api in (casa, tools):
        source = api.image()
        coordinate_tool = api.coordsys()
        coords = coordinate_tool.newcoordsys(spectral=True, linear=2)
        result = output_coords = None
        try:
            assert coords.setnames(["Offset", "Y"], "linear")
            assert source.fromarray(pixels=data, csys=coords.torecord())
            with pytest.raises(RuntimeError):
                source.rotate(pa="35deg", replicate=True)
            result = source.rotate(pa="35deg", dropdeg=True, replicate=True)
            np.testing.assert_array_equal(result.getchunk(), data[0])
            output_coords = result.coordsys()
            assert output_coords.axiscoordinatetypes() == ["Linear", "Linear"]
        finally:
            if output_coords is not None:
                output_coords.done()
            if result is not None:
                result.done()
            coords.done()
            coordinate_tool.done()
            source.done()


def test_rotate_rejects_dropped_direction_axis_safely():
    source = tools.image()
    try:
        assert source.fromshape(shape=[1, 26])
        with pytest.raises(RuntimeError):
            source.rotate(pa="35deg", dropdeg=True)
        assert source.isopen()
    finally:
        source.done()


@pytest.mark.parametrize("kind", ["direction", "linear"])
def test_rotate_composes_with_a_preexisting_coordinate_shear(casa, kind):
    data = _ramp()
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=list(data.shape), linear=kind == "linear")
            assert source.putchunk(data)
            coords = source.coordsys()
            try:
                assert coords.setlineartransform(type=kind, value=np.array([[.9, .4], [-.2, .8]]))
                assert source.setcoordsys(coords.torecord())
                existing = coords.lineartransform(type=kind)
            finally:
                coords.done()
            product = source.rotate(pa=.52, replicate=True)
            try:
                rotated = product.coordsys()
                try:
                    np.testing.assert_allclose(rotated.lineartransform(type=kind),
                                               _plane_rotation(.52) @ existing, atol=2e-14)
                finally:
                    rotated.done()
                np.testing.assert_array_equal(product.getchunk(), data)
            finally:
                product.done()
        finally:
            source.done()


@pytest.mark.parametrize("pixel_type", ["f", "d", "c", "cd"])
def test_rotate_preserves_pixel_precision_and_complex_components(casa, pixel_type):
    values = _ramp().astype(np.float64)
    if pixel_type.startswith("c"):
        values = values * (1 - 2j)
    measured = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=list(values.shape), type=pixel_type)
            assert source.putchunk(values)
            product = source.rotate(pa=.32, method="linear", decimate=1)
            try:
                assert product.pixeltype() == source.pixeltype()
                mask, data = product.getchunk(getmask=True), product.getchunk()
                assert data.dtype == source.getchunk().dtype
                measured.append((mask, data))
            finally:
                product.done()
        finally:
            source.done()
    np.testing.assert_array_equal(measured[1][0], measured[0][0])
    np.testing.assert_allclose(measured[1][1], measured[0][1], atol=2e-5, rtol=2e-5)


def test_rotate_region_and_stretched_input_mask(casa, tmp_path):
    data = _ramp()[..., None, None] + 10*np.arange(7, dtype=np.float32)
    manager = casa.regionmanager()
    region = manager.box([4, 5, 0, 1], [18, 20, 0, 5])
    manager.done()
    selected = np.broadcast_to(np.arange(24)[:, None, None, None] >= 9, (24, 26, 1, 1))
    products = []
    for api in (casa, tools):
        source = api.image()
        mask = api.image()
        path = tmp_path / f"{api.__name__}-rotation-mask.im"
        try:
            assert source.fromarray(pixels=data)
            assert mask.fromarray(str(path), pixels=selected.astype(np.float32))
            mask.done()
            options = dict(pa="35deg", region=region, mask=f'"{path}" > 0', method="linear")
            with pytest.raises(RuntimeError):
                source.rotate(**options, stretch=True)
            with pytest.raises(RuntimeError):
                source.rotate(**options, shape=[15, 16, 1, 5])
            product = source.rotate(**options, shape=[15, 16, 1, 5], stretch=True)
            try:
                coords = product.coordsys()
                try:
                    np.testing.assert_array_equal(coords.referencepixel()["numeric"], [8., 8., 0., 2.])
                finally:
                    coords.done()
                valid, values = product.getchunk(getmask=True), product.getchunk()
                assert values.shape == (15, 16, 1, 5)
                assert valid[7, 7].all()
                px, py = np.array([8., 8.]) + _plane_rotation(np.deg2rad(35)).T @ [-1., -1.]
                np.testing.assert_allclose(values[7, 7, 0], 4 + px + 2*(5 + py) + 10*np.arange(1, 6),
                                           atol=2e-5)
                products.append((valid, values))
            finally:
                product.done()
        finally:
            mask.done()
            source.done()
    np.testing.assert_array_equal(products[1][0], products[0][0])
    np.testing.assert_allclose(products[1][1], products[0][1], rtol=2e-5, atol=2e-5)
