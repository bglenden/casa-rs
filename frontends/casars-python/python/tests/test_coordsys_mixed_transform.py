"""Pixel, world and spectral-velocity units in one conversion."""

import numpy as np
import pytest

from casars import tools


PIXEL = ["pix"] * 3
MIXED = ["deg", "pix", "Hz"]
SHAPE = [30, 40, 50]
POINT = np.array([4.3, 5.4, 7.3])
REF_PIXEL = np.array([4.0, 5.0, 6.0])
INCREMENT = np.array([-0.13, 0.1, 1000.0])
REF_VALUE = np.array([25.0, 17.0, 1.415e9])
REST = 1.42e9
C_KMS = 299_792.458


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def sky(api):
    cs = api.coordsys().newcoordsys(direction=True, spectral=True)
    assert cs.setunits(["deg", "deg", "Hz"])
    assert cs.setreferencevalue(REF_VALUE)
    assert cs.setreferencepixel(REF_PIXEL)
    assert cs.setincrement(INCREMENT)
    assert cs.setrestfrequency("1.42GHz")
    return cs


def sky_world(pixel):
    # SIN's plane carries the east and north direction cosines. Its
    # reference line of sight supplies the remaining direction cosine.
    east, north = np.deg2rad((pixel - REF_PIXEL)[:2] * INCREMENT[:2])
    forward = np.sqrt(1.0 - east**2 - north**2)
    latitude = np.deg2rad(REF_VALUE[1])
    longitude = REF_VALUE[0] + np.rad2deg(
        np.arctan2(east, forward * np.cos(latitude) - north * np.sin(latitude))
    )
    declination = np.rad2deg(
        np.arcsin(forward * np.sin(latitude) + north * np.cos(latitude))
    )
    frequency = REF_VALUE[2] + (pixel[2] - REF_PIXEL[2]) * INCREMENT[2]
    return np.array([longitude, declination, frequency])


def linear(api, matrix=None):
    cs = api.coordsys().newcoordsys(linear=2)
    record = cs.torecord()
    record["linear0"]["units"] = ["m", "s"]
    assert cs.fromrecord(record)
    assert cs.setreferencevalue([100.0, 50.0])
    assert cs.setreferencepixel([5.0, 3.0])
    assert cs.setincrement([2.0, 4.0])
    if matrix is not None:
        assert cs.setlineartransform("linear", matrix)
    return cs


def test_numpy_batch_orientation_and_units():
    cs = linear(tools)
    points = np.array([[5.0, 99, 7, 99, 3, 99], [3, 99, 1, 99, 11, 99]])[:, ::2]
    expected = np.array([[10_000.0, 10_400.0, 9_600.0], [0.0, -8_000.0, 32_000.0]])
    actual = cs.convertmany(points, unitsin=["pix", "pix"], unitsout=["cm", "ms"], absout=[True, False])
    assert actual.dtype == np.float64
    np.testing.assert_allclose(actual, expected, atol=1e-11)
    recovered = cs.convertmany(
        actual.tolist(), absin=np.array([True, False]), unitsin=["cm", "ms"], unitsout=["pix", "pix"]
    )
    np.testing.assert_allclose(recovered, points, atol=1e-11)
    assert cs.convertmany(np.empty((2, 0))).shape == (2, 0)
    assert cs.convertmany([[], []]).shape == (2, 0)


def test_affine_mixed_flags_and_unit_forms(casa):
    matrix = np.array([[1.0, 0.25], [0.25, 1.0]])
    pixel = np.linalg.solve(matrix, [2.0, 0.0]) + [5.0, 3.0]
    pixel[1] = 7.0
    world = np.array([100.0, 50.0]) + np.array([2.0, 4.0]) * (matrix @ (pixel - [5.0, 3.0]))
    for api in (tools, casa):
        cs = linear(api, matrix)
        for unit_form in (None, [], (), ("bad", "bad"), np.array(["bad", "bad"]), b"bad", "Native"):
            np.testing.assert_allclose(cs.convert([100.0, 50.0], unitsin=unit_form), [100.0, 50.0])
        for value in ([400.0, 4.0], np.array([[400.0, 4.0]]), ["400", "4"]):
            np.testing.assert_allclose(
                cs.convert(value, unitsin=["cm", "pix"], absin=[False, False], unitsout=["pix", "pix"]),
                pixel, atol=1e-12,
            )
        actual = cs.convert([104.0, 7.0], unitsin=["m", "pix"], unitsout=["Native"])
        assert actual.dtype == np.float64
        np.testing.assert_allclose(actual, world, atol=1e-12)
        for value in ([[100.0, 104.0], [50.0, 50.0]], ((100.0, 104.0), (50.0, 50.0))):
            np.testing.assert_allclose(cs.convertmany(value), value, atol=1e-12)
        np.testing.assert_allclose(cs.convertmany(np.array([[True, False], [False, True]])), [[1, 0], [0, 1]], atol=1e-12)


def test_mixed_sky_axes_and_shape_lifetime(casa):
    points = np.column_stack((POINT, [6.6, 8.25, 5.8], [3.1, 4.2, 13.5]))
    world = np.column_stack([sky_world(point) for point in points.T])
    for api in (tools, casa):
        cs = sky(api)
        mixed = np.array([world[0], points[1], world[2]])
        options = dict(unitsin=MIXED, unitsout=PIXEL)
        with pytest.raises(RuntimeError, match="Invalid world coordinate"):
            cs.convert(mixed[:, 0], **options)
        np.testing.assert_allclose(
            cs.convertmany(mixed, shape=SHAPE, **options), points, rtol=0, atol=1e-8
        )
        np.testing.assert_allclose(cs.convert(mixed[:, 0], **options), POINT, rtol=0, atol=1e-8)
        np.testing.assert_allclose(cs.convert(mixed[:, 0], shape=[30], **options), POINT, rtol=0, atol=1e-8)
        with pytest.raises(RuntimeError, match="Invalid world coordinate"):
            cs.copy().convert(mixed[:, 0], **options)
        assert cs.setunits(["arcsec", "arcsec", "MHz"])
        np.testing.assert_allclose(cs.convert(mixed[:, 0], **options), POINT, rtol=0, atol=1e-8)
        inverse = np.array([points[0], world[1], points[2]])
        np.testing.assert_allclose(
            cs.convertmany(inverse, unitsin=["pix", "deg", "pix"], unitsout=PIXEL),
            points, rtol=0, atol=1e-8,
        )
        restored = api.coordsys()
        assert restored.fromrecord(cs.torecord())
        with pytest.raises(RuntimeError, match="Invalid world coordinate"):
            restored.convert(mixed[:, 0], **options)
        with pytest.raises(RuntimeError, match="Failed AlwaysAssert"):
            restored.convertmany(world[:, 0], shape=SHAPE)
        np.testing.assert_allclose(restored.convert(mixed[:, 0], **options), POINT, rtol=0, atol=1e-8)


def test_mixed_reference_offsets_with_doppler(casa):
    world = sky_world(POINT)
    reference_velocity = C_KMS * (REST / REF_VALUE[2] - 1.0)
    optical_velocity = C_KMS * (REST / world[2] - 1.0)
    radio_velocity = C_KMS * (1.0 - world[2] / REST)
    east, north = np.deg2rad((POINT - REF_PIXEL)[:2] * INCREMENT[:2])
    local_lon = np.rad2deg(np.arctan2(east, np.sqrt(1.0 - east**2 - north**2)))
    local_lat = np.rad2deg(np.arcsin(north))
    for api in (tools, casa):
        cs = sky(api)
        mixed = [local_lon, POINT[1] - REF_PIXEL[1], optical_velocity - reference_velocity]
        found = cs.convert(
            mixed, absin=[False, False, False], dopplerin="optical", unitsin=["deg", "pix", "km/s"],
            unitsout=PIXEL, shape=SHAPE,
        )
        # A single relative world input is first converted with the other
        # direction component held at its reference.
        angle = np.deg2rad(local_lon)
        expected_lon = REF_VALUE[0] + np.rad2deg(
            np.arctan2(np.sin(angle), np.cos(angle) * np.cos(np.deg2rad(REF_VALUE[1])))
        )
        np.testing.assert_allclose(sky_world(found)[0], expected_lon, atol=1e-9, rtol=0)
        np.testing.assert_allclose(found[1:], POINT[1:], atol=1e-8, rtol=0)
        expected = np.array([local_lon, local_lat, radio_velocity * 1000.0])
        output = cs.convert(
            POINT, unitsin=PIXEL, absout=[False, False, True], unitsout=["deg", "deg", "m/s"],
        )
        np.testing.assert_allclose(output, expected, atol=1e-6, rtol=0)


def test_native_writer_reader_pairs_preserve_mixed_geometry(casa, tmp_path):
    world = sky_world(POINT)
    for writer in (tools, casa):
        record = sky(writer).torecord()
        for domain in ("pixel", "world"):
            record[f"{domain}map0"] = np.array([1, 2], dtype=np.int32)
            record[f"{domain}map1"] = np.array([0], dtype=np.int32)
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [SHAPE[2], SHAPE[0], SHAPE[1]], record)
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                cs = image.coordsys()
                np.testing.assert_allclose(
                    cs.convert([world[2], world[0], POINT[1]], unitsin=["Hz", "deg", "pix"],
                               unitsout=PIXEL, shape=[SHAPE[2], SHAPE[0], SHAPE[1]]),
                    POINT[[2, 0, 1]], atol=1e-8, rtol=0,
                )
            finally:
                image.done()


@pytest.mark.parametrize("independent_world_order", [False, True])
def test_mixed_conversion_on_reopened_signed_axis_maps(casa, tmp_path, independent_world_order):
    seed = sky(casa)
    source = casa.image()
    selected = None
    try:
        assert source.fromshape(shape=SHAPE, csys=seed.torecord())
        selected = source.coordsys(axes=[2, 0])
        record = selected.torecord()
    finally:
        if selected is not None:
            selected.done()
        source.done()
        seed.done()

    shape = [SHAPE[2], SHAPE[0]]
    # The missing declination pixel stays at its reference in the projection.
    channel, longitude_pixel = np.array([1., 4., 6.]), np.array([3.5, 11., 7.])
    world = np.column_stack([
        sky_world(np.array([longitude, REF_PIXEL[1], channel_index]))
        for longitude, channel_index in zip(longitude_pixel, channel)
    ])
    pixel = np.vstack([channel, longitude_pixel])
    if independent_world_order:
        record["worldmap0"] = np.array([1], dtype=np.int32)
        record["worldmap1"] = np.array([0, -3], dtype=np.int32)
        selected_world = world[[0, 2]]
        supplied = np.vstack([longitude_pixel, world[2]])
        units_in, units_out = ["pix", "Hz"], ["deg", "pix"]
    else:
        selected_world = world[[2, 0]]
        supplied = np.vstack([world[2], longitude_pixel])
        units_in, units_out = ["Hz", "pix"], ["pix", "deg"]
    expected = np.vstack([channel, world[0]])
    for writer in (casa, tools):
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        try:
            assert image.fromshape(path, shape, record)
        finally:
            image.done()
        for reader in (casa, tools):
            image = reader.image()
            coordinates = None
            try:
                assert image.open(path)
                coordinates = image.coordsys()
                assert coordinates.naxes() == coordinates.naxes(True) == 2
                np.testing.assert_array_equal(coordinates.torecord()["pixelmap1"], [1, -3])
                options = dict(unitsin=units_in, unitsout=units_out, shape=shape)
                if independent_world_order:
                    for transform, values in (
                        (coordinates.convert, supplied[:, 1]),
                        (coordinates.convertmany, supplied),
                    ):
                        with pytest.raises(RuntimeError, match="duplicate pixel/world axes"):
                            transform(values, **options)
                else:
                    np.testing.assert_allclose(
                        coordinates.convert(supplied[:, 1], **options), expected[:, 1], atol=2e-7, rtol=0,
                    )
                    np.testing.assert_allclose(
                        coordinates.convertmany(supplied, **options), expected, atol=2e-7, rtol=0,
                    )
                assert image.isopen()
                np.testing.assert_array_equal(image.shape(), shape)
                np.testing.assert_allclose(
                    coordinates.toworldmany(pixel)["numeric"], selected_world, atol=2e-7, rtol=0,
                )
            finally:
                if coordinates is not None:
                    coordinates.done()
                image.done()


def test_shape_interval_can_cross_a_celestial_pole(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(direction=True)
        assert cs.setunits(["deg", "deg"])
        assert cs.setreferencevalue([25.0, 85.0])
        assert cs.setreferencepixel([10.0, 10.0])
        assert cs.setincrement([-1.0, 1.0])
        # The centre latitude is 85 degrees and the shape-based interval
        # extends 15 degrees on both sides. Its solution is the reference pixel.
        actual = cs.convert(
            [25.0, 10.0], unitsin=["deg", "pix"], unitsout=["pix", "pix"], shape=[20, 20],
        )
        np.testing.assert_allclose(actual, [10.0, 10.0], atol=1e-9, rtol=0)


@pytest.mark.parametrize("latitude", [-90.0, 90.0])
def test_unrotated_plate_carree_preserves_longitude_at_the_pole(casa, latitude):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(direction=True)
        assert cs.setdirection(
            proj="CAR", refpix=[0.0, 0.0],
            refval=["0deg", "0deg"], incr=["1deg", "1deg"],
        )
        assert cs.setunits(["deg", "deg"])
        absolute = [30.0, latitude]
        # With one degree per pixel and coincident poles, CAR is the
        # identity in longitude and latitude, including at both endpoints.
        actual = cs.convert(absolute, unitsin=["deg", "pix"], unitsout=["pix", "pix"])
        np.testing.assert_allclose(actual, absolute, atol=1e-9, rtol=0)
        np.testing.assert_allclose(cs.topixel(absolute)["numeric"], absolute, atol=1e-9, rtol=0)


def test_errors_and_stokes_units(casa):
    for api in (tools, casa):
        cs = sky(api)
        with pytest.raises(RuntimeError, match="Coordinates must all be of length nWorldAxes"):
            cs.convert([1.0, 2.0])
        for options in (dict(absin=[False]), dict(absout=None), dict(unitsin=["deg"])):
            with pytest.raises(RuntimeError, match="Inputs must all be of length nWorldAxes"):
                cs.convert(REF_VALUE, **options)
        with pytest.raises(RuntimeError, match="Illegal doppler"):
            cs.convert(REF_VALUE, dopplerout="invalid")
        with pytest.raises(RuntimeError, match="Units are not compatible dimensionally"):
            cs.convert(REF_VALUE, unitsin=["Hz", "deg", "Hz"])
        with pytest.raises(TypeError):
            cs.convert(REF_VALUE, absout="True")
        with pytest.raises(SystemError):
            cs.convert(tuple(REF_VALUE.tolist()))
        with pytest.raises(RuntimeError, match=r"coordIn.shape\(\).nelements\(\)==2"):
            cs.convertmany(REF_VALUE)
        stokes = api.coordsys().newcoordsys(stokes="I Q U V")
        np.testing.assert_array_equal(stokes.convert([1], unitsin="pix", unitsout="GHz"), [2.0])
        with pytest.raises(RuntimeError, match="neither Spectral nor Linear"):
            stokes.convert([1], unitsin="pix", unitsout="m/s")
