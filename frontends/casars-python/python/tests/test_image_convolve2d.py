"""Pinned public Gaussian convolution, including its physical units and beam."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def pixel_gaussian(shape, center, major, minor, pa, sky):
    x, y = np.indices(shape, dtype=float)
    x -= center[0]
    y -= center[1]
    angle = np.deg2rad(pa)
    if sky:
        x, y = y, -x
    along = np.cos(angle) * x + np.sin(angle) * y
    across = -np.sin(angle) * x + np.cos(angle) * y
    model = np.exp(-4 * np.log(2) * ((along / major)**2 + (across / minor)**2))
    side = 2 * (int(5 * major / np.sqrt(8 * np.log(2)) + .5) + 1) + 1
    xp, yp = np.indices(shape)
    model[(abs(xp - center[0]) > side // 2) | (abs(yp - center[1]) > side // 2)] = 0
    return model


@pytest.mark.parametrize("linear,units,axes,angle,scale", [
    (False, "", [0, 1], 0, -1.),
    (False, "", [], 31, 1.5),
    (False, "", [1, 0], -45, -1.),
    (False, "Jy/pixel", [0, 1], 30, -1.),
    (False, "Jy/pixel", [1, 0], -30, 2.),
    (True, "", [0, 1], 45, -1.),
    (True, "", [1, 0], 30, 1.5),
])
def test_convolve2d_impulse_gaussian_and_units(casa, linear, units, axes, angle, scale):
    shape, center = (23, 25), (11, 12)
    signal = np.zeros(shape, dtype=np.float32)
    signal[center] = 2.
    gaussian = pixel_gaussian(shape, center, 4., 2., angle, not linear)
    norm = 1. if units else 1. / gaussian.sum()
    expected = 2. * (norm if scale <= 0 else scale) * gaussian
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=signal, linear=linear)
        if units:
            assert source.setbrightnessunit(units)
        result = source.convolve2d(axes=axes, major="4pix", minor="2pix", pa=f"{angle}deg", scale=scale)
        try:
            np.testing.assert_allclose(result.getchunk(), expected, rtol=2e-5, atol=4e-7)
            assert result.brightnessunit() == ("Jy/beam" if units else units)
            beam = result.restoringbeam()
            if units:
                assert beam["major"]["value"] == pytest.approx(4.)
                assert beam["minor"]["value"] == pytest.approx(2.)
                assert beam["positionangle"]["value"] == pytest.approx(angle)
            else:
                assert beam == {}
            assert result.getchunk(getmask=True).all()
            np.testing.assert_array_equal(source.getchunk(), signal)
        finally:
            result.done()
            source.done()


@pytest.mark.parametrize("kind", ["f", "d"])
def test_convolve2d_record_and_string_native_products(casa, kind, tmp_path):
    shape, center = (19, 21), (9, 10)
    signal = np.zeros(shape, dtype=float)
    signal[center] = 3.
    mask = np.ones(shape, dtype=bool)
    mask[8, 9] = False
    expected = 3. * pixel_gaussian(shape, center, 3., 2., 27., True)
    options = {
        "major": "",
        "minor": "",
        "pa": "",
        "beam": {
            "major": {"value": 180., "unit": "arcsec"},
            "minor": "120arcsec",
            "positionangle": {"value": 27., "unit": "deg"},
        },
    }
    for writer in (casa, tools):
        source = writer.image()
        assert source.fromarray(pixels=signal, type=kind)
        assert source.putregion(pixelmask=mask)
        path = str(tmp_path / f"{writer.__name__}-{kind}.im")
        output = source.convolve2d(path, scale=1., **options)
        output.done()
        source.done()
        for reader in (casa, tools):
            result = reader.image()
            assert result.open(path)
            try:
                np.testing.assert_allclose(result.getchunk(), expected, rtol=2e-5, atol=4e-7)
                np.testing.assert_array_equal(result.getchunk(getmask=True), mask)
                assert result.pixeltype() == ("float" if kind == "f" else "double")
                assert result.getchunk().dtype == np.float64
                history = result.history()
                assert history[-4] == "Ran ia.convolve2d"
                assert "beam={...}" in history[-3]
                assert history[-2:] == [
                    "Original Temporary_Image has no beam",
                    f"Output {path} has no beam",
                ]
            finally:
                result.done()


def test_convolve2d_requires_explicit_empty_widths_with_a_beam(casa):
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[11, 11])
        try:
            with pytest.raises(RuntimeError, match="may not be specified if beam is specified"):
                source.convolve2d(beam={"major": "3pix", "minor": "2pix", "pa": "0deg"})
        finally:
            source.done()


@pytest.mark.parametrize("unit,scale,expected_peak", [
    ("Jy/beam", -1., 0.87883055),
    ("Jy/beam", 2., 4.),
    ("K", -1., 0.29383147),
    ("K", 2., 1.33737469),
])
def test_convolve2d_scales_a_sampled_input_beam(casa, unit, scale, expected_peak):
    shape, center = (25, 25), (12, 12)
    signal = np.zeros(shape, dtype=np.float32)
    signal[center] = 2.
    kernel = pixel_gaussian(shape, center, 3., 2., 25., True)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=signal)
        assert source.setbrightnessunit(unit)
        assert source.setrestoringbeam(major="120arcsec", minor="90arcsec", pa="22deg")
        result = source.convolve2d(major="180arcsec", minor="120arcsec", pa="25deg", scale=scale)
        try:
            np.testing.assert_allclose(result.getchunk(), expected_peak * kernel, rtol=4e-4, atol=2e-7)
            assert result.brightnessunit() == unit
            beam = result.restoringbeam()
            assert beam["major"]["value"] == pytest.approx(216.04094, rel=4e-4)
            assert beam["minor"]["value"] == pytest.approx(149.51836, rel=4e-4)
            assert beam["positionangle"]["value"] == pytest.approx(23.93931, abs=.02)
        finally:
            result.done()
            source.done()


@pytest.mark.parametrize("scale,peaks", [
    (-1., [2., 1.1604917, 1.28224123]),
    (2., [2., 4., 4.]),
])
def test_convolve2d_target_leaves_an_already_matching_beam_plane_alone(casa, scale, peaks):
    shape = (25, 25, 1, 3)
    values = np.zeros(shape, dtype=np.float32)
    values[12, 12, 0] = 2.
    values[13, 14, 0, 0] = 7.
    mask = values != 7.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        assert source.putregion(pixelmask=mask)
        assert source.setbrightnessunit("Jy/beam")
        for channel, (major, minor, pa) in enumerate([(180, 120, 25), (120, 90, 22), (100, 70, 10)]):
            assert source.setrestoringbeam(
                major=f"{major}arcsec", minor=f"{minor}arcsec", pa=f"{pa}deg", channel=channel,
            )
        result = source.convolve2d(
            major="180arcsec", minor="120arcsec", pa="25deg", scale=scale, targetres=True,
        )
        try:
            assert result.restoringbeam()["major"]["value"] == pytest.approx(180.)
            assert result.restoringbeam()["minor"]["value"] == pytest.approx(120.)
            assert result.restoringbeam()["positionangle"]["value"] == pytest.approx(25.)
            np.testing.assert_array_equal(result.getchunk()[..., 0], values[..., 0])
            np.testing.assert_array_equal(result.getchunk(getmask=True), mask)
            np.testing.assert_allclose(result.getchunk()[12, 12, 0], peaks, rtol=4e-4)
            assert result.getchunk()[13, 14, 0, 1] > 0.
        finally:
            result.done()
            source.done()


@pytest.mark.parametrize("args,message", [
    ({"axes": [0, 0]}, "Axes must be different"),
    ({"axes": [0, 2]}, "Axis value must be less"),
    ({"axes": [0]}, "exactly 2"),
    ({"type": "box"}, "Boxcar kernel not yet implemented"),
    ({"type": "hanning"}, "HANNING is not supported"),
    ({"type": "unknown"}, "Illegal kernel type"),
    ({"major": "1pix", "minor": "2pix"}, "Major axis is less"),
    ({"major": "2pix", "minor": "120arcsec"}, "unequal units"),
])
def test_convolve2d_rejects_incompatible_kernel_options(casa, args, message):
    parameters = {"major": "3pix", "minor": "2pix", "pa": "0deg"} | args
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[11, 11])
        try:
            with pytest.raises(RuntimeError, match=message):
                source.convolve2d(**parameters)
        finally:
            source.done()


def test_unreachable_target_resolution_preserves_existing_product(casa, tmp_path):
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[19, 21])
        assert source.setrestoringbeam(major="120arcsec", minor="90arcsec", pa="22deg")
        product = api.image()
        path = str(tmp_path / f"{api.__name__}-target.im")
        assert product.fromarray(path, np.float32([5., 6., 7.]))
        product.done()
        try:
            with pytest.raises(RuntimeError, match="Unable to reach target resolution"):
                source.convolve2d(
                    path, major="90arcsec", minor="50arcsec", pa="20deg",
                    overwrite=True, targetres=True,
                )
            assert source.restoringbeam()["major"]["value"] == pytest.approx(120.)
            assert product.open(path)
            np.testing.assert_array_equal(product.getchunk(), [5., 6., 7.])
        finally:
            product.done()
            source.done()


def test_convolve2d_beam_history_is_written_with_call_history_disabled(casa):
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[11, 13])
        assert source.setbrightnessunit("Jy/pixel")
        before = source.history()
        source.dohistory(False)
        result = source.convolve2d(major="180arcsec", minor="120arcsec", pa="25deg")
        try:
            assert result.history()[len(before):] == [
                "Original Temporary_Image has no beam",
                "Output  resolution major: 180 arcsec, minor: 120 arcsec, pa: 25 deg",
            ]
        finally:
            result.done()
            source.done()


@pytest.mark.parametrize("declination", [89.99305555555556, 90.])
def test_convolve2d_circular_kernel_is_symmetric_at_the_pole(casa, declination):
    data = np.zeros((17, 19), np.float32)
    data[8, 9] = 2.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=data)
        coords = source.coordsys()
        assert coords.setunits(["deg", "deg"])
        assert coords.setreferencevalue([45., declination])
        assert source.setcoordsys(coords.torecord())
        coords.done()
        result = source.convolve2d(major="2pix", minor="2pix", pa="0deg", scale=1.)
        try:
            np.testing.assert_allclose(result.getchunk()[8:11, 9:12], [
                [2., 1., .125], [1., .5, .0625], [.125, .0625, .0078125],
            ], rtol=4e-5, atol=2e-7)
        finally:
            result.done()
            source.done()


def test_convolve2d_target_resolution_needs_angular_widths(casa):
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[15, 15], linear=True)
        try:
            with pytest.raises(RuntimeError, match="Major axis must have angular units"):
                source.convolve2d(major="4pix", minor="2pix", pa="0deg", targetres=True)
        finally:
            source.done()


def test_convolve2d_target_tolerance_is_relative_for_beam_widths(casa):
    data = np.zeros((17, 19, 1, 2), np.float32)
    data[8, 9, 0, :] = 2.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=data)
        coords = source.coordsys()
        units = coords.units()
        assert coords.setunits(["arcsec", "arcsec", *units[2:]])
        increment = coords.increment()["numeric"]
        increment[:2] = [-.0002, .0002]
        assert coords.setincrement(increment)
        assert source.setcoordsys(coords.torecord())
        coords.done()
        assert source.setbrightnessunit("Jy/beam")
        assert source.setrestoringbeam(
            major=".004arcsec", minor=".004arcsec", pa="0deg", channel=0,
        )
        result = source.convolve2d(
            major=".009arcsec", minor=".009arcsec", pa="0deg", scale=2., targetres=True,
        )
        try:
            np.testing.assert_allclose(result.getchunk()[8, 9, 0], [4., 4.], atol=3e-6)
            assert np.all(result.getchunk()[9, 9, 0] > 3.9)
            assert result.restoringbeam()["major"]["value"] == pytest.approx(.009)
        finally:
            result.done()
            source.done()


def test_convolve2d_matches_a_wide_resolved_sampled_beam(casa):
    pixels = np.zeros((13, 15), np.float32)
    pixels[6, 7] = 2.
    # More than 513 pixels in the Gaussian footprint, even though the
    # selected image is small.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=pixels)
        assert source.setbrightnessunit("Jy/beam")
        assert source.setrestoringbeam(major="119'", minor="112'", pa="15deg")
        result = source.convolve2d(major="121pix", minor="92pix", pa="0deg")
        try:
            assert result.getchunk()[6, 7] == pytest.approx(.000292812649, rel=4e-4)
            beam = result.restoringbeam()
            assert beam["major"]["value"] == pytest.approx(10168.115, rel=3e-4)
            assert beam["minor"]["value"] == pytest.approx(8714.848, rel=3e-4)
            assert beam["positionangle"]["value"] == pytest.approx(3.044727, abs=.02)
        finally:
            result.done()
            source.done()


def test_convolve2d_very_wide_unit_volume_kernel_stays_bounded():
    pixels = np.zeros((13, 15), np.float32)
    pixels[6, 7] = 2.
    source = tools.image()
    assert source.fromarray(pixels=pixels)
    result = source.convolve2d(major="1950pix", minor="1800pix", pa="0deg")
    try:
        expected = 8. * np.log(2.) / (np.pi * 1950. * 1800.)
        assert result.getchunk()[6, 7] == pytest.approx(expected, rel=4e-5)
        assert result.brightnessunit() == ""
    finally:
        result.done()
        source.done()


@pytest.mark.parametrize("source_pa,kernel_pa,peak,beam", [
    (0, 0, .01110102329, (10183.4745, 230.51220, 0.)),
    (17, 19, .01401280519, (10181.9254, 291.01767, 18.01662)),
])
def test_convolve2d_wide_undersampled_beam_uses_discrete_samples(casa, source_pa, kernel_pa, peak, beam):
    pixels = np.zeros((13, 15), np.float32)
    pixels[6, 7] = 2.
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=pixels)
        assert source.setbrightnessunit("Jy/beam")
        assert source.setrestoringbeam(major="119'", minor="2.4'", pa=f"{source_pa}deg")
        result = source.convolve2d(major="121pix", minor="3pix", pa=f"{kernel_pa}deg")
        try:
            assert result.getchunk()[6, 7] == pytest.approx(peak, rel=4e-4)
            fitted = result.restoringbeam()
            assert fitted["major"]["value"] == pytest.approx(beam[0], rel=6e-4)
            assert fitted["minor"]["value"] == pytest.approx(beam[1], rel=6e-4)
            assert fitted["positionangle"]["value"] == pytest.approx(beam[2], abs=.02)
        finally:
            result.done()
            source.done()


def test_convolve2d_nondefault_axes_and_strided_masked_region(casa, tmp_path):
    shape = (17, 4, 19)
    values = np.zeros(shape, dtype=np.float32)
    values[3, 1, 6] = 5.
    values[3, 2, 6] = 7.
    spatial = np.ones(shape[:2], dtype=np.float32)
    spatial[5, 1] = 0.
    manager = casa.regionmanager()
    region = manager.box([1, 0, 2], [15, 3, 18], [2, 1, 2])
    manager.done()
    selection = np.s_[1:16:2, :, 2:19:2]
    selected_shape = values[selection].shape
    expected = np.zeros(selected_shape)
    footprint = pixel_gaussian((selected_shape[0], selected_shape[2]), (1, 2), 4., 2., 30., False)
    expected[:, 1, :] = 5. * footprint
    expected[:, 2, :] = 7. * footprint
    mask = np.broadcast_to(spatial[..., None] != 0., shape)[selection]
    for api in (casa, tools):
        source = api.image()
        path = str(tmp_path / f"{api.__name__}-mask.im")
        mask_image = api.image()
        mask_pixels = np.broadcast_to(spatial[..., None], shape).copy()
        assert mask_image.fromarray(path, mask_pixels, linear=True)
        mask_image.done()
        assert source.fromarray(pixels=values, linear=True)
        result = source.convolve2d(
            axes=[2, 0], major="4pix", minor="2pix", pa="30deg",
            scale=1., region=region, mask=f"'{path}' > 0", stretch=True,
        )
        original_coordinates = source.coordsys()
        selected_coordinates = result.coordsys()
        try:
            np.testing.assert_allclose(result.getchunk(), expected, rtol=2e-5, atol=9e-7)
            np.testing.assert_array_equal(result.getchunk(getmask=True), mask)
            np.testing.assert_allclose(
                selected_coordinates.toworld([1., 1., 2.])["numeric"],
                original_coordinates.toworld([3., 1., 6.])["numeric"],
            )
        finally:
            selected_coordinates.done()
            original_coordinates.done()
            result.done()
            source.done()
