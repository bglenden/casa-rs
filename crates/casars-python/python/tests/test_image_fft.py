"""Pinned image Fourier products, coordinates, masks, and native readback."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _fft(values, axes):
    return np.fft.fftshift(
        np.fft.fftn(np.fft.ifftshift(values, axes=axes), axes=axes), axes=axes,
    )


@pytest.mark.parametrize("shape_kind", ["world-box", "world-polygon", "strided-pixel"])
def test_fft_selected_single_region_with_mixed_precision_mask(casa, tmp_path, shape_kind):
    shape = (15, 17, 1, 5)
    pixels = np.arange(np.prod(shape), dtype=np.float32).reshape(shape) * np.float32(0.125)
    weights = np.indices(shape).sum(axis=0).astype(np.float64) * 0.5
    maker, region_tool = casa.image(), casa.regionmanager()
    primary, secondary = (tmp_path / name for name in ("single.im", "double.im"))
    coordinate = None
    try:
        assert maker.fromarray(str(secondary), weights, type="d")
        assert maker.done()
        assert maker.fromarray(str(primary), pixels)
        coordinate = maker.coordsys()
        record = coordinate.torecord()
        assert maker.done()
        if shape_kind == "world-box":
            region = region_tool.wbox(["4pix", "5pix"], ["10pix", "12pix"], [0, 1], record)
            view = np.s_[4:11, 5:13]
            membership = True
        elif shape_kind == "world-polygon":
            region = region_tool.fromtext(
                "poly[[4pix,5pix],[10pix,5pix],[4pix,12pix]]", list(shape), record,
            )
            view = np.s_[4:11, 5:13]
            x, y = np.ogrid[:7, :8]
            membership = (7 * x + 6 * y <= 42)[..., None, None]
        else:
            region = region_tool.box([4, 5, 0, 0], [10, 12, 0, 4], [2, 1, 1, 1])
            view = np.s_[4:11:2, 5:13]
            membership = True
    finally:
        if coordinate is not None:
            coordinate.done()
        region_tool.done()
        maker.done()
    accepted = (pixels + weights > 90)[view] & membership
    assert accepted.any() and not accepted.all()
    expected = _fft(np.where(accepted, pixels[view], 0), (0, 1))
    expression = f'("{primary}" + "{secondary}") > 90'
    for backend in (casa, tools):
        image, reopened = backend.image(), casa.image()
        output = str(tmp_path / f"{backend.__name__}-fourier.im")
        try:
            assert image.open(str(primary))
            assert image.fft(axes=[0, 1], region=region, mask=expression, complex=output)
            assert image.done()
            assert reopened.open(output)
            assert reopened.pixeltype() == "complex"
            np.testing.assert_array_equal(reopened.getchunk(getmask=True), accepted)
            np.testing.assert_allclose(reopened.getchunk(), expected, rtol=1e-6, atol=4e-4)
        finally:
            reopened.done()
            image.done()


@pytest.mark.parametrize("dtype, precision", [
    (np.float32, "f"), (np.complex64, "f"),
    (np.float64, "d"), (np.complex128, "d"),
])
def test_image_fft_all_products_and_native_readers(casa, tmp_path, dtype, precision):
    values = np.zeros((7, 8), dtype=dtype)
    values[3, 4] = 2
    if np.issubdtype(dtype, np.complexfloating):
        values[3, 4] += 1j
    values[4, 5] = -1.
    values[1, 2] = 400.
    valid = np.ones_like(values, dtype=bool)
    valid[1, 2] = False
    expected = _fft(np.where(valid, values, 0.), (0, 1))
    expected_products = {
        "real": expected.real, "imag": expected.imag, "amp": np.abs(expected),
        "phase": np.angle(expected), "complex": expected,
    }
    for index, maker in enumerate((casa, tools)):
        source = maker.image()
        assert source.fft() is False
        assert source.fromarray(pixels=values, type=precision)
        assert source.putregion(pixelmask=valid)
        if precision == "f":
            assert source.setbrightnessunit("Jy/beam")
        outfiles = {name: str(tmp_path / f"{index}-{name}.im") for name in expected_products}
        try:
            assert source.fft(**outfiles) is True
            np.testing.assert_array_equal(source.getchunk(), values)
            np.testing.assert_array_equal(source.getchunk(getmask=True), valid)
        finally:
            source.done()
        for reader in (casa, tools):
            for name, target in outfiles.items():
                output = reader.image()
                assert output.open(target)
                try:
                    actual = output.getchunk()
                    assert actual.dtype == (np.complex128 if name == "complex" else np.float64)
                    expected_type = (
                        ("complex" if precision == "f" else "dcomplex") if name == "complex"
                        else ("float" if precision == "f" else "double")
                    )
                    assert output.pixeltype() == expected_type
                    tolerance = 8e-7 if precision == "f" else 5e-14
                    np.testing.assert_allclose(actual, expected_products[name], rtol=tolerance, atol=tolerance)
                    np.testing.assert_array_equal(output.getchunk(getmask=True), valid)
                    assert output.brightnessunit() == ("rad" if name == "phase" else "Jy" if precision == "f" else "")
                    assert output.history()[-2] == "Ran ia.fft"
                    coords = output.coordsys()
                    try:
                        assert list(coords.names()) == ["UU", "VV"]
                        assert list(coords.units()) == ["lambda", "lambda"]
                        np.testing.assert_array_equal(coords.referencepixel()["numeric"], [3., 4.])
                        np.testing.assert_array_equal(coords.referencevalue()["numeric"], [0., 0.])
                        np.testing.assert_allclose(coords.increment()["numeric"],
                                                   [-10800. / (7 * np.pi), 10800. / (8 * np.pi)], rtol=1e-12)
                    finally:
                        coords.done()
                finally:
                    output.done()


def test_image_fft_rotated_direction_matrix(casa, tmp_path):
    coord = casa.coordsys()
    assert coord.newcoordsys(direction=True)
    matrix = np.array([[.8, -.6], [.6, .8]])
    assert coord.setlineartransform(type="direction", value=matrix)
    assert coord.setincrement(["-0.0005rad", "0.0007rad"], type="direction")
    record = coord.torecord()
    coord.done()
    pixels = np.zeros((7, 8), dtype=np.float32)
    pixels[4, 5] = 2.
    for index, api in enumerate((casa, tools)):
        source = api.image()
        try:
            assert source.fromarray(pixels=pixels, csys=record)
            path = str(tmp_path / f"rotated-{index}.im")
            assert source.fft(axes=[1, 0], complex=path)
            output = api.image()
            assert output.open(path)
            try:
                np.testing.assert_allclose(output.getchunk(), _fft(pixels, (0, 1)), atol=5e-7)
                coords = output.coordsys()
                try:
                    np.testing.assert_allclose(coords.lineartransform(type="linear"),
                                               np.linalg.inv(matrix), atol=1e-12)
                    np.testing.assert_allclose(coords.increment()["numeric"],
                                               [-1 / (.0005 * 7), 1 / (.0007 * 8)], rtol=1e-12)
                finally:
                    coords.done()
            finally:
                output.done()
        finally:
            source.done()


def test_image_fft_transforms_direction_and_linear_frequency(casa, tmp_path):
    coordinate = casa.coordsys()
    assert coordinate.newcoordsys(direction=True, spectral=True)
    record = coordinate.torecord()
    coordinate.done()
    values = np.zeros((7, 8, 3), dtype=np.float32)
    values[4, 5, 0] = 2.
    for index, api in enumerate((casa, tools)):
        source = api.image()
        try:
            assert source.fromarray(pixels=values, csys=record)
            path = str(tmp_path / f"spectral-{index}.im")
            assert source.fft(axes=[0, 1, 2], complex=path)
            output = api.image()
            assert output.open(path)
            try:
                np.testing.assert_allclose(output.getchunk(), _fft(values, (0, 1, 2)), atol=5e-7)
                coords = output.coordsys()
                try:
                    assert list(coords.names()) == ["UU", "VV", "Time"]
                    assert list(coords.units()) == ["lambda", "lambda", "s"]
                    np.testing.assert_array_equal(coords.referencepixel()["numeric"], [3., 4., 1.])
                    assert coords.increment()["numeric"][-1] == pytest.approx(1 / 3000.)
                finally:
                    coords.done()
            finally:
                output.done()
        finally:
            source.done()


def test_image_fft_preserves_per_channel_beam_records_for_sky_transform(casa, tmp_path):
    coordinate = casa.coordsys()
    assert coordinate.newcoordsys(direction=True, spectral=True)
    record = coordinate.torecord()
    coordinate.done()
    pixels = np.zeros((7, 8, 3), dtype=np.float32)
    pixels[3, 4] = [2., 5., 9.]
    for index, api in enumerate((casa, tools)):
        source = api.image()
        assert source.fromarray(pixels=pixels, csys=record)
        source.setbrightnessunit("Jy")
        try:
            for channel in range(3):
                source.setrestoringbeam(major=f"{(8+channel)/60}arcmin", minor="3arcsec",
                                        pa="7deg", channel=channel, polarization=0)
            beams = [source.restoringbeam(channel=i, polarization=0) for i in range(3)]
            path = str(tmp_path / f"sky-beams-{index}.im")
            assert source.fft(real=path)
            output = api.image()
            assert output.open(path)
            try:
                assert output.brightnessunit() == "Jy/beam"
                for channel, expected in enumerate(beams):
                    assert output.restoringbeam(channel=channel, polarization=0) == expected
                np.testing.assert_array_equal(output.getchunk(), np.broadcast_to([2., 5., 9.], pixels.shape))
            finally:
                output.done()
            path = tmp_path / f"time-beams-{index}.im"
            companion = tmp_path / f"time-complex-{index}.im"
            with pytest.raises(RuntimeError, match="Number of channels is not consistent"):
                source.fft(axes=[0, 1, 2], real=str(path), complex=str(companion))
            if api is not casa:
                assert not path.exists() and not companion.exists()
        finally:
            source.done()


def test_image_fft_default_sky_axes_follow_a_transposed_cube(casa, tmp_path):
    values = np.zeros((7, 8, 2), dtype=np.float32)
    values[3, 4] = [2., 5.]
    values[4, 5] = [1., 3.]
    for index, api in enumerate((casa, tools)):
        source = api.image()
        assert source.fromarray(pixels=values)
        transposed = source.transpose(order=201)
        try:
            path = str(tmp_path / f"transposed-{index}.im")
            assert transposed.fft(complex=path)
            output = api.image()
            assert output.open(path)
            try:
                np.testing.assert_allclose(output.getchunk(), _fft(values, (0, 1)).transpose(2, 0, 1),
                                           rtol=6e-7, atol=6e-7)
                coords = output.coordsys()
                try:
                    assert list(coords.names()) == ["Stokes", "UU", "VV"]
                    assert list(coords.units()) == ["", "lambda", "lambda"]
                    np.testing.assert_array_equal(coords.referencepixel()["numeric"], [0., 3., 4.])
                finally:
                    coords.done()
            finally:
                output.done()
        finally:
            transposed.done()
            source.done()


def test_image_fft_preserves_untransformed_spectral_display_fields(casa, tmp_path):
    coordinate = casa.coordsys()
    assert coordinate.newcoordsys(direction=True, spectral=True)
    record = coordinate.torecord()
    coordinate.done()
    spectral = next(value for key, value in record.items() if key.startswith("spectral"))
    settings = dict(nativeType=1, velUnit="m/s", waveUnit="mm", formatUnit="m/s")
    spectral.update(settings)
    values = np.ones((7, 8, 3), dtype=np.float32)
    for index, api in enumerate((casa, tools)):
        source = api.image()
        assert source.fromarray(pixels=values, csys=record)
        try:
            path = str(tmp_path / f"spectral-display-{index}.im")
            assert source.fft(complex=path)
            output = casa.image()
            assert output.open(path)
            try:
                coords = output.coordsys()
                try:
                    actual = next(value for key, value in coords.torecord().items() if key.startswith("spectral"))
                    assert {key: actual[key] for key in settings} == settings
                finally:
                    coords.done()
            finally:
                output.done()
        finally:
            source.done()


@pytest.mark.parametrize("unit, increment, name, transformed_unit, expected", [
    ("MHz", 2., "Time", "s", 1.25e-7),
    ("Hz", 2.e6, "Time", "s", 1.25e-7),
    ("ms", 2., "Frequency", "Hz", 125.),
    ("s", .002, "Frequency", "Hz", 125.),
])
def test_image_fft_uses_physical_frequency_and_time_spacing(casa, tmp_path, unit, increment,
                                                            name, transformed_unit, expected):
    coordinate = casa.coordsys()
    assert coordinate.newcoordsys(linear=2)
    record = coordinate.torecord()
    coordinate.done()
    linear = next(value for key, value in record.items() if key.startswith("linear"))
    linear["units"] = [unit, "km"]
    linear["cdelt"] = np.array([increment, 1.])
    source = tools.image()
    try:
        assert source.fromarray(pixels=np.ones((4, 6), dtype=np.float32), csys=record)
        path = str(tmp_path / "converted.im")
        assert source.fft(real=path, axes=[0])
        output = casa.image()
        assert output.open(path)
        try:
            coords = output.coordsys()
            try:
                assert coords.names()[0] == name
                assert coords.units()[0] == transformed_unit
                assert coords.increment()["numeric"][0] == pytest.approx(expected, rel=1e-12)
            finally:
                coords.done()
        finally:
            output.done()
    finally:
        source.done()


def test_image_fft_stretches_a_named_mask_before_transforming(casa, tmp_path):
    values = np.zeros((7, 8, 2), dtype=np.float32)
    values[3, 4] = [2., 5.]
    values[4, 5] = [90., 300.]
    mask = np.ones((7, 8), dtype=np.float32)
    mask[4, 5] = 0.
    mask_path = str(tmp_path / "mask.im")
    operand = casa.image()
    assert operand.fromarray(mask_path, mask)
    operand.done()
    for index, api in enumerate((casa, tools)):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            path = str(tmp_path / f"stretched-{index}.im")
            assert source.fft(complex=path, mask=f'"{mask_path}" > 0', stretch=True)
            output = api.image()
            assert output.open(path)
            try:
                np.testing.assert_array_equal(output.getchunk(), np.broadcast_to([2., 5.], values.shape))
                np.testing.assert_array_equal(output.getchunk(getmask=True),
                                              np.broadcast_to(mask[:, :, None] != 0., values.shape))
                assert f'mask=""{mask_path}" > 0", stretch=true' in output.history()[-1]
            finally:
                output.done()
        finally:
            source.done()


def test_image_fft_transforms_only_a_selected_linear_axis_after_cropping(casa, tmp_path):
    values = np.arange(12 * 15, dtype=np.float32).reshape(12, 15)
    regions = casa.regionmanager()
    region = regions.box([2, 3], [8, 10])
    regions.done()
    for index, api in enumerate((casa, tools)):
        source = api.image()
        try:
            assert source.fromarray(pixels=values, linear=True)
            path = str(tmp_path / f"cropped-{index}.im")
            assert source.fft(axes=[0], complex=path, region=region)
            output = api.image()
            assert output.open(path)
            try:
                np.testing.assert_allclose(output.getchunk(), _fft(values[2:9, 3:11], (0,)), atol=3e-5, rtol=5e-7)
                coords = output.coordsys()
                try:
                    assert list(coords.names()) == ["Inverse(linear1)", "linear2"]
                    assert list(coords.units()) == ["1/km", "km"]
                    np.testing.assert_array_equal(coords.referencepixel()["numeric"], [3., 4.])
                    np.testing.assert_allclose(coords.increment()["numeric"], [1/7, 1])
                finally:
                    coords.done()
            finally:
                output.done()
        finally:
            source.done()


def test_image_fft_honors_nonleading_explicit_axes(tmp_path):
    values = np.arange(7 * 8, dtype=np.float32).reshape(7, 8)
    source = tools.image()
    try:
        assert source.fromarray(pixels=values, linear=True)
        path = str(tmp_path / "second-axis.im")
        assert source.fft(axes=[1], complex=path)
        output = tools.image()
        assert output.open(path)
        try:
            np.testing.assert_allclose(output.getchunk(), _fft(values, (1,)), atol=3e-5, rtol=5e-7)
            coords = output.coordsys()
            try:
                assert list(coords.names()) == ["linear1", "Inverse(linear2)"]
                np.testing.assert_array_equal(coords.increment()["numeric"], [1., 1/8])
            finally:
                coords.done()
        finally:
            output.done()
    finally:
        source.done()


def test_image_fft_rejects_conflicting_outputs_before_publication(casa, tmp_path):
    for index, api in enumerate((casa, tools)):
        inputs = api.image()
        assert inputs.fromshape(shape=[7, 8])
        existing = str(tmp_path / f"existing-{index}.im")
        product = api.image()
        assert product.fromarray(existing, np.float32([[2., 3.]]))
        product.done()
        pending = tmp_path / f"pending-{index}.im"
        try:
            with pytest.raises(RuntimeError, match="No output image"):
                inputs.fft()
            with pytest.raises(RuntimeError, match="already exists"):
                inputs.fft(real=str(pending), amp=existing)
            assert not pending.exists()
            with pytest.raises(RuntimeError, match="both the DirectionCoordinate"):
                inputs.fft(axes=[0], complex=str(pending))
            assert not pending.exists()
            assert product.open(existing)
            np.testing.assert_array_equal(product.getchunk(), [[2., 3.]])
        finally:
            product.done()
            inputs.done()


def test_image_fft_rejects_a_partial_coupled_linear_transform(tmp_path):
    source = tools.image()
    coordinate = tools.coordsys()
    path = tmp_path / "coupled-axis.im"
    try:
        assert coordinate.newcoordsys(linear=2)
        assert coordinate.setlineartransform(type="linear", value=[[1., .2], [.5, 1.]])
        assert source.fromshape(shape=[7, 8], csys=coordinate.torecord())
        with pytest.raises(RuntimeError, match="Cannot invert non-diagonal PC matrix"):
            source.fft(axes=[0], complex=str(path))
        assert not path.exists()
    finally:
        coordinate.done()
        source.done()


def test_image_fft_does_not_publish_any_duplicate_outputs(tmp_path):
    source = tools.image()
    assert source.fromshape(shape=[7, 8])
    result = str(tmp_path / "same-destination.im")
    try:
        with pytest.raises(RuntimeError, match="separate destinations"):
            source.fft(real=result, complex=result)
        assert not (tmp_path / "same-destination.im").exists()
    finally:
        source.done()
