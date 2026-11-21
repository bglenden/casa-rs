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
