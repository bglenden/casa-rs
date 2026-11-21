"""Pinned CASA parity and native interoperability for the public image regrid tool."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _ramp():
    x, y, _, channel = np.ogrid[:31, :33, :1, :7]
    return (x + 2*y + 10*channel).astype(np.float32)


def _record(image, kind=None, shift=None):
    coords = image.coordsys()
    try:
        if kind is not None:
            ref = coords.referencepixel(type=kind)["numeric"]
            assert coords.setreferencepixel(value=ref + np.asarray(shift), type=kind)
        return coords.torecord()
    finally:
        coords.done()


def _read(image):
    coords = image.coordsys()
    try:
        kind = image.pixeltype()
        return (
            image.getchunk(), image.getchunk(getmask=True),
            image.brightnessunit(),
            coords.referencepixel()["numeric"], coords.increment()["numeric"],
            coords.axiscoordinatetypes(), kind,
        )
    finally:
        coords.done()


def _compare(actual, expected, *, tolerance=2e-5):
    assert actual[0].dtype == expected[0].dtype
    assert actual[1].dtype == expected[1].dtype == np.bool_
    np.testing.assert_array_equal(actual[1], expected[1])
    np.testing.assert_allclose(actual[0], expected[0], rtol=tolerance, atol=tolerance)
    assert actual[2] == expected[2]
    for index in (3, 4):
        np.testing.assert_allclose(actual[index], expected[index], atol=1e-6)
    assert actual[5:] == expected[5:]


@pytest.mark.parametrize("kind", ["f", "d", "c", "cd"])
def test_regrid_defaults_preserve_four_pixel_types_and_native_writer_readers(casa, tmp_path, kind):
    data = _ramp().astype(np.float64)
    if kind.startswith("c"):
        data = data * (1 + 2j)
    valid = np.ones(data.shape, dtype=bool)
    valid[8, 9, 0, 2] = False
    observed = {}
    for writer in (casa, tools):
        source = writer.image()
        path = str(tmp_path / f"{writer.__name__}-{kind}.im")
        try:
            assert source.fromshape(shape=data.shape, type=kind)
            assert source.putchunk(data)
            assert source.putregion(pixelmask=valid)
            if kind in ("f", "c"):
                assert source.setbrightnessunit("Jy/beam")
            assert source.sethistory(origin="test", history=["source regrid history"])
            temp = source.regrid()
            try:
                assert temp.isopen()
                assert temp.maskhandler("get") == ["mask0"]
                np.testing.assert_array_equal(temp.getchunk(getmask=True), valid)
            finally:
                temp.done()
            product = source.regrid(path)
            assert product.isopen()
            product.done()
            np.testing.assert_array_equal(source.getchunk(), data)
        finally:
            source.done()
        for reader in (casa, tools):
            product = reader.image()
            try:
                assert product.open(path)
                assert any("source regrid history" in row for row in product.history())
                assert product.maskhandler("default") == ["mask0"]
                observed[writer.__name__, reader.__name__] = _read(product)
            finally:
                product.done()
    expected = observed[casa.__name__, casa.__name__]
    np.testing.assert_array_equal(expected[0], data)
    np.testing.assert_array_equal(expected[1], valid)
    for actual in observed.values():
        _compare(actual, expected)


@pytest.mark.parametrize("method", ["nearest", "lin", "Cubic", "lanczos", "Z"])
def test_regrid_shifted_sky_plane_and_interpolation(casa, method):
    data = _ramp()
    results = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            target = _record(source, "direction", [.2, .3])
            product = source.regrid(csys=target, axes=[0], method=method, decimate=2)
            try:
                results.append(_read(product))
            finally:
                product.done()
        finally:
            source.done()
    _compare(results[1], results[0], tolerance=2e-4)
    region = np.s_[5:-5, 5:-5, :, :]
    if method in ("lin", "Cubic"):
        np.testing.assert_allclose(results[1][0][region], data[region] - .8, atol=2e-5)
    elif method == "nearest":
        np.testing.assert_array_equal(results[1][0][region], data[region])


@pytest.mark.parametrize("asvelocity", [False, True])
def test_regrid_spectral_interpolation_and_target_coordinates(casa, asvelocity):
    data = _ramp()
    results = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            target = _record(source, "spectral", [.5])
            result = source.regrid(csys=target, axes=[3], asvelocity=asvelocity, decimate=-1)
            try:
                results.append(_read(result))
                np.testing.assert_allclose(result.getchunk()[..., 1:], data[..., 1:] - 5, atol=2e-5)
                assert not result.getchunk(getmask=True)[..., 0].any()
                assert result.getchunk(getmask=True)[..., 1:].all()
            finally:
                result.done()
        finally:
            source.done()
    _compare(results[1], results[0])


def test_regrid_region_dropdeg_and_replicated_direction(casa):
    data = _ramp()
    region = casa.regionmanager().box([3, 4, 0, 1], [14, 19, 0, 5])
    results = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            products = [
                source.regrid(region=region, decimate=1),
                source.regrid(shape=[14, 18, 7], decimate=2, axes=[0, 1], dropdeg=True),
                source.regrid(csys=_record(source, "direction", [.5, .5]), axes=[0, 1], decimate=2, replicate=True),
            ]
            try:
                results.append([_read(product) for product in products])
                np.testing.assert_array_equal(products[2].getchunk(), data)
            finally:
                for product in products:
                    product.done()
        finally:
            source.done()
    for actual, expected in zip(results[1], results[0]):
        _compare(actual, expected)


def test_regrid_rejected_axes_and_coordinates_preserve_the_destination(casa, tmp_path):
    data = _ramp()
    for api in (casa, tools):
        source = api.image()
        output = api.image()
        path = str(tmp_path / f"{api.__name__}-regrid.im")
        try:
            assert source.fromarray(pixels=data)
            assert output.fromarray(path, pixels=data+1)
            output.done()
            for arguments in (
                {"axes": [0, 0], "decimate": 1},
                {"axes": [20], "decimate": 1},
                {"csys": {"invalid": 1}},
                {"method": "sinc"},
                {"shape": [0, 12, 1, 7], "axes": [0, 1], "decimate": 1},
            ):
                with pytest.raises(RuntimeError):
                    source.regrid(outfile=path, overwrite=True, **arguments)
                assert source.isopen()
            assert output.open(path)
            np.testing.assert_array_equal(output.getchunk(), data+1)
        finally:
            output.done()
            source.done()
