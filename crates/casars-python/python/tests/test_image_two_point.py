"""Structure-function values, masks and named CASA image interchange."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _ramp(shape, scalar=1):
    x, y = np.ogrid[:shape[0], :shape[1]]
    return (scalar*(x+2*y)).astype(np.complex64 if np.iscomplexobj(scalar) else np.float32)


def _expected_ramp(shape, scalar):
    x, y = np.ogrid[1-shape[0]:shape[0], 1-shape[1]:shape[1]]
    return (scalar*(x+2*y))**2


@pytest.mark.parametrize("shape", [(5, 7), (13, 16)])
@pytest.mark.parametrize("kind,scalar", [("f", 1), ("c", 1+2j)])
def test_two_point_analytic_ramp_and_native_interchange(casa, tmp_path, shape, kind, scalar):
    data = _ramp(shape, scalar)
    expected = _expected_ramp(shape, scalar)
    for writer in (casa, tools):
        source = writer.image()
        name = str(tmp_path/f"{writer.__name__}-lag.im")
        try:
            assert source.fromshape(shape=list(shape), type=kind)
            assert source.putchunk(data)
            assert source.setbrightnessunit("Jy/beam")
            assert source.setmiscinfo({"fixture": "lags"})
            assert source.setrestoringbeam(major="4arcmin", minor="2arcmin", pa="5deg")
            assert source.sethistory(origin="fixture", history=["lag input"])
            assert source.twopointcorrelation(name, method="STR") is True
            np.testing.assert_array_equal(source.getchunk(), data)
        finally:
            source.done()
        for reader in (casa, tools):
            product = reader.image()
            coords = None
            try:
                assert product.open(name)
                assert product.pixeltype() == ("complex" if kind == "c" else "float")
                assert product.brightnessunit() == "count"
                assert product.miscinfo()["fixture"] == "lags"
                assert product.restoringbeam()["major"] == {"value": 4., "unit": "arcmin"}
                assert any("lag input" in line for line in product.history())
                assert any("twopointcorrelation" in line for line in product.history())
                coords = product.coordsys()
                assert coords.axiscoordinatetypes() == ["Linear", "Linear"]
                assert coords.names() == ["Lag-Right Ascension", "Lag-Declination"]
                assert coords.units() == ["'", "'"]
                np.testing.assert_array_equal(coords.referencepixel()["numeric"], np.array(shape)-1)
                np.testing.assert_array_equal(coords.referencevalue()["numeric"], [0., 0.])
                np.testing.assert_allclose(coords.increment()["numeric"], [-1., 1.], atol=2e-14)
                np.testing.assert_array_equal(product.getchunk(getmask=True), np.ones(expected.shape, dtype=bool))
                np.testing.assert_allclose(product.getchunk(), expected, rtol=3e-7, atol=2e-8)
            finally:
                if coords is not None:
                    coords.done()
                product.done()


def _all_pairs(data, valid):
    nx, ny = data.shape
    shape = (2*nx-1, 2*ny-1)
    counts = np.zeros(shape, dtype=int)
    sums = np.zeros(shape, dtype=np.result_type(data.dtype, np.float64))
    for ax, ay in np.argwhere(valid):
        for bx, by in np.argwhere(valid):
            lag = (bx-ax+nx-1, by-ay+ny-1)
            sums[lag] += (data[ax, ay] - data[bx, by])**2
            counts[lag] += 1
    return np.divide(sums, counts, where=counts>0, out=np.zeros_like(sums)), counts>0


def test_two_point_expands_a_selected_plane_and_masks_empty_lags(casa, tmp_path):
    x, y = np.ogrid[:15, :16]
    plane = ((x-4)**2 + 3*y).astype(np.float32)
    data = plane[..., None, None] + 10*np.arange(5, dtype=np.float32)
    inherited = np.broadcast_to(((x+3*y)%7 != 0)[..., None, None], data.shape)
    external = np.broadcast_to(((x>4) & (x<10) & (y>4) & (y<12))[..., None, None], (15,16,1,1))
    region_manager = casa.regionmanager()
    region = region_manager.box([2,3,0,1], [12,13,0,3])
    region_manager.done()
    expected, expected_mask = _all_pairs(
        plane[2:13, 3:14],
        (inherited & external)[2:13, 3:14, 0, 1],
    )
    assert expected_mask.any() and not expected_mask.all()
    for api in (casa, tools):
        source, mask_image, output = api.image(), api.image(), api.image()
        mask_path = str(tmp_path/f"{api.__name__}-source-mask.im")
        output_path = str(tmp_path/f"{api.__name__}-output.im")
        try:
            assert source.fromarray(pixels=data)
            assert source.putregion(pixelmask=inherited)
            for channel in range(5):
                assert source.setrestoringbeam(major=f"{2+channel}arcmin", minor="1arcmin",
                                               pa="2deg", channel=channel, polarization=0)
            assert mask_image.fromarray(mask_path, pixels=external.astype(np.float32))
            mask_image.done()
            arguments = dict(region=region, mask=f'"{mask_path}" > 0', axes=[0,1])
            with pytest.raises(RuntimeError):
                source.twopointcorrelation(output_path, **arguments)
            assert source.twopointcorrelation(output_path, stretch=True, **arguments) is True
            assert output.open(output_path)
            assert output.shape().tolist() == [21,21,1,3]
            for channel in range(3):
                beam = output.restoringbeam(channel=channel, polarization=0)
                assert beam["major"] == {"value": pytest.approx(3+channel), "unit": "arcmin"}
                values = output.getchunk()[:, :, 0, channel]
                validity = output.getchunk(getmask=True)[:, :, 0, channel]
                np.testing.assert_array_equal(validity, expected_mask)
                np.testing.assert_allclose(values[validity], expected[validity], atol=2e-6)
        finally:
            output.done()
            mask_image.done()
            source.done()


@pytest.mark.parametrize("linear", [True, False])
def test_two_point_defaults_boolean_and_axis_refusals(casa, tmp_path, linear):
    data = _ramp((5,7))
    for api in (casa, tools):
        source, prior = api.image(), api.image()
        output = str(tmp_path/f"{api.__name__}-before.im")
        try:
            assert source.twopointcorrelation() is False
            assert source.fromshape(shape=list(data.shape), linear=linear)
            assert source.putchunk(data)
            assert source.twopointcorrelation() is True
            assert prior.fromarray(output, pixels=data+5.)
            prior.done()
            for options in (
                {"axes": [0]}, {"axes": [1,0]}, {"axes": [0,0]},
                {"axes": [-2,1]}, {"axes": [0,3]}, {"method": "correlation"},
            ):
                with pytest.raises(RuntimeError):
                    source.twopointcorrelation(output, overwrite=True, **options)
            assert prior.open(output)
            np.testing.assert_array_equal(prior.getchunk(), data+5.)
        finally:
            prior.done()
            source.done()


def test_two_point_keeps_linear_coordinates_on_non_sky_axes(casa, tmp_path):
    data = _ramp((5,7))
    for api in (casa, tools):
        source, output = api.image(), api.image()
        path = str(tmp_path/f"{api.__name__}-linear.im")
        coords = None
        try:
            assert source.fromshape(shape=list(data.shape), linear=True)
            assert source.putchunk(data)
            assert source.twopointcorrelation(path, axes=np.array([.6,1.8])) is True
            assert output.open(path)
            coords = output.coordsys()
            assert coords.names() == ["linear1", "linear2"]
            assert coords.units() == ["km", "km"]
            np.testing.assert_array_equal(coords.referencepixel()["numeric"], [2.,3.])
            np.testing.assert_array_equal(output.getchunk(), _expected_ramp(data.shape, 1))
        finally:
            if coords is not None:
                coords.done()
            output.done()
            source.done()


def test_two_point_refuses_tuple_axes_before_the_named_output_changes(casa, tmp_path):
    data = _ramp((5,7))
    for api, exception in ((casa,SystemError), (tools,TypeError)):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            with pytest.raises(exception):
                source.twopointcorrelation(axes=(0,1))
        finally:
            source.done()
    source, previous = tools.image(), tools.image()
    path = str(tmp_path/"previous.im")
    try:
        assert source.fromarray(pixels=data)
        assert previous.fromarray(path, pixels=data+3)
        previous.done()
        with pytest.raises(TypeError):
            source.twopointcorrelation(path, axes=(0,1), overwrite=True)
        assert previous.open(path)
        np.testing.assert_array_equal(previous.getchunk(), data+3)
    finally:
        previous.done()
        source.done()


@pytest.mark.parametrize("order,axes,shape,types,reference", [
    ("0213", None, [9,1,13,3], ["Linear","Stokes","Linear","Spectral"], [4.,0.,6.,1.]),
    ("1203", [0,2], [13,1,9,3], ["Linear","Stokes","Linear","Spectral"], [6.,0.,4.,1.]),
    ("2301", [0,2], [1,3,9,7], ["Stokes","Spectral","Direction","Direction"], [0.,1.,2.,3.]),
])
def test_two_point_uses_mapped_axes_and_keeps_other_axes(casa, tmp_path, order, axes, shape, types, reference):
    input_plane = _ramp((5,7))
    data = input_plane[..., None, None] + 10*np.arange(3, dtype=np.float32)
    for api in (casa, tools):
        source, output = api.image(), api.image()
        path = str(tmp_path/f"{api.__name__}-mapped.im")
        coords = mapped = None
        try:
            assert source.fromarray(pixels=data)
            mapped = source.transpose(order=order)
            if order == "1203":
                with pytest.raises(RuntimeError):
                    mapped.twopointcorrelation(path)
            assert mapped.twopointcorrelation(path, **({"axes": axes} if axes is not None else {}))
            assert output.open(path)
            assert output.shape().tolist() == shape
            coords = output.coordsys()
            assert coords.axiscoordinatetypes() == types
            np.testing.assert_array_equal(coords.referencepixel()["numeric"], reference)
            if order == "2301":
                expected = np.broadcast_to((np.arange(-4,5)**2)[None,:,None], (3,9,7))
                np.testing.assert_array_equal(output.getchunk()[0], expected)
            else:
                expected = _expected_ramp((5,7), 1)
                if order == "1203":
                    expected = expected.T
                np.testing.assert_array_equal(output.getchunk()[:, 0, :, 0], expected)
        finally:
            if coords is not None:
                coords.done()
            if mapped is not None:
                mapped.done()
            output.done()
            source.done()
