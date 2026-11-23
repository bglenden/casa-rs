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


def test_regrid_retains_selected_spectral_axis_without_a_template_coordinate(casa):
    square = np.arange(8**3, dtype=np.float32).reshape(8, 8, 8)
    sky = casa.coordsys().newcoordsys(direction=True).torecord()
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=square)
            expected = _read(source)
            for template in ({}, sky):
                result = source.regrid(csys=template, axes=[2], method="cubic", decimate=1)
                try:
                    _compare(_read(result), expected, tolerance=0)
                finally:
                    result.done()
        finally:
            source.done()

    # CASA compares the selected spectral length to the first output dimension
    # for axes=[2]. Check the correct axis on a rectangular cube.
    source = tools.image()
    try:
        assert source.fromarray(pixels=square[:, :, :5])
        expected = _read(source)
        result = source.regrid(axes=[2], method="cubic", decimate=1)
        try:
            _compare(_read(result), expected, tolerance=0)
        finally:
            result.done()
        with pytest.raises(RuntimeError, match="must keep its input length"):
            source.regrid(axes=[2], shape=[8, 8, 6], decimate=1)
    finally:
        source.done()


def test_regrid_forces_sampling_of_native_spectral_axis(casa):
    pixels = np.arange(8**3, dtype=np.float32).reshape(8, 8, 8)
    valid = np.ones_like(pixels, dtype=bool)
    valid[3, 3, 3] = False
    sampled = valid.copy()
    sampled[3, 3, 4] = False
    coords = casa.coordsys().newcoordsys(direction=True)
    try:
        sky = coords.torecord()
    finally:
        coords.done()
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=pixels)
            assert source.putregion(pixelmask=valid)
            for template in ({}, sky, _record(source)):
                for force, expected in ((False, valid), (True, sampled)):
                    result = source.regrid(csys=template, axes=[2], method="cubic",
                                           decimate=1, force=force)
                    try:
                        np.testing.assert_array_equal(result.getchunk(getmask=True), expected)
                        np.testing.assert_allclose(result.getchunk()[expected], pixels[expected])
                    finally:
                        result.done()
        finally:
            source.done()


def test_regrid_rejects_multiple_beams_on_selected_spectral_axis(casa):
    coords = casa.coordsys().newcoordsys(direction=True)
    try:
        sky = coords.torecord()
    finally:
        coords.done()
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=[8, 8, 8])
            for channel in range(8):
                assert source.setrestoringbeam(
                    major=f"{channel + 4}arcsec", minor="3arcsec", pa="0deg", channel=channel
                )
            for template in ({}, sky):
                with pytest.raises(RuntimeError, match="multiple beams"):
                    source.regrid(csys=template, axes=[2], decimate=1)
            assert source.isopen()
        finally:
            source.done()


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
            assert source.fromshape(shape=list(data.shape), type=kind)
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


@pytest.mark.parametrize("asvelocity,method", [(False, "linear"), (True, "linear"), (False, "lanczos")])
def test_regrid_spectral_interpolation_and_target_coordinates(casa, asvelocity, method):
    data = _ramp()
    results = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            target = _record(source, "spectral", [.5])
            result = source.regrid(csys=target, axes=[3], asvelocity=asvelocity, decimate=-1, method=method)
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
            same = _record(source)
            with pytest.raises(RuntimeError):
                source.regrid(region=region, decimate=1)
            products = [
                source.regrid(csys=same, region=region, decimate=1),
                source.regrid(csys=same, shape=[14, 18, 7], decimate=2, axes=[0, 1], dropdeg=True),
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
    actual, expected = results[1][0], results[0][0]
    selected = np.s_[3:15, 4:20, :, 1:6]
    valid = np.zeros(data.shape, dtype=bool)
    valid[selected] = True
    np.testing.assert_array_equal(actual[1], valid)
    assert expected[1][selected].all()
    np.testing.assert_allclose(actual[0][selected], data[selected], atol=2e-5)
    np.testing.assert_allclose(expected[0][selected], data[selected], atol=2e-5)
    # The spatial stencil must not create values outside the crop. An
    # extrapolated spectral bin can retain its nearest value while masked.
    assert not np.any(actual[0][:, :, 0, 1:6][~valid[:, :, 0, 1]])
    for actual, expected in zip(results[1][1:], results[0][1:]):
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
                {"csys": {"direction0": 5}},
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


def test_regrid_an_empty_target_leaves_the_input_grid_unchanged(casa):
    data = _ramp()
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            result = source.regrid(csys={"unused": 1}, shape=[-2, 3], force=True)
            try:
                np.testing.assert_allclose(result.getchunk(), data, atol=2e-7)
                assert result.maskhandler("get") == ["T"]
            finally:
                result.done()
        finally:
            source.done()


def test_regrid_converts_the_requested_sky_frame_and_honors_doref(casa):
    data = _ramp()
    results = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            coords = source.coordsys()
            try:
                assert coords.setreferencecode("GALACTIC", type="direction", adjust=True)
                target = coords.torecord()
            finally:
                coords.done()
            result = source.regrid(csys=target, axes=[0, 1], decimate=1)
            try:
                observed = result.coordsys()
                try:
                    assert observed.referencecode("direction") == ["GALACTIC"]
                finally:
                    observed.done()
                results.append(_read(result))
            finally:
                result.done()
            with pytest.raises(RuntimeError):
                source.regrid(csys=target, axes=[0, 1], decimate=1, doref=False)
        finally:
            source.done()
    _compare(results[1], results[0], tolerance=1e-3)


def test_regrid_stretches_expression_mask_on_the_input_grid(casa, tmp_path):
    data = _ramp()
    selected = np.broadcast_to(np.arange(data.shape[0])[:, None, None, None] >= 10, (*data.shape[:2], 1, 1))
    valid = np.broadcast_to(selected, data.shape)
    for api in (casa, tools):
        source = api.image()
        mask = api.image()
        path = tmp_path / f"{api.__name__}-regrid-mask.im"
        try:
            assert source.fromarray(pixels=data)
            assert mask.fromarray(str(path), pixels=selected.astype(np.float32))
            mask.done()
            expression = f'"{path}" > 0'
            with pytest.raises(RuntimeError):
                source.regrid(mask=expression)
            result = source.regrid(mask=expression, stretch=True)
            try:
                np.testing.assert_array_equal(result.getchunk(getmask=True), valid)
                np.testing.assert_array_equal(result.getchunk()[valid], data[valid])
            finally:
                result.done()
        finally:
            source.done()
            mask.done()


def test_regrid_preserves_blank_pixels_in_a_direct_fits_attachment(casa, tmp_path):
    data = _ramp()[:, :, 0, :].copy()
    data[7, 9, :] = np.nan
    path = str(tmp_path / "blank-regrid.fits")
    writer = tools.image()
    try:
        assert writer.fromarray(pixels=data)
        assert writer.tofits(outfile=path)
    finally:
        writer.done()
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.open(path)
            target = _record(source, "direction", [.2, .3])
            result = source.regrid(csys=target, axes=[0, 1], decimate=1, method="nearest")
            try:
                actual_mask = result.getchunk(getmask=True)
                np.testing.assert_array_equal(actual_mask, ~np.isnan(data))
                np.testing.assert_array_equal(result.getchunk()[actual_mask], data[actual_mask])
            finally:
                result.done()
        finally:
            source.done()


def test_regrid_on_transposed_axes_uses_the_mapped_sky_plane(casa):
    data = _ramp()
    expected = data.transpose(3, 1, 0, 2)
    results = []
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            mapped = source.transpose(order="3102")
            try:
                target = _record(mapped, "direction", [.2, .3])
                result = mapped.regrid(csys=target, axes=[2, 1], method="nearest", decimate=1)
                try:
                    results.append(_read(result))
                    np.testing.assert_array_equal(result.getchunk(), expected)
                finally:
                    result.done()
            finally:
                mapped.done()
        finally:
            source.done()
    _compare(results[1], results[0])


def test_regrid_refuses_a_position_velocity_grid(casa):
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=_ramp())
            strip = source.pv(start=[4, 7], end=[16, 7])
            try:
                with pytest.raises(RuntimeError, match="PV images are not supported"):
                    strip.regrid()
                assert strip.isopen()
            finally:
                strip.done()
        finally:
            source.done()


@pytest.mark.parametrize("method,replicate", [("lanczos", False), ("linear", True)])
def test_regrid_disjoint_sky_leaves_named_destination_intact(casa, tmp_path, method, replicate):
    data = np.full((32, 32), 7., dtype=np.float32)
    for api in (casa, tools):
        source = api.image()
        output = api.image()
        path = str(tmp_path / f"{api.__name__}-disjoint.im")
        try:
            assert source.fromarray(pixels=data)
            assert output.fromarray(path, pixels=data+1)
            output.done()
            target = _record(source, "direction", [40., 0.])
            with pytest.raises(RuntimeError, match="no overlap"):
                source.regrid(path, csys=target, axes=[0, 1], method=method, decimate=1,
                              replicate=replicate, overwrite=True)
            assert output.open(path)
            np.testing.assert_array_equal(output.getchunk(), data+1)
            target = _record(source, "direction", [4., 0.])
            overlap = source.regrid(csys=target, axes=[0, 1], method=method, decimate=1,
                                    replicate=replicate)
            try:
                assert overlap.getchunk(getmask=True)[15, 15]
                assert overlap.getchunk()[15, 15] == pytest.approx(7.)
                if method == "lanczos":
                    assert overlap.getchunk()[0, 15] == 0.
                    assert overlap.getchunk(getmask=True)[0, 15]
            finally:
                overlap.done()
        finally:
            output.done()
            source.done()


@pytest.mark.parametrize("doref", [False, True])
def test_regrid_resamples_when_only_the_direction_conversion_changes(casa, doref):
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=np.ones((32, 32), dtype=np.float32))
            coords = source.coordsys()
            try:
                coords.setconversiontype(direction="GALACTIC")
                target = coords.torecord()
            finally:
                coords.done()
            with pytest.raises(RuntimeError):
                source.regrid(csys=target, axes=[0, 1], decimate=1, doref=doref)
            assert source.isopen()
        finally:
            source.done()


@pytest.mark.parametrize("axes", [[-1], [0, 1, 2]])
def test_regrid_infers_the_shape_of_a_stokes_subset(casa, axes):
    x, y, pol, chan = np.ogrid[:32, :32, :4, :3]
    data = (x + 2*y + 100*pol + 1000*chan).astype(np.float32)
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            coords = source.coordsys()
            try:
                assert coords.setstokes("I Q")
                record = coords.torecord()
            finally:
                coords.done()
            for shape in ([-1], [32, 32, 2, 3]):
                result = source.regrid(csys=record, axes=axes, shape=shape, decimate=1)
                try:
                    np.testing.assert_array_equal(result.getchunk(), data[:, :, :2, :])
                    labels = result.coordsys()
                    try:
                        assert labels.stokes() == ["I", "Q"]
                    finally:
                        labels.done()
                finally:
                    result.done()
            with pytest.raises(RuntimeError):
                source.regrid(csys=record, axes=axes, shape=[32, 32, 4, 3], decimate=1)
        finally:
            source.done()


def test_regrid_shapes_only_selected_axes_and_coerces_numeric_indices(casa):
    data = np.arange(32*32, dtype=np.float32).reshape(32, 32)
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromarray(pixels=data)
            target = _record(source)
            for axes, shape in (([0], [32, 28]), ([1], [28, 32]), ([0, 1], [1, 1])):
                with pytest.raises(RuntimeError):
                    source.regrid(csys=target, axes=axes, shape=shape, decimate=1)
            cropped = source.regrid(csys=target, axes=[0], shape=[28, 32], decimate=1)
            accepted = source.regrid(csys=target, axes=np.asarray([0., 1.]),
                                     shape=[28.6, 30.9], decimate=1)
            try:
                np.testing.assert_allclose(cropped.getchunk(), data[:28], atol=2e-6)
                np.testing.assert_allclose(accepted.getchunk(), data[:28, :30], atol=2e-6)
            finally:
                cropped.done()
                accepted.done()
            with pytest.raises(TypeError):
                source.regrid(csys=None)
        finally:
            source.done()
