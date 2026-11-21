"""Source fitting and persisted Gaussian models checked against CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _gaussians(shape=(31, 34), components=((7., 14.2, 17.7, 6., 2.5),), offset=0.):
    x, y = np.ogrid[:shape[0], :shape[1]]
    return offset + sum(h * np.exp(-((x - cx)**2 / sx + (y - cy)**2 / sy))
                        for h, cx, cy, sx, sy in components)


def _component(result, index=0):
    assert result["converged"].dtype == np.bool_
    assert result["pixelsperarcsec"].dtype == np.float64
    return result["results"][f"component{index}"]


def _compare_fitted(actual, expected):
    assert actual["shape"]["type"] == expected["shape"]["type"] == "Gaussian"
    assert actual["shape"]["direction"]["refer"] == expected["shape"]["direction"]["refer"]
    assert actual["flux"]["unit"] == expected["flux"]["unit"]
    assert actual["peak"]["unit"] == expected["peak"]["unit"]
    assert actual["spectrum"] == expected["spectrum"]
    np.testing.assert_allclose(actual["pixelcoords"], expected["pixelcoords"], rtol=0., atol=0.002)
    np.testing.assert_allclose(actual["flux"]["value"], expected["flux"]["value"], rtol=.001, atol=2e-6)
    np.testing.assert_allclose(actual["flux"]["error"], expected["flux"]["error"], rtol=.002, atol=1e-6)
    assert actual["peak"]["value"] == pytest.approx(expected["peak"]["value"], rel=.001)
    assert actual["peak"]["error"] == pytest.approx(expected["peak"]["error"], rel=.002, abs=1e-6)
    for axis, unit in [("majoraxis", "arcsec"), ("minoraxis", "arcsec"), ("positionangle", "deg")]:
        assert actual["shape"][axis]["unit"] == expected["shape"][axis]["unit"] == unit
        assert actual["shape"][axis]["value"] == pytest.approx(expected["shape"][axis]["value"], rel=.001)
    for axis in ("m0", "m1"):
        actual_angle = actual["shape"]["direction"][axis]
        expected_angle = expected["shape"]["direction"][axis]
        assert actual_angle["unit"] == expected_angle["unit"] == "rad"
        assert actual_angle["value"] == pytest.approx(expected_angle["value"], abs=3e-7)
    assert actual["sum"]["unit"] == expected["sum"]["unit"]
    assert actual["sum"]["value"] == pytest.approx(expected["sum"]["value"], rel=.001)


@pytest.mark.parametrize("typecode", ["f", "d"])
def test_fitcomponents_gaussian_offset_and_uncertainty(casa, tmp_path, typecode):
    pixels = _gaussians(offset=.6)
    results = []
    for api in (casa, tools):
        attached = api.image()
        assert attached.fitcomponents() == {}
        assert attached.fromarray(pixels=pixels, type=typecode)
        if typecode == "f":
            assert attached.setbrightnessunit("Jy/pixel")
        try:
            estimates = tmp_path / f"{api.__name__}-refitted.txt"
            fit = attached.fitcomponents(dooff=True, offset=.4, rms="100mJy/pixel",
                                         noisefwhm=0.5, newestimates=str(estimates))
            x, y = map(float, estimates.read_text().split(",")[1:3])
            expected = [14.9998, 17.0002] if api is casa else [14.2, 17.7]
            np.testing.assert_allclose([x, y], expected, rtol=0., atol=.002)
            np.testing.assert_array_equal(fit["converged"], [True])
            assert fit["results"]["nelements"] == 1
            assert "deconvolved" not in fit
            assert fit["zerooff"]["unit"] == "Jy/pixel"
            assert fit["zerooff"]["value"][0] == pytest.approx(.6, abs=1e-5)
            assert fit["zeroofferr"]["value"][0] >= 0.
            component = _component(fit)
            np.testing.assert_allclose(component["pixelcoords"], [14.2, 17.7], atol=.002)
            assert component["peak"]["value"] == pytest.approx(7., rel=.001)
            expected_flux = 7. * np.pi * np.sqrt(6. * 2.5)
            assert component["flux"]["value"][0] == pytest.approx(expected_flux, rel=.001)
            np.testing.assert_allclose(attached.getchunk(), pixels, rtol=1e-6)
            results.append(component)
        finally:
            attached.done()
    _compare_fitted(results[1], results[0])


def test_fitcomponents_native_products_writer_reader_matrix(casa, tmp_path):
    pixels = np.stack([_gaussians(offset=.2),
                       _gaussians(components=((9., 14.2, 17.7, 6., 2.5),), offset=.5),
                       np.full((31, 34), 4.)], axis=-1).astype(np.float32)[:, :, None, :]
    mask = np.ones(pixels.shape, dtype=bool)
    mask[0, 2, :, :] = False
    makers = (casa, tools)
    fits = []
    for i, writer in enumerate(makers):
        path = str(tmp_path / f"source-{i}.im")
        source = writer.image()
        assert source.fromarray(path, pixels)
        assert source.setbrightnessunit("Jy/beam")
        assert source.setrestoringbeam(major="100arcsec", minor="70arcsec", pa="20deg")
        assert source.putregion(pixelmask=mask)
        assert source.done()
        source = writer.image()
        assert source.open(path)
        names = {product: str(tmp_path / f"{product}-{i}.im") for product in ("model", "residual", "complist")}
        existing = writer.image()
        assert existing.fromarray(names["model"], np.full((2, 2), 44., dtype=np.float32))
        assert existing.done()
        try:
            fit = source.fitcomponents(
                box="5,8,26,28", chans=1, dooff=True, offset=.5, fixoffset=True,
                rms=.2, **names,
            )
            np.testing.assert_array_equal(fit["converged"], [True])
            assert fit["zerooff"]["value"][0] == pytest.approx(.5)
            assert fit["zeroofferr"]["value"][0] == 0.
            assert fit["results"]["nelements"] == 1
            component = _component(fit)
            assert component["spectrum"]["channel"] == 1
            assert component["beam"]["beamarcsec"]["major"]["value"] == pytest.approx(100.)
            assert component["ispoint"] is False
            assert fit["deconvolved"]["component0"]["shape"]["majoraxis"]["value"] < component["shape"]["majoraxis"]["value"]
            fits.append(component)
        finally:
            source.done()
        for reader in makers:
            for name in ("model", "residual"):
                product = reader.image()
                assert product.open(names[name])
                try:
                    assert product.pixeltype() == "float"
                    assert product.brightnessunit() == "Jy/beam"
                    assert product.shape().tolist() == [31, 34, 1, 3]
                    np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
                    assert list(product.maskhandler("default")) == ["mask0"]
                    assert "ia.fitcomponents(" in product.history()[-1]
                    output = product.getchunk()
                    np.testing.assert_array_equal(output[:, :, :, [0, 2]], 0.)
                    expected = pixels[:, :, 0, 1] if name == "model" else 0.
                    np.testing.assert_allclose(output[:, :, 0, 1], expected, atol=2e-5)
                finally:
                    product.done()
            componentlist = reader.componentlist()
            try:
                assert componentlist.open(names["complist"])
                assert componentlist.length() == 1
                np.testing.assert_allclose(componentlist.getfluxvalue(0), fit["results"]["component0"]["flux"]["value"], rtol=.001)
            finally:
                componentlist.done()
    _compare_fitted(fits[1], fits[0])


def test_fitcomponents_estimate_files_fixed_parameters_and_reports(casa, tmp_path):
    terms = ((7., 14.2, 17.7, 6., 2.5), (4., 30.1, 24.8, 8., 3.))
    pixels = _gaussians((42, 39), terms).astype(np.float32)
    estimate = tmp_path / "estimate.txt"
    estimate.write_text(
        "# peak and position estimates\n"
        "6, 14.2, 17.7, 245arcsec, 158arcsec, 90deg, xy\n"
        "3, 30.1, 24.8, 280arcsec, 174arcsec, 90deg, xy\n"
    )
    records = []
    for i, api in enumerate((casa, tools)):
        attached = api.image()
        assert attached.fromarray(pixels=pixels)
        logfile = tmp_path / f"fit-{i}.log"
        summary = tmp_path / f"fit-{i}.summary"
        newestimates = tmp_path / f"fit-{i}.estimates"
        logfile.write_text("existing line\n")
        try:
            result = attached.fitcomponents(estimates=str(estimate), logfile=str(logfile),
                summary=str(summary), newestimates=str(newestimates), rms=.1)
            assert result["results"]["nelements"] == 2
            measurements = [_component(result, index) for index in range(2)]
            for component, (peak, x, y, *_rest) in zip(measurements, terms):
                assert component["peak"]["value"] == pytest.approx(peak, rel=.001)
                np.testing.assert_allclose(component["pixelcoords"], [x, y], atol=.002)
                direction_errors = component["shape"]["direction"]["error"]
                assert direction_errors["longitude"]["value"] == 0.
                assert direction_errors["latitude"]["value"] == 0.
            log = logfile.read_text()
            assert log.startswith("existing line\n")
            assert "Details of fit for channel number 0" in log
            assert "Number of pixels used in fit: 1638" in log
            assert "RMS of residual image" in log
            assert "ConMaj" in summary.read_text()
            assert "PeakErr" in summary.read_text()
            assert len(newestimates.read_text().splitlines()) == 2
            second = attached.fitcomponents(estimates=str(estimate), logfile=str(logfile), append=False)
            assert second["results"]["nelements"] == 2
            assert "existing line" not in logfile.read_text()
            records.append(measurements)
        finally:
            attached.done()
    for actual, expected in zip(records[1], records[0]):
        _compare_fitted(actual, expected)


def test_fitcomponents_rejects_bad_selection_and_estimates(casa, tmp_path):
    source_data = np.broadcast_to(_gaussians().astype(np.float32)[..., None, None],
                                  (31, 34, 1, 3)).copy()
    bad = tmp_path / "bad-estimate.txt"
    bad.write_text("not a comma-separated estimate\n")
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=source_data)
        try:
            with pytest.raises(RuntimeError, match="single contiguous channel range"):
                source.fitcomponents(chans="0,2")
            with pytest.raises(RuntimeError, match="both an include and an exclude"):
                source.fitcomponents(includepix=[0., 9.], excludepix=[2., 4.])
            with pytest.raises(RuntimeError, match="(?i)estimate|bad format"):
                source.fitcomponents(estimates=str(bad))
            np.testing.assert_array_equal(source.getchunk(), source_data)
        finally:
            source.done()


@pytest.mark.parametrize("stokes, index", [("Q", 1), ("U", 2)])
def test_fitcomponents_channel_and_polarization_with_dimensionless_rms(casa, tmp_path, stokes, index):
    base = _gaussians().astype(np.float32)
    polarization = np.arange(1., 4.)[:, None]
    channels = np.arange(1., 3.)[None, :]
    pixels = (base[..., None, None] * polarization * channels).astype(np.float32)
    results = []
    for api in (casa, tools):
        image = api.image()
        assert image.fromarray(pixels=pixels)
        try:
            summary = tmp_path / f"{api.__name__}-spectral.txt"
            model = str(tmp_path / f"{api.__name__}-{stokes}-model.im")
            fit = image.fitcomponents(chans=1, stokes=stokes, rms="0.1", noisefwhm="120arcsec",
                                      summary=str(summary), model=model)
            product = api.image()
            assert product.open(model)
            try:
                maxima = product.getchunk()[:, :, :, 1].max(axis=(0, 1))
                assert np.count_nonzero(maxima) == 1
                selected = 0 if api is casa else index
                assert maxima[selected] == pytest.approx(
                    pixels[:, :, index, 1].max(), abs=2e-5)
            finally:
                product.done()
            np.testing.assert_array_equal(fit["converged"], [True])
            c = _component(fit)
            table = summary.read_text().splitlines()
            assert table[0].split()[-1] == "GHz"
            record_frequency = c["spectrum"]["frequency"]["m0"]["value"]
            assert float(table[-1].split()[-1]) == pytest.approx(
                record_frequency * (1e9 if api is casa else 1.))
            flux = c["flux"]["value"]
            assert flux[index] == pytest.approx((index + 1) * 2. * 7. * np.pi * np.sqrt(6. * 2.5), rel=.001)
            assert np.count_nonzero(flux) == 1
            with pytest.raises(RuntimeError, match="Stokes|Flux integral"):
                image.fitcomponents(chans="1", stokes="IQ")
            results.append(c)
        finally:
            image.done()
    _compare_fitted(results[1], results[0])


def test_fitcomponents_excludes_masked_samples_from_residual_uncertainty(casa, tmp_path):
    pixels = _gaussians().astype(np.float32)
    pixels[18, 19] = 100.
    valid = np.ones(pixels.shape, dtype=bool)
    valid[18, 19] = False
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=pixels)
        assert source.setbrightnessunit("Jy/pixel")
        assert source.putregion(pixelmask=valid)
        try:
            fit = source.fitcomponents()
            output = str(tmp_path / f"{api.__name__}-masked.im")
            with_residual = source.fitcomponents(residual=output)
            plain_error = _component(fit)["peak"]["error"]
            written_error = _component(with_residual)["peak"]["error"]
            assert written_error < 1e-6
            if api is casa:
                assert plain_error > 1.
            else:
                assert plain_error == pytest.approx(written_error, abs=1e-9)
        finally:
            source.done()


def test_fitcomponents_labels_galactic_fit_directions(casa):
    values = _gaussians().astype(np.float32)
    fixture = casa.image()
    assert fixture.fromarray(pixels=values)
    cs = fixture.coordsys()
    assert cs.setconversiontype(direction="GALACTIC")
    record = cs.torecord()
    cs.done()
    fixture.done()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, csys=record)
        try:
            component = _component(source.fitcomponents())
            direction = component["shape"]["direction"]
            assert direction["refer"] == ("J2000" if api is casa else "GALACTIC")
            np.testing.assert_allclose(
                [direction["m0"]["value"], direction["m1"]["value"]],
                [1.68199966643, -1.0503952928], rtol=0., atol=7e-8,
            )
        finally:
            source.done()


def test_fitcomponents_uses_the_beam_visible_to_its_attachment(casa, tmp_path):
    pixels = _gaussians().astype(np.float32)
    expected = 7. * 60.**2 * 4. * np.log(2.) * np.sqrt(6. * 2.5) / (70. * 60.)
    for api in (casa, tools):
        path = str(tmp_path / f"{api.__name__}-beam-view.im")
        model = str(tmp_path / f"{api.__name__}-beam-model.im")
        first = api.image()
        second = api.image()
        output = api.image()
        assert first.fromarray(path, pixels)
        assert first.setbrightnessunit("Jy/beam")
        assert first.setrestoringbeam(major="70arcsec", minor="60arcsec", pa="0deg")
        assert second.open(path)
        try:
            assert second.setrestoringbeam(major="100arcsec", minor="80arcsec", pa="0deg")
            assert first.restoringbeam()["major"]["value"] == pytest.approx(70.)
            record = _component(first.fitcomponents(rms=.1, model=model))
            assert record["beam"]["beamarcsec"]["major"]["value"] == pytest.approx(70.)
            assert record["beam"]["beamarcsec"]["minor"]["value"] == pytest.approx(60.)
            assert record["flux"]["value"][0] == pytest.approx(expected, rel=.001)
            assert output.open(model)
            assert output.restoringbeam()["major"]["value"] == pytest.approx(70.)
        finally:
            output.done()
            second.done()
            first.done()


def test_fitcomponents_rejects_nested_artifact_paths_before_publication(tmp_path):
    source_path = tmp_path / "sky.im"
    model = tmp_path / "model.im"
    image = tools.image()
    pixels = _gaussians().astype(np.float32)
    assert image.fromarray(str(source_path), pixels)
    try:
        with pytest.raises(RuntimeError, match="overlap"):
            image.fitcomponents(model=str(model), summary=str(model / "table.dat"))
        assert not model.exists()
        with pytest.raises(RuntimeError, match="overlap"):
            image.fitcomponents(summary=str(source_path / "table.dat"))
        np.testing.assert_array_equal(image.getchunk(), pixels)
        reopened = tools.image()
        assert reopened.open(str(source_path))
        try:
            np.testing.assert_array_equal(reopened.getchunk(), pixels)
        finally:
            reopened.done()
        alias = tmp_path / "alias"
        alias.symlink_to(tmp_path, target_is_directory=True)
        with pytest.raises(RuntimeError, match="overlap"):
            image.fitcomponents(model=str(model), logfile=str(alias / "model.im" / "fit.log"))
        assert not model.exists()
    finally:
        image.done()


def test_fitcomponents_requires_a_plane_on_ancillary_axes(casa, tmp_path):
    base = _gaussians().astype(np.float32)
    values = np.stack([base, 4. * base], axis=-1)[:, :, None, None, :]
    regions = casa.regionmanager()
    region = regions.box([0, 0, 0, 0, 1], [30, 33, 0, 0, 1])
    regions.done()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            for selected in (False, True):
                path = str(tmp_path / f"{api.__name__}-{selected}.im")
                options = {"region": region} if selected else {}
                result = source.fitcomponents(model=path, **options)
                np.testing.assert_array_equal(result["converged"], [selected])
                assert result["results"]["nelements"] == int(selected)
                model = api.image()
                assert model.open(path)
                try:
                    pixels = model.getchunk()
                    if selected:
                        axis = 0 if api is casa else 1
                        np.testing.assert_allclose(pixels[..., axis], values[..., 1], atol=2e-5)
                        np.testing.assert_array_equal(pixels[..., 1-axis], 0.)
                    else:
                        np.testing.assert_array_equal(pixels, 0.)
                finally:
                    model.done()
        finally:
            source.done()


def test_fitcomponents_default_beam_sets_the_noise_correlation(casa):
    values = _gaussians().astype(np.float32)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        assert source.setbrightnessunit("Jy/beam")
        assert source.setrestoringbeam(major="30arcsec", minor="24arcsec", pa="0deg")
        try:
            default = _component(source.fitcomponents(rms=.1))
            explicit = _component(source.fitcomponents(rms=.1, noisefwhm=.5))
            assert default["peak"]["error"] == pytest.approx(.03744868, rel=.001)
            assert explicit["peak"]["error"] == pytest.approx(.05733663, rel=.001)
        finally:
            source.done()


def test_fitcomponents_sum_uses_the_reported_angular_ellipse(casa):
    x, y = np.ogrid[:31, :34]
    dx, dy = x - 14.2, y - 17.7
    values = (7. * np.exp(-((.8*dx+.6*dy)**2/14. + (-.6*dx+.8*dy)**2/2.5))).astype(np.float32)
    fixture = casa.image()
    assert fixture.fromarray(pixels=values)
    coords = fixture.coordsys()
    increments = coords.increment()["numeric"].copy()
    increments[0] *= 3.
    assert coords.setincrement(value=increments)
    record = coords.torecord()
    coords.done()
    fixture.done()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, csys=record)
        assert source.setbrightnessunit("Jy/pixel")
        try:
            component = _component(source.fitcomponents(rms=.1))
            shape = component["shape"]
            angle = np.deg2rad(shape["positionangle"]["value"])
            major = shape["majoraxis"]["value"] / (2. * 180.)
            minor = shape["minoraxis"]["value"] / (2. * 60.)
            dx, dy = x - component["pixelcoords"][0], y - component["pixelcoords"][1]
            u = (-dx*np.sin(angle) + dy*np.cos(angle)) / major
            v = (dx*np.cos(angle) + dy*np.sin(angle)) / minor
            expected = np.sum(values[u*u+v*v <= 1.], dtype=float)
            assert component["sum"]["value"] == pytest.approx(expected, abs=1e-6)
            assert expected == pytest.approx(78.6244931, rel=.001)
        finally:
            source.done()
