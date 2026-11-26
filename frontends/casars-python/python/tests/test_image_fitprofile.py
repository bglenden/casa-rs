"""Spectral fitting and native image products against CASA 6.7.6.14."""

import os
import re
import subprocess
import sys

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _line(x, amplitude, center, width, kind="g"):
    distance = (x - center) / width
    if kind == "l":
        return amplitude / (1 + 4 * distance ** 2)
    return amplitude * np.exp(-4 * np.log(2) * distance ** 2)


def _cube(data, shape=(2, 2)):
    return np.broadcast_to(data, (*shape, len(data))).astype(np.float32, copy=True)


def _fit(api, data, csys=None, **arguments):
    im = api.image()
    try:
        assert im.fromarray(pixels=data, **({} if csys is None else {"csys": csys}))
        return im.fitprofile(multifit=True, logresults=False, **arguments)
    finally:
        im.done()


def _read(api, path):
    im = api.image()
    try:
        assert im.open(str(path))
        return (im.getchunk(), im.getchunk(getmask=True), im.brightnessunit(),
                im.toworld([0] * len(im.shape()))["numeric"], im.history())
    finally:
        im.done()


def _compare_records(native, expected, *, compare_direction=True):
    assert native.keys() == expected.keys()
    for name in ("attempted", "succeeded", "converged", "valid"):
        assert native[name].dtype == expected[name].dtype == np.bool_
        np.testing.assert_array_equal(native[name], expected[name])
    for name in ("ncomps", "niter"):
        assert native[name].dtype == expected[name].dtype == np.int64
        assert native[name].shape == expected[name].shape
    np.testing.assert_array_equal(native["ncomps"], expected["ncomps"])
    np.testing.assert_array_equal(native["type"], expected["type"])
    if compare_direction and "direction" in expected:
        assert native["direction"].dtype.kind == "U"
        np.testing.assert_array_equal(native["direction"], expected["direction"])
    for name in ("xUnit", "yUnit"):
        assert native[name] == expected[name]
    for name, model in expected.items():
        if isinstance(model, dict):
            assert native[name].keys() == model.keys()
            for key, value in model.items():
                if isinstance(value, str):
                    assert native[name][key] == value
                else:
                    assert native[name][key].dtype == value.dtype == np.float64
                    np.testing.assert_allclose(native[name][key], value, rtol=.002, atol=4e-7)


@pytest.mark.parametrize("kind,form", [
    ("g", "scalar"), ("g", "tuple"), ("g", "automatic"), ("g", "file"), ("l", "scalar"),
])
def test_line_models_and_estimate_forms(casa, tmp_path, kind, form):
    x = np.arange(45.)
    h, c, w = 6.7, 22.4, 4.9
    noise = .011 * np.sin(x * 2.)
    data = _cube(_line(x, h, c, w, kind) + .28 - .004 * x + noise)
    kwargs = {"poly": 1}
    if form == "scalar":
        kwargs.update(pampest=5.7, pcenterest=23.1, pfwhmest=4., pfunc=kind)
    elif form == "tuple":
        kwargs.update(pampest=(5.7,), pcenterest=(23.1,), pfwhmest=(4.,), pfunc=["Gu"])
    elif form == "file":
        estimates = tmp_path / "estimates.txt"
        estimates.write_text("# A slightly perturbed peak\n5.9,23.1,5.2\n")
        kwargs["estimates"] = str(estimates)
    expected = _fit(casa, data, **kwargs)
    native = _fit(tools, data, **kwargs)
    _compare_records(native, expected)
    model = native[f"{kind}s"]
    increment = 299792.458 * 1000 / 1420405751.786
    assert model["amp"][0, 0, 0, 0] == pytest.approx(h, rel=.002)
    assert model["fwhm"][0, 0, 0, 0] == pytest.approx(increment * w, rel=.002)
    assert model["ampErr"][0, 0, 0, 0] > 0
    purpose = np.sqrt(np.pi/(4*np.log(2))) if kind == "g" else np.pi / 2
    assert model["integral"][0, 0, 0, 0] == pytest.approx(h * w * increment * purpose, rel=.004)


def test_multiplet_ties_and_fixed_parameters(casa, tmp_path):
    x = np.arange(51.)
    data = _cube(_line(x, 7., 14.3, 5.) + _line(x, 2.2, 30., 3., "l")
                 + _line(x, 6., 21., 2.) + _line(x, 3., 37., 3.) + .24)
    arguments = dict(
        pampest=[6., 2.], pcenterest=[14., 29.], pfwhmest=[5., 2.8],
        pfix=["f", ""], pfunc=["G", "L"], gmncomps=2,
        gmampest=[5., 2.5], gmcenterest=[21.1, 37.1], gmfwhmest=[2.2, 3.3],
        gmampcon=.5, gmcentercon=16., gmfwhmcon=1.5, poly=0, box="0,0,0,0",
    )
    reference = _fit(casa, data, **arguments)
    native = _fit(tools, data, **arguments, amp=str(tmp_path / "amp"))
    _compare_records(native, reference)
    np.testing.assert_array_equal(
        native["type"][0, 0, 0], ["GAUSSIAN MULTIPLET", "GAUSSIAN", "LORENTZIAN", "POLYNOMIAL"],
    )
    assert native["gs"]["fwhmErr"].item() == 0.
    gm = native["gm0"]
    np.testing.assert_allclose(gm["amp"].ravel(), [6., 3.], atol=2e-6)
    assert gm["centerErr"].ravel()[0] == gm["centerErr"].ravel()[1]
    assert gm["fwhm"].ravel()[1] == pytest.approx(1.5 * gm["fwhm"].ravel()[0])
    for name, value in [("amp_0", 7.), ("amp_ls_0", 2.2), ("amp_gm_0", 6.), ("amp_gm_1", 3.)]:
        for reader in (casa, tools):
            record = _read(reader, tmp_path / name)
            assert record[0].item() == pytest.approx(value, abs=1e-6)


def test_native_product_matrix_and_minimum_masked_points(casa, tmp_path):
    pixels = _cube(2. + .5 * np.arange(11.), (3, 2))
    mask = np.ones(pixels.shape, dtype=bool)
    mask[0, 0, 4:] = False
    mask[1, 0, :] = False
    mask[0, 1, 1] = False
    attempts = mask.sum(axis=-1, keepdims=True) >= 9
    expected_residual = np.where(attempts, 0., pixels)
    for writer in (casa, tools):
        name = writer.__name__
        source = writer.image()
        path = tmp_path / f"{name}.im"
        assert source.fromarray(str(path), pixels)
        assert source.setbrightnessunit("K")
        assert source.setrestoringbeam(major="5arcsec", minor="3arcsec", pa="14deg")
        assert source.setmiscinfo({"provenance": 58})
        assert source.putregion(pixelmask=mask)
        history = source.history()
        outputs = {k: str(tmp_path / f"{name}-{k}.im") for k in ("model", "residual", "outsigma")}
        fit = source.fitprofile(
            multifit=True, ngauss=0, poly=1, minpts=9, logresults=False,
            sigma=np.where(np.arange(11) == 3, 0., 2.), planes=list(range(8)), **outputs,
        )
        np.testing.assert_array_equal(fit["attempted"], attempts)
        np.testing.assert_array_equal(fit["valid"], attempts)
        np.testing.assert_array_equal(source.getchunk(), pixels)
        np.testing.assert_array_equal(source.getchunk(getmask=True), mask)
        assert source.history() == history
        assert source.done()
        for reader in (casa, tools):
            for label, expected in [("model", pixels), ("residual", expected_residual)]:
                product = reader.image()
                assert product.open(outputs[label])
                try:
                    assert product.brightnessunit() == "K"
                    assert product.miscinfo()["provenance"] == 58
                    assert product.restoringbeam()["major"]["value"] == pytest.approx(5.)
                    np.testing.assert_allclose(product.getchunk(), expected, atol=3e-6)
                    np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
                    assert product.history()[-1] == "Number valid             = 4"
                finally:
                    product.done()
            data, valid, unit, _, history = _read(reader, outputs["outsigma"])
            assert data.shape == pixels.shape
            assert unit == ""
            assert valid.all()
            np.testing.assert_array_equal(data[2, 0], np.where(np.arange(11) == 3, 0., 1.))
            assert history[-1] == "Number valid             = 4"


@pytest.mark.parametrize("first,last", [(2, 22), (4, 4)])
def test_profile_solution_images_use_the_collapsed_spectral_grid(casa, tmp_path, first, last):
    if first == last:
        pixels = np.full((3, 2, 31), 7., dtype=np.float32)
        arguments = dict(chans=str(first), pampest=7., pcenterest=0., pfwhmest=4., pfix="pcf")
    else:
        pixels = _cube(_line(np.arange(31.), 7., 13., 4.7), (3, 2))
        arguments = dict(chans=f"{first}~{last}", pampest=7., pcenterest=11., pfwhmest=4.7)
    for writer in (casa, tools):
        source = writer.image()
        name = str(tmp_path / f"{writer.__name__}-amp")
        try:
            assert source.fromarray(pixels=pixels)
            start = source.toworld([0, 0, first])["numeric"][2]
            end = source.toworld([2, 1, last])["numeric"][2]
            result = source.fitprofile(
                axis=2, multifit=True, logresults=False, amp=name, **arguments,
            )
            assert result["valid"].all()
        finally:
            source.done()

        for reader in (casa, tools):
            product = reader.image()
            try:
                assert product.open(name + "_0")
                coords = product.coordsys()
                try:
                    assert coords.referencepixel()["numeric"][2] == 0
                    assert coords.referencevalue()["numeric"][2] == (start + end) / 2
                finally:
                    coords.done()
                assert product.shape().tolist() == [3, 2, 1]
                values = product.getchunk()
                assert values.dtype == np.float64
                np.testing.assert_allclose(values, 7., atol=2e-6)
                assert product.getchunk(getmask=True).all()
            finally:
                product.done()


@pytest.mark.parametrize("form", ["vector", "array", "native"])
def test_sigma_inverse_variance(casa, tmp_path, form):
    n = 51
    weights = np.where(np.arange(n)==0,10.,1.)
    values = np.where(np.arange(n)==0,11.,1.)
    if form == "native":
        sigma = tmp_path / "sigma.im"
        maker = casa.image()
        assert maker.fromarray(str(sigma), _cube(weights))
        assert maker.done()
        sigma = str(sigma)
    elif form == "array":
        sigma = _cube(weights)
    else:
        sigma = weights.tolist()
    for api in (casa, tools):
        path = tmp_path / f"{api.__name__}.model"
        result = _fit(api, _cube(values), ngauss=0, poly=0, sigma=sigma, model=str(path))
        assert result["valid"].all()
        model, _, _, _, _ = _read(tools, path)
        expected = np.dot(values, 1 / weights**2) / np.sum(1 / weights**2)
        np.testing.assert_allclose(model, expected, atol=3e-7)


def test_constant_polynomial_produces_zero_native_residuals(casa, tmp_path):
    data = _cube(np.ones(128), shape=(3, 4))
    for writer in (casa, tools):
        model = tmp_path / f"{writer.__name__}.model"
        residual = tmp_path / f"{writer.__name__}.residual"
        fit = _fit(writer, data, ngauss=0, poly=0, axis=2, model=str(model), residual=str(residual))
        assert fit["valid"].all()
        for reader in (casa, tools):
            model_pixels, model_mask, *_ = _read(reader, model)
            residual_pixels, residual_mask, *_ = _read(reader, residual)
            np.testing.assert_array_equal(model_pixels, data)
            np.testing.assert_array_equal(residual_pixels, np.zeros_like(data))
            assert model_mask.all() and residual_mask.all()


def _freq_cube(api):
    frequencies = 1.3e9 + np.arange(41) * 15e6
    x = frequencies / 1e9
    cube = (7.5 * x ** (-.61 + .19 * np.log(x))).reshape(1, 1, -1)
    coords = api.coordsys()
    assert coords.newcoordsys(direction=True, spectral=True)
    assert coords.setreferencepixel([0, 0, 0])
    assert coords.setreferencevalue(value=[0., 0., frequencies[0]])
    assert coords.setincrement(value=[-1e-4, 1e-4, 15e6])
    record = coords.torecord()
    assert coords.done()
    return cube, record


@pytest.mark.parametrize("kind,divisor", [
    ("plp", []), ("plp", 1e9), ("PLP", "1GHz"), ("ltp", 1e9),
    ("ltp", {"value": 1.0, "unit": "GHz"}),
])
def test_spectral_polynomial_coefficients_and_native_products(casa, tmp_path, kind, divisor):
    results = []
    for api in (casa, tools):
        cube, csys = _freq_cube(api)
        prefix = tmp_path / api.__name__
        model = str(prefix) + ".model"
        residual = str(prefix) + ".residual"
        solution = str(prefix) + ".coeff"
        error = str(prefix) + ".err"
        result = _fit(
            api, cube, csys, spxtype=kind, spxest=[4.5, -.61, .1],
            spxfix=[False, True, False], div=divisor,
            model=model, residual=residual, spxsol=solution, spxerr=error,
        )
        results.append(result)
        is_log = kind == "ltp"
        expected = [np.log(7.5) if is_log else 7.5, -.61, .19]
        fit = result[kind.lower()]
        np.testing.assert_allclose(fit["solution"].ravel(), expected, atol=6e-7)
        assert fit["error"].ravel()[1] == 0.
        for reader in (casa, tools):
            saved, mask, _, axis, _ = _read(reader, model)
            np.testing.assert_allclose(saved, np.log(cube) if is_log else cube, atol=8e-7)
            assert mask.all()
            assert axis[-1] == pytest.approx(1.3e9)
            np.testing.assert_allclose(_read(reader, residual)[0], 0., atol=6e-7)
            for index, value in enumerate(expected):
                pixels, mask, unit, world, _ = _read(reader, f"{solution}_{index}")
                assert pixels.item() == pytest.approx(value, abs=6e-7)
                assert mask.item()
                assert unit == ""
                assert world[-1] == pytest.approx(1.6e9)
                uncertainty = _read(reader, f"{error}_{index}")[0].item()
                assert uncertainty == pytest.approx(fit["error"].ravel()[index], rel=1e-6)
    _compare_records(*results[::-1])


def test_fully_fixed_log_spectrum_is_evaluated(tmp_path):
    cube, csys = _freq_cube(tools)
    model = tmp_path / "fixed.model"
    fixed = [np.log(7.5), -.61, .19]
    result = _fit(tools, cube, csys, spxtype="ltp", spxest=fixed, spxfix=[True] * 3,
                  div="1GHz", model=str(model))
    assert result["valid"].item()
    assert result["niter"].item() == 0
    np.testing.assert_array_equal(result["ltp"]["solution"].ravel(), fixed)
    np.testing.assert_array_equal(result["ltp"]["error"], 0.)
    np.testing.assert_allclose(_read(tools, model)[0], np.log(cube), atol=1.2e-7)


def test_collapsed_profile_uses_selected_world_center_and_skips_solution_images(tmp_path):
    image = tools.image()
    x = np.arange(31.)
    pixels = _cube(_line(x, 7., 15., 5.), (4, 6))
    assert image.fromarray(pixels=pixels)
    model, amp = tmp_path / "center.model", tmp_path / "ignored.amp"
    world = image.toworld([1.5, 2.5, 0])["numeric"]
    try:
        result = image.fitprofile(pampest=6., pcenterest=15., pfwhmest=5.,
                                  model=str(model), amp=str(amp))
        assert result["valid"].item()
        assert not (tmp_path / "ignored.amp_0").exists()
        data, mask, _, coordinate, _ = _read(tools, model)
        np.testing.assert_allclose(data, pixels[:1, :1], atol=6e-7)
        assert mask.all()
        np.testing.assert_allclose(coordinate, world, atol=2e-9)
    finally:
        image.done()


def test_explicit_nonspectral_axis_and_spectral_selection(casa, tmp_path):
    grid = np.arange(29.).reshape(-1, 1, 1, 1)
    data = np.broadcast_to(_line(grid, 8., 13.5, 5.1), (29, 4, 2, 6)).astype(np.float32)
    arguments = dict(
        axis=0, box="3,1,25,2", chans="2~4", stokes="Q",
        multifit=True, pampest=7., pcenterest=10.5, pfwhmest=4., logresults=False,
    )
    oracle = casa.image()
    assert oracle.fromarray(pixels=data)
    try:
        with pytest.raises(RuntimeError, match="X vector"):
            oracle.fitprofile(**arguments)
    finally:
        oracle.done()
    im = tools.image()
    assert im.fromarray(pixels=data)
    target = tmp_path / "explicit.mod"
    try:
        fit = im.fitprofile(model=str(target), **arguments)
        assert fit["valid"].shape == (1, 2, 1, 3)
        assert fit["valid"].all()
        assert "direction" not in fit
        assert fit["xUnit"] == "rad"
        # The input's default direction coordinate has one arcminute pixels.
        increment = np.deg2rad(1. / 60.)
        np.testing.assert_allclose(fit["gs"]["amp"], 8., atol=2e-6)
        np.testing.assert_allclose(fit["gs"]["fwhm"], 5.1 * increment, atol=1e-8)
    finally:
        im.done()
    for reader in (casa, tools):
        product, valid, _, _, _ = _read(reader, target)
        np.testing.assert_allclose(product, data[3:26, 1:3, 1:2, 2:5], atol=5e-7)
        assert valid.all()


def test_user_region_and_expression_mask(casa, tmp_path):
    x = np.arange(31.)
    expected = _line(x, 3.5, 12.1, 6.4)
    source = _cube(expected, (3, 3))
    selected = source[:2, :2, 4:28]
    records = []
    region = casa.regionmanager()
    region_record = region.box(blc=[0, 0, 4], trc=[1, 1, 27])
    region.done()
    for api in (casa, tools):
        prefix = tmp_path / api.__name__
        maker = api.image()
        assert maker.fromarray(str(prefix), source)
        target = str(prefix) + ".model"
        try:
            fit = maker.fitprofile(
                region=region_record, mask=f'"{prefix}">0.001',
                multifit=True, pampest=3., pcenterest=8., pfwhmest=6.,
                model=target, logresults=False,
            )
            records.append(fit)
        finally:
            maker.done()
        for reader in (casa, tools):
            actual, valid, _, _, _ = _read(reader, target)
            np.testing.assert_allclose(actual, selected, atol=5e-7)
            np.testing.assert_array_equal(valid, selected > .001)
    _compare_records(*records[::-1])


def test_fit_range_rejection_and_unseeded_profile(casa, tmp_path):
    x = np.arange(27.)
    data = _cube(_line(x, 5., 14., 4.5))
    for api in (casa, tools):
        prefix = tmp_path / api.__name__
        peak = str(prefix) + ".amp"
        rejected = _fit(api, data, pampest=5., pcenterest=14., pfwhmest=4., goodamprange=[4., 3.],
                        amp=peak)
        assert rejected["attempted"].all() and rejected["converged"].all()
        assert not rejected["valid"].any()
        np.testing.assert_array_equal(rejected["ncomps"], -1)
        np.testing.assert_array_equal(rejected["type"], "UNDEF")
        pixels, mask, *_ = _read(tools, peak + "_0")
        assert np.isnan(pixels).all()
        assert not mask.any()
        empty = _fit(api, _cube(np.zeros_like(x)), amp=str(prefix) + ".unused")
        assert not empty["attempted"].any()
        assert not empty["valid"].any()
        assert not (tmp_path / (api.__name__ + ".unused_0")).exists()


def test_fit_log_append_and_replace(casa, tmp_path):
    for api in (casa, tools):
        logfile = tmp_path / f"{api.__name__}.log"
        logfile.write_text("previous fit\n")
        common = dict(chans="2~22", pampest=5., pcenterest=12., pfwhmest=4., logfile=str(logfile))
        data = _cube(_line(np.arange(27.), 5., 14., 4.5))
        assert _fit(api, data, **common)["valid"].all()
        first = logfile.read_text()
        assert first.startswith("previous fit")
        assert "Number valid" in first
        assert "Fit" in first
        assert all(field in first for field in ("Peak", "Center", "FWHM", "Integral"))
        pixel_values = _logged_pixel_values(first)
        assert len(pixel_values) >= 2
        np.testing.assert_allclose(pixel_values[:2], [14., 4.5], atol=1e-6)
        assert _fit(api, data, append=False, **common)["valid"].all()
        assert "previous fit" not in logfile.read_text()


def _logged_pixel_values(report):
    return [float(value) for value in re.findall(
        r"^\s+([-+\d.eE]+)\s+\+/-\s+[-+\d.eE]+\s+pixel\s*$", report, re.MULTILINE
    )]


def test_fit_log_uses_local_optical_velocity_scale(casa, tmp_path):
    data = _cube(_line(np.arange(51.), 5., 30., 4.))
    reports = []
    for api in (casa, tools):
        source = api.image()
        logfile = tmp_path / f"{api.__name__}.log"
        try:
            assert source.fromarray(pixels=data)
            coords = source.coordsys()
            try:
                record = coords.torecord()
            finally:
                coords.done()
            spectral = next(value for key, value in record.items() if key.startswith("spectral"))
            spectral["velType"] = 1
            spectral["wcs"]["cdelt"] = 1e7
            assert source.setcoordsys(record)
            result = source.fitprofile(
                axis=2, multifit=True, logresults=False, logfile=str(logfile),
                pampest=5., pcenterest=30., pfwhmest=4.,
            )
            assert result["valid"].all()
            np.testing.assert_allclose(result["gs"]["fwhm"], 12443.12100631, rtol=1e-7)
        finally:
            source.done()
        reports.append(_logged_pixel_values(logfile.read_text())[:2])
    np.testing.assert_allclose(reports, [[30., 6.3143091955]] * 2, rtol=1e-7)


def test_fit_log_preserves_multiplet_grouping(casa, tmp_path):
    x = np.arange(55.)
    data = _cube(_line(x, 6., 21., 4.) + _line(x, 3., 37., 6.), (1, 1))
    for api in (casa, tools):
        logfile = tmp_path / f"{api.__name__}.log"
        fit = _fit(
            api, data, ngauss=0, gmncomps=2, gmampest=[5., 2.5],
            gmcenterest=[20., 36.], gmfwhmest=[3.7, 5.6],
            gmampcon=[.5], gmcentercon=[16.], gmfwhmcon=[1.5], logfile=str(logfile),
        )
        assert fit["valid"].item() and fit["ncomps"].item() == 1
        report = logfile.read_text()
        assert len(re.findall(r"Type\s+: GAUSSIAN MULTIPLET", report)) == 1
        assert "Results for subcomponent 0" in report
        assert "Results for subcomponent 1" in report
        assert "Results for component 1:" not in report


def test_fit_log_uses_source_location_and_reports_rejected_iterations(casa, tmp_path):
    data = _cube(_line(np.arange(55.), 5., 20., 4.), (3, 3))
    for api in (casa, tools):
        logfile = tmp_path / f"{api.__name__}.log"
        fit = _fit(
            api, data, box="1,1,1,1", pampest=5., pcenterest=20., pfwhmest=4.,
            goodamprange=[0., 1.], logfile=str(logfile),
        )
        assert fit["converged"].item() and not fit["valid"].item()
        assert fit["niter"].item() == -1
        report = logfile.read_text()
        source_location = re.search(r"Pixel\s*:\s*\[\s*1,\s*1,\s*\*\s*\]", report)
        iterations = re.search(r"Iterations\s*:\s*[1-9]\d*", report)
        assert (source_location is not None, iterations is not None) == (True, True)


def test_output_failure_preserves_inputs_and_existing_image(tmp_path):
    image = tools.image()
    data = _cube(_line(np.arange(31.), 7., 15.2, 6.))
    original, existing, model = (tmp_path / f"{name}.im" for name in ("orig", "old", "model"))
    assert image.fromarray(str(original), data)
    old = tools.image()
    assert old.fromarray(str(existing), np.full((2, 2), 8., dtype=np.float32))
    assert old.done()
    args = dict(multifit=True, pampest=6., pcenterest=15., pfwhmest=5., logresults=False)
    try:
        with pytest.raises(RuntimeError):
            image.fitprofile(model=str(model), residual=str(existing), **args)
        assert not model.exists()
        np.testing.assert_array_equal(_read(tools, existing)[0], 8.)
        with pytest.raises(RuntimeError):
            image.fitprofile(model=str(model), logfile=str(original / "new-log"), **args)
        assert not model.exists()
        assert not (original / "new-log").exists()
        with pytest.raises(RuntimeError):
            image.fitprofile(model=str(original), **args)
        with pytest.raises(RuntimeError):
            image.fitprofile(model=str(model), logfile=str(tmp_path / "missing" / "fit.log"), **args)
        assert not model.exists()
        np.testing.assert_array_equal(image.getchunk(), data)
    finally:
        image.done()



def test_rejected_spectrum_writes_the_whole_current_axis(casa, tmp_path):
    # CASA caches the failed lane shape across calls in the same process.
    script = """
import importlib
import sys
import numpy as np
api = importlib.import_module(sys.argv[1])
for n, limits in [(11, []), (23, [0., 1.])]:
    x = np.arange(n)
    a = (5*np.exp(-4*np.log(2)*((x-5)/4)**2)).reshape(1,1,-1)
    source = api.image()
    assert source.fromarray(pixels=a)
    fit = source.fitprofile(multifit=True, pampest=5., pcenterest=5., pfwhmest=4.,
                            model=f"model{n}", goodamprange=limits, logresults=False)
    assert fit["valid"].item() == (n == 11)
    source.done()
out = api.image()
assert out.open("model23")
print("invalid-count", (~out.getchunk(getmask=True)).sum(), np.isnan(out.getchunk()).sum())
out.done()
"""
    for api, count in ((casa, 11), (tools, 23)):
        work = tmp_path / api.__name__
        work.mkdir()
        result = subprocess.run([sys.executable, "-c", script, api.__name__],
                                cwd=work, env=os.environ.copy(), text=True,
                                capture_output=True, check=True)
        assert f"invalid-count {count} {count}" in result.stdout


def test_sigma_output_can_replace_a_closed_native_product(casa, tmp_path):
    data = _cube(np.linspace(1., 3., 11))
    for api in (casa, tools):
        path = str(tmp_path / f"{api.__name__}.sigma")
        values = np.ones(11)
        assert _fit(api, data, ngauss=0, poly=1, sigma=values, outsigma=path)["valid"].all()
        values[2] = 8.
        assert _fit(api, data, ngauss=0, poly=1, sigma=values, outsigma=path)["valid"].all()
        for reader in (casa, tools):
            np.testing.assert_allclose(_read(reader, path)[0][0, 0], values/8.)


@pytest.mark.parametrize("frame", ["GALACTIC", "ECLIPTIC", "conversion"])
def test_profile_direction_reports_the_frame_used_for_the_angles(casa, frame):
    coords = casa.coordsys()
    assert coords.newcoordsys(direction=True, spectral=True)
    if frame == "conversion":
        assert coords.setconversiontype(direction="GALACTIC")
    else:
        assert coords.setreferencecode(value=frame, type="direction", adjust=False)
    assert coords.setreferencepixel([0, 0, 0])
    assert coords.setreferencevalue(value=[.1, .2, 1.4e9])
    assert coords.setincrement(value=[-.0002, .0002, 1000])
    csys = coords.torecord()
    coords.done()
    data = _cube(_line(np.arange(11.), 3., 5., 4.), (1, 1))
    arguments = dict(pampest=3., pcenterest=5., pfwhmest=4.)
    expected = _fit(casa, data, csys, **arguments)
    native = _fit(tools, data, csys, **arguments)
    if frame == "conversion":
        assert expected["direction"].item().startswith("J2000 ")
        name, longitude, latitude = native["direction"].item().split()
        assert name == "GALACTIC"
        # CASA evaluated the same coordinates in the converted frame, then
        # formatted their longitude as equatorial hours.
        np.testing.assert_allclose([float(longitude), float(latitude)],
                                   [96.3432845, -60.1862163], atol=5e-6)
        _compare_records(native, expected, compare_direction=False)
    else:
        _compare_records(native, expected)


@pytest.mark.parametrize("amplitude,reference_stops_early", [
    (1., False), (1.1e-4, True), (9.9e-5, True), (1e-15, True),
])
def test_fixed_width_line_is_invariant_under_amplitude_scaling(
    casa, tmp_path, amplitude, reference_stops_early,
):
    x = np.arange(31.)
    data = _cube(_line(x, amplitude, 15., 4.), (1, 1))
    fits = []
    for api in (casa, tools):
        path = tmp_path / f"{api.__name__}.model"
        fit = _fit(api, data, pampest=amplitude, pcenterest=14., pfwhmest=4., pfix="pf",
                   model=str(path))
        assert fit["valid"].item()
        model = _read(tools, path)[0]
        if api is casa and reference_stops_early:
            error = np.linalg.norm((model - data) / amplitude) / np.linalg.norm(data / amplitude)
            assert error > .01
        else:
            np.testing.assert_allclose(model / amplitude, data / amplitude, atol=2e-6)
        fits.append(fit["gs"]["center"].item())
    if reference_stops_early:
        assert abs(fits[0] - fits[1]) > .01
    else:
        assert fits[0] == pytest.approx(fits[1], abs=2e-6)


def test_profile_convergence_uses_only_free_parameters(casa):
    amplitude = 1e10
    data = _cube(_line(np.arange(55.), amplitude, 20., 5.), (1, 1))
    arguments = dict(pampest=amplitude, pcenterest=16., pfwhmest=8., pfix="p")
    expected = _fit(casa, data, **arguments)
    actual = _fit(tools, data, **arguments)
    assert expected["valid"].item() and actual["valid"].item()
    assert actual["niter"].item() > 1
    for key in ("amp", "center", "fwhm"):
        np.testing.assert_allclose(actual["gs"][key], expected["gs"][key], rtol=1e-7)


def test_profile_corrects_dependent_free_amplitudes(casa):
    data = _cube(_line(np.arange(55.), 8., 20., 5.), (1, 1))
    arguments = dict(pampest=[3., 3.], pcenterest=[20., 20.], pfwhmest=[5., 5.],
                     pfix=["cf", "cf"])
    expected = _fit(casa, data, **arguments)
    actual = _fit(tools, data, **arguments)
    assert expected["valid"].item() and actual["valid"].item()
    np.testing.assert_allclose(actual["gs"]["amp"], 4., atol=2e-6)
    _compare_records(actual, expected)


@pytest.mark.parametrize("model,parameters,message", [
    ("gaussian", dict(pampest=0., pcenterest=8., pfwhmest=3.), "amplitude cannot equal 0"),
    ("gaussian", dict(pampest=2., pcenterest=1., pfwhmest=3.), "not enough points"),
    ("multiplet", dict(gmncomps=2, gmampest=[2., 1.], gmcenterest=[10., 20.],
                      gmfwhmest=[3., 3.], gmampcon=.5, gmfix=["", "p"]), "non-reference Gaussian"),
])
def test_invalid_line_estimates_fail_before_publication(casa, tmp_path, model, parameters, message):
    length = 3 if "not enough" in message else 31
    data = _cube(np.arange(length, dtype=np.float32), (1, 1))
    for api in (casa, tools):
        path = tmp_path / f"{api.__name__}-{model}.im"
        with pytest.raises(RuntimeError, match=message):
            _fit(api, data, model=str(path), **parameters)
        assert not path.exists()


@pytest.mark.parametrize("multifit", [False, True])
def test_default_stokes_fits_first_and_explicit_stokes_fits_selected(casa, tmp_path, multifit):
    pixels = np.broadcast_to(np.array([2., 10.], dtype=np.float32).reshape(1,1,2,1), (2,1,2,11)).copy()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=pixels)
        try:
            for selection, value in [("", 2.), ("Q", 10.)]:
                path = tmp_path / f"{api.__name__}-{selection}.im"
                fit = source.fitprofile(ngauss=0, poly=0, stokes=selection, multifit=multifit,
                                        model=str(path), logresults=False)
                assert fit["valid"].shape == (2 if multifit else 1,1,1,1)
                assert fit["valid"].all()
                output = _read(tools, path)[0]
                assert output.shape == (2 if multifit else 1,1,1,11)
                np.testing.assert_allclose(output, value, atol=1e-7)
        finally:
            source.done()


def test_polynomial_coefficients_in_detailed_log(casa, tmp_path):
    import re

    for api in (casa, tools):
        logfile = tmp_path / f"{api.__name__}.log"
        data = _cube(2. + 3. * np.arange(11.), (1, 1))
        assert _fit(api, data, ngauss=0, poly=1, logfile=str(logfile))["valid"].item()
        report = logfile.read_text()
        for order, coefficient in [(0, 2.), (1, 3.)]:
            values = re.findall(rf"c{order} *: *([-+e.0-9]+)", report)
            assert len(values) == 2
            assert float(values[0]) == pytest.approx(coefficient, abs=1e-8)


def test_profile_log_refuses_region_alias_and_fails_with_image_rollback(tmp_path):
    data = _cube(_line(np.arange(31.), 7., 15.2, 6.), (3, 3))
    estimate = dict(pampest=6., pcenterest=15., pfwhmest=5.)
    region = tmp_path / "region.crtf"
    region.write_text("#CRTFv0\nbox[[0pix, 0pix], [1pix, 2pix]]\n")
    expected = region.read_bytes()
    model = tmp_path / "model.im"
    with pytest.raises(RuntimeError):
        _fit(tools, data, region=str(region), logfile=str(region), model=str(model), append=False, **estimate)
    assert region.read_bytes() == expected
    assert not model.exists()

    import pathlib
    full_device = pathlib.Path("/dev/full")
    if full_device.exists():
        saved_sigma = tmp_path / "existing.im"
        maker = tools.image()
        assert maker.fromarray(str(saved_sigma), np.full((2, 2), 8., dtype=np.float32))
        assert maker.done()
        with pytest.raises(RuntimeError, match="regular log file"):
            _fit(tools, data, logfile=str(full_device), model=str(model), outsigma=str(saved_sigma),
                 sigma=np.ones(31), **estimate)
        np.testing.assert_array_equal(_read(tools, saved_sigma)[0], 8.)
        assert not model.exists()


def test_native_sigma_image_metadata_is_copied(casa, tmp_path):
    data = _cube(2. + 3. * np.arange(11.))
    for writer in (casa, tools):
        source = writer.image()
        sigma = writer.image()
        original = str(tmp_path / (writer.__name__ + ".sigma"))
        product = str(tmp_path / (writer.__name__ + ".normalized"))
        assert source.fromarray(pixels=data)
        assert source.sethistory(origin="science", history="science history marker")
        assert sigma.fromarray(original, np.full_like(data, 2.))
        assert sigma.setbrightnessunit("K")
        assert sigma.setmiscinfo({"source": "sigma metadata"})
        assert sigma.sethistory(origin="sigma", history="sigma history marker")
        assert sigma.done()
        assert source.fitprofile(ngauss=0, poly=1, multifit=True, sigma=original,
                                 outsigma=product, logresults=False)["valid"].all()
        assert source.done()
        for reader in (casa, tools):
            output = reader.image()
            assert output.open(product)
            assert output.brightnessunit() == "K"
            assert output.miscinfo()["source"] == "sigma metadata"
            np.testing.assert_array_equal(output.getchunk(), 1.)
            entries = output.history()
            assert "sigma history marker" in entries
            assert "science history marker" not in entries
            assert entries[-1] == "Number valid             = 4"
            assert output.done()


def test_absorption_integral_uncertainty_is_nonnegative(casa):
    x = np.arange(31.)
    data = _cube(_line(x, -4., 15.1, 4.) + .02 * np.sin(3*x), (1, 1))
    args = dict(pampest=-3.9, pcenterest=15., pfwhmest=4.2)
    oracle = _fit(casa, data, **args)["gs"]
    native = _fit(tools, data, **args)["gs"]
    assert oracle["integralErr"].item() < 0. < native["integralErr"].item()
    assert native["integral"].item() == pytest.approx(oracle["integral"].item(), rel=.001)
    assert native["integralErr"].item() == pytest.approx(-oracle["integralErr"].item(), rel=.002)


def test_good_range_applies_to_secondary_multiplet_peak(casa):
    x = np.arange(51.)
    data = _cube(_line(x, 6., 17., 4.) + _line(x, 3., 29., 4.), (1, 1))
    args = dict(gmncomps=2, gmampest=[5., 2.5], gmcenterest=[17., 29.],
                gmfwhmest=[4., 4.], gmampcon=.5, gmcentercon=12., gmfwhmcon=1.,
                goodamprange=[4., 7.])
    assert _fit(casa, data, **args)["valid"].item()
    native = _fit(tools, data, **args)
    assert native["converged"].item()
    assert not native["valid"].item()
    assert native["type"].item() == "UNDEF"
