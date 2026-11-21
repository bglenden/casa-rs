"""Source positions, flux and sky shapes against CASA and analytic fixtures."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _sources(shape, components):
    x, y = np.ogrid[:shape[0], :shape[1]]
    values = np.zeros(shape)
    for height, cx, cy, sx, sy in components:
        values += height * np.exp(-((x - cx)**2 / sx + (y - cy)**2 / sy))
    return values.astype(np.float32)


def _components(record):
    assert set(record) == {"nelements", *(f"component{i}" for i in range(record["nelements"]))}
    return [record[f"component{i}"] for i in range(record["nelements"])]


def _direction(component):
    record = component["shape"]["direction"]
    assert record["type"] == "direction"
    assert record["refer"] == "J2000"
    assert all(record[key]["unit"] == "rad" for key in ("m0", "m1"))
    return [record["m0"]["value"], record["m1"]["value"]]


def _compare(left, right, fitted=False):
    assert len(left) == len(right)
    for native, oracle in zip(left, right):
        assert native["label"] == oracle["label"] == ""
        assert native["shape"]["type"] == oracle["shape"]["type"]
        assert native["flux"]["unit"] == oracle["flux"]["unit"]
        assert native["flux"]["polarisation"] == oracle["flux"]["polarisation"] == "Stokes"
        assert native["spectrum"] == oracle["spectrum"]
        np.testing.assert_array_equal(native["flux"]["error"], oracle["flux"]["error"])
        np.testing.assert_allclose(native["flux"]["value"], oracle["flux"]["value"], rtol=1e-3 if fitted else 2e-6)
        np.testing.assert_allclose(_direction(native), _direction(oracle), atol=7e-8 if fitted else 2e-10)
        if native["shape"]["type"] == "Gaussian":
            for axis in ("majoraxis", "minoraxis"):
                actual = native["shape"][axis]
                expected = oracle["shape"][axis]
                assert actual["unit"] == expected["unit"] == "arcsec"
                assert actual["value"] == pytest.approx(expected["value"], rel=.001)
            for key in ("majoraxiserror", "minoraxiserror", "positionangleerror"):
                assert native["shape"][key] == oracle["shape"][key]
            pa = native["shape"]["positionangle"]
            expected = oracle["shape"]["positionangle"]
            assert pa["unit"] == expected["unit"] == "deg"
            difference = (pa["value"] - expected["value"] + 90.) % 180. - 90.
            assert abs(difference) < .15


def test_findsources_native_writer_reader_matrix(casa, tmp_path):
    sources = [(8., 7.4, 9.2, 3., 3.), (4.5, 20.2, 22.7, 4., 4.), (-6., 12.2, 16.7, 2.9, 3.2)]
    values = _sources((29, 32), sources)
    mask = np.full(values.shape, True)
    mask[0, :] = False
    mask[-1, -1] = False
    all_results = {}
    choices = [{}, {"cutoff": .7}, {"nmax": 1}, {"negfind": True, "cutoff": .2}]
    for writer in (casa, tools):
        path = str(tmp_path / f"{writer.__name__}-sources.im")
        with_source = writer.image()
        assert with_source.fromarray(path, values)
        assert with_source.setbrightnessunit("mJy/beam")
        assert with_source.putregion(pixelmask=mask)
        assert with_source.done()
        for reader in (casa, tools):
            source = reader.image()
            assert source.open(path)
            try:
                all_results[writer.__name__, reader.__name__] = [
                    _components(source.findsources(**arguments)) for arguments in choices
                ]
                np.testing.assert_array_equal(source.getchunk(), values)
                np.testing.assert_array_equal(source.getchunk(getmask=True), mask)
            finally:
                source.done()
    oracle = all_results[casa.__name__, casa.__name__]
    assert [len(r) for r in oracle] == [2, 1, 1, 3]
    assert min(r["flux"]["value"][0] for r in oracle[-1]) < 0.
    assert 0.007 < oracle[0][0]["flux"]["value"][0] < 0.008
    for results in all_results.values():
        for actual, expected in zip(results, oracle):
            _compare(actual, expected)


@pytest.mark.parametrize("brightness", [
    "", "mJy/beam", "Jy/pixel", "K", "mK", "Jy/beam.km/s",
    "mJy/beam.km/s", "Jy.km/s/pixel",
])
@pytest.mark.parametrize("height", [7., -7.])
def test_findsources_fitted_ellipses_and_flux(casa, brightness, height):
    values = _sources((31, 34), [(height, 14.2, 17.7, 6., 2.5)])
    results = []
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        assert source.setbrightnessunit(brightness)
        if "beam" in brightness:
            assert source.setrestoringbeam(major="140arcsec", minor="90arcsec", pa="20deg")
        try:
            components = _components(source.findsources(point=False, width=7, negfind=True))
            assert len(components) == 1
            component = components[0]
            assert component["shape"]["type"] == "Gaussian"
            assert component["shape"]["majoraxis"]["value"] == pytest.approx(120. * np.sqrt(6. * np.log(2.)), rel=.001)
            assert component["shape"]["minoraxis"]["value"] == pytest.approx(120. * np.sqrt(2.5 * np.log(2.)), rel=.001)
            if brightness in ("", "Jy/pixel", "mK", "mJy/beam.km/s", "Jy.km/s/pixel"):
                integral = height * np.pi * np.sqrt(6. * 2.5)
                assert component["flux"]["value"][0] == pytest.approx(integral, rel=.001)
            elif brightness in ("mJy/beam", "Jy/beam.km/s"):
                gain = 1e-3 if brightness == "mJy/beam" else 1.
                integral = height * gain * 60.**2 * 4. * np.log(2.) * np.sqrt(6. * 2.5) / (140. * 90.)
                assert component["flux"]["value"][0] == pytest.approx(integral, rel=.001)
            results.append(components)
        finally:
            source.done()
    _compare(*results[::-1], fitted=True)
    # The component list accepts the binding's returned record as a native model.
    for api in (casa, tools):
        componentlist = api.componentlist()
        try:
            record = {"component0": results[-1][0], "nelements": 1}
            if brightness == "Jy/beam.km/s":
                with pytest.raises(RuntimeError, match="dimensions of the units"):
                    componentlist.fromrecord(record)
            else:
                assert componentlist.fromrecord(record)
                assert componentlist.length() == 1
        finally:
            componentlist.done()


def test_findsources_selection_masks_fits_and_early_rows(casa, tmp_path):
    values = _sources((25, 29), [(9., 8., 2., 1.2, 1.2), (6., 5.4, 13.1, 3., 3.), (5., 18., 18., 2., 2.)])
    valid = np.full(values.shape, True)
    valid[19, 18] = False
    x = np.broadcast_to(np.arange(25, dtype=np.float32)[:, None], values.shape).copy()
    mask_path = str(tmp_path / "mask.im")
    mask = casa.image()
    assert mask.fromarray(mask_path, x)
    assert mask.done()
    region_tool = casa.regionmanager()
    region = region_tool.box([2, 1], [21, 25])
    region_tool.done()
    fits_path = str(tmp_path / "sky.fits")
    records = []
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        assert source.putregion(pixelmask=valid)
        try:
            selected = source.findsources(region=region, mask=f"'{mask_path}' > 2")
            text_region = "box[[2pix, 1pix], [21pix, 25pix]]"
            alternate = source.findsources(region=text_region, mask=f"'{mask_path}' > 2")
            _compare(_components(alternate), _components(selected))
            fallback = source.findsources(region=region, mask=f"'{mask_path}' > 2", point=False, width=0)
            records.append(_components(selected))
            assert len(records[-1]) == 1
            _compare(records[-1], _components(fallback))
            if api is casa:
                assert source.tofits(fits_path, dropdeg=True)
        finally:
            source.done()
        source = api.image()
        assert source.open(fits_path)
        try:
            _compare(_components(source.findsources(region=region)), records[-1])
        finally:
            source.done()
    _compare(records[0], records[1])


def test_findsources_safe_input_limits_and_transposed_axes():
    image = tools.image()
    assert image.findsources() == {}
    values = _sources((23, 23), [(8., 8., 13., 3., 3.)])
    assert image.fromarray(pixels=values)
    transposed = image.transpose(order="10")
    try:
        assert image.findsources(nmax=0) == {"nelements": 0}
        with pytest.raises(RuntimeError, match="nmax must be nonnegative"):
            image.findsources(nmax=-1)
        expected = _components(image.findsources())
        _compare(_components(transposed.findsources()), expected)
        np.testing.assert_array_equal(image.getchunk(), values)
    finally:
        transposed.done()
        image.done()


def test_findsources_selected_cube_and_nan_cutoff(casa):
    values = _sources((23, 23), [(8., 8., 13., 3., 3.), (3., 16., 18., 2., 2.)])
    region_tool = casa.regionmanager()
    region = region_tool.box([0, 0, 1], [22, 22, 1])
    region_tool.done()
    results = []
    for api in (casa, tools):
        cube = api.image()
        assert cube.fromarray(pixels=np.stack([values] * 3, axis=-1))
        try:
            results.append(_components(cube.findsources(region=region, cutoff=float("nan"))))
            assert len(results[-1]) == 2
            if api is tools:
                with pytest.raises(RuntimeError, match="exactly two nondegenerate sky axes"):
                    cube.findsources()
        finally:
            cube.done()
    _compare(results[1], results[0])


@pytest.mark.parametrize("kind", ["c", "d", "cd"])
def test_findsources_refuses_non_float_images(casa, kind):
    for api in (casa, tools):
        source = api.image()
        assert source.fromshape(shape=[12, 12], type=kind)
        try:
            with pytest.raises(RuntimeError, match="only float-valued images"):
                source.findsources()
        finally:
            source.done()


def test_findsources_refuses_multiple_beams_even_with_plane_selected(casa):
    values = _sources((21, 23), [(6., 9.1, 12.3, 3., 3.)])
    cube = np.broadcast_to(values[..., None, None], (21, 23, 1, 2)).copy()
    manager = casa.regionmanager()
    region = manager.box([0, 0, 0, 1], [20, 22, 0, 1])
    manager.done()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=cube)
        try:
            assert source.setrestoringbeam(major="90arcsec", minor="55arcsec", pa="20deg", channel=0)
            assert source.setrestoringbeam(major="80arcsec", minor="60arcsec", pa="10deg", channel=1)
            with pytest.raises(RuntimeError, match="does not support images with multiple beams"):
                source.findsources(region=region)
            np.testing.assert_array_equal(source.getchunk(), cube)
        finally:
            source.done()


@pytest.mark.parametrize("center", [1, 2])
def test_findsources_early_rows_do_not_admit_masked_pixels(casa, center):
    values = _sources((16, 17), [(8., 7., center, 1.2, 1.2)])
    valid = np.ones(values.shape, dtype=bool)
    valid[:, :2] = False
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            assert source.findsources()["nelements"] == 0
            assert source.putregion(pixelmask=valid)
            assert source.findsources()["nelements"] == (1 if api is casa else 0)
        finally:
            source.done()


@pytest.mark.parametrize("point", [True, False])
def test_findsources_labels_the_coordinate_conversion_frame(casa, point):
    values = _sources((31, 34), [(7., 14.2, 17.7, 6., 2.5)])
    original = casa.image()
    assert original.fromarray(pixels=values)
    coordinates = original.coordsys()
    assert coordinates.setconversiontype(direction="GALACTIC")
    record = coordinates.torecord()
    coordinates.done()
    original.done()
    directions = []
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values, csys=record)
        try:
            result = _components(source.findsources(point=point))
            direction = result[0]["shape"]["direction"]
            assert direction["refer"] == ("J2000" if api is casa else "GALACTIC")
            directions.append([direction["m0"]["value"], direction["m1"]["value"]])
        finally:
            source.done()
    np.testing.assert_allclose(directions[1], directions[0], rtol=0., atol=7e-8 if not point else 2e-10)
    reference = [1.68201015445542, -1.0503864157] if point else [1.68199966643, -1.0503952928]
    np.testing.assert_allclose(directions[1], reference, rtol=0., atol=7e-8)


@pytest.mark.parametrize("width", [-2, 12])
def test_findsources_default_gaussian_when_window_is_unavailable(casa, width):
    values = _sources((31, 34), [(7., 8.2, 17.7, 6., 2.5)])
    fitted = []
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            point = _components(source.findsources())[0]
            result = _components(source.findsources(point=False, width=width))
            assert result[0]["shape"]["type"] == "Gaussian"
            np.testing.assert_allclose(_direction(result[0]), _direction(point), rtol=0., atol=1e-10)
            expected = point["flux"]["value"][0] * np.pi / (4. * np.log(2.)) * width**2
            assert result[0]["flux"]["value"][0] == pytest.approx(expected, rel=.001)
            fitted.append(result)
        finally:
            source.done()
    _compare(fitted[1], fitted[0], fitted=True)


@pytest.mark.parametrize("case", ["single_pixel", "rank_one", "invalid", "masked_invalid"])
def test_findsources_gaussian_fallback_for_degenerate_or_invalid_fits(casa, case):
    values = np.zeros((31, 34), dtype=np.float32)
    if case == "single_pixel":
        values[14, 18] = 7.
    elif case == "rank_one":
        values[14, 17:20] = [1., 7., 1.]
    else:
        values = _sources((31, 34), [(7., 14.2, 17.7, 6., 2.5)])
        values[17, 19] = np.nan
    records = []
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=values)
        try:
            if case == "masked_invalid":
                assert source.putregion(pixelmask=np.isfinite(values))
            result = _components(source.findsources(point=False))
            if case == "masked_invalid":
                assert result[0]["shape"]["type"] == "Gaussian"
            else:
                assert result[0]["shape"]["type"] == "Point"
                _compare(result, _components(source.findsources()))
            records.append(result)
        finally:
            source.done()
    _compare(records[1], records[0], fitted=True)


@pytest.mark.parametrize("argument", [{"region": 3}, {"mask": 3}])
def test_findsources_checks_attachment_and_pixel_type_before_variant_arguments(casa, argument):
    for api in (casa, tools):
        source = api.image()
        assert source.findsources(**argument) == {}
        assert source.fromshape(shape=[12, 12], type="d")
        try:
            with pytest.raises(RuntimeError, match="only float-valued images"):
                source.findsources(**argument)
        finally:
            source.done()
