"""Classical statistics through the CASA-compatible image tool."""

import os

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_lazy_statistics_reacquire_native_input_read_lock(casa, tmp_path):
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    for backend in (casa, tools):
        source = backend.image()
        path = str(tmp_path / f"{backend.__name__}.im")
        assert source.fromarray(path, pixels)
        assert source.done()
        result = source.imagecalc(pixels=f'"{path}" + 1')
        assert result.unlock()
        np.testing.assert_array_equal(result.haslock(), [False, False])
        np.testing.assert_array_equal(result.statistics()["sum"], [78.0])
        np.testing.assert_array_equal(result.haslock(), [True, False])
        assert result.done()


@pytest.mark.parametrize("pixel_type", ["f", "d"])
def test_statistics_records_and_display_axes(casa, pixel_type):
    values = np.array([[[2.0, -3.0], [4.0, 7.0]], [[1.0, 9.0], [-2.0, 3.0]]])
    rust, oracle = tools.image(), casa.image()
    for image in rust, oracle:
        assert image.statistics() == {}
        assert image.fromshape(shape=list(values.shape), type=pixel_type)
        assert image.putchunk(values)
    for kwargs in (
        {},
        dict(axes=[1], robust=True),
        dict(axes=[0, 1]),
        dict(axes=[2, 1, 0], force=True, clmethod="framework"),
        dict(includepix=[7.0, -2.0], robust=True),
        dict(excludepix=[-7.0], clmethod="tiled"),
        dict(includepix=[25.0, 33.0], robust=True),
        dict(axes=[0], includepix=[7.0, 9.0], robust=True),
        dict(axes=[0], mask="F", robust=True),
        dict(mask=False),
    ):
        _assert_value(rust.statistics(**kwargs), oracle.statistics(**kwargs))
    for image in rust, oracle:
        with pytest.raises(RuntimeError, match="only give one"):
            image.statistics(includepix=[2.0], excludepix=[3.0])
        with pytest.raises(RuntimeError, match="Too many elements"):
            image.statistics(includepix=[2.0, 3.0, 4.0])
        with pytest.raises(RuntimeError, match="Invalid cursor axes"):
            image.statistics(axes=[5])
        assert image.done()


def test_statistics_reads_persisted_masks_and_region(casa, tmp_path):
    path = str(tmp_path / "statistics.im")
    values = np.arange(60, dtype=np.float64).reshape(5, 4, 3)
    creator = tools.image()
    assert creator.fromarray(path, values)
    assert creator.calcmask(f'"{path}" < 53', name="valid")
    assert creator.setbrightnessunit("K")
    assert creator.done()
    rust, oracle = tools.image(), casa.image()
    region = casa.regionmanager().box([1, 1, 0], [4, 3, 2])
    for image in rust, oracle:
        assert image.open(path)
        assert image.unlock()
    args = dict(region=region, mask=f'"{path}" > 21', robust=True)
    actual = rust.statistics(**args)
    expected = oracle.statistics(**args)
    _assert_value(actual, expected)
    selected = values[1:5, 1:4, :].ravel()
    selected = selected[(selected > 21) & (selected < 53)]
    _assert_value(actual["sum"], np.array([selected.sum()]))
    _assert_value(actual["sigma"], np.array([selected.std(ddof=1)]))
    for image in rust, oracle:
        assert image.done()


def test_statistics_compacts_region_planes_without_dropping_filtered_pixels(casa):
    shape = (3, 4, 8, 4)
    pixels = np.broadcast_to(
        np.arange(8)[None, None, :, None] + 10 * np.arange(4)[None, None, None, :],
        shape,
    ).astype(np.float32)
    rg = casa.regionmanager()
    for api in (casa, tools):
        image = api.image()
        try:
            assert image.fromarray(pixels=pixels)
            coordinates = image.coordsys().torecord()
            region = rg.frombcs(csys=coordinates, shape=shape, chans="1~2;5~6", stokes="IV")
            expected = np.array([[1, 31], [2, 32], [5, 35], [6, 36]])
            for limit, mean in [([], expected), ([0, 15], expected * [1, 0])]:
                result = image.statistics(axes=[0, 1], region=region, includepix=limit)
                np.testing.assert_array_equal(result["mean"], mean)
                np.testing.assert_array_equal(result["npts"], 12 * (mean > 0))
                assert result["npts"].dtype == np.float64
            if api is tools:
                # All channels remain when only the second display axis has exclusions.
                stokes = rg.frombcs(csys=coordinates, shape=shape, stokes="IV")
                result = image.statistics(axes=[0, 1], region=stokes)
                np.testing.assert_array_equal(result["mean"], np.arange(8)[:, None] + [0, 30])
                np.testing.assert_array_equal(result["npts"], np.full((8, 2), 12))
        finally:
            image.done()


@pytest.mark.parametrize("pixel_type", ["f", "d"])
@pytest.mark.parametrize("options", [
    {},
    dict(axes=[0, 1], robust=True),
    dict(axes=[0, 1], algorithm="hinges", fence=1.5, robust=True),
    dict(axes=[0, 1], algorithm="fittohalf", center="median", lside=False, robust=True),
    dict(axes=[0, 1], algorithm="chauvenet", zscore=3.0, maxiter=2, robust=True),
    dict(axes=[0, 1], algorithm="biweight", niter=3),
    dict(axes=[0, 1], includepix=[-300., 200.], robust=True),
])
def test_statistics_disk_matches_memory_and_casa(casa, pixel_type, options):
    x, y, _, frequency = np.ogrid[:13, :15, :1, :7]
    pixels = (x + 2*y + 11*frequency).astype(np.float32)
    pixels[6, 7, 0, 3] = 500.
    valid = pixels % 5 != 1
    saved = []
    for api in (casa, tools):
        image = api.image()
        try:
            assert image.fromshape(shape=list(pixels.shape), type=pixel_type)
            assert image.putchunk(pixels)
            assert image.putregion(pixelmask=valid)
            memory = image.statistics(disk=False, **options)
            disk = image.statistics(disk=True, **options)
            _assert_value(disk, memory)
            saved.append(disk)
        finally:
            image.done()
    _assert_value(saved[1], saved[0])
    if not options:
        assert saved[1]["npts"].item() == np.count_nonzero(valid)
        np.testing.assert_allclose(saved[1]["sum"], [pixels[valid].sum(dtype=np.float64)])


def test_statistics_disk_tracks_a_rejected_edit_and_an_accepted_edit():
    image = tools.image()
    try:
        values = np.arange(42, dtype=np.float32).reshape(6, 7)
        assert image.fromarray(pixels=values)
        expected = image.statistics(disk=True)["sum"]
        with pytest.raises(RuntimeError):
            image.statistics(disk=True, includepix=[0, 4], excludepix=[10, 20])
        np.testing.assert_array_equal(image.statistics(disk=True)["sum"], expected)
        assert image.putchunk(np.array([[100.]], dtype=np.float32), blc=[0, 0])
        np.testing.assert_array_equal(image.statistics(disk=True, force=True)["sum"], expected + 100)
        np.testing.assert_array_equal(image.statistics(disk=False)["sum"], expected + 100)
    finally:
        image.done()


def test_statistics_disk_reads_columns_across_multiple_robust_result_blocks():
    x, y = np.ogrid[:5, :537]
    pixels = (x + 2*y).astype(np.float32)
    image = tools.image()
    try:
        assert image.fromarray(pixels=pixels)
        options = dict(axes=[0], algorithm="hinges", fence=1.5, robust=True)
        actual = image.statistics(disk=True, **options)
        _assert_value(actual, image.statistics(disk=False, **options))
        np.testing.assert_array_equal(actual["npts"], np.full(537, 5.))
        np.testing.assert_array_equal(actual["sum"], 10 + 10*np.arange(537))
        np.testing.assert_array_equal(actual["median"], 2 + 2*np.arange(537))
    finally:
        image.done()


def test_statistics_disk_reads_both_writers_with_both_readers(casa, tmp_path):
    x, y, _, z = np.ogrid[:13, :15, :1, :4]
    pixels = (x + 2*y + 100*z).astype(np.float32)
    valid = pixels % 4 != 1
    manager = casa.regionmanager()
    region = manager.box([2,3,0,1], [11,12,0,3])
    manager.done()
    expected = []
    for channel in range(1,4):
        block = pixels[2:12, 3:13, 0, channel]
        mask = valid[2:12, 3:13, 0, channel]
        expected.append((mask.sum(), block[mask].sum(dtype=np.float64), np.median(block[mask])))
    expected = np.asarray(expected).T
    for writer in (casa, tools):
        source = writer.image()
        name = str(tmp_path/f"{writer.__name__}-disk-stats.im")
        try:
            assert source.fromarray(name, pixels=pixels)
            assert source.putregion(pixelmask=valid)
            assert source.setbrightnessunit("K")
        finally:
            source.done()
        for reader in (casa, tools):
            image = reader.image()
            try:
                assert image.open(name)
                result = image.statistics(disk=True, axes=[0,1], region=region, robust=True)
                for row, field in zip(expected, ("npts", "sum", "median")):
                    np.testing.assert_array_equal(result[field], row)
            finally:
                image.done()


@pytest.mark.parametrize("bitpix,blank", [(16, -999), (-32, np.nan)])
def test_statistics_on_attached_fits_matches_casa_and_excludes_blanks(casa, tmp_path, bitpix, blank):
    path = tmp_path / "masked.fits"
    content = _hdu(True, [1, blank, 3, 4], bitpix=bitpix)
    path.write_bytes(content)
    rust, oracle = tools.image(), casa.image()
    assert rust.open(str(path)) and oracle.open(str(path))
    for arguments in (
        {},
        {"axes": [0], "robust": True},
        {"region": casa.regionmanager().box([0, 1], [1, 1]), "includepix": [3, 4]},
        {"mask": f'"{path}" > 2'},
    ):
        _assert_value(rust.statistics(**arguments), oracle.statistics(**arguments))
    full = rust.statistics()
    _assert_value(full["npts"], np.array([3.0]))
    _assert_value(full["sum"], np.array([8.0]))
    _assert_value(full["maxpos"], np.array([1, 1]))
    assert rust.done() and oracle.done()
    assert path.read_bytes() == content


def test_fits_expression_preserves_blank_validity_on_an_unmasked_target(casa, tmp_path):
    target = tmp_path / "unmasked.im"
    input_path = tmp_path / "unmasked.fits"
    operand = tmp_path / "mask.fits"
    input_path.write_bytes(_hdu(True, [1, 2, 3, 4]))
    operand.write_bytes(_hdu(True, [5, -999, 7, 8]))
    writer = casa.image()
    assert writer.fromfits(str(target), str(input_path)) and writer.done()
    region = casa.regionmanager().box([0, 0], [1, 0])
    rust, oracle = tools.image(), casa.image()
    for tool in (rust, oracle):
        assert tool.open(str(target))
    arguments = {"mask": f'mask("{operand}")', "region": region}
    result = rust.statistics(**arguments)
    _assert_value(result, oracle.statistics(**arguments))
    _assert_value(result["npts"], np.array([1.0]))
    _assert_value(result["sum"], np.array([1.0]))
    assert rust.done() and oracle.done()


def test_statistics_strided_extrema_are_absolute_image_pixels(casa):
    image = tools.image()
    values = np.arange(24, dtype=np.float64).reshape(4, 3, 2)
    assert image.fromarray(pixels=values)
    region = casa.regionmanager().box([1, 1, 0], [3, 2, 1], [2, 1, 1])
    result = image.statistics(region=region)
    selected = values[1:4:2, 1:3, :]
    _assert_value(image.getregion(region=region), selected)
    _assert_value(result["npts"], np.array([selected.size], dtype=np.float64))
    _assert_value(result["max"], np.array([selected.max()]))
    _assert_value(result["maxpos"], np.array([3, 2, 1]))


def test_statistics_breaks_extremum_ties_in_casa_pixel_order(casa):
    values = np.array([[3., 1., 5.], [1., 5., 3.], [5., 3., 1.]])
    for make in (tools.image, casa.image):
        image = make()
        assert image.fromarray(pixels=values)
        for method in ("auto", "tiled", "framework"):
            result = image.statistics(clmethod=method)
            _assert_value(result["minpos"], np.array([1, 0]))
            _assert_value(result["maxpos"], np.array([2, 0]))


def test_statistics_include_all_pixels_with_linear_coordinates(tmp_path):
    path = str(tmp_path / "linear.im")
    values = np.arange(40, dtype=np.float32).reshape(5, 4, 2)
    image = tools.image()
    assert image.fromarray(path, values, linear=True) and image.done()
    assert image.open(path)
    result = image.statistics()
    assert result["npts"][0] == values.size
    assert result["sum"][0] == values.sum()
    np.testing.assert_array_equal(result["trc"], [4, 3, 1])
    assert image.done()


def test_statistics_independent_moments_and_flux():
    image = tools.image()
    values = np.arange(24, dtype=np.float64).reshape(4, 3, 2)
    assert image.fromarray(pixels=values)
    assert image.setbrightnessunit("K")
    result = image.statistics(axes=[0, 1], robust=True)
    expected_mean = values.mean(axis=(0, 1))
    _assert_value(result["mean"], expected_mean)
    _assert_value(result["median"], np.median(values, axis=(0, 1)))
    _assert_value(result["npts"], np.array([12.0, 12.0]))
    _assert_value(result["flux"], values.sum(axis=(0, 1)) * 3600.0)


def test_statistics_flux_depends_on_which_axes_are_collapsed(casa):
    values = np.arange(72, dtype=np.float64).reshape(4, 3, 2, 3)
    rust, oracle = tools.image(), casa.image()
    for image in rust, oracle:
        assert image.fromarray(pixels=values)
        assert image.setbrightnessunit("Jy/beam")
        assert image.setrestoringbeam(major="60arcsec", minor="60arcsec", pa="0deg")
    for axes in ([], [0], [0, 1], [0, 1, 2], [0, 1, 3], [3]):
        _assert_value(rust.statistics(axes=axes), oracle.statistics(axes=axes))


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(algorithm="biweight"),
        dict(algorithm="Biw", axes=[1], niter=3, robust=True),
        dict(algorithm="hinges-fences", axes=[1], fence=1.0, robust=True),
        dict(algorithm="fit-half", center="mean", lside=False, robust=True),
        dict(algorithm="fit-half", center="median", lside=False, robust=True),
        dict(algorithm="fit-half", center="zero", lside=True, robust=True),
        dict(algorithm="chauvenet", zscore=1.4, maxiter=2, robust=True),
    ],
)
def test_statistics_sample_algorithms(casa, kwargs):
    values = np.array([[-25., 2., 3.], [4., 5., 7.], [9., 11., 100.]])
    rust, oracle = tools.image(), casa.image()
    for image in rust, oracle:
        assert image.fromarray(pixels=values)
    _assert_value(rust.statistics(**kwargs), oracle.statistics(**kwargs))


def test_statistics_handles_empty_biweight_sample():
    image = tools.image()
    assert image.fromshape(shape=[4, 3])
    with pytest.raises(RuntimeError, match="at least two points"):
        image.statistics(algorithm="biweight", mask="F")


@pytest.mark.parametrize("algorithm", ["classic", "hinges-fences", "fit-half", "chauvenet"])
def test_statistics_retains_variance_on_large_double_baseline(casa, algorithm):
    pixels = 1e12 + np.arange(6.0).reshape(3, 2)
    for make in (tools.image, casa.image):
        image = make()
        assert image.fromshape(shape=[3, 2], type="d")
        assert image.putchunk(pixels)
        result = image.statistics(algorithm=algorithm)
        _assert_value(result["npts"], np.array([6.0]))
        _assert_value(result["sigma"], np.array([np.std(np.arange(6.0), ddof=1)]))


@pytest.mark.parametrize("algorithm", ["classic", "hinges-fences"])
def test_statistics_filters_nan_and_uses_float_thresholds_on_double_data(casa, algorithm):
    rust, oracle = tools.image(), casa.image()
    finite = np.arange(6.0).reshape(3, 2)
    nan_pixels = finite.copy()
    nan_pixels[0, 1] = np.nan
    for image in rust, oracle:
        assert image.fromshape(shape=[3, 2], type="d")
        assert image.putchunk(nan_pixels)
    kwargs = dict(algorithm=algorithm, fence=100.0)
    result = rust.statistics(includepix=[0.0, 10.0], **kwargs)
    _assert_value(result, oracle.statistics(includepix=[0.0, 10.0], **kwargs))
    _assert_value(result["npts"], np.array([5.0]))
    for image in rust, oracle:
        assert image.putchunk(1 + finite * 1e-8)
    limits = [1.000000009, 1.000000031]
    for name, count in (("includepix", 1.0), ("excludepix", 5.0)):
        args = {name: limits, **kwargs}
        result = rust.statistics(**args)
        _assert_value(result, oracle.statistics(**args))
        _assert_value(result["npts"], np.array([count]))


def test_statistics_strided_direction_flux_and_fit_half_tie(casa):
    values = np.array([[0., 3., 2.], [3., 2., 3.]])
    region = casa.regionmanager().box([0, 0], [1, 2], [1, 2])
    for make in (tools.image, casa.image):
        image = make()
        assert image.fromarray(pixels=values)
        assert image.setbrightnessunit("K")
        _assert_value(image.statistics(region=region)["flux"], np.array([57600.0]))
        _assert_value(image.statistics(algorithm="fit-half", lside=False)["maxpos"], np.array([1, 0]))


@pytest.mark.parametrize("invalid_unit", ["K/beam", "W/beam"])
def test_statistics_omits_flux_for_incompatible_beam_units(casa, invalid_unit):
    for make in (tools.image, casa.image):
        image = make()
        assert image.fromarray(pixels=np.ones((3, 2)))
        assert image.setrestoringbeam(major="60arcsec", minor="60arcsec", pa="0deg")
        assert image.setbrightnessunit(invalid_unit)
        assert "flux" not in image.statistics()
