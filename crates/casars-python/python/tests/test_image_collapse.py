"""CASA image collapse statistics, coordinate grids, masks, and native products."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize("function,expected,valid", [
    ("su", [4., -19., 0.], [True, True, False]),
    (" MEA ", [4. / 3., -19. / 4., 0.], [True, True, False]),
    ("med", [2., -4.5, 0.], [True, True, False]),
    ("mi", [-2., -9., 0.], [True, True, False]),
    ("max", [4., -1., 0.], [True, True, False]),
    ("mad", [2., 2.5, 0.], [True, True, False]),
    ("xm", [2. * 1.4826022185, 2.5 * 1.4826022185, 0.], [True, True, False]),
    ("n", [3., 4., 0.], [True, True, False]),
    ("r", [np.sqrt(8.), np.sqrt(127. / 4.), 0.], [True, True, False]),
    ("st", [np.sqrt(28. / 3.), np.sqrt(12.25), 0.], [True, True, False]),
    ("v", [28. / 3., 12.25, 0.], [True, True, False]),
    ("sqrtsum", [2., 0., 0.], [True, True, False]),
    ("sqrtsum_npix", [2. / 3., 0., np.nan], [True, True, False]),
    ("sqrtsum_npix_beam", [2., 0., 0.], [True, True, False]),
    ("z", [0., 0., 0.], [True, True, True]),
])
def test_collapse_masked_real_statistics(casa, function, expected, valid):
    data = np.array([[[-2., 2.], [-1., -3.], [8., 8.]],
                     [[4., 30.], [-6., -9.], [9., 9.]]], np.float32)
    mask = np.ones_like(data, dtype=bool)
    mask[1, 0, 1] = False
    mask[:, 2, :] = False
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=data, linear=True)
        assert source.putregion(pixelmask=mask)
        region = api.regionmanager().box([0, 0, 0], [1, 2, 1])
        result = source.collapse(function, [0, 2], region=region)
        try:
            assert result.shape().tolist() == [1, 3, 1]
            np.testing.assert_allclose(result.getchunk().reshape(-1), expected, rtol=2e-6)
            np.testing.assert_array_equal(result.getchunk(getmask=True).reshape(-1), valid)
            np.testing.assert_array_equal(source.getchunk(), data)
            np.testing.assert_array_equal(source.getchunk(getmask=True), mask)
        finally:
            result.done()
            source.done()


@pytest.mark.parametrize("function,expected", [
    ("min", 1. + 2.j), ("max", 6.j), ("median", .5 - .5j),
    ("madm", (np.sqrt(4.5) + np.sqrt(6.5)) / 2.),
    ("xmadm", (np.sqrt(4.5) + np.sqrt(6.5)) / 2. * 1.4826022185),
    ("sum", 2. + 7.j), ("mean", .5 + 1.75j), ("npts", 4.),
    ("variance", -9.25 + 5.j / 3.), ("stddev", np.sqrt(-9.25 + 5.j / 3.)),
    ("rms", np.sqrt(-9.75 + 3.j)), ("sqrtsum", np.sqrt(2. + 7.j)),
])
def test_collapse_complex_ordering_and_algebra(casa, function, expected):
    data = np.array([1. + 2.j, 2. + 1.j, -1. - 2.j, 6.j], np.complex64)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=data)
        result = source.collapse(function, 0)
        try:
            np.testing.assert_allclose(result.getchunk(), [expected], rtol=2e-6, atol=2e-7)
            assert result.getchunk(getmask=True).item() is True
        finally:
            result.done()
            source.done()


@pytest.mark.parametrize("complex_pixels", [False, True])
def test_native_collapse_values_mask_coordinates_and_history(casa, tmp_path, complex_pixels):
    raw = np.arange(1, 13, dtype=np.float32).reshape(2, 3, 2)
    data = np.complex64(raw * (1. + 2.j)) if complex_pixels else raw
    mask = np.ones_like(raw, dtype=bool)
    mask[:, 1, :] = False
    sums = np.array([[[18.], [0.], [34.]]], dtype=np.float32)
    expected = sums * (1. + 2.j) if complex_pixels else sums
    for writer in (casa, tools):
        source = writer.image()
        source_path = str(tmp_path / f"source-{writer.__name__}.im")
        assert source.fromarray(source_path, data, linear=True)
        assert source.putregion(pixelmask=mask)
        assert source.setbrightnessunit("K")
        assert source.setmiscinfo({"observer_note": "collapse"})
        path = str(tmp_path / f"product-{writer.__name__}.im")
        region = writer.regionmanager().box([0, 0, 0], [1, 2, 1])
        result = source.collapse("sum", [0, 2], outfile=path, region=region)
        result.done()
        source.done()
        for reader in (casa, tools):
            image = reader.image()
            assert image.open(path)
            coords = image.coordsys()
            try:
                np.testing.assert_array_equal(image.getchunk(), expected)
                np.testing.assert_array_equal(image.getchunk(getmask=True), [[[True], [False], [True]]])
                assert image.brightnessunit() == "K"
                assert image.miscinfo()["observer_note"] == "collapse"
                np.testing.assert_allclose(coords.referencepixel()["numeric"], [0., 1., 0.])
                np.testing.assert_allclose(coords.referencevalue()["numeric"], [-.5, 0., -.5])
                assert image.history()[-2] == f"Ran ia.collapse on {source_path}"
                assert image.history()[-1] == (
                    f"ia.collapse(function='sum', axes=[0,2], outfile='{path}', region={{...}}, "
                    "box='', chans='', stokes='', mask='', overwrite=false, stretch=false)"
                )
            finally:
                coords.done()
                image.done()


def test_collapse_axis_names_and_omitted_axis_forms(casa):
    data = np.arange(6, dtype=np.float32).reshape((2, 3))
    for api in (tools, casa):
        detached = api.image().collapse()
        assert not detached.isopen()
        detached.done()
        source = api.image()
        assert source.fromarray(pixels=data)
        try:
            for axes in (0, [0], "RIGHT AS", ["r"], np.array([0], dtype=np.int32)):
                result = source.collapse("sum", axes)
                try:
                    np.testing.assert_array_equal(result.getchunk(), [[3., 5., 7.]])
                finally:
                    result.done()
            same = source.collapse("sum", np.array([], dtype=np.int32))
            np.testing.assert_array_equal(same.getchunk(), data)
            same.done()
            for axes, message in [
                ([], "axes must be specified"), ("ra", "No axis matches"), ("", "Multiple axes"),
                (0., "Unsupported type"), (True, "Unsupported type"), (2, "must be less than"),
            ]:
                with pytest.raises(RuntimeError, match=message):
                    source.collapse("sum", axes)
        finally:
            source.done()


def test_collapse_flux_selected_beams_and_units(casa):
    data = np.arange(1, 25, dtype=np.float32).reshape(2, 2, 2, 3)
    cases = [("K", "K.arcsec2", 3600.),
             ("mJy/beam", "mJy", 2 * np.log(2.) / (3. * np.pi))]
    for api in (tools, casa):
        source = api.image()
        assert source.fromarray(pixels=data)
        assert source.setrestoringbeam(major="3arcmin", minor="2arcmin", pa="20deg")
        try:
            for raw_units, result_units, scale in cases:
                assert source.setbrightnessunit(raw_units)
                result = source.collapse("flux", [0, 1], box="0,0,1,1", chans="1~2", stokes="Q")
                try:
                    np.testing.assert_allclose(
                        result.getchunk(), data[:, :, 1:2, 1:3].sum(axis=(0, 1), keepdims=True) * scale,
                        rtol=2e-6,
                    )
                    assert result.brightnessunit() == result_units
                    major = result.restoringbeam()["major"]
                    arcseconds = major["value"] * {"arcsec": 1., "arcmin": 60., "deg": 3600.}[major["unit"]]
                    assert arcseconds == pytest.approx(180.)
                    assert result.getchunk(getmask=True).all()
                finally:
                    result.done()
            with pytest.raises(RuntimeError, match="not a direction axis"):
                source.collapse("flux", [0, 3])
        finally:
            source.done()


def test_collapse_invalid_axes_keep_existing_output(tmp_path):
    source = tools.image()
    assert source.fromarray(pixels=np.arange(6, dtype=np.float32).reshape(2, 3))
    path = str(tmp_path / "existing.im")
    existing = tools.image()
    assert existing.fromarray(path, np.float32([11., 13.]))
    existing.done()
    try:
        for axes, diagnostic in [([-1], "nonnegative"), ([0, 0], "distinct")]:
            with pytest.raises(RuntimeError, match=diagnostic):
                source.collapse("sum", axes, outfile=path, overwrite=True)
        assert existing.open(path)
        np.testing.assert_array_equal(existing.getchunk(), [11., 13.])
    finally:
        source.done()
        existing.done()


def test_collapsed_longitude_uses_short_arc_at_wrap():
    source = tools.image()
    assert source.fromarray(pixels=np.ones((4, 4), np.float32))
    output = source.collapse("sum", [0, 1])
    coordinates = output.coordsys()
    try:
        np.testing.assert_array_equal(output.getchunk(), [[16.]])
        np.testing.assert_allclose(coordinates.referencevalue()["numeric"], [.5, -.5], atol=1e-6)
    finally:
        coordinates.done()
        output.done()
        source.done()


def test_collapse_large_median_uses_bounded_sample_storage():
    data = np.float32(np.arange(262_183) % 997) - 51.
    valid = data.astype(np.int32) % 7 != 0
    source = tools.image()
    assert source.fromarray(pixels=data)
    assert source.putregion(pixelmask=valid)
    product = source.collapse("median", [0])
    try:
        np.testing.assert_array_equal(product.getchunk(), [np.median(data[valid])])
    finally:
        product.done()
        source.done()

def test_collapse_implicit_region_for_three_linear_axes(casa):
    data = np.arange(1, 13, dtype=np.float32).reshape(2, 3, 2)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=data, linear=True)
        product = source.collapse("sum", [0, 2])
        try:
            np.testing.assert_array_equal(product.getchunk(), [[[3.]]])
        finally:
            product.done()
            source.done()

def test_collapse_trims_only_output_path(casa, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=np.float32([1., 2., 3.]))
        temporary = source.collapse("sum", 0, outfile="  ")
        assert temporary.name() == "Temporary_Image"
        temporary.done()
        raw = f" {api.__name__}.im "
        result = source.collapse("sum", 0, outfile=raw)
        try:
            assert (tmp_path / raw.strip()).is_dir()
            assert not (tmp_path / raw).exists()
            assert f"outfile='{raw}'" in result.history()[-1]
        finally:
            result.done()
            source.done()
