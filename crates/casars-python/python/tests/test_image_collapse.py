"""CASA image collapse statistics, coordinate grids, masks, and native products."""

import os
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


@pytest.mark.parametrize("selector,first,last", [
    ("range=[4pix,2pix]", 2, 4),
    ("range=[-2pix,3pix]", 0, 3),
    ("range=[2.4pix,4.6pix]", 2, 5),
    ("range=[1.414998GHz,1.415GHz]", 2, 4),
])
def test_collapse_spectral_range_argument(casa, selector, first, last):
    pixels = np.broadcast_to(np.arange(8, dtype=np.float32), (3, 4, 8))
    for api in (casa, tools):
        source = api.image()
        result = None
        try:
            assert source.fromarray(pixels=pixels)
            result = source.collapse("mean", axes=2, chans=selector)
            assert result.shape().tolist() == [3, 4, 1]
            expected = (first + last) / 2
            np.testing.assert_array_equal(result.getchunk(), np.full((3, 4, 1), expected))
            assert result.getchunk(getmask=True).all()
            frequency = source.toworld([0, 0, expected])["numeric"][2]
            assert result.toworld([0, 0, 0])["numeric"][2] == frequency
        finally:
            if result is not None:
                result.done()
            source.done()


def test_collapse_rejects_invalid_spectral_range_argument(casa):
    for api in (casa, tools):
        source = api.image()
        try:
            assert source.fromshape(shape=[3, 4, 8])
            for selector in ("range=[1,3]", "range=[2pix,4kHz]", "range=[20pix,30pix]"):
                with pytest.raises(RuntimeError):
                    source.collapse("mean", axes=2, chans=selector)
                assert source.shape().tolist() == [3, 4, 8]
        finally:
            source.done()


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
        region = casa.regionmanager().box([0, 0, 0], [1, 2, 1])
        result = source.collapse(function, [0, 2], region=region)
        try:
            assert result.shape().tolist() == [1, 3, 1]
            np.testing.assert_allclose(result.getchunk().reshape(-1), expected, rtol=2e-6)
            np.testing.assert_array_equal(result.getchunk(getmask=True).reshape(-1), valid)
            if not all(valid):
                assert result.maskhandler("default") == ["mask0"]
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
        region = casa.regionmanager().box([0, 0, 0], [1, 2, 1])
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


@pytest.mark.parametrize("first,stride,total,midpoint,increment", [
    (0, 1, 15., 41.5, 15.4),
    (1, 1, 15., 48.9, 15.4),
    (1, 2, 9., 25.4, 30.8),
    (0, 2, 6., 10., 30.8),
])
def test_collapse_lookup_axis_uses_selected_world_grid(casa, first, stride, total, midpoint, increment):
    manager = casa.regionmanager()
    region = manager.box([first], [5], [stride])
    manager.done()
    for api in (casa, tools):
        coordinate = api.coordsys()
        assert coordinate.newcoordsys(tabular=True)
        assert coordinate.settabular(pixel=[0, 1, 2, 3, 4, 5], world=[3, 4, 10, 14, 64, 80])
        source = api.image()
        assert source.fromarray(pixels=np.arange(6, dtype=np.float32), csys=coordinate.torecord())
        coordinate.done()
        result = source.collapse("sum", 0, region=region)
        coords = result.coordsys()
        try:
            np.testing.assert_array_equal(result.getchunk(), [total])
            np.testing.assert_allclose(coords.referencepixel()["numeric"], [0.], atol=1e-12)
            np.testing.assert_allclose(coords.referencevalue()["numeric"], [midpoint], atol=1e-12)
            np.testing.assert_allclose(coords.increment()["numeric"], [increment], atol=1e-12)
            np.testing.assert_allclose(coords.toworld([0.])["numeric"], [midpoint], atol=1e-12)
        finally:
            coords.done()
            result.done()
            source.done()


def test_collapse_flux_uses_each_selected_beam(casa):
    pixels = np.arange(24, dtype=np.float32).reshape(2, 2, 2, 3)
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=pixels)
        assert source.setbrightnessunit("Jy/beam")
        for channel in range(3):
            for stokes in range(2):
                assert source.setrestoringbeam(
                    major=f"{3 + channel}arcmin", minor=f"{2 + stokes / 2}arcmin", pa="20deg",
                    channel=channel, polarization=stokes,
                )
        flux = source.collapse("flux", [0, 1], chans="1~2", stokes="Q")
        channels = source.collapse("sum", 3, chans="1~2")
        try:
            areas = np.pi * np.array([4., 5.]) * 2.5 / (4. * np.log(2.))
            np.testing.assert_allclose(flux.getchunk().reshape(-1), np.array([52., 56.]) / areas, rtol=2e-6)
            assert flux.restoringbeam()["nChannels"] == 2
            major = channels.restoringbeam()["major"]
            arcseconds = major["value"] * {"arcsec": 1., "arcmin": 60., "deg": 3600.}[major["unit"]]
            assert arcseconds == pytest.approx(240.)
            np.testing.assert_array_equal(channels.getchunk(), pixels[..., 1:3].sum(axis=-1, keepdims=True))
        finally:
            flux.done()
            channels.done()
            source.done()

@pytest.mark.parametrize("function", ["sqrtsum", "rms", "stddev"])
def test_collapse_keeps_nan_values_and_mask(casa, function):
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=np.float32([np.nan, 1.]))
        product = source.collapse(function, 0)
        try:
            assert np.isnan(product.getchunk().item())
            assert product.getchunk(getmask=True).item() is True
        finally:
            product.done()
            source.done()


def test_collapse_preserves_existing_output_when_all_input_is_masked(tmp_path):
    source = tools.image()
    assert source.fromarray(pixels=np.float32([1., 5.]))
    assert source.putregion(pixelmask=np.zeros(2, dtype=bool))
    path = str(tmp_path / "existing.im")
    existing = tools.image()
    assert existing.fromarray(path, np.float32([11., 13.]))
    existing.done()
    before = set(tmp_path.iterdir())
    try:
        with pytest.raises(RuntimeError, match="All selected pixels are masked"):
            source.collapse("mean", 0, outfile=path, overwrite=True)
        assert set(tmp_path.iterdir()) == before
        assert existing.open(path)
        np.testing.assert_array_equal(existing.getchunk(), [11., 13.])
    finally:
        existing.done()
        source.done()


def test_collapse_named_axis_uses_independent_maps(casa):
    coords = casa.coordsys().newcoordsys(stokes="I,Q", spectral=True)
    assert coords.setreferencepixel([0., 0.])
    assert coords.setreferencevalue([1., 1.4e9])
    assert coords.setincrement([1., 2e6])
    record = coords.torecord()
    coords.done()
    record["worldmap0"] = np.array([1], dtype=np.int32)
    record["worldmap1"] = np.array([0], dtype=np.int32)
    source = tools.image()
    assert source.fromarray(pixels=np.float32([[1., 2., 3.], [10., 20., 30.]]), csys=record)
    result = source.collapse("sum", "Frequency")
    try:
        np.testing.assert_array_equal(result.getchunk(), [[6.], [60.]])
        np.testing.assert_allclose(result.toworld([1, 0])["numeric"], [1.402e9, 2.], rtol=0., atol=1e-6)
    finally:
        result.done()
        source.done()


def test_collapse_retains_center_of_longitude_field_over_half_a_turn(casa):
    coords = casa.coordsys().newcoordsys(direction=True)
    assert coords.setdirection(
        proj="CAR", refpix=[135., 0.], refval=["140deg", "0deg"], incr=["1deg", "1deg"],
    )
    assert coords.setunits(["deg", "deg"])
    record = coords.torecord()
    coords.done()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=np.ones((271, 1), dtype=np.float32), csys=record)
        product = source.collapse("sum", 0)
        coordinates = product.coordsys()
        try:
            np.testing.assert_array_equal(product.getchunk(), [[271.]])
            np.testing.assert_allclose(coordinates.referencevalue()["numeric"], [140., 0.], atol=1e-6)
        finally:
            coordinates.done()
            product.done()
            source.done()


@pytest.mark.parametrize("sparse", [False, True])
def test_collapse_complex_tied_median_preserves_casa_partition_order(casa, sparse):
    length = 262_147
    if sparse:
        data = np.zeros(length, dtype=np.complex64)
        data[:4] = [1. + 2.j, 2. + 1.j, -1. - 2.j, 6.j]
        valid = np.arange(length) < 4
        expected = .5 - .5j
    else:
        data = np.resize(np.complex64([1. + 2.j, 2. + 1.j, -1. - 2.j, -2. - 1.j]), length)
        valid = None
        expected = -1. - 2.j
    if not sparse:
        # CASA's parallel collector can vary the order of equal-magnitude samples.
        subprocess.run(
            [sys.executable, "-c", """
from casatools import image
import numpy as np
source = image()
assert source.fromarray(pixels=np.resize(np.complex64([1+2j, 2+1j, -1-2j, -2-1j]), 262147))
product = source.collapse("median", 0)
assert product.getchunk().item() == -1-2j
product.done()
source.done()
"""],
            env=dict(os.environ, OMP_NUM_THREADS="1"), check=True, capture_output=True,
        )
    for api in ((casa, tools) if sparse else (tools,)):
        source = api.image()
        assert source.fromarray(pixels=data)
        if valid is not None:
            assert source.putregion(pixelmask=valid)
        product = source.collapse("median", 0)
        try:
            np.testing.assert_array_equal(product.getchunk(), [expected])
        finally:
            product.done()
            source.done()

def test_collapse_disjoint_spatial_selections(casa):
    manager = casa.regionmanager()
    orig = casa.image()
    assert orig.fromshape(shape=[4, 5])
    coordinates = orig.coordsys()
    record = coordinates.torecord()
    coordinates.done()
    orig.done()
    members = {
        str(index): manager.fromtext(f"box [[{x}pix,{y}pix],[{x}pix,{y}pix]]", [4, 5], record)
        for index, (x, y) in enumerate(((0, 0), (3, 4)))
    }
    union = manager.makeunion(members)
    manager.done()
    for api in (casa, tools):
        source = api.image()
        assert source.fromarray(pixels=np.arange(20, dtype=np.float32).reshape(4, 5), csys=record)
        for options in ({"box": "0,0,0,0,3,4,3,4"}, {"region": union}):
            product = source.collapse("sum", [0, 1], **options)
            try:
                np.testing.assert_array_equal(product.getchunk(), [[19.]])
                assert product.getchunk(getmask=True).item() is True
            finally:
                product.done()
        source.done()
