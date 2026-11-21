"""CASA 6.7.6.14 moment images and native image interoperability."""

from pathlib import Path

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _cube():
    profile = 1. + 5. * np.exp(-((np.arange(19) - 9.3) / 2) ** 2)
    x, y = np.ogrid[:7, :6]
    return ((1. + x/12. + y/18.)[:, :, None, None] * profile).astype(np.float32)


def _read(image):
    return image.getchunk(), image.getchunk(getmask=True), image.brightnessunit()


def _compare(actual, expected):
    data, valid, unit = actual
    reference, selection, reference_unit = expected
    assert data.dtype == reference.dtype == np.float64
    assert valid.dtype == selection.dtype == np.bool_
    assert unit == reference_unit
    np.testing.assert_array_equal(valid, selection)
    np.testing.assert_allclose(data, reference, rtol=1e-3, atol=2e-5)


@pytest.mark.parametrize("number", range(-1, 12))
def test_public_moment_numbers(casa, number):
    results = []
    for api in (casa, tools):
        image = api.image()
        product = None
        try:
            values = _cube()
            assert image.fromarray(pixels=values)
            assert image.setbrightnessunit("K")
            mask = np.ones(values.shape, dtype=bool)
            mask[0, 0, :, :] = False
            mask[2, 2, :, 2:4] = False
            assert image.putregion(pixelmask=mask)
            options = {"includepix": [.8, 20.]} if number == 4 else {}
            product = image.moments(moments=[number], **options)
            assert image.isopen() and product.isopen()
            assert not product.ispersistent()
            results.append(_read(product))
            assert tuple(product.shape()) == (7, 6, 1)
            assert not results[-1][1][0, 0, 0]
            assert any("ia.moments(" in line for line in product.history())
        finally:
            if product is not None:
                product.done()
            image.done()
    _compare(results[1], results[0])


@pytest.mark.parametrize("options", [
    {"drop": False}, {"axis": 1}, {"axis": 2}, {"axis": -1},
    {"doppler": "OPTICAL"}, {"doppler": "RELATIVISTIC"}, {"doppler": "invalid"},
    {"doppler": "RATIO"}, {"doppler": "GAMMA"}, {"doppler": "TRUE"},
    {"includepix": [3.]}, {"excludepix": [1.25, 2.5]},
    {"method": ["window"], "stddev": .1}, {"method": ["fit"], "stddev": .1},
    {"method": ["window", "fit"], "stddev": .1},
    {"method": ["fit"]}, {"method": ["window", "fit"]},
    {"method": ["unknown"]}, {"method": ["window"], "stddev": .1, "peaksnr": 100.},
    {"smoothaxes": [3], "smoothtypes": "boxcar", "smoothwidths": [3.], "includepix": [2., 7.]},
    {"smoothaxes": [3], "smoothtypes": ["gaussian"], "smoothwidths": [2.], "includepix": [2., 7.]},
    {"smoothaxes": [3], "smoothtypes": ["hanning"], "includepix": [2., 7.]},
])
def test_moment_method_axis_and_velocity_variants(casa, options):
    results = []
    for api in (casa, tools):
        image = api.image()
        product = None
        try:
            assert image.fromarray(pixels=_cube())
            assert image.setbrightnessunit("K")
            product = image.moments(**options)
            results.append(_read(product))
        finally:
            if product is not None:
                product.done()
            image.done()
    _compare(results[1], results[0])


def test_moment_products_writer_reader_matrix(casa, tmp_path):
    results = {}
    coordinates = {}
    widths = (".weighted_coord", ".integrated", ".maximum")
    values = _cube()
    region = casa.regionmanager().box([1, 1, 0, 2], [5, 4, 0, 17], [1, 1, 1, 2])
    for writer in (casa, tools):
        infile = str(tmp_path / f"{writer.__name__}-input.im")
        outfile = str(tmp_path / f"{writer.__name__}-out.im")
        image = writer.image()
        product = None
        try:
            assert image.fromarray(infile, values)
            assert image.setbrightnessunit("K")
            assert image.sethistory(origin="test", history=["original cube"])
            product = image.moments([1, 0, 8], region=region, outfile=outfile)
            assert product.ispersistent()
            assert product.name().endswith(widths[0])
            assert tuple(product.shape()) == (5, 4, 1)
            np.testing.assert_array_equal(image.getchunk(), values)
        finally:
            if product is not None:
                product.done()
            image.done()
        for reader in (casa, tools):
            opened = reader.image()
            try:
                for suffix in widths:
                    assert opened.open(outfile + suffix)
                    assert any("original cube" in row for row in opened.history())
                    results[writer.__name__, reader.__name__, suffix] = _read(opened)
                    cs = opened.coordsys()
                    try:
                        coordinates[writer.__name__, reader.__name__, suffix] = (
                            cs.axiscoordinatetypes(), cs.ncoordinates(), cs.units(),
                            cs.referencepixel()["numeric"], cs.toworld([2., 1., 0.], "n")["numeric"],
                        )
                    finally:
                        cs.done()
                    opened.close()
            finally:
                opened.done()
    for key, result in results.items():
        suffix = key[-1]
        _compare(result, results[casa.__name__, casa.__name__, suffix])
        observed = coordinates[key]
        expected = coordinates[casa.__name__, casa.__name__, suffix]
        assert observed[:3] == expected[:3]
        np.testing.assert_allclose(observed[3], expected[3], atol=1e-10)
        np.testing.assert_allclose(observed[4], expected[4], atol=1e-7)


@pytest.mark.parametrize("kernel", ["boxcar", "gaussian", "hanning"])
def test_smoothing_image_has_input_coordinates_mask_and_miscinfo(casa, tmp_path, kernel):
    inputs = _cube()
    mask = np.ones(inputs.shape, dtype=bool)
    mask[..., 4] = False
    result = {}
    for writer in (casa, tools):
        image = writer.image()
        product = None
        output = str(tmp_path / f"{writer.__name__}.im")
        try:
            assert image.fromarray(pixels=inputs)
            assert image.setbrightnessunit("K")
            assert image.setrestoringbeam(major="2arcmin", minor="1arcmin", pa="4deg")
            assert image.setmiscinfo({"observer": "smoothing regression"})
            assert image.putregion(pixelmask=mask)
            product = image.moments(
                smoothaxes=[3], smoothtypes=kernel, smoothwidths=[3.],
                includepix=[.9, 20.], smoothout=output,
            )
            result[writer.__name__, "moment"] = _read(product)
            assert product.restoringbeam()
            np.testing.assert_array_equal(image.getchunk(getmask=True), mask)
        finally:
            if product is not None:
                product.done()
            image.done()
        for reader in (casa, tools):
            image = reader.image()
            try:
                assert image.open(output)
                assert tuple(image.shape()) == inputs.shape
                assert image.restoringbeam() == {}
                assert image.miscinfo()["observer"] == "smoothing regression"
                assert image.history() == []
                value = _read(image)
                assert value[2] == ""
                np.testing.assert_array_equal(value[1], mask)
                result[writer.__name__, reader.__name__] = value
            finally:
                image.done()
    for (writer, reader), value in result.items():
        expected = result[casa.__name__, "moment" if reader == "moment" else casa.__name__]
        _compare(value, expected)


def test_moment_mask_image_stretch_preserves_output_mask(casa, tmp_path):
    selection = np.ones((7, 6, 1, 1), dtype=np.float32)
    selection[0, 0] = 0.
    path = str(tmp_path / "selection.im")
    mask_image = tools.image()
    assert mask_image.fromarray(path, selection)
    assert mask_image.done()
    results = []
    for api in (casa, tools):
        image = api.image()
        product = None
        try:
            assert image.fromarray(pixels=_cube())
            with pytest.raises(RuntimeError):
                image.moments(mask=f'"{path}" > 0')
            product = image.moments(mask=f'"{path}" > 0', stretch=True)
            assert product.getchunk(getmask=True).sum() == selection.sum()
            results.append(_read(product))
        finally:
            if product is not None:
                product.done()
            image.done()
    _compare(results[1], results[0])


def test_spectral_moments_convolve_varying_beams_to_common_resolution(casa):
    x, y, f = np.ogrid[:24, :26, :19]
    values = ((1 + x/12 + y/18) * (1. + 5. * np.exp(-((f - 9.3) / 2) ** 2))).astype(np.float32)
    results = []
    for api in (casa, tools):
        image = api.image()
        product = None
        try:
            assert image.fromarray(pixels=values[:, :, None, :])
            assert image.setbrightnessunit("K")
            for channel in range(19):
                major, minor = ("3arcmin", "2.4arcmin") if channel % 2 else ("2arcmin", "1arcmin")
                assert image.setrestoringbeam(major=major, minor=minor, pa="0deg",
                                              channel=channel, polarization=0)
            product = image.moments()
            results.append(_read(product))
            beam = product.restoringbeam()
            assert beam["major"] == {"unit": "arcmin", "value": pytest.approx(3.)}
            assert beam["minor"] == {"unit": "arcmin", "value": pytest.approx(2.4)}
            assert beam["positionangle"] == {"unit": "deg", "value": pytest.approx(0.)}
            np.testing.assert_array_equal(image.getchunk()[:, :, 0, :], values)
        finally:
            if product is not None:
                product.done()
            image.done()
    _compare(results[1], results[0])


def test_moments_rejects_invalid_requests_without_replacing_a_product(tmp_path):
    input_path = str(tmp_path / "source.im")
    output_path = str(tmp_path / "output.im")
    image = tools.image()
    existing = tools.image()
    try:
        assert image.fromarray(input_path, _cube())
        assert existing.fromarray(output_path, np.array([[8.]], dtype=np.float32))
        assert existing.done()
        for options in (
            {"moments": []}, {"moments": [12]}, {"moments": [4]},
            {"axis": 4}, {"includepix": [1., 2.], "excludepix": [3., 4.]},
            {"smoothaxes": [3], "smoothtypes": ["boxcar"], "smoothwidths": [3.]},
        ):
            with pytest.raises(RuntimeError):
                image.moments(outfile=output_path, overwrite=True, **options)
            assert image.isopen()
            assert existing.open(output_path)
            np.testing.assert_array_equal(existing.getchunk(), [[8.]])
            existing.close()
        with pytest.raises(RuntimeError):
            image.moments(outfile=input_path, overwrite=True)
        assert Path(input_path).is_dir()
    finally:
        existing.done()
        image.done()
