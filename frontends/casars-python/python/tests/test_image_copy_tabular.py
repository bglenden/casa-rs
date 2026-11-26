"""Generic tabular axes retain the CASA lookup after image copy and reopen."""

import numpy as np
import pytest

from casars import tools


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("case", ["cropped", "strided", "dropped"])
def test_copy_tabular_coordinate_reopens_with_matching_lookup(casa, tmp_path, method, case):
    pixels = np.arange(6 * 9, dtype=np.float32).reshape(6, 9)
    stop = [5, 8]
    if case == "strided":
        start, stride = [1, 2], [1, 2]
        expected_samples, reference, increment = [17, 25, 57, 73], -1, 7
    elif case == "cropped":
        start, stop, stride = [1, 3], [5, 6], [1, 1]
        expected_samples, reference, increment = [20.5, 24.5, 40.5, 48.5], -3, 3.5
    else:
        start, stop, stride = [1, 4], [5, 4], [1, 1]
    drop = case == "dropped"

    section = tuple(slice(lo, hi + 1, step) for lo, hi, step in zip(start, stop, stride))
    expected = pixels[section]
    if drop:
        expected = expected[:, 0]

    manager = casa.regionmanager()
    try:
        region = manager.box(start, stop, inc=stride)
    finally:
        manager.done()

    for api in (casa, tools):
        source = str(tmp_path / f"{api.__name__}-source.im")
        destination = str(tmp_path / f"{api.__name__}-copy.im")
        image = api.image()
        product = None
        coordinates = api.coordsys().newcoordsys(linear=1, tabular=True)
        try:
            assert coordinates.settabular([0, 2, 5, 8], [10, 14, 30, 38])
            assert image.fromarray(source, pixels, coordinates.torecord())
            assert image.calcmask(f'"{source}" < 40', "valid")
            assert image.done()
            result = getattr(image, method)(
                infile=source, outfile=destination, region=region, dropdeg=drop,
            )
            product = image if result is True else result
            np.testing.assert_array_equal(product.getchunk(), expected)
            np.testing.assert_array_equal(product.getchunk(getmask=True), expected < 40)
            if not drop:
                live = product.coordsys()
                try:
                    lookup = live.torecord()["tabular1"]
                    np.testing.assert_allclose(lookup["crpix"], [reference], atol=1e-12)
                    np.testing.assert_allclose(lookup["crval"], [10], atol=1e-12)
                    np.testing.assert_allclose(lookup["worldvalues"], expected_samples, atol=1e-12)
                finally:
                    live.done()
        finally:
            coordinates.done()
            if product is not None and product is not image:
                product.done()
            image.done()

        for reader_api in (casa, tools):
            reader = reader_api.image()
            coordinates = None
            try:
                assert reader.open(destination)
                np.testing.assert_array_equal(reader.getchunk(), expected)
                np.testing.assert_array_equal(reader.getchunk(getmask=True), expected < 40)
                coordinates = reader.coordsys()
                record = coordinates.torecord()
                if drop:
                    assert "tabular1" not in record
                    np.testing.assert_allclose(coordinates.toworld([0])["numeric"], [1])
                    continue
                lookup = record["tabular1"]
                np.testing.assert_array_equal(lookup["pixelvalues"], [0, 2, 5, 8])
                np.testing.assert_allclose(lookup["worldvalues"], expected_samples, atol=1e-12)
                np.testing.assert_allclose(lookup["crpix"], [0], atol=1e-12)
                np.testing.assert_allclose(lookup["crval"], [expected_samples[0]], atol=1e-12)
                np.testing.assert_allclose(lookup["cdelt"], [increment], atol=1e-12)
                for pixel in [0, 1.25, 3]:
                    world = np.interp(pixel, [0, 2, 5, 8], expected_samples)
                    np.testing.assert_allclose(coordinates.toworld([0, pixel])["numeric"], [1, world])
                    np.testing.assert_allclose(coordinates.topixel([1, world])["numeric"], [0, pixel])
            finally:
                if coordinates is not None:
                    coordinates.done()
                reader.done()
