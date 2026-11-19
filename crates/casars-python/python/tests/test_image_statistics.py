"""Classical statistics through the CASA-compatible image tool."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


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
        dict(axes=[0], includepix=[7.0, 9.0]),
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


def test_statistics_strided_extrema_are_absolute_image_pixels(casa):
    image = tools.image()
    values = np.arange(24, dtype=np.float64).reshape(4, 3, 2)
    assert image.fromarray(pixels=values)
    region = casa.regionmanager().box([1, 1, 0], [3, 2, 1], [2, 1, 1])
    result = image.statistics(region=region)
    selected = values[1:4:2, 1:3, :]
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
