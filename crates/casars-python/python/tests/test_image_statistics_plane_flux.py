"""Per-beam flux from the selected native channel and Stokes planes."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _image(api, path=""):
    cs = api.coordsys().newcoordsys(direction=True, spectral=True, stokes="I,Q")
    record = cs.torecord()
    values = np.broadcast_to(
        np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32),
        (4, 3, 2, 3),
    ).copy()
    image = api.image()
    assert image.fromarray(path, pixels=values, csys=record)
    assert image.setbrightnessunit("Jy/beam")
    for channel in range(3):
        for stokes in range(2):
            assert image.setrestoringbeam(
                major=f"{channel + stokes + 1}arcmin",
                minor="1arcmin",
                pa="0deg",
                channel=channel,
                polarization=stokes,
            )
    spectral = record["spectral2"]
    width = 299792.458 * abs(spectral["wcs"]["cdelt"]) / spectral["restfreq"]
    return image, values, width


def _expected_density(values):
    major = np.array([[1.0, 2.0, 3.0], [2.0, 3.0, 4.0]])
    beam_area_in_pixels = major * np.pi / (4 * np.log(2))
    return values.sum(axis=(0, 1)) / beam_area_in_pixels


@pytest.mark.parametrize("algorithm", ["classic", "chauvenet"])
def test_flux_uses_beam_from_each_retained_native_plane(casa, algorithm):
    left, values, _ = _image(tools)
    right, _, _ = _image(casa)
    density = _expected_density(values)
    for axis, expected in (([0, 1], density), ([0], np.broadcast_to(density / 3, (3, 2, 3)))):
        left_result = left.statistics(axes=axis, algorithm=algorithm)
        right_result = right.statistics(axes=axis, algorithm=algorithm)
        np.testing.assert_allclose(left_result["flux"], expected)
        _assert_value(left_result, right_result)
    for axis in ([], [0, 1, 2], [0, 1, 3], [2], [3]):
        assert "flux" not in left.statistics(axes=axis, algorithm=algorithm)
        assert "flux" not in right.statistics(axes=axis, algorithm=algorithm)
    assert left.done() and right.done()


@pytest.mark.parametrize("algorithm", ["classic", "chauvenet"])
def test_box_flux_integrates_selected_channels_after_applying_each_beam(casa, algorithm):
    left, values, width = _image(tools)
    right, _, _ = _image(casa)
    regionmanager = casa.regionmanager()
    density = _expected_density(values)
    selection = regionmanager.box([0, 0, 1, 0], [3, 2, 1, 2], [1, 1, 1, 2])
    for image in (left, right):
        result = image.statistics(region=selection, algorithm=algorithm)
        np.testing.assert_allclose(result["flux"], [density[1, ::2].sum() * 2 * width])
        result = image.statistics(region=selection, axes=[0, 1], algorithm=algorithm)
        np.testing.assert_allclose(result["flux"], density[1, ::2])
        assert "flux" not in image.statistics(region=selection, axes=[0, 1, 3], algorithm=algorithm)
        single_plane = regionmanager.box([0, 0, 1, 2], [3, 2, 1, 2])
        np.testing.assert_allclose(image.statistics(region=single_plane, algorithm=algorithm)["flux"], [density[1, 2]])
        one_channel = regionmanager.box([0, 0, 0, 1], [3, 2, 1, 1])
        np.testing.assert_allclose(image.statistics(axes=[0, 1], region=one_channel, algorithm=algorithm)["flux"], density[:, 1])
        assert image.done()


def test_individual_beam_flux_after_all_native_write_reopen_pairings(casa, tmp_path):
    for index, writer in enumerate((tools, casa)):
        path = str(tmp_path / f"beams-{index}.im")
        image, values, width = _image(writer, path)
        assert image.done()
        density = _expected_density(values)
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            np.testing.assert_allclose(image.statistics(axes=[0, 1])["flux"], density)
            region = casa.regionmanager().box([0, 0, 1, 0], [3, 2, 1, 2])
            np.testing.assert_allclose(image.statistics(region=region)["flux"], [density[1].sum() * width])
            assert image.done()
