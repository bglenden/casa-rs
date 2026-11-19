"""Public native beam queries on images written by CASA 6.7.6.14."""

import math

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize("kind", ["f", "c", "d", "cd"])
def test_single_restoring_beam_preserves_stored_units(tmp_path, casa, kind):
    path = str(tmp_path / "single.im")
    source = casa.image()
    assert source.fromshape(path, [2, 2], type=kind)
    assert source.setrestoringbeam("0.1arcmin", "2arcsec", "2rad", log=False)
    assert source.done()
    assert source.open(path)

    image = tools.image()
    assert image.restoringbeam() == image.beamarea() == image.commonbeam() == {}
    assert image.open(path)
    _assert_value(image.restoringbeam(9, 9), source.restoringbeam(9, 9))
    _assert_value(image.beamarea(), source.beamarea())
    assert image.beamarea()["arcsec2"] == pytest.approx(math.pi * 6 * 2 / (4 * math.log(2)))
    if kind in {"d", "cd"}:
        for backend in (source, image):
            with pytest.raises(RuntimeError, match="does not support images with"):
                backend.commonbeam()
    else:
        _assert_value(image.commonbeam(), source.commonbeam())
    for backend in (source, image):
        with pytest.raises(RuntimeError, match="does not"):
            backend.restoringbeam(mbret="matrix")
        assert backend.done()


@pytest.mark.parametrize("shape", [[2, 2, 2, 3], [2, 2, 3], [2, 2, 2, 1], [2, 2, 1, 3]])
def test_per_plane_beam_list_matrix_and_areas(tmp_path, casa, shape):
    path = str(tmp_path / "per-plane.im")
    source = casa.image()
    assert source.fromshape(path, shape)
    assert source.setrestoringbeam("0.1arcmin", "2arcsec", "2rad", channel=0, polarization=0, log=False)
    channel = 1 if len(shape) == 4 and shape[3] > 1 else 0
    polarization = 1 if shape[2] > 1 else 0
    assert source.setrestoringbeam("0.15arcmin", "4arcsec", "15deg", channel=channel, polarization=polarization, log=False)
    assert source.done()
    assert source.open(path)
    image = tools.image()
    assert image.open(path)
    for mode in ("LiSt", "m"):
        _assert_value(image.restoringbeam(mbret=mode), source.restoringbeam(mbret=mode))
    _assert_value(image.restoringbeam(channel, polarization), source.restoringbeam(channel, polarization))
    _assert_value(image.beamarea(), source.beamarea())
    _assert_value(image.beamarea(channel, polarization), source.beamarea(channel, polarization))
    expected_common = source.commonbeam()
    actual_common = image.commonbeam()
    for field in ("major", "minor", "pa"):
        assert actual_common[field]["unit"] == expected_common[field]["unit"]
        assert actual_common[field]["value"] == pytest.approx(expected_common[field]["value"], rel=1e-3)
    for backend in (source, image):
        with pytest.raises(RuntimeError, match="both channel and polarization"):
            backend.restoringbeam(channel, polarization, mbret="matrix")
        with pytest.raises(RuntimeError, match="Unsupported mbret"):
            backend.restoringbeam(mbret="")
        with pytest.raises(RuntimeError, match="channels"):
            backend.beamarea(9, 0)
        with pytest.raises(RuntimeError, match="both channel and polarization"):
            backend.beamarea(0, -1)
        assert backend.done()


def test_beam_queries_on_an_image_without_a_beam():
    image = tools.image()
    assert image.fromshape(shape=[2, 2])
    assert image.restoringbeam() == {}
    for query in (image.commonbeam, image.beamarea):
        with pytest.raises(RuntimeError, match=r"This image has no beam\(s\)"):
            query()
    assert image.done()
