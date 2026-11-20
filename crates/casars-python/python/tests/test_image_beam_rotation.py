"""Public CASA 6.7.6.14 beam rotation and native/FITS metadata readback."""

from pathlib import Path

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().startswith("6.7.6-14")
    return module


@pytest.mark.parametrize("pixeltype", ["f", "c"])
@pytest.mark.parametrize("scope", ["single", "grid", "empty"])
def test_rotatebeam_matches_casa_and_native_round_trip(casa, tmp_path, pixeltype, scope):
    name = "beam.im"
    observations = []
    for directory, factory in (("casa", casa.image), ("rust", tools.image)):
        image = factory()
        path = tmp_path / directory / name
        path.parent.mkdir()
        shape = [3, 4, 2, 3] if scope == "grid" else [3, 4]
        assert image.fromshape(str(path), shape, type=pixeltype)
        if scope == "single":
            assert image.setrestoringbeam("3arcmin", "4arcsec", "1rad")
        elif scope == "grid":
            assert image.setrestoringbeam("8arcsec", "4arcsec", "100deg", channel=0, polarization=0)
            assert image.setrestoringbeam("15arcsec", "6arcsec", "2rad", channel=1, polarization=1)

        before = len(image.history())
        assert image.rotatebeam(angle="75deg")
        if scope == "single":
            assert image.restoringbeam()["positionangle"]["value"] == pytest.approx(
                1 + np.deg2rad(75) - np.pi
            )
        assert image.dohistory(False) is True
        assert image.rotatebeam({"value": -1, "unit": "rad"})
        current = image.restoringbeam()
        history = [line.replace(str(path), name) for line in image.history()[before:]]
        assert sum(line == "Ran ia.rotatebeam" for line in history) == 1
        np.testing.assert_array_equal(image.getchunk(), np.zeros(shape))
        image.done()

        reader = casa.image()
        assert reader.open(str(path))
        _assert_value(reader.restoringbeam(), current)
        reader.done()
        observations.append((current, history))
    _assert_value(observations[0][0], observations[1][0])
    assert observations[0][1] == observations[1][1]


def test_rotatebeam_rejects_nonangular_and_unsupported_inputs(casa):
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.rotatebeam() is False
        assert image.fromshape(shape=[3, 4])
        assert image.setrestoringbeam("5arcsec", "4arcsec", "3deg")
        before = image.restoringbeam()
        history = image.history()
        for angle in (None, "1m", 15.0):
            with pytest.raises(RuntimeError, match="not an angle"):
                image.rotatebeam(angle)
            assert image.restoringbeam() == before
            assert image.history() == history
        image.done()
        for kind in ("d", "cd"):
            assert image.fromshape(shape=[3, 4], type=kind)
            with pytest.raises(RuntimeError, match="precision pixel values"):
                image.rotatebeam("15deg")
            image.done()


def test_rotatebeam_on_direct_fits_changes_only_open_handle(casa, tmp_path):
    path = str(tmp_path / "beam.fits")
    image = casa.image()
    assert image.fromshape(shape=[3, 4])
    assert image.setrestoringbeam("3arcsec", "2arcsec", "4deg")
    assert image.tofits(path)
    image.done()
    before = Path(path).read_bytes()
    for factory in (casa.image, tools.image):
        attached = factory()
        assert attached.open(path)
        assert attached.rotatebeam("15deg")
        assert attached.restoringbeam()["positionangle"]["value"] == pytest.approx(19.0)
        attached.done()
        assert before == Path(path).read_bytes()
        assert attached.open(path)
        assert attached.restoringbeam()["positionangle"]["value"] == pytest.approx(4.0)
        attached.done()
