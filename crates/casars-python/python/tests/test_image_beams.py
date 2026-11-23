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


def test_selected_beams_keep_saved_quantities_after_native_reopen(casa, tmp_path):
    region = casa.regionmanager().box([1, 1, 2, 1], [3, 3, 5, 2])
    expected = {
        "major": {"value": 0.125, "unit": "arcmin"},
        "minor": {"value": 3.5, "unit": "arcsec"},
        "positionangle": {"value": 11., "unit": "deg"},
    }
    for writer in (casa, tools):
        source = writer.image()
        try:
            assert source.fromshape(shape=[6, 5, 8, 4])
            assert source.setrestoringbeam("7arcsec", "3arcsec", "6deg", channel=0, polarization=0)
            assert source.setrestoringbeam(
                "0.125arcmin", "3.5arcsec", "11deg", channel=2, polarization=1,
            )
            paths = {
                name: str(tmp_path / f"{writer.__name__}-{name}.im")
                for name in ("subimage", "collapsed", "line", "continuum")
            }
            source.subimage(paths["subimage"], region=region).done()
            source.collapse("mean", [2], paths["collapsed"], region=region).done()
            source.continuumsub(
                outline=paths["line"], outcont=paths["continuum"], region=region,
            ).done()
        finally:
            source.done()
        for name, path in paths.items():
            for reader in (casa, tools):
                image = reader.image()
                try:
                    assert image.open(path)
                    beam = image.restoringbeam() if name == "collapsed" else image.restoringbeam(0, 0)
                    assert beam == expected, (writer.__name__, name, reader.__name__)
                finally:
                    image.done()


@pytest.mark.parametrize("writer", ["casa", "casars"])
def test_write_native_beams_and_copy_to_another_image(tmp_path, casa, writer):
    source_module, reader_module = (casa, tools) if writer == "casa" else (tools, casa)
    path = str(tmp_path / "written.im")
    copied = str(tmp_path / "copied.im")
    tool = source_module.image()
    assert tool.fromshape(path, [2, 2, 2, 3])
    assert tool.setrestoringbeam("0.1arcmin", "2arcsec", "2rad", channel=0, log=False)
    assert tool.setrestoringbeam("7arcsec", "3arcsec", "17deg", polarization=0, log=False)
    with pytest.raises(RuntimeError, match="Failed AlwaysAssert"):
        tool.setrestoringbeam(7, 3, 17, polarization=0, log=False)
    assert tool.setrestoringbeam(0.15, 4, 1, channel=2, polarization=1, log=False)
    assert tool.setrestoringbeam(
        beam={
            "major": {"value": 9, "unit": "arcsec"},
            "minor": {"value": 4, "unit": "arcsec"},
            "positionangle": {"value": 190, "unit": "deg"},
        },
        channel=1, polarization=1, log=False,
    )
    wanted = tool.restoringbeam()
    assert wanted["beams"]["*0"]["*1"]["major"] == {"unit": "arcmin", "value": 0.1}
    assert wanted["beams"]["*0"]["*0"]["major"] == {"unit": "arcsec", "value": 7.0}
    assert wanted["beams"]["*1"]["*1"]["positionangle"]["value"] == pytest.approx(10.0)
    assert wanted["beams"]["*2"]["*1"]["major"] == {"unit": "arcmin", "value": 0.15}
    assert tool.history()[-2] == "Ran ia.setrestoringbeam"
    assert tool.done()

    reader = reader_module.image()
    assert reader.open(path)
    _assert_value(reader.restoringbeam(), wanted)
    assert "'value': 190" in reader.history()[-1]
    assert reader.done()
    assert reader.fromshape(copied, [2, 2, 2, 3], type="cd")
    assert reader.setrestoringbeam(imagename=path, log=False)
    _assert_value(reader.restoringbeam(), wanted)
    assert reader.done()

    assert tool.open(copied)
    _assert_value(tool.restoringbeam(), wanted)
    prior_history = tool.history()
    assert tool.dohistory(False)
    assert tool.setrestoringbeam(remove=True, log=False)
    assert tool.history() == prior_history
    assert tool.restoringbeam() == {}
    assert tool.done()
    assert reader.open(copied)
    assert reader.restoringbeam() == {}
    assert reader.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_beam_update_failures_preserve_temporary_state(tmp_path, backend):
    if backend == "casa":
        module = pytest.importorskip("casatools")
    else:
        module = tools
    tool = module.image()
    assert tool.setrestoringbeam(remove=True) is False
    assert tool.fromshape(shape=[2, 2])
    with pytest.raises(RuntimeError, match="major must be specified"):
        tool.setrestoringbeam()
    assert tool.setrestoringbeam(major={"value": 0.1, "unit": "arcmin"}, minor=3, pa="2rad", log=False)
    original = tool.restoringbeam()
    original_history = tool.history()
    with pytest.raises(RuntimeError, match="greater or equal"):
        tool.setrestoringbeam("2arcsec", "3arcsec", "0deg", log=False)
    with pytest.raises(RuntimeError, match="global restoring beam"):
        tool.setrestoringbeam(5, 2, 0, channel=0, log=False)
    with pytest.raises(RuntimeError, match="positionangle missing"):
        tool.setrestoringbeam(beam={"major": original["major"], "minor": original["minor"], "pa": original["positionangle"]})
    with pytest.raises(RuntimeError, match="does not exist"):
        tool.setrestoringbeam(imagename=str(tmp_path / "missing.im"))
    _assert_value(tool.restoringbeam(), original)
    assert tool.history() == original_history
    assert tool.setrestoringbeam(2.5, 1.1, 0, log=False)
    assert tool.commonbeam()["major"] == {"unit": "arcmin", "value": 2.5}
    assert tool.done()


def test_invalid_beam_removal_preserves_native_metadata(tmp_path):
    path = str(tmp_path / "invalid-removal.im")
    tool = tools.image()
    assert tool.fromshape(path, [2, 2])
    assert tool.setrestoringbeam(3, 2, 12, log=False)
    beam = tool.restoringbeam()
    history = tool.history()
    with pytest.raises(RuntimeError, match="field major is unknown"):
        tool.setrestoringbeam(remove=True, beam={"junk": 2}, log=False)
    assert tool.done()
    assert tool.open(path)
    _assert_value(tool.restoringbeam(), beam)
    assert tool.history() == history
    assert tool.done()
