"""Public CASA 6.7.6.14 beam rotation and native/FITS metadata readback."""

import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pytest

from casars import tools
from test_image_deconvolve_components import components
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


@pytest.mark.parametrize("has_beam", [False, True])
def test_rotatebeam_keeps_independent_open_views(casa, tmp_path, has_beam):
    for directory, factory in (("casa", casa.image), ("rust", tools.image)):
        path = str(tmp_path / f"{directory}.im")
        first, second = factory(), factory()
        assert first.fromshape(path, [3, 4])
        if has_beam:
            assert first.setrestoringbeam("3arcsec", "2arcsec", "4deg")
        assert second.open(path)
        assert first.rotatebeam("13deg")
        assert second.rotatebeam("15deg")
        if has_beam:
            assert first.restoringbeam()["positionangle"]["value"] == pytest.approx(17.0)
            assert second.restoringbeam()["positionangle"]["value"] == pytest.approx(19.0)
        else:
            assert second.history()[-4].endswith("pa: 0 deg")
            assert second.history()[-3].endswith("pa: 15 deg")
        first.done()
        second.done()
        assert second.open(path)
        if has_beam:
            assert second.restoringbeam()["positionangle"]["value"] == pytest.approx(19.0)
        second.done()


def test_beam_consumers_preserve_each_open_tools_view(casa, tmp_path):
    source = components(casa, major="12arcsec", minor="9arcsec")
    observations = []
    for directory, factory in (("casa", casa.image), ("rust", tools.image)):
        path = str(tmp_path / f"{directory}.im")
        writer, reader = factory(), factory()
        assert writer.fromshape(path, [3, 4])
        assert writer.setbrightnessunit("Jy/beam")
        assert writer.setrestoringbeam("4arcsec", "2arcsec", "12deg")
        assert reader.open(path)
        assert writer.setrestoringbeam("8arcsec", "4arcsec", "27deg")
        assert writer.rotatebeam("15deg")
        records = []
        for tool, major in ((writer, 8.0), (reader, 4.0)):
            beam = tool.summary(list=False)["restoringbeam"]
            assert beam["major"] == {"unit": "arcsec", "value": major}
            assert beam == tool.restoringbeam()
            flux = tool.convertflux("3Jy", "12arcsec", "9arcsec")
            deconvolved = tool.deconvolvecomponentlist(source)["component0"]["shape"]
            header = tool.fitsheader()
            beam_cards = {key: header[key] for key in ("BMAJ", "BMIN", "BPA")}
            assert beam_cards["BMAJ"] == pytest.approx(major / 3600)
            records.append((beam, flux, deconvolved, beam_cards))
            tool.done()
        assert records[0][1] != records[1][1]
        assert records[0][2] != records[1][2]
        observations.append(records)
    for actual, expected in zip(observations[1], observations[0], strict=True):
        for actual_record, expected_record in zip(actual, expected, strict=True):
            _assert_value(actual_record, expected_record)


@pytest.mark.parametrize(
    ("metadata", "message"),
    [(1, "imageinfo is not a record"), ({"imagetype": 1}, "imageinfo.imagetype")],
)
def test_beam_consumers_reject_malformed_native_metadata(tmp_path, metadata, message):
    path = str(tmp_path / "malformed.im")
    image = tools.image()
    assert image.fromshape(path, [3, 4])
    assert image.setbrightnessunit("Jy/beam")
    assert image.done()
    table = tools.table()
    assert table.open(path, nomodify=False)
    assert table.putkeyword("imageinfo", metadata)
    assert table.done()
    assert image.open(path)
    with pytest.raises(RuntimeError, match="Major axis must have angular units"):
        image.convertflux("3Jy", "1s", "2arcsec")
    with pytest.raises(RuntimeError, match=message):
        image.convertflux("3Jy", "4arcsec", "2arcsec")
    with pytest.raises(RuntimeError, match=message):
        image.deconvolvecomponentlist({})
    assert image.done()


def test_rotatebeam_resets_empty_view_on_rename(casa, tmp_path):
    for directory, factory in (("casa", casa.image), ("rust", tools.image)):
        tool = factory()
        assert tool.fromshape(str(tmp_path / f"{directory}.im"), [3, 4])
        assert tool.rotatebeam("13deg")
        assert tool.rename(str(tmp_path / f"{directory}-renamed.im"))
        assert tool.rotatebeam("3deg")
        assert tool.history()[-4].endswith("pa: 0 deg")
        assert tool.history()[-3].endswith("pa: 3 deg")
        tool.done()


def test_rotatebeam_keeps_previous_extremum_on_equal_area(casa):
    for factory in (casa.image, tools.image):
        tool = factory()
        assert tool.fromshape(shape=[3, 4, 2, 3])
        assert tool.setrestoringbeam("5arcsec", "2arcsec", "0deg", channel=0, polarization=0)
        assert tool.setrestoringbeam("10arcsec", "4arcsec", "30deg", channel=2, polarization=0)
        assert tool.setrestoringbeam("10arcsec", "4arcsec", "10deg", channel=1, polarization=0)
        assert tool.rotatebeam("1deg")
        assert tool.history()[-2].endswith("pa: 30 deg")
        assert tool.history()[-1].endswith("pa: 31 deg")
        tool.done()


def test_rotatebeam_normalizes_existing_one_plane_grid(casa, tmp_path):
    for directory, factory in (("casa", casa.image), ("rust", tools.image)):
        path = str(tmp_path / f"{directory}.im")
        tool = factory()
        assert tool.fromshape(path, [3, 4])
        assert tool.setrestoringbeam("3arcsec", "2arcsec", "4deg")
        tool.done()
        table = casa.table()
        assert table.open(path, nomodify=False)
        info = table.getkeyword("imageinfo")
        beam = info.pop("restoringbeam")
        info["perplanebeams"] = {"nChannels": 1, "nStokes": 1, "*0": beam}
        assert table.putkeyword("imageinfo", info)
        table.done()
        assert tool.open(path)
        assert tool.rotatebeam("13deg")
        assert tool.restoringbeam()["positionangle"]["value"] == pytest.approx(17.0)
        tool.done()
        assert table.open(path)
        assert "perplanebeams" not in table.getkeyword("imageinfo")
        table.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_rotatebeam_waits_for_an_existing_native_log_writer(casa, tmp_path):
    path = tmp_path / "locked-log.im"
    creator = casa.image()
    assert creator.fromshape(str(path), [3, 4])
    assert creator.setrestoringbeam("3arcsec", "2arcsec", "4deg")
    creator.done()
    tool = tools.image()
    assert tool.open(str(path))
    history = tool.history()
    local = tools.table()
    assert local.open(str(path / "logtable"), nomodify=False, lockoptions={"option": "user"})
    assert local.lock(True, 1)
    try:
        with pytest.raises(RuntimeError, match="lock"):
            tool.rotatebeam("13deg")
        assert tool.restoringbeam()["positionangle"]["value"] == pytest.approx(4.0)
    finally:
        assert local.unlock() and local.done()
    assert tool.history() == history
    holder = subprocess.Popen(
        [sys.executable, "-c", """
import sys
import time
from pathlib import Path
from casatools import table
log = table()
assert log.open(sys.argv[1], nomodify=False, lockoptions={"option": "user"})
assert log.lock(True, 1)
Path(sys.argv[2]).touch()
time.sleep(1.2)
assert log.unlock() and log.done()
""", str(path / "logtable"), str(tmp_path / "ready")],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not (tmp_path / "ready").exists() and holder.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert (tmp_path / "ready").exists()
        assert tool.rotatebeam("13deg")
        assert holder.wait(timeout=5) == 0, holder.stderr.read()
        assert tool.restoringbeam()["positionangle"]["value"] == pytest.approx(17.0)
        assert tool.history().count("Ran ia.rotatebeam") == 1
        tool.done()
    finally:
        if holder.poll() is None:
            holder.kill()
        holder.communicate(timeout=5)
