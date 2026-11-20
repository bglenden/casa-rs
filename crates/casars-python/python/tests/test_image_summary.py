"""Structured and printed image.summary metadata against CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools

from test_coordsys_inspection import assert_equal as assert_casa_result


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def test_detached_summary_is_empty():
    assert tools.image().summary() == {}


@pytest.mark.parametrize(
    "shape, kind, linear",
    [
        ([3, 4], "f", True),
        ([5, 6, 7], "d", False),
        ([3, 4, 2, 5], "c", False),
        ([3, 4], "cd", False),
    ],
)
def test_temporary_summary_in_all_pixel_types(casa, shape, kind, linear):
    actual, expected = tools.image(), casa.image()
    for image in [actual, expected]:
        assert image.fromshape(shape=shape, type=kind, linear=linear)
    for doppler in ["RADIO", "OPTICAL", "BETA", "undefined"]:
        assert_casa_result(
            actual.summary(doppler, list=False),
            expected.summary(doppler, list=False),
        )
    info = actual.summary()
    assert info["messages"].dtype == np.dtype("U5")
    assert info["messages"].size == 0
    for image in [actual, expected]:
        assert image.done()


def test_summary_reads_native_masks_and_global_beam(casa, tmp_path):
    path = str(tmp_path / "summary.im")
    expected = casa.image()
    assert expected.fromshape(path, [3, 4, 2, 5])
    assert expected.setbrightnessunit("Jy/beam")
    assert expected.setrestoringbeam("4arcsec", "3arcsec", "-25deg")
    assert expected.calcmask(f'"{path}" > 1', "bright")
    assert expected.calcmask(f'"{path}" < 1', "faint", False)
    reference = expected.summary(list=False)
    assert expected.done()
    actual = tools.image()
    assert actual.open(path)
    assert_casa_result(actual.summary(list=False), reference)
    assert actual.done()


def test_summary_returns_per_plane_beams_in_channel_order(casa, tmp_path):
    path = str(tmp_path / "summarized-beams.im")
    expected = casa.image()
    assert expected.fromshape(path, [3, 4, 2, 3])
    for pol in range(2):
        for chan in range(3):
            assert expected.setrestoringbeam(
                f"{3 + chan + pol}arcsec", "2arcsec", "45deg",
                channel=chan, polarization=pol,
            )
    reference = expected.summary(list=False)
    assert expected.done()
    actual = tools.image()
    assert actual.open(path)
    assert_casa_result(actual.summary(list=False), reference)
    assert actual.done()


@pytest.mark.parametrize("format_unit", ["GHz", "mm"])
def test_summary_respects_saved_frame_spectral_units_and_beams(casa, format_unit, caplog):
    expected = casa.image()
    actual = tools.image()
    for image in [actual, expected]:
        assert image.fromshape(shape=[3, 4, 2, 4])
    cs = expected.coordsys()
    assert cs.setreferencecode("GALACTIC", type="direction", adjust=False)
    record = cs.torecord()
    assert cs.done()
    spectral = next(value for key, value in record.items() if key.startswith("spectral"))
    spectral.update(
        formatUnit=format_unit,
        restfreqs=np.array([1.42040575e9, 1.5e9]),
        velType=1,
        velUnit="m/s",
    )
    measures = casa.measures()
    record["telescopeposition"] = measures.position("WGS84", "10deg", "40deg", "20m")
    assert measures.done()
    for image in [actual, expected]:
        assert image.setcoordsys(record)
        assert image.setrestoringbeam("4arcsec", "3arcsec", "-90deg", channel=0, polarization=0)
    reference = expected.summary(list=False, pixelorder=False, verbose=True)
    assert "Max    0" in reference["messages"][0]
    assert "1145728" in reference["messages"][0]
    assert "1.42041e+09 [1.5e+09] Hz" in reference["messages"][1]
    assert_casa_result(actual.summary(list=False, pixelorder=False, verbose=True), reference)
    with caplog.at_level("INFO", logger="casars.tools.image"):
        assert actual.summary()["messages"].size == 0
    assert any("Max    0" in event.message for event in caplog.records)
    caplog.clear()
    with caplog.at_level("INFO", logger="casars.tools.image"):
        assert actual.summary(verbose=True)["messages"].size == 0
    assert any("  3 1.4150010e+09" in event.message for event in caplog.records)
    for image in [actual, expected]:
        assert image.done()


def test_summary_labels_a_slant_sin_north_celestial_pole_projection(casa):
    actual, expected = tools.image(), casa.image()
    for image in [actual, expected]:
        assert image.fromshape(shape=[3, 4])
    cs = expected.coordsys()
    record = cs.torecord()
    assert cs.done()
    direction = record["direction0"]
    direction["crval"] = np.array([0.0, 2700.0])
    direction["projection_parameters"] = np.array([0.0, 1.0])
    for image in [actual, expected]:
        assert image.setcoordsys(record)
    reference = expected.summary(list=False)
    assert "Right Ascension   NCP" in reference["messages"][1]
    assert_casa_result(actual.summary(list=False), reference)
    for image in [actual, expected]:
        assert image.done()


def test_summary_aligns_columns_for_saved_non_ascii_axis_names(casa):
    actual, expected = tools.image(), casa.image()
    for image in [actual, expected]:
        assert image.fromshape(shape=[3, 4], linear=True)
    cs = expected.coordsys()
    assert cs.setnames(["x-αβγδε", "δ-xyz"])
    record = cs.torecord()
    assert cs.done()
    for image in [actual, expected]:
        assert image.setcoordsys(record)
    assert_casa_result(actual.summary(list=False), expected.summary(list=False))
    for image in [actual, expected]:
        assert image.done()


@pytest.mark.parametrize("bitpix, blank, expected_mask", [(-32, None, True), (16, None, False), (16, -99, True)])
def test_summary_of_direct_fits_stays_available_after_rename(casa, tmp_path, bitpix, blank, expected_mask):
    path = tmp_path / "summary.fits"
    keywords = [("SIMPLE", "T"), ("BITPIX", bitpix), ("NAXIS", 2), ("NAXIS1", 3), ("NAXIS2", 4)]
    if blank is not None:
        keywords.append(("BLANK", blank))
    cards = "".join(f"{key:8}= {value:>20}".ljust(80) for key, value in keywords)
    path.write_bytes((cards + "END".ljust(80)).encode().ljust(2880, b" ") + bytes(2880))
    actual, expected = tools.image(), casa.image()
    for image in [actual, expected]:
        assert image.open(str(path))
    reference = expected.summary(list=False)
    assert reference["hasmask"] is expected_mask
    path.rename(tmp_path / "moved.fits")
    assert_casa_result(actual.summary(list=False), reference)
    assert actual.done()
    assert expected.done()
