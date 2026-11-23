"""Read FITS headers through the public image tool without exporting pixels."""

import re

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


@pytest.mark.parametrize("shape,linear", [([3, 4], False), ([3, 4, 2, 5], False), ([3, 4, 2, 5], True)])
def test_fitsheader_matches_casa_dictionary(casa, shape, linear):
    output = []
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.fromshape(shape=shape, linear=linear)
        assert image.setbrightnessunit("Jy/beam")
        assert image.setrestoringbeam("4arcsec", "2arcsec", "120deg")
        assert image.setmiscinfo({"floats": [1.25, 2.5], "label": "sample", "flag": True})
        assert image.sethistory("test", ["native header marker"])
        header = image.fitsheader()
        assert header["BITPIX"] == -32
        np.testing.assert_array_equal(header["NAXIS"], [len(shape), *shape])
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}", header["DATE"])
        assert "native header marker" in header["HISTORY"]
        assert "HISTORY" not in image.fitsheader(exclude="HISTORY")
        assert "HISTORY" in image.fitsheader(exclude=["history", "NAXIS1"])
        assert "NAXIS" not in image.fitsheader(exclude=["NAXIS", "TIME"])
        with pytest.raises(TypeError, match="must contain strings"):
            image.fitsheader(exclude=[4])
        output.append(image.fitsheader(False, ["DATE", "HISTORY"]))
        image.done()
    _assert_value(output[1], output[0])


@pytest.mark.parametrize(
    "mjd,expected",
    [
        (61364.60953093624, "2026-11-20T14:37:43.472892"),
        (61364.60953093631, "2026-11-20T14:37:43.472896"),
    ],
)
def test_fitsheader_matches_casa_at_date_obs_rounding_boundaries(casa, mjd, expected):
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.fromshape(shape=[5, 6])
        coords = image.coordsys()
        record = coords.torecord()
        assert coords.done()
        record["obsdate"]["m0"]["value"] = mjd
        assert image.setcoordsys(record)
        assert image.fitsheader()["DATE-OBS"] == expected
        assert image.done()


def test_fitsheader_preserves_stored_beam_angle_and_defaults_unset_epoch_to_utc(casa):
    for api in (casa, tools):
        image = api.image()
        try:
            assert image.fromshape(shape=[5, 6])
            assert image.setrestoringbeam("5arcsec", "4arcsec", "6deg")
            coordinates = image.coordsys()
            record = coordinates.torecord()
            assert coordinates.done()
            for mjd, system, date in [
                (0., "UTC", None),
                (51544., "LAST", "2000-01-01T00:00:00.000000"),
            ]:
                record["obsdate"] = {
                    "type": "epoch", "refer": "LAST", "m0": {"value": mjd, "unit": "d"},
                }
                assert image.setcoordsys(record)
                header = image.fitsheader(exclude="HISTORY")
                assert header["TIMESYS"].strip() == system
                assert header.get("DATE-OBS") == date
                assert header["BPA"] == 6.0
        finally:
            image.done()


def test_primary_fits_beam_preserves_exact_angle_on_attach_and_import(casa, tmp_path):
    fits = str(tmp_path / "beam.fits")
    source = casa.image()
    try:
        assert source.fromshape(shape=[5, 6])
        assert source.setrestoringbeam("5arcsec", "4arcsec", "6deg")
        assert source.tofits(fits)
    finally:
        source.done()

    for writer in (casa, tools):
        product = str(tmp_path / f"{writer.__name__}-beam.im")
        image = writer.image()
        try:
            assert image.open(fits)
            assert image.restoringbeam()["positionangle"] == {"value": 6., "unit": "deg"}
            assert image.done()
            assert image.fromfits(product, fits)
        finally:
            image.done()
        for reader in (casa, tools):
            image = reader.image()
            try:
                assert image.open(product)
                assert image.restoringbeam()["positionangle"] == {"value": 6., "unit": "deg"}
            finally:
                image.done()


def test_fitsheader_raw_cards_and_native_name(casa, tmp_path):
    path = str(tmp_path / ("native-" + "x" * 75 + ".im"))
    creator = tools.image()
    assert creator.fromshape(path, [3, 4])
    assert creator.sethistory("test", ["preserve native history"])
    assert creator.done()
    outputs = []
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.open(path)
        assert image.fitsheader(exclude="HISTORY")["IMAGENME"] == path[-68:]
        raw = image.fitsheader(True, "HISTORY")
        assert len(raw) % 80 == 0
        cards = [raw[index:index + 80] for index in range(0, len(raw), 80)]
        assert any(card.startswith("HISTORY preserve native history") for card in cards)
        assert cards[-2].strip() == "END"
        assert cards[-1].startswith("IMAGENME= ")
        outputs.append([card for card in cards if not card.startswith(("DATE    ", "HISTORY"))])
        image.done()
    assert outputs[0] == outputs[1]


def test_fitsheader_detached_and_nonfloat(casa):
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.fitsheader() == {}
        assert image.fitsheader(True) == ""
        for kind in ("d", "c", "cd"):
            assert image.fromshape(shape=[3, 4], type=kind)
            with pytest.raises(RuntimeError, match="only supports floating point"):
                image.fitsheader()
            image.done()


def test_fitsheader_uses_casa_misc_keyword_values(casa):
    outputs = []
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.fromshape(shape=[3, 4])
        assert image.setmiscinfo({
            "quotes": "O'Brian",
            "utf": "é",
            "positive": 112233445566,
            "ignored": np.array([1 + 2j, 3 + 4j]),
            "nested": {"ignored": 1},
        })
        before = image.history()
        header = image.fitsheader(exclude=["DATE", "HISTORY"])
        assert header["POSITIVE"] == 564295870
        assert header["UTF"] == "é      "
        assert header["QUOTES"] == "O'Brian "
        assert "IGNORED" not in header and "NESTED" not in header
        assert image.history() == before
        outputs.append(header)
        image.done()
    _assert_value(outputs[1], outputs[0])


@pytest.mark.parametrize("value", [3 + 4j, [[1, 2], [3, 4]], "z" * 120])
def test_fitsheader_rejects_misc_values_that_casa_cannot_encode(casa, value):
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.fromshape(shape=[3, 4])
        assert image.setmiscinfo({"invalid": value})
        with pytest.raises(RuntimeError, match="header creation failed"):
            image.fitsheader(exclude="INVALID")
        assert image.shape().tolist() == [3, 4]
        image.done()


def test_fitsheader_preserves_matrix_cards_and_repeated_text(casa):
    outputs = []
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.fromshape(shape=[3, 4])
        assert image.setmiscinfo({
            "xy": [[1, 2], [3, 4]],
            "xz": [[5, 6], [7, 8]],
            "COMMENTone": "remark",
            "COMMENTtwo": "another remark",
            "HISTORYmine": "custom log",
            "OBSERVER": "custom observer",
        })
        header = image.fitsheader(exclude=["DATE", "HISTORY"])
        assert header["XY2_1"] == 3
        np.testing.assert_array_equal(header["COMMENT"], ["remark", "another remark"])
        np.testing.assert_array_equal(header["OBSERVER"], ["Karl Jansky", "custom observer"])
        raw = image.fitsheader(True)
        assert "HISTORY custom log" in raw
        names = [raw[index:index + 8].strip() for index in range(0, len(raw), 80)]
        assert [name for name in names if name.startswith(("XY", "XZ"))] == [
            "XY1_1", "XZ1_1", "XY2_1", "XZ2_1", "XY1_2", "XZ1_2", "XY2_2", "XZ2_2",
        ]
        outputs.append(header)
        image.done()
    _assert_value(outputs[1], outputs[0])


@pytest.mark.parametrize("stokes", ["XX YY", "RR LL", "I V", "I Q V"])
def test_fitsheader_encodes_actual_stokes_planes(casa, tmp_path, stokes):
    path = str(tmp_path / "stokes.im")
    source = casa.image()
    assert source.fromshape(path, [3, 4, len(stokes.split()), 3])
    coords = source.coordsys()
    assert coords.setstokes(stokes)
    assert source.setcoordsys(coords.torecord())
    coords.done()
    source.done()
    outputs = []
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.open(path)
        outputs.append(image.fitsheader(exclude=["DATE", "HISTORY"]))
        image.done()
    _assert_value(outputs[1], outputs[0])


@pytest.mark.parametrize("frame", ["B1950_VLA", "AZEL", "ICRS"])
def test_fitsheader_uses_native_celestial_frame(casa, tmp_path, frame):
    path = str(tmp_path / "frame.im")
    source = casa.image()
    assert source.fromshape(path, [3, 4])
    coords = source.coordsys()
    assert coords.setreferencecode(frame, "direction", False)
    assert source.setcoordsys(coords.torecord())
    coords.done()
    source.done()
    outputs = []
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.open(path)
        outputs.append(image.fitsheader(exclude=["DATE", "HISTORY"]))
        image.done()
    _assert_value(outputs[1], outputs[0])


def test_fitsheader_linear_name_no_rest_frequency_and_multibeam_card(casa):
    oracle = casa.coordsys().newcoordsys(linear=2, spectral=True)
    assert oracle.setnames(["averylongaxis", "blah"], "linear")
    assert oracle.setrestfrequency("0Hz")
    coordinates = oracle.torecord()
    oracle.done()
    outputs = []
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.fromshape(shape=[3, 4, 3], csys=coordinates)
        assert image.setrestoringbeam("8arcsec", "4arcsec", "30deg", channel=0)
        assert image.setrestoringbeam("15arcsec", "6arcsec", "20deg", channel=1)
        header = image.fitsheader(exclude=["DATE", "HISTORY"])
        assert "SPECSYS" not in header
        assert "AVERYLON" in header["CTYPE"]
        raw = image.fitsheader(True)
        names = [raw[index:index + 8].strip() for index in range(0, len(raw), 80)]
        assert names[names.index("ORIGIN") + 1] == "CASAMBM"
        outputs.append(header)
        image.done()
    _assert_value(outputs[1], outputs[0])


def test_fitsheader_groups_history_once_per_row_and_keeps_misc_card_names(casa):
    outputs = []
    for factory in (casa.image, tools.image):
        image = factory()
        assert image.fromshape(shape=[3, 4])
        assert image.sethistory("review", ["aa\nbb"])
        assert image.setmiscinfo({
            "COMMENTfoo": "v" * 75,
            "COMMENTzero": "",
            "Crval1": "x",
            "ab": np.arange(6).reshape(2, 3),
            "ac": np.arange(6).reshape(3, 2),
        })
        header = image.fitsheader(exclude="DATE")
        assert header["COMMENT"] == "v" * 72
        assert header["CRVAL1"] == "x       "
        assert header["AB3_1"] == 1 and header["AB1_2"] == 4
        history = header.pop("HISTORY").tolist()
        index = history.index("aa")
        assert "SRCCODE='::review'" in history[index - 1] and history[index + 1] == "bb"
        raw = image.fitsheader(True)
        assert " " * 80 in [raw[index:index + 80] for index in range(0, len(raw), 80)]
        outputs.append(header)
        image.done()
    _assert_value(outputs[1], outputs[0])


def test_fitsheader_preserves_projection_with_independent_world_axes(casa, tmp_path):
    path = str(tmp_path / "world-transposed.im")
    writer = casa.image()
    assert writer.fromshape(path, [4, 5])
    cs = writer.coordsys()
    record = cs.torecord()
    record["worldmap0"] = [1, 0]
    assert writer.setcoordsys(record)
    assert cs.done()
    assert writer.done()
    outputs = []
    for factory in (casa.image, tools.image):
        image = factory()
        try:
            assert image.open(path)
            header = image.fitsheader()
            cards = image.fitsheader(True)
            matrix = {}
            for index in range(0, len(cards), 80):
                card = cards[index:index + 80]
                match = re.match(r"PC\s*0*(\d+)_(?:0*)(\d+)\s*=\s*([^/ ]+)", card)
                if match:
                    world, pixel, value = match.groups()
                    matrix[int(world), int(pixel)] = float(value)
            outputs.append((header, matrix))
        finally:
            image.done()
    (reference, expected_pc), (observed, actual_pc) = outputs
    for field in ("CTYPE", "CRPIX", "CRVAL", "CDELT"):
        _assert_value(observed[field], reference[field])
    assert observed["CTYPE"].tolist() == ["DEC--SIN", "RA---SIN"]
    assert actual_pc == expected_pc == {(1, 1): 0.0, (1, 2): 1.0, (2, 1): 1.0, (2, 2): 0.0}
