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
