"""Beam sizing and deconvolution through the public Python image tool."""

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


SMALL = ["3arcsec", "2arcsec", "10deg"]
LARGE = ["5arcsec", "4arcsec", "30deg"]
ZERO = ["0arcsec", "0arcsec", "0deg"]


@pytest.mark.parametrize("source,beam", [
    (LARGE, SMALL),
    (SMALL, LARGE),
    (LARGE, LARGE),
    (SMALL, SMALL),
    (ZERO, SMALL),
    (SMALL, ZERO),
    ("5arcsec", ("2arcsec",)),
    (["5arcsec", "4arcsec"], ["2arcsec"]),
    ("5 arcsec, 4 arcsec, 30 deg", "3arcsec 2arcsec 10deg"),
    (np.array(LARGE), np.array(SMALL)),
    (["0.005deg", "6arcsec", "0.7rad"], SMALL),
    (LARGE, ["2arcsec", "1arcsec", "0deg", "9deg"]),
    (LARGE, ["2arcsec", "0arcsec", "15deg"]),
    (["5arcsec", "4arcsec", "0deg"], ["5.01arcsec", "4arcsec", "0deg"]),
    (LARGE, ["3arcsec", "2arcsec", "190deg"]),
])
def test_deconvolvefrombeam_matches_casa(casa, source, beam):
    expected = casa.image().deconvolvefrombeam(source, beam)
    actual = tools.image().deconvolvefrombeam(source=source, beam=beam)
    _assert_value(actual, expected)


@pytest.mark.parametrize("source,target", [
    (SMALL, LARGE),
    (ZERO, SMALL),
    (SMALL, SMALL),
    ("3 arcsec, 2 arcsec, 10 deg", ["0.005deg", "6arcsec", "0.7rad"]),
    (SMALL, [*LARGE, "9deg"]),
])
def test_beamforconvolvedsize_matches_casa(casa, source, target):
    expected = casa.image().beamforconvolvedsize(source, target)
    actual = tools.image().beamforconvolvedsize(source=source, convolved=target)
    _assert_value(actual, expected)


@pytest.mark.parametrize("method,args", [
    ("deconvolvefrombeam", []),
    ("beamforconvolvedsize", []),
    ("deconvolvefrombeam", [SMALL, [3.0, 2.0, 10.0]]),
    ("deconvolvefrombeam", [{"unit": "arcsec", "value": 3}, SMALL]),
    ("deconvolvefrombeam", [SMALL, ["wrong"]]),
    ("deconvolvefrombeam", [SMALL, ["3pix", "2pix", "10deg"]]),
    ("deconvolvefrombeam", [SMALL, ["3arcsec", "-2arcsec", "10deg"]]),
    ("deconvolvefrombeam", [SMALL, ["2arcsec", "3arcsec", "10deg"]]),
    ("beamforconvolvedsize", [LARGE, SMALL]),
])
def test_beam_deconvolution_errors_match_casa(casa, method, args):
    with pytest.raises(RuntimeError) as expected:
        getattr(casa.image(), method)(*args)
    with pytest.raises(RuntimeError) as actual:
        getattr(tools.image(), method)(*args)
    assert str(actual.value).splitlines()[0] == str(expected.value).splitlines()[0]


@pytest.mark.parametrize("invalid", [[], [1, 2, 3], ["5arcsec"], ["5arcsec", "4arcsec"]])
def test_beamforconvolvedsize_rejects_missing_target_components(invalid):
    with pytest.raises(RuntimeError, match="Cannot understand target values"):
        tools.image().beamforconvolvedsize(SMALL, invalid)


def test_deconvolution_of_aligned_ellipses_has_known_axes():
    image = tools.image()
    assert image.fromshape(shape=[3, 2], type="cd")
    expected = {
        "major": {"value": 4.0, "unit": "arcsec"},
        "minor": {"value": math.sqrt(12.0), "unit": "arcsec"},
        "pa": {"value": 0.0, "unit": "deg"},
    }
    source = ["3arcsec", "2arcsec", "0deg"]
    total = ["5arcsec", "4arcsec", "0deg"]
    _assert_value(image.beamforconvolvedsize(source, total), expected)
    result = image.deconvolvefrombeam(total, source)
    _assert_value(result, {"fit": {**expected, "success": True}, "return": False})
    assert image.pixeltype() == "dcomplex"
    assert image.done()
