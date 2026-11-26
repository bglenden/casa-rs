"""Public spectral rest-frequency updates, with CASA and analytic references."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


def test_setrestfrequency_append_and_active_velocities(casa):
    rust = tools.coordsys().newcoordsys(spectral=True)
    pinned = casa.coordsys().newcoordsys(spectral=True)
    for cs in (rust, pinned):
        assert cs.setunits(["MHz"], "spectral")
        assert cs.setrestfrequency(["1.420GHz", "1430MHz"], which=1)
        assert cs.setrestfrequency({"value": [1.45, 1.47], "unit": "GHz"}, 2, True)
        np.testing.assert_allclose(cs.restfrequency()["value"], [1450, 1420, 1430, 1470])
        record = cs.referencevalue("nm", "spectral")
        np.testing.assert_allclose(record["numeric"], [1415])
        speed = record["measure"]["spectral"]["radiovelocity"]["m0"]
        assert speed["unit"] == "m/s"
        assert speed["value"] == pytest.approx(299792458.0 * (1 - 1415 / 1450))
        assert cs.setrestfrequency(np.array([], dtype=np.float32), which=1, append=True)
        np.testing.assert_allclose(cs.restfrequency()["value"], [1430, 1420, 1450, 1470])
    _assert_value(rust.restfrequency(), pinned.restfrequency())
    _assert_value(rust.referencevalue("nm", "spectral"), pinned.referencevalue("nm", "spectral"))


@pytest.mark.parametrize(
    "value,expected",
    [
        (np.float32(1420.0), [1420.0]),
        (np.array(1420.0), [1420.0]),
        ([1420, 1430.0], [1420.0, 1430.0]),
        (np.array([[1420.0, 1430.0], [1440.0, 1450.0]]), [1420, 1440, 1430, 1450]),
        ({"value": np.array([1420.0], dtype=np.float32), "unit": "MHz"}, [1.42e9]),
        ({"value": [2.0, 3.0], "unit": "m"}, [299792458.0 / 2, 299792458.0 / 3]),
        (["2GHz", "3m", "2Jy"], [2.0e9, 299792458.0 / 3, 2.0e-26]),
        (" 2GHz,   3MHz ", [2.0e9, 3.0e6]),
        (["2 GHz", "3 MHz"], [2.0e9, 3.0e6]),
        ("2ns", [2e-9]),
        (0.0, [0.0]),
    ],
)
def test_setrestfrequency_casa_input_forms(casa, value, expected):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(spectral=True)
        assert cs.setrestfrequency(value)
        quantity = cs.restfrequency()
        assert quantity["unit"] == "Hz"
        np.testing.assert_allclose(quantity["value"], expected, rtol=1e-12, atol=0)


def test_setrestfrequency_errors_preserve_list(casa):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(spectral=True)
        assert cs.setrestfrequency([1.0, 2.0, 3.0], 1)
        assert cs.setrestfrequency() is False
        for value in (1420, [1420, 1430], np.array([1420]), True, [],
                      {"value": 1420, "unit": "MHz"}, {"value": np.array(2.0), "unit": "Hz"},
                      {"value": 2.0}, {"value": np.array([[2.0]]), "unit": "Hz"}):
            assert cs.setrestfrequency(value) is False
        cases = (
            (([3.0], -1), "Illegal index"),
            (([3.0], 4, True), "which<restfreqs_p.nelements"),
            ((np.array([], dtype=float),), "which<restfreqs_p.nelements"),
            (("2 GHz",), "Value is not a valid frequency"),
            (("bogus",), "Error in QuantumHolder::fromString"),
            (({"value": [2.0], "unit": "invalid"},), "Illegal unit string"),
            (([-1.0],), "non-negative|restFrequencies"),
        )
        for args, message in cases:
            with pytest.raises(RuntimeError, match=message):
                cs.setrestfrequency(*args)
        np.testing.assert_array_equal(cs.restfrequency()["value"], [2, 1, 3])
        with pytest.raises(RuntimeError, match="No coordinate of type Spectral"):
            backend.coordsys().newcoordsys(linear=1).setrestfrequency(1.0)


def test_setrestfrequency_rejects_infinity_without_mutating_spectral_state(casa):
    pinned = casa.coordsys().newcoordsys(spectral=True)
    assert pinned.setrestfrequency(float("inf"))
    assert np.isinf(pinned.restfrequency()["value"][0])
    cs = tools.coordsys().newcoordsys(spectral=True)
    assert cs.setrestfrequency([1.0e9, 2.0e9], 1)
    prior = cs.referencevalue("nm", "spectral")
    with pytest.raises(RuntimeError, match="non-negative and finite"):
        cs.setrestfrequency([float("inf"), 3.0e9], which=1, append=True)
    _assert_value(cs.referencevalue("nm", "spectral"), prior)
    np.testing.assert_array_equal(cs.restfrequency()["value"], [2.0e9, 1.0e9])


def test_setrestfrequency_empty_token_list_keeps_the_coordinate_state():
    cs = tools.coordsys().newcoordsys(spectral=True)
    assert cs.setrestfrequency(np.array(1.415e9))
    with pytest.raises(RuntimeError, match="Value is not a valid frequency"):
        cs.setrestfrequency("  ")
    np.testing.assert_array_equal(cs.restfrequency()["value"], [1.415e9])


def test_setrestfrequency_native_image_interoperability(casa, tmp_path):
    for index, writer in enumerate((tools, casa)):
        image = writer.image()
        path = str(tmp_path / f"spectral-{index}.im")
        assert image.fromshape(path, [3, 4, 1, 2])
        cs = image.coordsys()
        assert cs.setunits(["MHz"], "spectral")
        assert cs.setrestfrequency(["1.420GHz", "1.450GHz"], 1)
        assert image.setcoordsys(cs.torecord())
        assert image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            reopened = image.coordsys()
            rest = reopened.restfrequency()
            assert rest["unit"] == "MHz"
            np.testing.assert_allclose(rest["value"], [1450.0, 1420.0])
            _assert_value(reopened.referencevalue("nm", "spectral"), cs.referencevalue("nm", "spectral"))
            assert image.done()
