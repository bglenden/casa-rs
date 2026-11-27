"""`casars.tools.quanta`: CASA's `qa` tool (ADR-0017, #841).

Signatures are compared with CASA 6.7.6's installed binding
(`data/casa_quanta_signatures.json`, and a live CASA when importable) and
with the object-surface bundle. Every expected value and message is CASA
6.7.6's for the same call (probed 2026-10-21); all match to the last bit.
"""

from __future__ import annotations

import json
import logging
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest

import casars
from casars.tools import quanta

from _casa_tools import (
    assert_bundle_describes,
    assert_casa_signatures,
    assert_live_signatures,
    casa_signatures,
)

CASA = casa_signatures("quanta")

IMPLEMENTED = {
    'abs', 'acos', 'add', 'angle', 'asin', 'atan', 'atan2', 'canon',
    'canonical', 'ceil', 'check', 'checkfreq', 'compare', 'constants', 'convert',
    'convertdop', 'convertfreq', 'cos',
    'define', 'div', 'done', 'exp', 'fits', 'floor', 'formxxx', 'getformat', 'getunit', 'getvalue', 'setformat',
    'isangle', 'isquantity', 'log', 'log10', 'map', 'maprec', 'mul', 'neg', 'norm', 'pow', 'quantity',
    'sin', 'splitdate', 'sqrt', 'sub', 'tan', 'time', 'toangle', 'tos',
    'totime', 'type', 'unit', 'eq', 'ne', 'lt', 'le', 'gt', 'ge',
}
NOT_YET_IMPLEMENTED = set(CASA["methods"]) - IMPLEMENTED


def test_constants_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_constants.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        if "error" in case:
            with pytest.raises(TypeError) as error:
                qa.constants(case["input"])
            assert str(error.value) == case["error"]["message"]
        else:
            assert qa.constants(case["input"]) == case["result"]
    assert qa.constants() == {"unit": "", "value": math.pi}

def test_sqrt_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_sqrt.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                qa.sqrt(case["input"])
            assert str(error.value) == case["error"]["message"]
        else:
            result = qa.sqrt(case["input"])
            expected = case["result"]
            assert result["unit"] == expected["unit"]
            if expected["value"] == "NaN":
                assert math.isnan(result["value"])
            else:
                assert result["value"] == pytest.approx(expected["value"], rel=1e-14, abs=1e-15)
                if result["value"] == 0:
                    assert math.copysign(1, result["value"]) == math.copysign(1, expected["value"])
    assert qa.sqrt() == {"unit": "", "value": 0.0}
    assert qa.sqrt("4_-2") == {"unit": "_-1", "value": 2.0}
    assert qa.sqrt("4_._") == {"unit": "_", "value": 2.0}
    assert qa.sqrt("4_+2") == {"unit": "_", "value": 2.0}

def test_atan2_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_atan2.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                qa.atan2(case["x"], case["y"])
            assert str(error.value) == case["error"]["message"]
        else:
            result = qa.atan2(case["x"], case["y"])
            assert result["unit"] == case["result"]["unit"]
            assert result["value"] == pytest.approx(case["result"]["value"], abs=1e-14)
            if result["value"] == 0:
                assert math.copysign(1, result["value"]) == math.copysign(1, case["result"]["value"])
    assert qa.atan2() == {"unit": "rad", "value": 0.0}
    assert qa.atan2(1.0)["value"] == pytest.approx(math.pi / 2)

def test_trigonometry_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_trigonometry.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        method = getattr(qa, case["method"])
        if "error" in case:
            assert case["error"]["type"] == "RuntimeError"
            with pytest.raises(RuntimeError) as error:
                method(case["input"])
            assert str(error.value) == case["error"]["message"]
        else:
            result = method(case["input"])
            expected = case["result"]
            assert result["unit"] == expected["unit"]
            if expected["value"] == "NaN":
                assert math.isnan(result["value"])
            else:
                assert result["value"] == pytest.approx(expected["value"], rel=1e-14, abs=1e-15)


def test_checkfreq_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_checkfreq.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        if "error" in case:
            assert case["error"]["type"] == "RuntimeError"
            with pytest.raises(RuntimeError) as error:
                qa.checkfreq(case["input"])
            assert str(error.value) == case["error"]["message"]
        else:
            assert qa.checkfreq(case["input"]) is case["result"]
    assert qa.checkfreq() is False


def test_logarithms_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_logarithms.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        method = getattr(qa, case["method"])
        if "error" in case:
            assert case["error"]["type"] == "RuntimeError"
            with pytest.raises(RuntimeError) as error:
                method(case["input"])
            assert str(error.value) == case["error"]["message"]
        else:
            result = method(case["input"])
            expected = case["result"]
            assert result["unit"] == expected["unit"]
            if expected["value"] == "NaN":
                assert math.isnan(result["value"])
            elif expected["value"] in ("Infinity", "-Infinity"):
                assert result["value"] == float(expected["value"])
            else:
                assert result["value"] == pytest.approx(expected["value"], rel=1e-14, abs=1e-15)


def test_isangle_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_isangle.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        if "error" in case:
            assert case["error"]["type"] == "RuntimeError"
            with pytest.raises(RuntimeError) as error:
                qa.isangle(case["input"])
            assert str(error.value) == case["error"]["message"]
        else:
            assert qa.isangle(case["input"]) is case["result"]
    assert qa.isangle() is False


def test_rounding_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_rounding.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        method = getattr(qa, case["method"])
        if "error" in case:
            assert case["error"]["type"] == "RuntimeError"
            with pytest.raises(RuntimeError) as error:
                method(case["input"])
            assert str(error.value) == case["error"]["message"]
        else:
            result = method(case["input"])
            assert result == case["result"]
            if result["value"] == 0:
                assert math.copysign(1, result["value"]) == math.copysign(1, case["result"]["value"])
    assert qa.ceil() == qa.floor() == {"unit": "", "value": 0.0}


def q(value: Any, unit: str) -> dict[str, Any]:
    return {"unit": unit, "value": value}


def test_the_tool_is_importable_from_casars_tools() -> None:
    assert casars.tools.quanta is quanta
    assert quanta.__module__ == "casars.tools"
    assert (quanta().type(), quanta().done()) == ("quanta", True)


def test_every_casa_method_exists_with_casas_signature() -> None:
    assert_casa_signatures(quanta, CASA)
    assert len(CASA["methods"]) == 57


def test_signatures_match_a_live_casa_when_one_is_importable() -> None:
    casatools = pytest.importorskip("casatools")
    assert_live_signatures(quanta, casatools.quanta)


def test_the_bundle_describes_the_class() -> None:
    assert_bundle_describes(
        quanta, "quanta", {"implemented": IMPLEMENTED, "not_yet_implemented": NOT_YET_IMPLEMENTED}
    )


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (lambda qa: qa.quantity("1.5km"), q(1.5, "km")),
        (lambda qa: qa.quantity(), q(0.0, "")),
        (lambda qa: qa.quantity(3, "m"), q(3.0, "m")),
        (lambda qa: qa.quantity(3), q(3.0, "")),
        (lambda qa: qa.quantity({"value": 2.0, "unit": "s"}), q(2.0, "s")),
        (lambda qa: qa.quantity("1.5km", "m"), q(1.5, "km")),
        (lambda qa: qa.quantity("12h30m"), q(187.5, "deg")),
        (lambda qa: qa.quantity("2020/01/01/00:00:00"), q(58849.0, "d")),
        (lambda qa: qa.quantity("45d30m00"), q(45.5, "deg")),
        (lambda qa: qa.quantity(True), q(1.0, "")),
        (lambda qa: qa.quantity("1 d"), q(1.0, "d")),
        (lambda qa: qa.quantity("1Jy/beam"), q(1.0, "Jy/beam")),
        (lambda qa: qa.quantity("1 km/s"), q(1.0, "km/s")),
        (lambda qa: qa.unit(2, "Hz"), q(2.0, "Hz")),
        (lambda qa: qa.convert("1.5km", "m"), q(1500.0, "m")),
        (lambda qa: qa.convert(qa.quantity("1.5km"), "m"), q(1500.0, "m")),
        (lambda qa: qa.convert("1.5km", qa.quantity("1m")), q(1500.0, "m")),
        (lambda qa: qa.convert("1.5km"), q(1500.0, "m")),
        (lambda qa: qa.convert("1.5km", "s"), q(1500.0, "s.m.s-1")),
        (lambda qa: qa.convert("1h", "deg"), q(15.0, "deg")),
        (lambda qa: qa.convert("15deg", "min"), q(60.00000000000001, "min")),
        (lambda qa: qa.convert("1GHz", "m"), q(1e9, "m.m-1.s-1")),
        (lambda qa: qa.convert(5, "m"), q(5.0, "m.m-1")),
        (lambda qa: qa.convert("-67.75deg", "rad"), q(-1.1824605682261582, "rad")),
        (lambda qa: qa.canonical("1.5km/h"), q(0.4166666666666667, "m.s-1")),
        (lambda qa: qa.canonical("1Jy"), q(1e-26, "kg.s-2")),
        (lambda qa: qa.canon("1deg"), q(0.017453292519943295, "rad")),
        (lambda qa: qa.canon("5"), q(5.0, "")),
        (lambda qa: qa.add("1km", "1m"), q(1.001, "km")),
        (lambda qa: qa.add("1m", "1km"), q(1001.0, "m")),
        (lambda qa: qa.sub("1km", "1m"), q(0.999, "km")),
        (lambda qa: qa.mul("2km", "3s"), q(6.0, "km.s")),
        (lambda qa: qa.mul("2km", 3), q(6.0, "km")),
        (lambda qa: qa.mul(3, "2km"), q(6.0, "km")),
        (lambda qa: qa.div("2km", "4s"), q(0.5, "km/(s)")),
        (lambda qa: qa.div(1, "4s"), q(0.25, "(s)-1")),
        (lambda qa: qa.div("2km", "0s"), q(math.inf, "km/(s)")),
        (lambda qa: qa.neg("2km"), q(-2.0, "km")),
        (lambda qa: qa.abs("-2km"), q(2.0, "km")),
        (lambda qa: qa.totime("45deg"), q(0.125, "d")),
        (lambda qa: qa.totime("2h"), q(2.0, "h")),
        (lambda qa: qa.toangle("2h"), q(0.5235987755982988, "rad")),
        (lambda qa: qa.toangle("2deg"), q(2.0, "deg")),
    ],
)
def test_quantities_compute_as_casa_computes_them(call: Any, expected: dict[str, Any]) -> None:
    found = call(quanta())
    assert found == expected
    assert type(found["value"]) is float
    assert list(found) == ["unit", "value"]


@pytest.mark.parametrize(
    ("call", "unit", "values"),
    [
        (lambda qa: qa.quantity([1, 2, 3], "m"), "m", [1.0, 2.0, 3.0]),
        (lambda qa: qa.quantity(np.array([1.0, 2.5]), "m"), "m", [1.0, 2.5]),
        (lambda qa: qa.quantity(np.array([[1.0, 2.5], [3, 4]]), "m", True), "m", [[1.0, 2.5], [3.0, 4.0]]),
        # Without keepshape, casatools flattens in Fortran order.
        (lambda qa: qa.quantity(np.array([[1.0, 2.5], [3, 4]]), "m"), "m", [1.0, 3.0, 2.5, 4.0]),
        (lambda qa: qa.quantity({"value": np.array([2.0, 3.0]), "unit": "s"}), "s", [2.0, 3.0]),
        (lambda qa: qa.convert(qa.quantity([1, 2], "km"), "m"), "m", [1000.0, 2000.0]),
    ],
)
def test_arrays_stay_arrays(call: Any, unit: str, values: list[Any]) -> None:
    found = call(quanta())
    assert found["unit"] == unit
    assert isinstance(found["value"], np.ndarray) and found["value"].dtype == np.float64
    assert found["value"].tolist() == values


@pytest.mark.parametrize(
    ("unit", "outunit", "expected"),
    [("km", "m", "m"), ("km", "", "m"), ("km", "s", "s.m.s-1"), ("h", "deg", "deg")],
)
def test_an_empty_array_converts_its_unit(unit: str, outunit: str, expected: str) -> None:
    # The unit does not depend on how many values there are; `''` is SI.
    found = quanta().convert(q(np.empty((0, 2)), unit), outunit)
    assert found["unit"] == expected
    assert isinstance(found["value"], np.ndarray) and found["value"].dtype == np.float64
    assert found["value"].shape == (0, 2)


def test_values_and_units_are_read_back() -> None:
    qa = quanta()
    for value, expected in (("1.5km", [1.5]), (qa.quantity([1, 2], "m"), [1.0, 2.0]), (5, [5.0])):
        found = qa.getvalue(value)
        assert isinstance(found, np.ndarray) and found.tolist() == expected
    assert (qa.getunit("1.5km"), qa.getunit(qa.quantity([1, 2], "m"))) == ("km", "m")


def test_predicates_answer_as_casa_does() -> None:
    qa = quanta()
    assert [qa.isquantity(v) for v in ("2km", "garbage", 5, {"value": 1.0, "unit": "m"},
                                       qa.quantity([1, 2], "m"), ["1m", "x"])] == [
        True, False, False, True, True, False]
    assert (qa.compare("1km", "1m"), qa.compare("1km", "1s")) == (True, False)
    assert (qa.check("1km"), qa.check("1 furlong"), qa.check("5")) == (True, False, True)


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (lambda qa: qa.time("12h30m15.5s"), "12:30:16"),
        (lambda qa: qa.time("12h30m15.123456789s", prec=9), "12:30:15.123"),
        (lambda qa: qa.time(qa.quantity("2020/01/01/12:30:15.5"), form="ymd"), "2020/01/01/12:30:15"),
        (lambda qa: qa.time(qa.quantity("2020/01/01/12:30:15.5"), form=["dmy"]), "01-Jan-2020/12:30:15"),
        (lambda qa: qa.time(qa.quantity("2020/01/01/12:30:15.5"), form=["fits"], prec=7),
         "2020-01-01T12:30:15.5"),
        (lambda qa: qa.time(qa.quantity("2020/01/01/12:30:15.5"), form=["ymd", "no_time"]), "2020/01/01"),
        (lambda qa: qa.time("58849.5d", form="ymd"), "2020/01/01/12:00:00"),
        (lambda qa: qa.time("45deg"), "03:00:00"),
        (lambda qa: qa.time("-1h"), "23:00:00"),
        (lambda qa: qa.time("12h30m", showform=True), "12:30:00"),
        (lambda qa: qa.angle("45.5deg"), "+045.30.00"),
        (lambda qa: qa.angle("45.5deg", prec=8), "+045.30.00.00"),
        (lambda qa: qa.angle("190deg", form="dig2"), "+**.00.00"),
        (lambda qa: qa.angle("190deg", form="time"), "12:40:00"),
        (lambda qa: qa.angle("190deg", form=["clean"]), " 190.00.00"),
        (lambda qa: qa.angle("45.5deg", form=["no_d"]), "+.30.00"),
        (lambda qa: qa.angle("1rad", prec=7), "+057.17.44.8"),
        (lambda qa: qa.angle("12h"), "+180.00.00"),
        (lambda qa: qa.angle("-23.02deg", prec=9, form=["dig2"]), "-23.01.12.000"),
    ],
)
def test_times_and_angles_format_as_casa_formats_them(call: Any, expected: str) -> None:
    assert call(quanta()) == [expected]


def test_splitdate_gives_casas_fields() -> None:
    found = quanta().splitdate("2020/02/29/12:30:15.25")
    assert found == {
        "hour": 12, "min": 30, "mjd": 58908.52100983796, "month": 2, "monthday": 29, "msec": 249,
        "s": 15.249999845400453, "sec": 15, "usec": 249999, "week": 9, "weekday": 6, "year": 2020,
        "yearday": 60,
    }
    assert list(found) == sorted(found)
    assert {key for key, value in found.items() if type(value) is float} == {"mjd", "s"}


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda qa: qa.quantity("garbage"),
         'Error in QuantumHolder::fromString with input string "garbage": Illegal input units or '
         "format\n in converting quantity"),
        (lambda qa: qa.quantity(["1m", "2m"]),
         'Error in QuantumHolder::fromString with input string "["1m","2m"]": Illegal input units or '
         "format\n in converting quantity"),
        (lambda qa: qa.quantity(3, "furlong"), "Unit::check Illegal unit string 'furlong'"),
        (lambda qa: qa.canon(qa.quantity([1, 2], "km")), "Wrong QuantumHolder to convert asQuantumDouble"),
        (lambda qa: qa.add("1m", "1s"), "Quantum::operator+ unequal units 'm, 's'"),
        (lambda qa: qa.add("1m", 2), "Quantum::operator+ unequal units 'm, ''"),
        (lambda qa: qa.time(qa.quantity([1, 2], "h")), "Wrong QuantumHolder to convert asQuantumDouble"),
        (lambda qa: qa.time(1.0), "Quantum::assure non-conforming unit type ''"),
        (lambda qa: qa.angle(1.0), "Quantum::assure non-conforming unit type ''"),
        (lambda qa: qa.totime(1.0), "Quantum::assure non-conforming unit type ''"),
        (lambda qa: qa.splitdate(), "Quantum::assure non-conforming unit type ''"),
    ],
)
def test_casa_exceptions_raise_runtime_error(call: Any, message: str) -> None:
    with pytest.raises(RuntimeError) as raised:
        call(quanta())
    assert str(raised.value) == message


def test_map_and_maprec_match_casa() -> None:
    assert not NOT_YET_IMPLEMENTED
    fixture = Path(__file__).parent / "data" / "casa_quanta_map.json"
    qa = quanta()
    for method in (qa.map, qa.maprec):
        for bad in (None, 123, [1]):
            with pytest.raises(TypeError, match=r"^argument _v must be a string$"):
                method(bad)
    # Run in a fresh subprocess so the process-wide User map is untouched by
    # other tests' qa.define / qa.fits calls.
    program = r'''
import json, sys
from casars.tools import quanta
probe = json.load(open(sys.argv[1]))
qa = quanta()
assert qa.map() == probe["map_all"]
assert qa.maprec() == probe["maprec_all"]
for key, arg in [
    ("map_all", "all"),
    ("map_all", b"all"),
    ("map_all", "a"),
    ("map_constants", "constants"),
    ("map_constants", "Constants"),
    ("map_constants", "const"),
    ("map_constants", "Const"),
    ("map_prefix", "Prefix"),
    ("map_prefix", "pre"),
    ("map_si", "SI"),
    ("map_si", "si"),
    ("map_custom", "Custom"),
    ("map_custom", "cust"),
    ("map_custom", "c"),
    ("map_user", "User"),
    ("map_user", "u"),
    ("map_bogus", "bogus"),
    ("map_bogus", ""),
]:
    assert qa.map(arg) == probe[key], (key, arg)
for key, arg in [
    ("maprec_all", "all"),
    ("maprec_all", b"all"),
    ("maprec_all", "a"),
    ("maprec_prefix", "Prefix"),
    ("maprec_prefix", "pre"),
    ("maprec_si", "SI"),
    ("maprec_si", "si"),
    ("maprec_custom", "Custom"),
    ("maprec_custom", "cust"),
    ("maprec_user", "User"),
    ("maprec_user", "u"),
    ("maprec_constants", "constants"),
    ("maprec_bogus", "bogus"),
]:
    assert qa.maprec(arg) == probe[key], (key, arg)
assert qa.define("jy_beam", "5Jy")
user_rec = qa.maprec("user")
assert user_rec["== User =="] == "== User ==== 2 ===="
assert user_rec["User_jy_beam"] == "jy_beam   (User defined)               5e-26 kg.s-2"
'''
    result = subprocess.run([sys.executable, "-c", program, str(fixture)], text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_casa_tool_quanta_suite() -> None:
    """Port of casatools' test_tool_quanta.py (quanta_convert_test and quanta_quantity_test)."""
    qa = quanta()
    # quanta_convert_test.test_exceptions
    with pytest.raises(RuntimeError) as error:
        qa.convert(v=qa.quantity("5kg"), outunit="bogus")
    assert (
        str(error.value)
        == 'Error in QuantumHolder::fromString with input string "bogus": Illegal input units or format\n in converting quantity'
    )
    # quanta_convert_test.test_value_as_string & quanta_quantity_test.test_value_as_string
    for ks in (True, False):
        for code in (qa.quantity, qa.unit):
            assert code("5kg", keepshape=ks) == {"value": 5.0, "unit": "kg"}
            assert code("5kg", "pc", keepshape=ks) == {"value": 5.0, "unit": "kg"}
    # quanta_convert_test.test_valid
    assert qa.convert("5kg", "g") == {"value": 5000.0, "unit": "g"}
    assert qa.convert(qa.quantity("5kg"), "g") == {"value": 5000.0, "unit": "g"}
    vec = [5, 9.25, 8]
    res = qa.convert(qa.quantity(vec, "kg"), "g")
    assert res["unit"] == "g"
    assert (res["value"] / 1000 == vec).all()
    rng = np.random.default_rng(42)
    arr4d = rng.random((5, 2, 8, 9))
    res = qa.convert(qa.quantity(arr4d, "kg", keepshape=True), "g")
    assert res["unit"] == "g"
    assert np.isclose(res["value"] / 1000, arr4d).all()
    assert qa.convert(5, "m") == {"unit": "m.m-1", "value": 5.0}
    # quanta_quantity_test.test_exceptions
    for code in (qa.quantity, qa.unit):
        with pytest.raises(RuntimeError) as error:
            code(v="blah")
        assert (
            str(error.value)
            == 'Error in QuantumHolder::fromString with input string "blah": Illegal input units or format\n in converting quantity'
        )
        with pytest.raises(RuntimeError) as error:
            code(v=5, unitname="zz")
        assert str(error.value) == "Unit::check Illegal unit string 'zz'"
    # quanta_quantity_test.test_scalar_value & test_vector_value
    for ks in (True, False):
        for code in (qa.quantity, qa.unit):
            assert code(5, "kg", keepshape=ks) == {"value": 5.0, "unit": "kg"}
            res = code([5, 8, 7.5], "kg", keepshape=ks)
            assert res["unit"] == "kg"
            assert (res["value"] == [5, 8, 7.5]).all()
    # quanta_quantity_test.test_multidimensional_arra_value
    arr2d = rng.random((4, 5))
    for ks in (True, False):
        for code in (qa.quantity, qa.unit):
            res = code(arr2d, "kg", keepshape=ks)
            assert res["unit"] == "kg"
            expected = arr2d if ks else arr2d.ravel("F")
            assert (res["value"] == expected).all()
    # quanta_quantity_test.test_value_as_record
    for v in (
        {"unit": "arcsec", "value": 840},
        {"unit": "arcsec", "value": [840.0, 840.0, 840.0, 840.0]},
        {"unit": "arcsec", "value": [[840.0, 840.0, 840.0, 840.0]]},
    ):
        for ks in (True, False):
            for code in (qa.quantity, qa.unit):
                res = code(v, keepshape=ks)
                assert res["unit"] == v["unit"]
                assert (res["value"] == np.array(v["value"])).all()
                assert np.shape(res["value"]) == np.shape(v["value"])


def test_nested_list_and_tuple_quantity_records() -> None:
    """Nested sequence quantity-record values and diagnostics (#1170)."""
    qa = quanta()
    rec_2d_float = {"unit": "m", "value": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]}
    res = qa.quantity(rec_2d_float)
    assert res["unit"] == "m"
    assert res["value"].shape == (2, 3)
    np.testing.assert_array_equal(res["value"], np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]))
    conv = qa.convert(rec_2d_float, "cm")
    assert conv["unit"] == "cm"
    np.testing.assert_array_equal(conv["value"], np.array([[100.0, 200.0, 300.0], [400.0, 500.0, 600.0]]))
    # Nested tuple and 3-D nested sequence preserve shape and values.
    rec_2d_tuple = {"unit": "m", "value": ((1.0, 2.0), (3.0, 4.0))}
    res_tuple = qa.quantity(rec_2d_tuple)
    assert res_tuple["unit"] == "m" and res_tuple["value"].shape == (2, 2)
    np.testing.assert_array_equal(res_tuple["value"], np.array([[1.0, 2.0], [3.0, 4.0]]))
    rec_3d = {"unit": "m", "value": [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]}
    res_3d = qa.quantity(rec_3d)
    assert res_3d["unit"] == "m" and res_3d["value"].shape == (2, 2, 2)
    np.testing.assert_array_equal(
        res_3d["value"],
        np.array([[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]),
    )
    for method, args in ((qa.getvalue, (rec_2d_float,)), (qa.getunit, (rec_2d_float,)), (qa.add, (rec_2d_float, "1m"))):
        with pytest.raises(RuntimeError, match="^Wrong QuantumHolder to convert asQuantumDouble$"):
            method(*args)
    # 1-element 2-D float list is a 2-D array for quantity/convert and a scalar for getvalue/getunit/add.
    rec_1x1_float = {"unit": "m", "value": [[7.0]]}
    res_1x1 = qa.quantity(rec_1x1_float)
    assert res_1x1["unit"] == "m" and res_1x1["value"].shape == (1, 1)
    np.testing.assert_array_equal(res_1x1["value"], np.array([[7.0]]))
    np.testing.assert_array_equal(qa.getvalue(rec_1x1_float), np.array([7.0]))
    assert qa.getunit(rec_1x1_float) == "m"
    assert qa.add(rec_1x1_float, "1m") == {"unit": "m", "value": 8.0}
    # 1-element 2-D integer list narrows to scalar float; multi-element integer list fails asQuantumDouble.
    assert qa.quantity({"unit": "m", "value": [[7]]}) == {"unit": "m", "value": 7.0}
    with pytest.raises(RuntimeError, match="^Wrong QuantumHolder to convert asQuantumDouble$"):
        qa.quantity({"unit": "m", "value": [[1, 2], [3, 4]]})
    with pytest.raises(RuntimeError, match="^Unit::check Illegal unit string 'bad'$"):
        qa.quantity({"unit": "bad", "value": [[1, 2], [3, 4]]})
    # Bool, string, empty, and ragged nested lists fail fromRecord.
    for bad_val in ([[True]], [["bad"]], [[]], [[1.0, "bad"], [2.0]], [[1.0, 2.0], [3.0]]):
        with pytest.raises(
            RuntimeError,
            match=r"^Error Illegal Quantum record in QuantumHolder::fromRecord\n in converting quantity$",
        ):
            qa.quantity({"unit": "m", "value": bad_val})


@pytest.mark.parametrize(
    ("value", "form", "prec", "written"),
    [
        # CASA 6.7.6 (tools/simulation/simutil_oracle.py).
        ("18h00m00.031s", "hms", 5, "18:00:00.03100"),
        ("-22d59m59.6s", "dms", 5, "-022.59.59.600000"),
        ("359.9999999deg", "dms", 5, "-000.00.00.000360"),
        ("86399.9999999s", "hms", 5, "24:00:00.00000"),
        ("720.25deg", "deg", 2, "0.25"),
        ("-0.5deg", "+deg", 2, "359.5"),
        ("3.14159265358979rad", "rad", 2, "3.14159"),
    ],
)
def test_formxxx_writes_casas_strings(value: str, form: str, prec: int, written: str) -> None:
    assert quanta().formxxx(value, format=form, prec=prec) == written


def test_formxxx_answers_an_unknown_format_with_an_empty_string(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="casars.tools.quanta"):
        assert quanta().formxxx("1deg", format="xyz") == ""
    assert caplog.messages == ["Don't understand xyz"]
    with pytest.raises(RuntimeError, match="non-conforming unit"):
        quanta().formxxx("1Jy", format="hms")


def test_tos_writes_fixed_decimals_and_the_unit() -> None:
    qa = quanta()
    assert qa.tos(qa.quantity("100GHz")) == "100.000000000GHz"
    assert qa.tos("229.98GHz", prec=0) == "230GHz"
    assert qa.tos("1e-12Jy", prec=15) == "0.000000000001000Jy"
