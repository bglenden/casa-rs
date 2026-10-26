"""`casars.tools.quanta`: CASA's `qa` tool (ADR-0019, #841).

Signatures are compared with CASA 6.7.6's installed binding
(`data/casa_quanta_signatures.json`, and a live CASA when importable) and
with the object-surface bundle. Every expected value and message is CASA
6.7.6's for the same call (probed 2026-10-21); all match to the last bit.
"""

from __future__ import annotations

import logging
import math
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
    "abs", "add", "angle", "canon", "canonical", "check", "compare", "convert", "div", "done",
    "formxxx", "getunit", "getvalue", "isquantity", "mul", "neg", "quantity", "splitdate", "sub", "time",
    "toangle", "tos", "totime", "type", "unit",
}
NOT_YET_IMPLEMENTED = set(CASA["methods"]) - IMPLEMENTED


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


@pytest.mark.parametrize("method", sorted(NOT_YET_IMPLEMENTED))
def test_unimplemented_methods_raise_not_implemented(method: str) -> None:
    required = [entry for entry in CASA["methods"][method] if entry["default"] is None]
    with pytest.raises(NotImplementedError, match=f"quanta.{method} is not yet implemented in casa-rs"):
        getattr(quanta(), method)(*["x" for _ in required])


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
