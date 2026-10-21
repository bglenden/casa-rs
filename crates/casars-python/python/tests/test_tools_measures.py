"""`casars.tools.measures`: CASA's `me` tool (ADR-0019, #841).

Signatures are compared with CASA 6.7.6's installed binding
(`data/casa_measures_signatures.json`, and a live CASA when importable) and
with the object-surface bundle. Expected values are CASA 6.7.6's, probed
2026-10-21 with the same calls (`me.direction('J2000', '12h00m00',
'45d00m00')` and so on). Measures built from their arguments, and
conversions that need no Earth orientation (TAI, TDB, ITRF<->WGS84 of a given
position) match CASA to the last bit or within the stated tolerances; those
that need Earth orientation (UT1, AZEL, HADEC) depend on the measures data
and are checked loosely.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pytest

import casars
from casars.tools import measures

from _casa_tools import (
    assert_bundle_describes,
    assert_casa_signatures,
    assert_live_signatures,
    casa_signatures,
    close,
)

LOGGER = "casars.tools.measures"
CASA = casa_signatures("measures")

IMPLEMENTED = {
    "direction", "dirshow", "doframe", "done", "epoch", "framenow", "frequency", "getref",
    "gettype", "getvalue", "ismeasure", "listcodes", "measure", "observatory", "posangle",
    "position", "separation", "show", "source", "type",
}
NOT_YET_IMPLEMENTED = set(CASA["methods"]) - IMPLEMENTED


def q(value: float, unit: str) -> dict[str, Any]:
    return {"unit": unit, "value": value}


def direction(refer: str, longitude: float, latitude: float) -> dict[str, Any]:
    return {"m0": q(longitude, "rad"), "m1": q(latitude, "rad"), "refer": refer, "type": "direction"}


def epoch(refer: str, days: float) -> dict[str, Any]:
    return {"m0": q(days, "d"), "refer": refer, "type": "epoch"}


def position(refer: str, longitude: float, latitude: float, length: float) -> dict[str, Any]:
    return {
        "m0": q(longitude, "rad"), "m1": q(latitude, "rad"), "m2": q(length, "m"),
        "refer": refer, "type": "position",
    }


def frequency(refer: str, hz: float) -> dict[str, Any]:
    return {"m0": q(hz, "Hz"), "refer": refer, "type": "frequency"}


POLE = direction("J2000", 0.0, 1.5707963267948966)
VLA = position("ITRF", -1.8782942581394362, 0.5916750987501983, 6373576.280915651)
ALMA = position("WGS84", -1.1825465955049892, -0.4018251640113072, 5056.8)


@pytest.fixture(scope="module")
def measures_data() -> None:
    """Skip when no measures runtime is installed (`scripts/test-python-package.sh`
    provides the bundled snapshot)."""
    try:
        measures().observatory("VLA")
    except RuntimeError as error:
        if "measures data" not in str(error):
            raise
        pytest.skip(f"no measures runtime: {error}")


needs_measures_data = pytest.mark.usefixtures("measures_data")


def test_the_tool_is_importable_from_casars_tools() -> None:
    assert casars.tools.measures is measures
    assert measures.__module__ == "casars.tools"
    assert measures().type() == "measures"


def test_every_casa_method_exists_with_casas_signature() -> None:
    assert_casa_signatures(measures, CASA)
    assert len(CASA["methods"]) == 48


def test_signatures_match_a_live_casa_when_one_is_importable() -> None:
    casatools = pytest.importorskip("casatools")
    assert_live_signatures(measures, casatools.measures)


def test_the_bundle_describes_the_class() -> None:
    assert_bundle_describes(
        measures,
        "measures",
        {"implemented": IMPLEMENTED, "not_yet_implemented": NOT_YET_IMPLEMENTED},
    )


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (lambda me: me.direction("J2000", "12h00m00", "45d00m00"),
         direction("J2000", 3.141592653589793, 0.7853981633974482)),
        (lambda me: me.direction(), POLE),
        # Numbers are not read: CASA's default direction.
        (lambda me: me.direction("J2000", 1.0, 0.5), POLE),
        (lambda me: me.direction("AZELNE", "10deg", "20deg"),
         direction("AZEL", 0.17453292519943295, 0.3490658503988659)),
        (lambda me: me.direction("SUN"), direction("SUN", 0.0, 1.5707963267948966)),
        (lambda me: me.direction("J2000", "270deg", "20deg"),
         direction("J2000", -1.5707963267948968, 0.3490658503988659)),
        (lambda me: me.direction("J2000", {"value": 180.0, "unit": "deg"}, {"value": 45.0, "unit": "deg"}),
         direction("J2000", 3.141592653589793, 0.7853981633974482)),
        (lambda me: me.epoch("UTC", "2020/01/01/00:00:00"), epoch("UTC", 58849.0)),
        (lambda me: me.epoch(), epoch("UTC", 0.0)),
        (lambda me: me.epoch("TT", "2020/01/01"), epoch("TDT", 58849.0)),
        (lambda me: me.epoch("UTC", 50000), epoch("UTC", 0.0)),
        (lambda me: me.epoch("UTC", "2020-01-01T12:00:00"), epoch("UTC", 58849.5)),
        (lambda me: me.epoch("FOO", "2020/01/01"), epoch("UTC", 58849.0)),
        (lambda me: me.epoch("UTC", "2026/03/21/07:13:42.25"), epoch("UTC", 61120.301183449075)),
        (lambda me: me.position("WGS84", "-67.75deg", "-23.02deg", "5050m"),
         position("WGS84", -1.1824605682261582, -0.40177479380909464, 5050.0)),
        (lambda me: me.position(), position("WGS84", 0.0, 1.5707963267948966, 1e-06)),
        (lambda me: me.position("ITRF", "-1601185.4m", "-5041977.5m", "3554875.9m"),
         position("ITRF", -1.8782942671343237, 0.5916751056499899, 6373576.269260471)),
        (lambda me: me.position("WGS84", "-1601185.4m", "-5041977.5m", "3554875.9m"),
         position("WGS84", -1.8782942671343237, 0.5916751056499899, 6373576.269260471)),
        (lambda me: me.position("ITRF", "-107.6deg", "34.07deg", "6373000m"),
         position("ITRF", -1.8779742751458985, 0.5946336761544679, 6373000.0)),
        (lambda me: me.position("WGS84", "10deg", "20deg", "30m"),
         position("WGS84", 0.17453292519943295, 0.3490658503988659, 30.000000000000004)),
        # A length between two angles does not read: the origin.
        (lambda me: me.position("WGS84", "30m", "10deg", "20deg"), position("WGS84", 0.0, 0.0, 0.0)),
        (lambda me: me.position("WGS84", "10deg", "20deg", "0m"),
         position("WGS84", 0.17453292519943295, 0.3490658503988659, 1e-06)),
        (lambda me: me.position("FOO", "-1601185.4m", "-5041977.5m", "3554875.9m"),
         position("ITRF", -1.8782942671343237, 0.5916751056499899, 6373576.269260471)),
        (lambda me: me.frequency("LSRK", "1.4GHz"), frequency("LSRK", 1.4e9)),
        (lambda me: me.frequency(), frequency("LSRK", 0.0)),
        (lambda me: me.frequency("LSRK", "21cm"), frequency("LSRK", 1427583133.3333333)),
        (lambda me: me.frequency("LSRK", "1s"), frequency("LSRK", 1.0)),
        (lambda me: me.frequency("FOO", "1GHz"), frequency("LSRK", 1e9)),
    ],
)
def test_measures_are_defined_as_casa_defines_them(call: Any, expected: dict[str, Any]) -> None:
    found = call(measures())
    assert found == expected
    assert list(found) == sorted(found), "casatools sorts record keys"


@pytest.mark.parametrize(
    ("call", "messages"),
    [
        (lambda me: me.direction("J2000", "13h", "20deg"),
         ["Exception Reports: Quantum::cos illegal unit type 'h'"]),
        (lambda me: me.direction("J2000", "10deg"),
         ["Exception Reports: Quantum::cos illegal unit type ''"]),
        (lambda me: me.epoch("UTC", "59000d"),
         ["Exception Reports: Quantum::assure non-conforming unit type 'deg'"]),
        (lambda me: me.frequency("LSRK", "1Jy"),
         ["Exception Reports: Quantum::assure non-conforming unit type 'Jy'"]),
        (lambda me: me.epoch("UTC", "garbage"), [
            'Error in QuantumHolder::fromString with input string "garbage": Illegal input units or '
            "format\n in converting quantity from string ",
            "Exception Reported: Empty QuantumHolder argument for asQuantumDouble",
            "Exception Reports: Empty QuantumHolder argument for asQuantumDouble",
        ]),
    ],
)
def test_failures_casa_logs_return_an_empty_record(
    call: Any, messages: list[str], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert call(measures()) == {}
    assert caplog.messages == messages


def test_unknown_reference_codes_warn_as_casa_does(caplog: pytest.LogCaptureFixture) -> None:
    me = measures()
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        me.direction("FOO", "12h00m00", "45d00m00")
        me.frequency("FOO", "1GHz")
        me.position("FOO", "0m", "0m", "1m")
        me.epoch("FOO", "2020/01/01")
    assert caplog.messages == [
        "Illegal reference frame string.  Reference string set to DEFAULT",
        "Illegal reference frame string.",
        "Illegal or unknown reference type 'FOO' for position definition. DEFAULT (ITRF) assumed.",
    ]


def test_record_arguments_must_be_dicts() -> None:
    me = measures()
    for call in (lambda: me.getref("x"), lambda: me.measure("x", "J2000"), lambda: me.doframe("x"),
                 lambda: me.epoch("UTC", "2020/01/01", "x")):
        with pytest.raises(TypeError):
            call()


def test_records_are_queried_as_casa_queries_them(caplog: pytest.LogCaptureFixture) -> None:
    me = measures()
    assert me.getref(VLA) == "ITRF"
    assert (me.gettype(VLA), me.gettype(me.epoch()), me.gettype(me.frequency())) == (
        "Position", "Epoch", "Frequency")
    assert me.getvalue(VLA) == {key: VLA[key] for key in ("m0", "m1", "m2")}
    assert me.getvalue({"return": me.epoch("UTC", "2020/01/01")}) == {"m0": q(58849.0, "d")}
    assert me.ismeasure(VLA) is True
    assert me.ismeasure({"value": 1.0, "unit": "m"}) is False
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert me.getvalue({"a": 1}) == {}
        assert me.getref({"a": 1}) == ""
        assert me.show({}) == ""
    assert caplog.messages == [
        "Illegal Measure record in MeasureHolder::fromRecord\nIncorrect input type for getvalue()",
        "Illegal Measure record in MeasureHolder::fromRecord\nIncorrect input type for getref()",
        "Illegal Measure record in MeasureHolder::fromRecord\nNon-measure input",
    ]


def test_measures_show_as_casa_shows_them() -> None:
    me = measures()
    assert me.show(me.direction("J2000", "12h00m00", "45d00m00")) == "[3.14159, 0.785398] J2000"
    assert me.show(me.epoch("UTC", "2020/01/01/00:00:00")) == "58849 UTC"
    assert me.show(me.epoch("UTC", "2020/01/01/06:00")) == "58849.2 UTC"
    assert me.show(VLA) == "[6.37358e+06, -1.87829, 0.591675] ITRF"
    assert me.show(me.position("WGS84", "10deg", "20deg", "30m")) == "[30, 0.174533, 0.349066] WGS84"
    assert me.show(me.frequency("LSRK", "1.4GHz"), False) == "1.4e+09 Hz"
    assert me.dirshow(me.direction("J2000", "12h00m00", "45d00m00")) == "[180, 45] deg  J2000"


def test_listcodes_lists_casacores_codes() -> None:
    me = measures()
    codes = me.listcodes(me.direction())
    assert codes["normal"].dtype.kind == codes["extra"].dtype.kind == "U"
    assert codes["normal"].tolist()[:6] == ["J2000", "JMEAN", "JTRUE", "APP", "B1950", "B1950_VLA"]
    assert len(codes["normal"]) == 24
    assert codes["extra"].tolist() == [
        "MERCURY", "VENUS", "MARS", "JUPITER", "SATURN", "URANUS", "NEPTUNE", "PLUTO", "SUN", "MOON", "COMET"]
    assert me.listcodes(me.epoch())["normal"].tolist() == [
        "LAST", "LMST", "GMST1", "GAST", "UT1", "UT2", "UTC", "TAI", "TDT", "TCG", "TDB", "TCB", "IAT",
        "GMST", "TT", "ET", "UT"]
    assert me.listcodes(me.epoch())["extra"].tolist() == []
    assert me.listcodes(me.frequency())["extra"].tolist() == ["Undefined"]


def test_separation_and_position_angle_follow_casacore() -> None:
    me = measures()
    origin = me.direction("J2000", "0deg", "0deg")
    assert me.separation(origin, me.direction("J2000", "1deg", "1deg")) == q(1.4141776609521137, "deg")
    assert me.posangle(origin, me.direction("J2000", "1deg", "1deg")) == q(44.995636455344844, "deg")
    assert me.posangle(origin, me.direction("J2000", "0deg", "10deg")) == q(-0.0, "deg")
    assert me.posangle(origin, me.direction("J2000", "10deg", "0deg")) == q(90.0, "deg")
    zero = me.separation(me.epoch(), me.direction())
    assert zero["unit"] == "deg" and np.array_equal(zero["value"], [0.0])


@needs_measures_data
def test_observatories_are_the_measures_datas() -> None:
    me = measures()
    close(me.observatory("VLA"), VLA, rel=1e-14)
    assert me.observatory() == ALMA == me.observatory("alma")
    # Degree-tabulated entries are converted to radians by the platform's
    # libm, which may round the last bit differently (Linux CI against macOS).
    close(me.observatory("gbt"), position("WGS84", -1.3934679600924218, 0.6707844683607905, 824.595),
          rel=1e-14)
    close(me.observatory("MeerKAT"), position("WGS84", 0.374266659468412, -0.5360464638235224, 1050.0),
          rel=1e-14)
    close(me.observatory("ATCA"), position("ITRF", 2.6101423190348916, -0.5261379196128062,
                                           6372960.257723439), rel=1e-14)


@needs_measures_data
def test_unknown_names_log_and_return_an_empty_record(caplog: pytest.LogCaptureFixture) -> None:
    me = measures()
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        assert me.observatory("BOGUS") == {}
        assert me.source("BOGUS") == {}
    assert caplog.messages == ["Unknown observatory asked for", "Unknown source asked for"]


@needs_measures_data
def test_sources_are_casa_directions() -> None:
    me = measures()
    assert me.source("0002-478") == direction("ICRS", 0.02004624063228046, -0.8308717646901654)
    assert me.source("SUN") == direction("SUN", 0.0, 1.5707963267948966)
    expected = direction("J2000", 0.17453292519943295, 0.3490658503988659)
    assert me.source("J2000 10deg 20deg") == expected
    assert me.source(me.direction("J2000", "10deg", "20deg")) == expected


@needs_measures_data
def test_conversions_without_earth_orientation_match_casa() -> None:
    me = measures()
    assert me.measure(ALMA, "WGS84") == ALMA
    # ITRF -> WGS84 agrees to 2 nm; WGS84 -> ITRF to 7 micrometres and 5e-12 rad.
    close(me.measure(me.observatory("VLA"), "WGS84"),
          position("WGS84", -1.8782942581394362, 0.5947874979128073, 2114.890229039825), rel=2e-12)
    close(me.measure(ALMA, "ITRF"),
          position("ITRF", -1.1825465955049892, -0.3994149869262737, 6379946.01326443),
          rel=2e-12, abs_=1e-11)
    utc = me.epoch("UTC", "2020/01/01/00:00:00")
    assert me.measure(utc, "TAI") == epoch("TAI", 58849.00042824074)
    assert me.measure(utc, "TDB") == epoch("TDB", 58849.00080073967)
    assert me.measure(utc, "TT") == epoch("TDT", 58849.00080074074)
    assert me.measure(utc, "FOO") == utc
    j2000 = me.direction("J2000", "10deg", "20deg")
    assert me.measure(j2000, "J2000") == j2000
    close(me.measure(j2000, "ICRS"),
          direction("ICRS", 0.17453284765818966, 0.34906593547799764), abs_=1e-11)


@needs_measures_data
def test_conversions_with_the_frame_match_casa_within_the_data() -> None:
    me = measures()
    target = me.direction("J2000", "12h00m00", "45d00m00")
    assert me.doframe(me.observatory("VLA")) is True
    assert me.doframe(me.epoch("UTC", "2020/01/01/00:00:00")) is True
    # Earth orientation comes from the measures data: CASA's and casa-rs's
    # differ by ~1e-10 rad with the same data, ~3e-9 rad with the snapshot.
    close(me.measure(target, "AZEL"),
          direction("AZEL", -0.09750451030886151, -0.19022892823948867), abs_=1e-8)
    close(me.measure(target, "HADEC"),
          direction("HADEC", 3.006257120884181, 0.7834133776932579), abs_=1e-8)
    close(me.measure(target, "APP"),
          direction("APP", -3.1371838138783303, 0.7834114999016991), abs_=1e-12)
    close(me.measure(me.epoch("UTC", "2020/01/01/00:00:00"), "UT1"),
          epoch("UT1", 58848.99999794946), abs_=1e-8)
    assert me.doframe(target) is True
    close(me.measure(me.frequency("LSRK", "1.4GHz"), "TOPO"),
          frequency("TOPO", 1400123730.9507515), rel=1e-12)
    close(me.measure(me.frequency("TOPO", "1.4GHz"), "BARY"),
          frequency("BARY", 1399908345.614245), rel=1e-12)
    assert me.doframe(me.frequency("LSRK", "1GHz")) is False


@needs_measures_data
def test_conversions_without_their_frame_fail_with_casas_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    me = measures()
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert me.measure(me.direction("J2000", "12h00m00", "45d00m00"), "AZEL") == {}
    assert caplog.messages[0].startswith(
        "Cannot convert due to missing frame information\nCannot convert measure")
    assert me.doframe(me.epoch("UTC", "2020/01/01")) is True
    assert me.done() is True
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert me.measure(me.direction("J2000", "12h00m00", "45d00m00"), "AZEL") == {}


def test_framenow_sets_an_epoch() -> None:
    assert measures().framenow() is True


@pytest.mark.parametrize("method", sorted(NOT_YET_IMPLEMENTED))
def test_unimplemented_methods_raise_not_implemented(method: str) -> None:
    required = [entry for entry in CASA["methods"][method] if entry["default"] is None]
    with pytest.raises(NotImplementedError, match=f"measures.{method} is not yet implemented in casa-rs"):
        getattr(measures(), method)(*[{} for _ in required])
