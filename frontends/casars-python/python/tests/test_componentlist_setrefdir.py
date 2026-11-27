"""CASA component direction setters, error safety and table persistence."""

import json
import sys
import tarfile
from pathlib import Path

import pytest

from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / "data"
ORACLE = json.loads((DATA / "casa_componentlist_setrefdir.json").read_text())
UNSAFE = json.loads((DATA / "casa_componentlist_setrefdir_crashes.json").read_text())


def components(cl):
    assert cl.addcomponent(flux=1, dir="J2000 10deg 20deg", label="first", shape="Gaussian",
                           majoraxis="2arcmin", minoraxis="1arcmin", positionangle="20deg")
    assert cl.addcomponent(flux=2, dir="GALACTIC 30deg -15deg", label="second")


def snapshot(cl):
    return [cl.getrefdir(i) for i in range(cl.length())]


def assert_directions(actual, expected):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert a["type"] == e["type"]
        assert a["refer"] == e["refer"]
        for axis in ("m0", "m1"):
            assert a[axis]["unit"] == e[axis]["unit"]
            assert a[axis]["value"] == pytest.approx(e[axis]["value"], rel=1e-14, abs=1e-15)


def test_setrefdir_matches_casa_and_preserves_on_failed_index():
    for case in ORACLE["cases"]:
        cl = componentlist()
        if case["state"] != "empty":
            components(cl)
        if case["state"] == "closed":
            assert cl.done()
        assert_directions(snapshot(cl), case["before"])
        if "error" in case:
            expected = case["error"]
            error_type = {"TypeError": TypeError, "RuntimeError": RuntimeError,
                          "SystemError": SystemError}[expected["type"]]
            with pytest.raises(error_type) as error:
                cl.setrefdir(*case["args"], **case["kwargs"])
            assert expected["message"] in str(error.value), case
            # CASA mutates every component on these failed index conversions.
            safe_after = case["before"] if expected["type"] == "SystemError" else case["after"]
            assert_directions(snapshot(cl), safe_after)
        else:
            assert cl.setrefdir(*case["args"], **case["kwargs"]) is case["result"], case
            assert_directions(snapshot(cl), case["after"])
        assert cl.done()


def test_unsafe_casa_inputs_are_catchable_and_preserve_components():
    cases = {case["case"]: case for case in UNSAFE["cases"]}
    assert cases["empty_all"]["returncode"] == -11
    assert cases["cycle"]["returncode"] == -11
    for name in ("nan", "inf", "-inf"):
        assert cases[name]["result"] == {"result": True, "longitude_is_nan": True}
    cl = componentlist()
    with pytest.raises(RuntimeError, match="empty list"):
        cl.setrefdir(-1, "1rad", "0.5rad")
    components(cl)
    before = snapshot(cl)
    cyclic = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValueError, match="cyclic"):
        cl.setrefdir(0, cyclic, "0deg")
    for number in (float("nan"), float("inf"), -float("inf")):
        with pytest.raises(RuntimeError, match="non-finite"):
            cl.setrefdir(0, {"value": number, "unit": "deg"}, "0deg")
    assert_directions(snapshot(cl), before)
    assert cl.done()


def test_direction_persistence_and_read_only_reopen(tmp_path):
    with tarfile.open(DATA / "casa_componentlist_setrefdir.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / "directions.cl")
    cl = componentlist()
    assert cl.open(path, nomodify=False)
    shape = cl.getshape(0)
    assert cl.setrefdir(0, "1rad", "0.5rad")
    assert cl.getshape(0) == shape
    assert cl.done()
    assert cl.open(path, nomodify=True)
    assert_directions(snapshot(cl), ORACLE["persisted"])
    assert cl.setrefdir(0, "2rad", "-0.5rad")
    assert_directions(snapshot(cl), ORACLE["read_only_memory"])
    assert cl.done()
    assert cl.open(path, nomodify=True)
    assert_directions(snapshot(cl), ORACLE["read_only_disk"])
    assert cl.done()
    tb = table()
    assert tb.open(path)
    assert tb.getkeyword("stable") == 17
    assert [tb.getcell("Label", i) for i in range(2)] == ["first", "second"]
    assert tb.getcell("Direction_Error", 0).tolist() == pytest.approx(ORACLE["direction_error"])
    assert tb.close()


def test_direction_setter_is_marked_mutating():
    contract = next(item for item in schema_bundle()["semantic"]["objects"] if item["name"] == "componentlist")
    method = next(item for item in contract["methods"] if item["name"] == "setrefdir")
    assert method["mutating"] is True
