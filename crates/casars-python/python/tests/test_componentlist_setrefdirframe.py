"""CASA direction-frame relabeling, unsupported frames and persistence."""

import json
import logging
import sys
import tarfile
from pathlib import Path

import pytest

from casars.tools import componentlist, table
from casars.data import schema_bundle

DATA = Path(__file__).parent / "data"
ORACLE = json.loads((DATA / "casa_componentlist_setrefdirframe.json").read_text())
INVALID = json.loads((DATA / "casa_componentlist_setrefdirframe_invalid.json").read_text())
UNSUPPORTED = {"MARS", "SUN"}


def components(cl):
    assert cl.addcomponent(flux=1, dir="J2000 10deg 20deg", label="first", shape="Gaussian",
                           majoraxis="2arcmin", minoraxis="1arcmin", positionangle="20deg")
    assert cl.addcomponent(flux=2, dir="GALACTIC 30deg -15deg", label="second")


def assert_directions(actual, expected):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert a["type"] == e["type"]
        assert a["refer"] == e["refer"]
        for axis in ("m0", "m1"):
            assert a[axis]["unit"] == e[axis]["unit"]
            assert a[axis]["value"] == pytest.approx(e[axis]["value"], rel=1e-14, abs=1e-15)


def snapshot(cl):
    return [cl.getrefdir(i) for i in range(cl.length())]


def test_setrefdirframe_matches_casa():
    for case in ORACLE["cases"]:
        cl = componentlist()
        if case["state"] == "populated":
            components(cl)
        assert_directions(snapshot(cl), case["before"])
        before = snapshot(cl)
        args = [bytes(arg["bytes"]) if isinstance(arg, dict) and "bytes" in arg else arg for arg in case["args"]]
        frame = args[1]
        if isinstance(frame, str) and frame in UNSUPPORTED:
            with pytest.raises(NotImplementedError, match="shared measure model.*#1053"):
                cl.setrefdirframe(*args, **case["kwargs"])
            assert_directions(snapshot(cl), before)
        elif "error" in case:
            expected = case["error"]
            error_type = {"TypeError": TypeError, "RuntimeError": RuntimeError}[expected["type"]]
            with pytest.raises(error_type) as error:
                cl.setrefdirframe(*args, **case["kwargs"])
            assert str(error.value).endswith(expected["message"]), case
            assert_directions(snapshot(cl), case["after"])
        else:
            assert cl.setrefdirframe(*args, **case["kwargs"]) is case["result"], case
            assert_directions(snapshot(cl), case["after"])
        assert cl.done()


def test_invalid_frames_leave_the_direction_unchanged(caplog):
    cl = componentlist()
    components(cl)
    before = snapshot(cl)
    with caplog.at_level(logging.ERROR, logger="casars.tools.componentlist"):
        for case in INVALID["cases"]:
            assert case["returncode"] == 0 and case["result"]["error"] == "RuntimeError"
            caplog.clear()
            frame = case["frame"]
            if frame.startswith("hex:"):
                frame = bytes.fromhex(frame[4:])
            with pytest.raises(RuntimeError, match="Could not parse the direction frame"):
                cl.setrefdirframe(0, frame)
            assert caplog.messages == ["Could not parse the 'frame' string: Direction frame not changed"]
            assert_directions(snapshot(cl), before)
    assert cl.done()


def test_frame_persistence_and_read_only_reopen(tmp_path):
    with tarfile.open(DATA / "casa_componentlist_setrefdirframe.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / "frames.cl")
    cl = componentlist()
    assert cl.open(path, nomodify=False)
    shape = cl.getshape(0)
    assert cl.setrefdirframe(0, "GALACTIC")
    assert cl.getshape(0) == shape
    assert cl.done()
    assert cl.open(path, nomodify=True)
    assert_directions(snapshot(cl), ORACLE["persisted"])
    assert cl.setrefdirframe(0, "ICRS")
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


def test_frame_setter_is_marked_mutating():
    contract = next(item for item in schema_bundle()["semantic"]["objects"] if item["name"] == "componentlist")
    method = next(item for item in contract["methods"] if item["name"] == "setrefdirframe")
    assert method["mutating"] is True
