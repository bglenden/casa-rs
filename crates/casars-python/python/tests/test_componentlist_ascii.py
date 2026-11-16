"""CASA's ASCII-conversion placeholder and argument validation."""
import json
import logging
from pathlib import Path

import pytest
from casars.tools import componentlist

ORACLE = json.loads((Path(__file__).parent / "data" / "casa_componentlist_ascii.json").read_text())


def convert(value):
    if isinstance(value, dict) and set(value) == {"_bytes"}:
        return bytes(value["_bytes"])
    return value


def test_ascii_placeholder_matches_casa(caplog):
    for state in ("empty", "populated", "closed"):
        cl = componentlist()
        if state != "empty":
            cl.addcomponent(flux=1, dir="J2000 00h00m00s +00d00m00s")
        if state == "closed":
            cl.done()
        for case in ORACLE["cases"]:
            if case["state"] != state:
                continue
            args = [convert(value) for value in case["args"]]
            kwargs = {key: convert(value) for key, value in case["kwargs"].items()}
            with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
                caplog.clear()
                if "error" in case:
                    error_type = {"TypeError": TypeError, "SystemError": SystemError}[case["error"]["type"]]
                    with pytest.raises(error_type) as error:
                        cl.asciitocomponentlist(*args, **kwargs)
                    assert str(error.value) == case["error"]["message"]
                else:
                    result = cl.asciitocomponentlist(*args, **kwargs)
                    assert result == case["result"] and type(result) is int
                    assert "asciitocomponentlist not implemented yet" in caplog.text
            assert cl.length() == case["length"]
        cl.done()


def test_ignored_record_integer_overflow_and_cycles(caplog):
    cl = componentlist()
    with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
        with pytest.raises(SystemError, match="returned a result with an exception set"):
            cl.asciitocomponentlist("unused", "unused", direction={"large": 2**100})
        assert "asciitocomponentlist not implemented yet" in caplog.text
    cycle = {}
    cycle["self"] = cycle
    assert ORACLE["cycle_returncode"] == -11
    assert cl.asciitocomponentlist("unused", "unused", direction=cycle) == 0


def test_ascii_placeholder_does_not_create_a_table(tmp_path):
    source = tmp_path / "components.txt"
    destination = tmp_path / "components.cl"
    source.write_text("not a component list\n")
    assert componentlist().asciitocomponentlist(str(destination), str(source)) == 0
    assert source.read_text() == "not a component list\n"
    assert not destination.exists()
