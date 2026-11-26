"""CASA setlabel comparisons and table round trips."""
import json
from pathlib import Path

import pytest
from casars.tools import componentlist


def test_setlabel_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_setlabel.json").read_text())
    for case in fixture["cases"]:
        cl = componentlist()
        if case["state"] == "populated":
            assert cl.addcomponent(label="first")
            assert cl.addcomponent(label="second")
        value = case["value"]
        if isinstance(value, dict):
            value = value["bytes"].encode() if "bytes" in value else bytes.fromhex(value["hex"])
        if "error" in case:
            expected = case["error"]
            with pytest.raises({"TypeError": TypeError, "RuntimeError": RuntimeError}[expected["type"]]) as error:
                cl.setlabel(case["which"], value, case["log"])
            assert str(error.value).endswith(expected["message"]), case
        else:
            assert cl.setlabel(case["which"], value, case["log"]) is case["result"], case
        assert [cl.getlabel(i) for i in range(cl.length())] == case["labels"], case


def test_setlabel_table_roundtrip_and_readonly(tmp_path: Path) -> None:
    expected = json.loads((Path(__file__).parent / "data" / "casa_componentlist_setlabel.json").read_text())["table"]
    filename = str(tmp_path / "components")
    cl = componentlist()
    assert cl.addcomponent(label="first")
    assert cl.addcomponent(label="second")
    assert cl.rename(filename)
    assert cl.setlabel(1, "changed")
    assert cl.close()
    assert cl.open(filename, nomodify=True)
    assert [cl.getlabel(i) for i in range(2)] == expected["reopened"]
    assert cl.setlabel(0, "temporary")
    assert [cl.getlabel(i) for i in range(2)] == expected["readonly_memory"]
    assert cl.close()
    assert cl.open(filename)
    assert [cl.getlabel(i) for i in range(2)] == expected["after_readonly"]
    assert cl.done()


def test_invalid_utf8_does_not_replace_a_readable_label() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_setlabel.json").read_text())
    assert fixture["invalid_utf8"] == {"set": True, "read_error": "UnicodeDecodeError"}
    cl = componentlist()
    assert cl.addcomponent(label="preserved")
    with pytest.raises(UnicodeDecodeError):
        cl.setlabel(0, b"\xff")
    assert cl.getlabel(0) == "preserved"
