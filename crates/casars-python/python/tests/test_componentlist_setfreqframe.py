"""CASA-recorded component frequency-frame changes and table behavior."""
import json
import logging
import sys
import tarfile
from pathlib import Path

import pytest
from casars.tools import componentlist, table

DATA = Path(__file__).parent / "data"
FIXTURE = json.loads((DATA / "casa_componentlist_setfreqframe.json").read_text())


def compare_frequency(actual, expected):
    assert actual["type"] == expected["type"]
    assert actual["refer"] == expected["refer"]
    assert actual["m0"]["unit"] == expected["m0"]["unit"]
    assert actual["m0"]["value"] == pytest.approx(expected["m0"]["value"], rel=1e-14, abs=1e-14)


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda c: repr((c["which"], c.get("frame", "default"), c.get("log", "default"))))
def test_setfreqframe_matches_casa(case, caplog):
    cl = componentlist()
    assert cl.addcomponent(freq="1.4GHz", label="first")
    assert cl.addcomponent(freq="2.25GHz", label="second")
    assert cl.convertfrequnit(0, "MHz")
    frame = case.get("frame")
    if isinstance(frame, dict):
        frame = frame["bytes"].encode()
    args = [case["which"]]
    if not case.get("omit_frame"):
        args.append(frame)
    if "log" in case:
        args.append(case["log"])
    with caplog.at_level(logging.ERROR, logger="casars.tools.componentlist"):
        if "error" in case:
            expected = case["error"]
            with pytest.raises({"TypeError": TypeError, "RuntimeError": RuntimeError,
                                "SystemError": SystemError}[expected["type"]]) as error:
                cl.setfreqframe(*args)
            assert str(error.value).endswith(expected["message"])
        else:
            assert cl.setfreqframe(*args) is case["result"]
            if case["result"] is False:
                assert "Could not parse the 'frame' string: Frequency frame not changed" in caplog.text
    for i, expected in enumerate(case["after"]):
        compare_frequency(cl.getspectrum(i)["frequency"], expected)
        compare_frequency(cl.getcomponent(i)["spectrum"]["frequency"], expected)
        compare_frequency(cl.torecord()[f"component{i}"]["spectrum"]["frequency"], expected)
    assert cl.done()


def test_casa_table_keeps_frequency_unit_value_and_error(tmp_path):
    with tarfile.open(DATA / "casa_componentlist_setfreqframe.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / "source.cl")
    expected = FIXTURE["table"]
    cl = componentlist()
    assert cl.open(path)
    assert cl.convertfrequnit(0, "MHz")
    assert cl.setfreqframe(0, "BARY")
    compare_frequency(cl.getspectrum(0)["frequency"], expected["spectrum"])
    assert cl.done()
    tb = table()
    assert tb.open(path)
    compare_frequency(tb.getcell("Spectral_Record", 0)["frequency"], expected["saved"]["raw"])
    assert tb.getcell("Reference_Frequency", 0) == pytest.approx(expected["saved"]["hz"])
    assert tb.getcell("Frequency_Frame", 0) == expected["saved"]["frame"]
    assert tb.getcell("Frequency_Error", 0) == pytest.approx(expected["saved"]["error"])
    assert tb.getcell("Frequency_Error_Units", 0) == expected["saved"]["error_unit"]
    tb.close()
    assert cl.open(path)
    compare_frequency(cl.getspectrum(0)["frequency"], expected["reopened"])
    assert cl.done()
