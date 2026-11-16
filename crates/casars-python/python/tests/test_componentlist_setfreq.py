"""CASA-recorded component frequency changes and failure behavior."""
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from casars.tools import componentlist, table

DATA = Path(__file__).parent / "data"
FIXTURE = json.loads((DATA / "casa_componentlist_setfreq.json").read_text())


def compare_frequency(actual, expected):
    assert actual["type"] == expected["type"]
    assert actual["refer"] == expected["refer"]
    assert actual["m0"]["unit"] == expected["m0"]["unit"]
    assert actual["m0"]["value"] == pytest.approx(expected["m0"]["value"], rel=1e-14, abs=1e-14)


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda c: repr((c["which"], c["value"], c.get("unit", "default"), c.get("log", "default"))))
def test_setfreq_matches_casa(case):
    cl = componentlist()
    assert cl.addcomponent(freq="1.4GHz", label="first")
    assert cl.addcomponent(freq="2.25GHz", label="second")
    unit = case.get("unit")
    if isinstance(unit, dict):
        unit = unit["bytes"].encode()
    args = [case["which"], case["value"]]
    if not case.get("omit_unit"):
        args.append(unit)
    if "log" in case:
        args.append(case["log"])
    if "error" in case:
        expected = case["error"]
        with pytest.raises({"TypeError": TypeError, "RuntimeError": RuntimeError,
                            "SystemError": SystemError}[expected["type"]]) as error:
            cl.setfreq(*args)
        assert str(error.value).endswith(expected["message"])
    else:
        assert cl.setfreq(*args) is case["result"]
    for i, expected in enumerate(case["after"]):
        compare_frequency(cl.getspectrum(i)["frequency"], expected)
        compare_frequency(cl.getcomponent(i)["spectrum"]["frequency"], expected)
        compare_frequency(cl.torecord()[f"component{i}"]["spectrum"]["frequency"], expected)
    assert cl.done()


def test_bad_unit_is_rejected_without_mutating_the_component():
    assert FIXTURE["nonconforming"] == {"exit": -6, "stdout": "set=RuntimeError\nread=RuntimeError\n"}
    cl = componentlist()
    assert cl.addcomponent(freq="1.4GHz")
    before = cl.getspectrum(0)
    with pytest.raises(RuntimeError, match="frequency unit"):
        cl.setfreq(0, 3, "m")
    assert cl.getspectrum(0) == before
    assert cl.done()


def test_casa_table_preserves_frame_and_frequency_error(tmp_path):
    with tarfile.open(DATA / "casa_componentlist_setfreq.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / "source.cl")
    cl = componentlist()
    assert cl.open(path)
    expected = FIXTURE["table"]
    assert cl.setfreq(0, 1500, "MHz")
    compare_frequency(cl.getspectrum(0)["frequency"], expected["spectrum"])
    assert cl.done()
    tb = table()
    assert tb.open(path)
    compare_frequency(tb.getcell("Spectral_Record", 0)["frequency"], expected["saved"]["raw"])
    assert tb.getcell("Reference_Frequency", 0) == pytest.approx(expected["saved"]["hz"])
    assert tb.getcell("Frequency_Error", 0) == pytest.approx(expected["saved"]["error"])
    assert tb.getcell("Frequency_Error_Units", 0) == expected["saved"]["error_unit"]
    tb.close()
    assert cl.open(path)
    compare_frequency(cl.getspectrum(0)["frequency"], expected["reopened"])
    assert cl.done()


def test_redefined_hz_matches_casa():
    code = ("import json; from casars.tools import componentlist,quanta; "
            "c=componentlist(); c.addcomponent(freq='1.4GHz'); "
            "q=quanta(); q.define('Hz','2s-1'); c.setfreq(0,5,'MHz'); "
            "print(json.dumps(c.getspectrum(0)['frequency'])); c.done()")
    run = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    compare_frequency(json.loads(run.stdout), FIXTURE["redefined_hz"])
