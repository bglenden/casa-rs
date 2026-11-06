"""CASA frequency-unit conversion, recovery and table persistence."""
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from casars.tools import componentlist, table


DATA = Path(__file__).parent / "data"
FIXTURE = json.loads((DATA / "casa_componentlist_convertfrequnit.json").read_text())


def make_list():
    cl = componentlist()
    assert cl.addcomponent(freq="1.4GHz", label="first")
    assert cl.addcomponent(freq="2.25GHz", label="second")
    return cl


def check_spectrum(actual, expected):
    assert actual["type"] == expected["type"]
    assert actual["frequency"]["type"] == expected["frequency"]["type"]
    assert actual["frequency"]["refer"] == expected["frequency"]["refer"]
    got = actual["frequency"]["m0"]
    wanted = expected["frequency"]["m0"]
    assert got["unit"] == wanted["unit"]
    assert got["value"] == pytest.approx(wanted["value"], rel=1e-14, abs=1e-14)


def check_snapshot(cl, expected):
    assert cl.length() == len(expected)
    record = cl.torecord()
    for i, item in enumerate(expected):
        assert cl.getlabel(i) == item["label"]
        check_spectrum(cl.getspectrum(i), item["spectrum"])
        check_spectrum(cl.getcomponent(i)["spectrum"], item["spectrum"])
        check_spectrum(record[f"component{i}"]["spectrum"], item["spectrum"])


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda c: repr((c["which"], c.get("unit", "default"))))
def test_conversion_matches_casa(case):
    cl = make_list()
    unit = case.get("unit")
    if isinstance(unit, dict):
        unit = unit["bytes"].encode()
    args = (case["which"],) if case.get("omit_unit") else (case["which"], unit)
    if "error" in case:
        expected = case["error"]
        with pytest.raises({"TypeError": TypeError, "RuntimeError": RuntimeError}[expected["type"]]) as error:
            cl.convertfrequnit(*args)
        assert str(error.value).endswith(expected["message"])
    else:
        assert cl.convertfrequnit(*args) is case["result"]
    check_snapshot(cl, case["after"])
    assert cl.done()


def test_nonfrequency_unit_is_refused_before_corrupting_component():
    assert FIXTURE["nonconforming"]["exit"] == -6
    assert FIXTURE["nonconforming"]["stdout"] == "convert=RuntimeError\nread=RuntimeError\n"
    cl = make_list()
    with pytest.raises(RuntimeError, match="frequency unit"):
        cl.convertfrequnit(0, "m")
    check_snapshot(cl, FIXTURE["table"]["reopened"])
    assert cl.done()


def test_recovery_preserves_the_frequency_unit():
    cl = make_list()
    assert cl.convertfrequnit(0, "MHz")
    assert cl.remove(0)
    check_snapshot(cl, FIXTURE["recovery"]["removed"])
    assert cl.recover() is False
    check_snapshot(cl, FIXTURE["recovery"]["recovered"])
    assert cl.done()


def test_table_record_and_reopen(tmp_path):
    cl = make_list()
    assert cl.convertfrequnit(0, "MHz")
    check_snapshot(cl, FIXTURE["table"]["in_memory"])
    path = str(tmp_path / "components.cl")
    assert cl.rename(path)
    assert cl.close()
    tb = table()
    assert tb.open(path)
    raw = tb.getcell("Spectral_Record", 0)["frequency"]["m0"]
    assert raw["unit"] == FIXTURE["table"]["raw"]["unit"]
    assert raw["value"] == pytest.approx(FIXTURE["table"]["raw"]["value"])
    assert tb.getcell("Reference_Frequency", 0) == pytest.approx(FIXTURE["table"]["table_hz"])
    assert tb.getcell("Frequency_Error_Units", 0) == FIXTURE["table"]["error_unit"]
    tb.close()
    assert cl.open(path, nomodify=True)
    check_snapshot(cl, FIXTURE["table"]["reopened"])
    assert cl.done()


def test_casa_table_frequency_error_is_preserved(tmp_path):
    with tarfile.open(DATA / "casa_convertfrequnit.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / "source.cl")
    cl = componentlist()
    assert cl.open(path)
    assert cl.convertfrequnit(0, "kHz")
    check_snapshot(cl, FIXTURE["table"]["changed_with_error"])
    assert cl.done()
    tb = table()
    assert tb.open(path)
    expected = FIXTURE["table"]["error_after"]
    assert tb.getcell("Frequency_Error", 0) == pytest.approx(expected["value"])
    assert tb.getcell("Frequency_Error_Units", 0) == expected["unit"]
    tb.close()


def test_unit_redefinition_uses_the_stored_target_and_current_hz():
    # Definitions are process-wide; isolate changes from the pytest process.
    code = ("import json; from casars.tools import componentlist,quanta; "
            "q=quanta(); q.define('casaFreq','1MHz'); c=componentlist(); c.addcomponent(freq='1.4GHz'); "
            "c.convertfrequnit(0,'casaFreq'); a=c.getspectrum(0)['frequency']['m0']; "
            "q.define('casaFreq','1kHz'); b=c.getspectrum(0)['frequency']['m0']; "
            "q.define('Hz','2Hz'); d=c.getspectrum(0)['frequency']['m0']; "
            "print(json.dumps([a,b,d])); c.done()")
    run = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert json.loads(run.stdout) == FIXTURE["redefined_units"]


def test_nonconforming_hz_definition_does_not_destroy_the_spectrum(tmp_path):
    assert FIXTURE["nonconforming_hz"] == {"exit": -6, "stdout": "read=RuntimeError\n"}
    code = ("import json; from casars.tools import componentlist,quanta; "
            "import sys; c=componentlist(); c.addcomponent(freq='1.4GHz'); c.convertfrequnit(0,'MHz'); "
            "c.rename(sys.argv[1]); "
            "q=quanta(); q.define('Hz','1m'); "
            "\na=c.getspectrum(0)['frequency']['m0']; c.close(); q.define('Hz','1s-1'); "
            "c.open(sys.argv[1]); b=c.getspectrum(0)['frequency']['m0']; print(json.dumps([a,b])); c.done()")
    run = subprocess.run([sys.executable, "-c", code, str(tmp_path / "components.cl")], capture_output=True, text=True, check=True)
    before_close, after_reopen = json.loads(run.stdout)
    assert before_close["unit"] == "MHz"
    assert before_close["value"] == pytest.approx(1400.0)
    assert after_reopen["unit"] == "GHz"
    assert after_reopen["value"] == pytest.approx(1.4)


def test_destructor_saves_one_unit_definition_during_concurrent_redefinitions(tmp_path):
    # The destructor releases the GIL while saving. Unit definitions are
    # process-wide, so run this in a subprocess and inspect the raw records.
    code = """
import sys, threading
from casars.tools import componentlist, quanta, table
c = componentlist()
for _ in range(1500):
    c.addcomponent(freq='1.4GHz')
    c.convertfrequnit(c.length() - 1, 'MHz')
c.rename(sys.argv[1])
q = quanta()
started = threading.Event()
stop = threading.Event()
def redefine():
    while not stop.is_set():
        q.define('Hz', '1s-1')
        q.define('Hz', '2s-1')
        started.set()
t = threading.Thread(target=redefine)
t.start(); started.wait()
del c
stop.set(); t.join()
tb = table(); tb.open(sys.argv[1])
values = {tb.getcell('Spectral_Record', i)['frequency']['m0']['value'] for i in range(1500)}
tb.close()
assert len(values) == 1 and values <= {1400.0, 2800.0}, values
"""
    run = subprocess.run([sys.executable, "-c", code, str(tmp_path / "components.cl")],
                         capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
