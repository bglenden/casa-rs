"""CASA flux-unit conversions, preserving complex values and errors."""
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest
from casars.tools import componentlist, table


DATA = Path(__file__).parent / "data"
FIXTURE = json.loads((DATA / "casa_componentlist_convertfluxunit.json").read_text())


def unpack(directory, archive_name="casa_convertfluxunit.cl.tgz"):
    directory.mkdir()
    with tarfile.open(DATA / archive_name) as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(directory, **extract)
    return directory / "source.cl"


def read_raw(path):
    tb = table()
    assert tb.open(str(path))
    rows = []
    for i in range(tb.nrows()):
        row = {"unit": tb.getcell("Flux_Unit", i)}
        for name in ("Flux", "Flux_Error"):
            row[name] = [[float(v.real), float(v.imag)] for v in tb.getcell(name, i)]
        rows.append(row)
    tb.close()
    return rows


def assert_raw(actual, expected):
    assert len(actual) == len(expected)
    for got, wanted in zip(actual, expected, strict=True):
        assert got["unit"] == wanted["unit"]
        for name in ("Flux", "Flux_Error"):
            np.testing.assert_allclose(got[name], wanted[name], rtol=1e-13, atol=1e-13)


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda c: repr((c["which"], c.get("unit", "default"))))
def test_conversion_matches_casa_and_preserves_complex_values(tmp_path, case):
    path = unpack(tmp_path / "case")
    assert_raw(read_raw(path), FIXTURE["before"])
    cl = componentlist()
    assert cl.open(str(path))
    unit = case.get("unit")
    if isinstance(unit, dict):
        unit = unit["bytes"].encode()
    args = (case["which"],) if case.get("omit_unit") else (case["which"], unit)
    if "error" in case:
        expected = case["error"]
        with pytest.raises({"TypeError": TypeError, "SystemError": SystemError, "RuntimeError": RuntimeError}[expected["type"]]) as error:
            cl.convertfluxunit(*args)
        assert str(error.value) == expected["message"]
    else:
        assert cl.convertfluxunit(*args) is case["result"]
    assert cl.done()
    assert_raw(read_raw(path), case["after"])


def test_nonconforming_source_cannot_corrupt_any_component(tmp_path):
    case = FIXTURE["nonconforming"]
    assert case["result"] is True and case["after"][1]["unit"] == "mJy"
    path = unpack(tmp_path / "case", "casa_convertfluxunit_nonconforming.cl.tgz")
    assert_raw(read_raw(path), case["before"])
    cl = componentlist()
    assert cl.open(str(path))
    with pytest.raises(RuntimeError, match="non-conforming"):
        cl.convertfluxunit(-1, "mJy")
    assert cl.done()
    assert_raw(read_raw(path), case["before"])


def test_convert_all_empty_is_catchable():
    assert FIXTURE["empty_exit"] == -11
    cl = componentlist()
    with pytest.raises(RuntimeError, match="empty list would crash CASA"):
        cl.convertfluxunit(-1, "mJy")
    assert cl.length() == 0
    assert cl.convertfluxunit(-1, "m") is False


def test_same_unit_name_is_a_noop_after_redefinition():
    # The definition is process-wide, so keep it out of the pytest process.
    code = ("import json; from casars.tools import componentlist,quanta; "
            "c=componentlist(); c.addcomponent(flux=2); q=quanta(); q.define('Jy','1m'); "
            "r=c.convertfluxunit(0,'Jy'); "
            "print(json.dumps({'result':r,'unit':c.getfluxunit(0),'values':c.getfluxvalue(0).tolist()}))")
    run = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert json.loads(run.stdout) == FIXTURE["same_name_override"]
