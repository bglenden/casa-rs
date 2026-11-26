"""CASA Undefined frequency frame records, conversion and table round trips."""
import json
import sys
import tarfile
from pathlib import Path

import pytest
from casars.tools import componentlist, measures, table

DATA = Path(__file__).parent / "data"
FIXTURE = json.loads((DATA / "casa_undefined_frequency.json").read_text())


def compare_frequency(actual, expected):
    assert actual["type"] == expected["type"]
    assert actual["refer"] == expected["refer"]
    assert actual["m0"]["unit"] == expected["m0"]["unit"]
    assert actual["m0"]["value"] == pytest.approx(expected["m0"]["value"], rel=1e-14)


def test_measures_preserve_undefined_and_refuse_cross_frame_conversion():
    expected = FIXTURE["measures"]
    me = measures()
    undefined = me.frequency("Undefined", "1.4GHz")
    compare_frequency(undefined, expected["undefined"])
    compare_frequency(me.measure(undefined, "Undefined"), expected["same"])
    assert me.measure(undefined, "LSRK") == expected["to_lsrk"]
    lsrk = me.frequency("LSRK", "1.4GHz")
    assert me.measure(lsrk, "Undefined") == expected["from_lsrk"]


def test_casa_undefined_component_table_round_trip(tmp_path):
    with tarfile.open(DATA / "casa_undefined_frequency.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / "source.cl")
    expected = FIXTURE["table"]
    cl = componentlist()
    assert cl.open(path)
    compare_frequency(cl.getspectrum(0)["frequency"], expected["reopened"])
    assert cl.convertfrequnit(0, "MHz")
    compare_frequency(cl.getspectrum(0)["frequency"], expected["spectrum"])
    assert cl.done()
    tb = table()
    assert tb.open(path)
    saved = expected["saved"]
    assert tb.getcell("Frequency_Frame", 0) == saved["frame"]
    assert tb.getcell("Reference_Frequency", 0) == pytest.approx(saved["hz"])
    assert tb.getcell("Frequency_Error", 0) == pytest.approx(saved["error"])
    assert tb.getcell("Frequency_Error_Units", 0) == saved["error_unit"]
    compare_frequency(tb.getcell("Spectral_Record", 0)["frequency"], saved["raw"])
    tb.close()
    assert cl.open(path)
    assert cl.setfreqframe(0, "BARY")
    compare_frequency(cl.getspectrum(0)["frequency"], expected["relabeled"])
    assert cl.done()


@pytest.mark.parametrize(("fixture", "path_name", "code", "key"), [
    ("casa_undefined_frequency_integer.cl.tgz", "integer.cl", 64, "integer_reference"),
    ("casa_undefined_frequency_remapped.cl.tgz", "remapped.cl", 9, "remapped_reference"),
    ("casa_undefined_frequency_sparse.cl.tgz", "sparse.cl", 36, "sparse_reference"),
])
def test_casa_component_table_with_integer_reference_code(tmp_path, fixture, path_name, code, key):
    with tarfile.open(DATA / fixture) as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / path_name)
    tb = table()
    assert tb.open(path)
    assert tb.getcell("Frequency_Code", 0) == code
    tb.close()
    cl = componentlist()
    assert cl.open(path, nomodify=True)
    compare_frequency(cl.getspectrum(0)["frequency"], FIXTURE[key]["spectrum"])
    assert cl.done()


def test_casa_rejects_noncanonical_integer_mapping_type(tmp_path):
    assert FIXTURE["alias_reference"]["stdout"] == "open=RuntimeError\n"
    with tarfile.open(DATA / "casa_undefined_frequency_alias.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    cl = componentlist()
    with pytest.raises(RuntimeError, match="unknown frame code 40"):
        cl.open(str(tmp_path / "alias.cl"), nomodify=True)
    assert cl.done()
