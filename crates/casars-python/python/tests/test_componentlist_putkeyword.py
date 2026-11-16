"""CASA component-list keyword writes, persistence, and concurrent updates."""
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest
from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / "data"
ORACLE = json.loads((DATA / "casa_componentlist_putkeyword.json").read_text())["results"]


def normalize(value):
    if isinstance(value, np.ndarray):
        return {"array": value.tolist(), "dtype": value.dtype.kind, "shape": list(value.shape)}
    if isinstance(value, dict):
        return {key: normalize(value[key]) for key in sorted(value)}
    if isinstance(value, np.generic):
        return value.item()
    return value


def compare(expected, action):
    if "error" in expected:
        with pytest.raises({"TypeError": TypeError, "RuntimeError": RuntimeError}[expected["error"]]) as error:
            action()
        assert str(error.value).splitlines()[0] == expected["message"]
    else:
        assert normalize(action()) == expected["value"]


def fixture_path(tmp_path):
    with tarfile.open(DATA / "casa_componentlist_putkeyword.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    return str(tmp_path / "keywords.cl")


def test_casa_putkeyword_and_persistence(tmp_path):
    cl = componentlist()
    compare(ORACLE["unattached"], lambda: cl.putkeyword("x", 3))
    path = fixture_path(tmp_path)
    assert cl.open(path)
    steps = [
        ("default", ()),
        ("integer", ("count", 42)), ("integer_update", ("count", 43)),
        ("mismatch", ("count", 1.5)),
        ("record", ("record", {"x": 1, "nested": {"n": 2}})),
        ("child_update", ("record.x", 9)), ("child_add", ("record.new", "yes")),
        ("missing_parent", ("missing.x", 5)), ("scalar_parent", ("record.x.y", 5)),
        ("invalid_name", (".bad", 1)), ("empty_1", ("", 7)), ("empty_2", ("", 8)),
        ("bool", ("flag", True)),
        ("matrix", ("matrix", np.array([[1, 2], [3, 4]], dtype=np.int32))),
        ("typed_empty", ("typed_empty", np.array([], dtype=np.int32))),
        ("nested_empty", ("nested_empty", {"none": None, "empty": [], "tuple": ()})),
        ("none", ("none", None)), ("wrong_name", (None, 3)),
    ]
    for name, args in steps:
        compare(ORACLE[name], lambda args=args: cl.putkeyword(*args))
    compare(ORACLE["record_after"], lambda: cl.getkeyword("record"))
    compare(ORACLE["count_after"], lambda: cl.getkeyword("count"))
    assert cl.done()
    assert cl.open(path, nomodify=True)
    compare(ORACLE["readonly_put"], lambda: cl.putkeyword("readonly", {"value": 19}))
    compare(ORACLE["readonly_get"], lambda: cl.getkeyword("readonly"))
    assert cl.done()
    tb = table()
    assert tb.open(path)
    assert normalize(tb.getkeywords()) == ORACLE["persisted"]
    assert tb.getcell("Label", 0) == ORACLE["component_label"]
    assert tb.close()


def test_overflow_preserves_new_values_and_refuses_loss_on_replacement(tmp_path):
    cl = componentlist()
    assert cl.open(fixture_path(tmp_path))
    assert cl.putkeyword("large", 2**40) is None
    assert cl.getkeyword("large") == 2**40
    assert cl.putkeyword("small", 2) is None
    with pytest.raises(RuntimeError, match="does not fit the integer keyword small"):
        cl.putkeyword("small", 2**40)
    assert cl.getkeyword("small") == 2
    assert cl.done()


def test_keyword_and_pending_component_change_both_survive_close(tmp_path):
    path = fixture_path(tmp_path)
    cl = componentlist()
    assert cl.open(path)
    assert cl.setlabel(0, "changed")
    assert cl.putkeyword("new", 42) is None
    assert cl.done()
    tb = table()
    assert tb.open(path)
    assert tb.getcell("Label", 0) == "changed"
    assert tb.getkeyword("new") == 42
    assert tb.close()


def test_two_processes_keep_each_others_keywords(tmp_path):
    path = fixture_path(tmp_path)
    code = """
import sys
from casars.tools import componentlist
cl=componentlist(); cl.open(sys.argv[1], nomodify=True)
print('ready', flush=True); sys.stdin.readline()
cl.putkeyword(sys.argv[2], int(sys.argv[3])); cl.done()
"""
    children = [subprocess.Popen([sys.executable, "-c", code, path, key, str(value)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
                for key, value in (("first", 11), ("second", 22))]
    try:
        for child in children:
            assert child.stdout.readline().strip() == "ready"
        for child in children:
            child.stdin.write("go\n")
            child.stdin.flush()
        for child in children:
            _, err = child.communicate(timeout=60)
            assert child.returncode == 0, err
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()
    cl = componentlist()
    assert cl.open(path, nomodify=True)
    assert cl.getkeyword("first") == 11
    assert cl.getkeyword("second") == 22
    assert cl.getkeyword("initial") == {"stable": 17}
    assert cl.done()


@pytest.mark.parametrize("option", ["auto", "user", "autonoread", "usernoread"])
def test_an_open_table_tool_sees_the_component_list_write(tmp_path, option):
    path = fixture_path(tmp_path)
    alias = tmp_path / "alias.cl"
    alias.symlink_to(path, target_is_directory=True)
    cl = componentlist()
    assert cl.open(path)
    tb = table()
    assert tb.open(str(alias), lockoptions={"option": option})
    assert cl.putkeyword("shared", 42) is None
    assert tb.getkeyword("shared") == 42
    assert "shared" in tb.keywordnames()
    if option.endswith("noread"):
        assert not tb.haslock(write=False)
    assert tb.close()
    assert cl.done()


@pytest.mark.parametrize("option", ["auto", "user"])
def test_an_open_writable_table_keeps_component_list_keywords(tmp_path, option):
    path = fixture_path(tmp_path)
    tb = table()
    assert tb.open(path, nomodify=False, lockoptions={"option": option})
    if option == "user":
        assert tb.lock(write=False)
    cl = componentlist()
    assert cl.open(path, nomodify=True)
    assert cl.putkeyword("from_cl", 42) is None
    if option == "user":
        assert tb.lock(write=True)
    assert tb.putkeyword("from_tb", 99)
    assert tb.close()
    assert cl.getkeyword("from_cl") == 42
    assert cl.getkeyword("from_tb") == 99
    assert cl.done()


def test_putkeyword_result_schema_is_mutating_and_not_a_boolean():
    contract = next(item for item in schema_bundle()["semantic"]["objects"] if item["name"] == "componentlist")
    method = next(item for item in contract["methods"] if item["name"] == "putkeyword")
    assert method["mutating"] is True
    assert method["result_schema"] == {}
