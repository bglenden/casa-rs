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


def test_pending_keyword_keeps_write_lock_until_close(tmp_path):
    """CASA leaves table.dat unchanged and excludes an external reader."""
    path = fixture_path(tmp_path)
    cl = componentlist()
    assert cl.open(path, nomodify=True)
    before = (Path(path) / "table.dat").read_bytes()
    assert cl.putkeyword("pending", 42) is None
    assert (Path(path) / "table.dat").read_bytes() == before
    code = """
import json, sys
from casars.tools import table
reader=table()
assert reader.open(sys.argv[1], lockoptions={'option':'usernoread'})
locked=reader.lock(write=False, nattempts=1)
print(json.dumps({'locked': locked, 'value': reader.getkeyword('pending') if locked else None}))
reader.close()
"""
    def probe():
        result = subprocess.run([sys.executable, "-c", code, path], capture_output=True, text=True, check=True, timeout=20)
        return json.loads(result.stdout)
    assert probe() == {"locked": False, "value": None}
    assert cl.done()
    assert probe() == {"locked": True, "value": 42}


def test_component_handles_see_each_others_pending_keywords(tmp_path):
    path = fixture_path(tmp_path)
    first, second = componentlist(), componentlist()
    assert first.open(path, nomodify=True)
    assert first.putkeyword("first", 11) is None
    assert second.open(path, nomodify=True)
    assert second.getkeyword("first") == 11
    assert second.putkeyword("second", 22) is None
    assert first.getkeyword("second") == 22
    assert first.getkeyword("first") == 11
    assert first.done()
    assert second.done()


def test_cached_reader_uses_publication_counter_not_file_timestamp(tmp_path):
    """A valid new table generation can retain its old size and timestamp."""
    import os
    path = fixture_path(tmp_path)
    cl = componentlist()
    assert cl.open(path, nomodify=True)
    assert cl.putkeyword("counter", 42) is None
    assert cl.done()
    assert cl.open(path, nomodify=True)
    assert cl.getkeyword("counter") == 42
    # CASA retains a read lock. Explicit clearlocks permits the external
    # writer; reacquisition must still use the publication counter, not mtime.
    assert table().clearlocks()
    control = Path(path) / "table.dat"
    before = control.stat()
    code = """
from casars.tools import table
import sys
t=table(); assert t.open(sys.argv[1], nomodify=False)
assert t.putkeyword('counter',43); t.close()
"""
    subprocess.run([sys.executable, "-c", code, path], capture_output=True, text=True, check=True, timeout=20)
    assert control.stat().st_size == before.st_size
    os.utime(control, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert cl.getkeyword("counter") == 43
    assert cl.done()



def test_clean_keyword_cache_retains_casa_read_lock_until_clearlocks(tmp_path):
    path = fixture_path(tmp_path)
    cl = componentlist()
    assert cl.open(path, nomodify=True)
    code = """
import json, sys
from casars.tools import table
writer=table(); assert writer.open(sys.argv[1], nomodify=False, lockoptions={'option':'usernoread'})
locked=writer.lock(write=True, nattempts=1)
if locked: assert writer.putkeyword('external',43)
writer.close(); print(json.dumps({'locked':locked}))
"""
    def probe():
        result=subprocess.run([sys.executable, '-c', code, path], capture_output=True, text=True, check=True, timeout=20)
        return json.loads(result.stdout)
    try:
        assert probe() == {"locked": False}
        assert cl.haskeyword("external") is False
        assert probe() == {"locked": False}
        assert table().clearlocks()
        assert probe() == {"locked": True}
        assert cl.getkeyword("external") == 43
        assert probe() == {"locked": False}
    finally:
        cl.done()
    assert probe() == {"locked": True}
