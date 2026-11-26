"""Keyword reads compared with CASA 6.7.6-14 and a CASA-created table."""
import json
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest
from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / "data"
ORACLE = json.loads((DATA / "casa_componentlist_keyword_read.json").read_text())["results"]


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
    with tarfile.open(DATA / "casa_componentlist_keyword_read.cl.tgz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    return str(tmp_path / "keywords.cl")


def test_unattached_and_casa_keyword_reads(tmp_path):
    cl = componentlist()
    compare(ORACLE["unattached_get"], cl.getkeyword)
    compare(ORACLE["unattached_has"], cl.haskeyword)
    path = fixture_path(tmp_path)
    assert cl.open(path, nomodify=True)
    for name in ("metadata", "metadata.answer", "metadata.nested.x", "metadata.absent",
                 "missing.x", "metadata.answer.x", ".x", "metadata.", "", "Metadata", "scalar", "matrix"):
        compare(ORACLE["has:" + name], lambda name=name: cl.haskeyword(name))
        compare(ORACLE["get:" + name], lambda name=name: cl.getkeyword(name))
    compare(ORACLE["has_default"], cl.haskeyword)
    compare(ORACLE["get_default"], cl.getkeyword)
    compare(ORACLE["has_none"], lambda: cl.haskeyword(None))
    compare(ORACLE["get_none"], lambda: cl.getkeyword(None))
    compare(ORACLE["get_bytes"], lambda: cl.getkeyword(b"scalar"))
    compare(ORACLE["readonly"], lambda: cl.getkeyword("metadata"))
    assert cl.done()
    assert cl.open(path, nomodify=True)
    compare(ORACLE["get_default"], cl.getkeyword)
    assert cl.done()


def test_reads_latest_keyword_written_by_another_tool(tmp_path):
    path = fixture_path(tmp_path)
    cl = componentlist()
    assert cl.open(path, nomodify=True)
    tb = table()
    assert tb.open(path, nomodify=False)
    assert tb.putkeyword("external", {"value": 19})
    assert tb.close()
    assert cl.haskeyword("external")
    assert cl.getkeyword("external") == {"value": 19}
    assert cl.done()


def test_keyword_result_schema_allows_casas_nonrecord_results():
    objects = schema_bundle()["semantic"]["objects"]
    contract = next(item for item in objects if item["name"] == "componentlist")
    method = next(item for item in contract["methods"] if item["name"] == "getkeyword")
    assert method["result_schema"] == {}
