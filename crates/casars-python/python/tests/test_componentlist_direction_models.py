# SPDX-License-Identifier: LGPL-3.0-or-later
"""All CASA component direction labels, including the dynamical labels."""
import json
import sys
import tarfile
from pathlib import Path

import pytest

from casars.tools import componentlist

DATA = Path(__file__).parent / "data"
ORACLE = json.loads((DATA / "casa_componentlist_direction_models.json").read_text())


def assert_direction(actual, expected):
    assert actual.keys() == expected.keys()
    assert actual["refer"] == expected["refer"]
    assert actual["type"] == expected["type"]
    for key in ["m0", "m1"]:
        assert actual[key]["unit"] == expected[key]["unit"]
        assert actual[key]["value"] == pytest.approx(expected[key]["value"], rel=1e-9, abs=1e-10)


@pytest.mark.parametrize("case", ORACLE["cases"], ids=lambda case: f"{case.get('source', 'J2000')}:{case['method']}:{case['frame']}")
def test_direction_model_labels_match_casa(case):
    cl = componentlist()
    assert cl.addcomponent(dir="J2000 10deg 20deg")
    if "source" in case:
        assert cl.setrefdirframe(0, case["source"])
    if "error" in case:
        with pytest.raises(RuntimeError) as error:
            getattr(cl, case["method"])(0, case["frame"])
        assert str(error.value) == case["error"]
    else:
        assert getattr(cl, case["method"])(0, case["frame"]) is case["result"]
    assert_direction(cl.getrefdir(0), case["after"])
    cl.done()


def test_direction_model_table_casa_to_native_and_native_reopen(tmp_path):
    with tarfile.open(DATA / "casa_componentlist_direction_models.cl.tgz") as archive:
        archive.extractall(tmp_path, **({"filter": "data"} if sys.version_info >= (3, 12) else {}))
    cl = componentlist()
    assert cl.open(str(tmp_path / "frames.cl"), nomodify=False)
    expected = [case for case in ORACLE["cases"] if case["method"] == "setrefdirframe"]
    for i, case in enumerate(expected):
        assert cl.getlabel(i) == case["frame"]
        assert_direction(cl.getrefdir(i), case["after"])
    assert cl.done()
    assert cl.open(str(tmp_path / "frames.cl"))
    for i, case in enumerate(expected):
        assert_direction(cl.getrefdir(i), case["after"])
    assert cl.done()
