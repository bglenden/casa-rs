# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 orderings and exact visible summary text."""
import json
import logging
from pathlib import Path

import pytest

from casars.tools import componentlist

DATA = Path(__file__).parent / "data"
SORT = json.loads((DATA / "casa_componentlist_sort.json").read_text())
SUMMARIES = json.loads((DATA / "casa_componentlist_summaries.json").read_text())["summaries"]


@pytest.mark.parametrize("case", SORT["cases"], ids=lambda case: repr(case["criteria"]))
def test_sort_matches_casa(case):
    cl = componentlist()
    for flux, direction, label, major in SORT["entries"]:
        assert cl.addcomponent(flux=flux, dir=direction, label=label, shape="Gaussian", majoraxis=major, minoraxis="0.5arcsec", positionangle="10deg")
    shapes = {cl.getlabel(i): cl.getshape(i) for i in range(cl.length())}
    assert cl.select([0, 2])
    if "error" in case:
        with pytest.raises(RuntimeError) as error:
            cl.sort(case["criteria"])
        expected = "\n".join(line.split("\t", 3)[-1] for line in case["error"]["message"].splitlines())
        assert str(error.value) == expected
    else:
        assert cl.sort(case["criteria"]) is case["result"]
    assert [cl.getlabel(i) for i in range(cl.length())] == case["labels"]
    assert list(cl.selected()) == case["selected"]
    for i in range(cl.length()):
        assert cl.getshape(i) == shapes[cl.getlabel(i)]
    cl.done()


@pytest.mark.parametrize("name", SUMMARIES)
def test_summarize_visible_text_matches_casa(name, caplog):
    cl = componentlist()
    shape = {"point": "Point", "disk": "Disk"}.get(name, "Gaussian")
    assert cl.addcomponent(flux=[3, 1, 0.5, 0.25], dir="J2000 10deg -20deg", label="example", shape=shape, majoraxis="1arcmin" if name == "disk" else "4arcsec", minoraxis="0.5arcmin" if name == "disk" else "2arcsec", positionangle="-30deg" if name == "disk" else "30deg")
    if name in ["errors", "precision_boundary", "limbdisk"]:
        record = cl.getcomponent(0)
        if name in ["errors", "precision_boundary"]:
            record["shape"]["direction"]["error"] = {"longitude": {"value": 1.50015 if name == "precision_boundary" else 0.02, "unit": "arcsec"}, "latitude": {"value": 3.2 if name == "precision_boundary" else 0.03, "unit": "arcsec"}}
            for key, value in [("majoraxiserror", 0.2), ("minoraxiserror", 0.1), ("positionangleerror", 0.5)]:
                record["shape"][key] = {"value": value, "unit": "deg" if key == "positionangleerror" else "arcsec"}
        else:
            record["shape"]["type"] = "Limbdarkeneddisk"
            record["optionalParameters"] = [0.5]
        cl.done()
        assert cl.add(record)
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="casars.tools.componentlist"):
        assert cl.summarize(0)
    assert caplog.messages == [SUMMARIES[name].rstrip("\n")]
    cl.done()


@pytest.mark.parametrize("which", [-1, 0, 3])
def test_summarize_empty_list_exact_error(which, caplog):
    cl = componentlist()
    message = f"List has only 0 components, but zero-based component {which} specified. Please specify a component less than 0"
    with pytest.raises(RuntimeError) as error:
        cl.summarize(which)
    assert str(error.value) == message
    assert caplog.messages == ["Exception Reported: " + message]
    cl.done()


def test_sort_accepts_bytes_and_checks_log():
    cl = componentlist()
    assert cl.sort(b"fLuX\0ignored", log=False)
    with pytest.raises(TypeError, match="argument _criteria must be a string"):
        cl.sort(3)
    with pytest.raises(TypeError, match="argument 3 of type 'bool'"):
        cl.sort(log=1)
    cl.done()
