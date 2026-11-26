# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA offset records stay relative in memory and become absolute on disk."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from casars.tools import componentlist
from test_componentlist_records import assert_record

CASES = json.loads((Path(__file__).parent / "data/casa_componentlist_offsets.json").read_text())["cases"]


def act(cl, name):
    return {
        "identity": lambda: True,
        "convert_same": lambda: cl.convertrefdir(0, "J2000"),
        "convert_galactic": lambda: cl.convertrefdir(0, "GALACTIC"),
        "set_direction": lambda: cl.setrefdir(0, "30deg", "40deg"),
        "set_direction_frame": lambda: cl.setrefdirframe(0, "GALACTIC"),
        "set_shape": lambda: cl.setshape(0, "Gaussian", "4arcsec", "2arcsec", "30deg"),
        "set_frequency": lambda: cl.setfreq(0, 2.0, "GHz"),
        "set_frequency_frame": lambda: cl.setfreqframe(0, "LSRK"),
    }[name]()


def compare_record(actual, expected, case):
    if case["action"] == "convert_galactic":
        # Use test_componentlist_convertrefdir's existing scientific comparator.
        # casacore's 10-digit Galactic matrix is not exactly orthogonal; the
        # shared native converter normalizes it (a difference below 1e-10 rad).
        actual = deepcopy(actual)
        for key in ["m0", "m1"]:
            quantity = actual["shape"]["direction"][key]
            reference = expected["shape"]["direction"][key]
            assert quantity["value"] == pytest.approx(reference["value"], rel=1e-9, abs=1e-10)
            quantity["value"] = reference["value"]
    assert_record(actual, expected)


@pytest.mark.parametrize("case", CASES, ids=lambda c: c["name"])
def test_offset_record_and_persistence_match_casa(case, tmp_path):
    cl = componentlist()
    try:
        assert cl.add(case["record"])
        assert act(cl, case["action"]) is case["result"]
        compare_record(cl.getcomponent(0), case["before"], case)
        compare_record(cl.torecord()["component0"], case["before"], case)
        direction = {k: v for k, v in case["before"]["shape"]["direction"].items() if k != "error"}
        actual_direction = cl.getrefdir(0)
        if case["action"] == "convert_galactic":
            for key in ["m0", "m1"]:
                assert actual_direction[key]["value"] == pytest.approx(direction[key]["value"], rel=1e-9, abs=1e-10)
                actual_direction[key]["value"] = direction[key]["value"]
        assert_record(actual_direction, direction)
        path = str(tmp_path / "offset.cl")
        assert cl.rename(path)
        compare_record(cl.getcomponent(0), case["renamed"], case)
        assert cl.done()
        assert cl.open(path)
        compare_record(cl.getcomponent(0), case["after"], case)
    finally:
        cl.done()


def test_offsets_follow_components_through_sort_remove_recover_and_concatenate():
    record = deepcopy(CASES[0]["record"])
    cl = componentlist()
    other = componentlist()
    try:
        assert cl.addcomponent(flux=2, label="plain")
        assert cl.add(record)
        assert cl.sort("Flux")
        assert_record(cl.getcomponent(1), CASES[0]["before"])
        assert cl.remove([1])
        assert cl.recover() is False
        assert_record(cl.getcomponent(1), CASES[0]["before"])
        assert other.concatenate(cl.torecord(), [1])
        assert_record(other.getcomponent(0), CASES[0]["before"])
    finally:
        cl.done()
        other.done()
