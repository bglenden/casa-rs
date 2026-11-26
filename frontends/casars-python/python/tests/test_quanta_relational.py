"""Scalar quantity comparisons recorded from CASA 6.7.6.14."""

import json
from pathlib import Path

import pytest

from casars.tools import quanta


def test_relational_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_relational.json").read_text())
    special = {"NaN": float("nan"), "Infinity": float("inf"), "-Infinity": -float("inf")}
    qa = quanta()
    for case in fixture["cases"]:
        method = getattr(qa, case["method"])
        args = [special.get(value, value) if isinstance(value, str) else value for value in (case["left"], case["right"])]
        if "error" in case:
            assert case["error"]["type"] == "RuntimeError"
            with pytest.raises(RuntimeError) as error:
                method(*args)
            assert str(error.value) == case["error"]["message"], case
        else:
            assert method(*args) is case["result"], case
    assert qa.eq() is True
    assert qa.lt() is False
