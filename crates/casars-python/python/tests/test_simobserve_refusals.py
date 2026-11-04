"""CASA 6.7.6.14's simobserve type refusals (#1099)."""

import json
from pathlib import Path

import pytest

from casars import tasks


CASES = json.loads((Path(__file__).parent / "data" / "casa_simobserve_refusals.json").read_text())


@pytest.mark.parametrize("case", [case for case in CASES if case["type"] == "AssertionError"])
def test_integer_type_refusal_matches_casa(case: dict) -> None:
    # casa-rs raises TypeError for invalid parameter types by the #931 decision.
    with pytest.raises(TypeError) as error:
        tasks.simobserve(**case["args"])
    assert str(error.value) == case["message"]
