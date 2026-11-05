"""CASA's special underscore unit in numeric, string, and record quantities."""

import json
from pathlib import Path

import pytest

from casars.tools import quanta


def test_underscore_units_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_underscore_units.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                qa.quantity(*case["args"])
            expected = case["error"]["message"]
            # Record conversion currently wraps the underlying Unit::check
            # message; the parser's acceptance and error are the subject here.
            assert str(error.value) in (expected, f"Error {expected} in converting quantity"), case
        else:
            assert qa.quantity(*case["args"]) == case["result"], case
