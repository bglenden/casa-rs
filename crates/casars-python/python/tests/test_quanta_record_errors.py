"""CASA's invalid-unit and malformed quantity-record diagnostics."""

import json
from pathlib import Path

import pytest

from casars.tools import quanta


def test_quantity_record_errors_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_record_errors.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        with pytest.raises(RuntimeError) as error:
            getattr(qa, case["method"])(*case["args"])
        assert str(error.value) == case["error"]["message"], case
    # CASA also checks an invalid unit before converting complex holders.
    for value in (1 + 1j, [1 + 1j], [1 + 1j, 2 + 2j]):
        for method, rest in ((qa.quantity, ()), (qa.add, (1.0,))):
            with pytest.raises(RuntimeError) as error:
                method({"value": value, "unit": "not-a-unit"}, *rest)
            assert str(error.value) == "Unit::check Illegal unit string 'not-a-unit'"
