"""CASA's dimensionless unit '1' in quantity inputs and outputs."""
import json
from pathlib import Path

import pytest

from casars.tools import quanta


def test_unit_one_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_unit_one.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        method = getattr(qa, case["method"])
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                method(*case["args"])
            assert str(error.value) == case["error"]["message"], case
        else:
            assert method(*case["args"]) == case["result"], case
