"""CASA scalar integer kinds inside quantity records."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_record_scalar_types_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_record_scalar_types.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        value = case["value"]
        if isinstance(value, dict):
            value = np.dtype(value["dtype"]).type(value["numpy_scalar"])
        args = ({"value": value, "unit": case["unit"]},)
        if case["method"] == "add":
            args += ("1m",)
        method = getattr(qa, case["method"])
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                method(*args)
            assert str(error.value) == case["error"]["message"], case
        else:
            assert method(*args) == case["result"], case
