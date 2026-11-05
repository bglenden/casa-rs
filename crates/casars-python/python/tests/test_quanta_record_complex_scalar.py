"""CASA quantity-record complex scalar types and diagnostics."""
import json
import warnings
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_record_complex_scalar_diagnostics_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_record_complex_scalar.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        value = case["value"]
        number = complex(value["real"], value["imag"])
        number = number if value["dtype"] == "python_complex" else np.dtype(value["dtype"]).type(number)
        args = ({"value": number, "unit": case["unit"]},)
        if case["method"] == "add":
            args += ("1m",)
        with warnings.catch_warnings(record=True) as emitted:
            warnings.simplefilter("always")
            with pytest.raises(RuntimeError) as error:
                getattr(qa, case["method"])(*args)
            assert str(error.value) == case["error"]["message"], case
        assert not emitted, case
