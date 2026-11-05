"""CASA frequency and Doppler quantity conversions."""
import json
from pathlib import Path

import pytest

from casars.tools import quanta


def test_conversions_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_spectral_conversion.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        method = getattr(qa, case["method"])
        args = (case["value"],) if case["unit"] is None else (case["value"], case["unit"])
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                method(*args)
            assert str(error.value) == case["error"]["message"], case
        else:
            actual = method(*args)
            expected = case["result"]
            assert actual["unit"] == expected["unit"], case
            assert actual["value"] == pytest.approx(expected["value"], rel=1e-14, abs=0, nan_ok=True), case


def test_conversion_arguments_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_spectral_conversion.json").read_text())
    qa = quanta()
    for case in fixture["argument_cases"]:
        args = [v["bytes"].encode() if isinstance(v, dict) and "bytes" in v else v for v in case["args"]]
        method = getattr(qa, case["method"])
        if "error" in case:
            error_type = {"RuntimeError": RuntimeError, "TypeError": TypeError}[case["error"]["type"]]
            with pytest.raises(error_type) as error:
                method(*args)
            assert str(error.value) == case["error"]["message"], case
        else:
            assert method(*args) == case["result"], case
