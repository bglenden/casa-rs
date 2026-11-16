"""CASA diagnostics for complex NumPy arrays in quantity records."""
import json
import warnings
from pathlib import Path

import numpy as np
import pytest

from casars.tools import componentlist, measures, quanta


def test_empty_complex_flux_uses_casa_zero_default() -> None:
    for dtype in (np.complex64, np.complex128):
        for shape in ((0,), (0, 2), (2, 0)):
            cl = componentlist()
            assert cl.addcomponent(flux=np.empty(shape, dtype=dtype))
            np.testing.assert_array_equal(cl.getfluxvalue(0), np.zeros(4))
            cl.done()


def test_empty_complex_direction_and_frequency_use_casa_defaults() -> None:
    baseline = componentlist()
    assert baseline.addcomponent()
    expected = baseline.torecord()["component0"]
    baseline.done()
    for dtype in (np.complex64, np.complex128):
        for shape in ((0,), (0, 2), (2, 0)):
            for field in ("dir", "freq"):
                cl = componentlist()
                assert cl.addcomponent(**{field: np.empty(shape, dtype=dtype)})
                record = cl.torecord()["component0"]
                assert record["shape"]["direction"] == expected["shape"]["direction"]
                assert record["spectrum"] == expected["spectrum"]
                cl.done()


def test_empty_complex_measure_uses_casa_zero_default() -> None:
    me = measures()
    for dtype in (np.complex64, np.complex128):
        value = np.array([], dtype=dtype)
        assert me.epoch("UTC", value)["m0"] == {"unit": "d", "value": 0.0}
        assert me.frequency("LSRK", value)["m0"] == {"unit": "Hz", "value": 0.0}


def make_array(case: dict) -> np.ndarray:
    shape = case["shape"]
    count = int(np.prod(shape)) if shape else 1
    return np.array([complex(i + 1, i + 2) for i in range(count)], dtype=case["dtype"]).reshape(shape)


def test_record_complex_array_diagnostics_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_record_complex_array.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        value = make_array(case)
        args = ({"value": value, "unit": case["unit"]},)
        if case["method"] == "add":
            args += ("1m",)
        with warnings.catch_warnings(record=True) as emitted:
            warnings.simplefilter("always")
            with pytest.raises(RuntimeError) as error:
                getattr(qa, case["method"])(*args)
            assert str(error.value) == case["error"]["message"], case
        assert not emitted, case


def test_bare_complex_arrays_do_not_silently_lose_values() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_record_complex_array.json").read_text())
    qa = quanta()
    for case in fixture["bare"]:
        with warnings.catch_warnings(record=True) as emitted:
            warnings.simplefilter("always")
            # CASA returns zero even for nonzero complex inputs. An explicit
            # refusal avoids silently discarding the user's values.
            assert case["result"] == {"value": 0.0, "unit": ""}
            with pytest.raises(RuntimeError, match="Wrong QuantumHolder to convert asQuantumDouble"):
                qa.quantity(make_array(case))
        assert not emitted, case
