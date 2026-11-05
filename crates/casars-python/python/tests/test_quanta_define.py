"""CASA's process-wide unit definitions and cache invalidation."""
import subprocess
import sys
from pathlib import Path

from casars.tools import quanta


def test_define_matches_casa() -> None:
    # CASA permits overriding a built-in unit. Isolate the process-wide change.
    fixture = Path(__file__).parent / "data" / "casa_quanta_define.json"
    program = r'''
import json, math, sys
from casars.tools import quanta
fixture = json.load(open(sys.argv[1]))
q, other = quanta(), quanta()
for case in fixture["cases"]:
    method = case["method"]
    target = other.convert if method == "other_convert" else getattr(q, method)
    try:
        result = target(*case["args"])
    except Exception as error:
        assert case.get("error") == {"type": type(error).__name__, "message": str(error)}, case
    else:
        assert "error" not in case, case
        expected = case["result"]
        if isinstance(expected, dict):
            assert result["unit"] == expected["unit"], case
            assert math.isclose(result["value"], expected["value"], rel_tol=1e-12, abs_tol=1e-15), case
        else:
            assert result == expected, case
'''
    result = subprocess.run([sys.executable, "-c", program, str(fixture)], text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_define_accepts_bytes_names() -> None:
    q = quanta()
    assert q.define(b"casaUserBytes", "2m")
    assert q.convert("1casaUserBytes", "m")["value"] == 2.0
