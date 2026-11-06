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


def test_concurrent_redefinitions_do_not_change_self_conversion() -> None:
    program = r'''
import threading
from casars.tools import quanta
qa = quanta()
assert qa.define("casaStableUnit", "2m")
start = threading.Barrier(2)
errors = []
def redefine():
    start.wait()
    for i in range(4000):
        qa.define("casaStableUnit", "2m" if i % 2 else "3m")
thread = threading.Thread(target=redefine)
thread.start()
start.wait()
for _ in range(4000):
    result = qa.convert("1casaStableUnit", "casaStableUnit")
    if result["value"] != 1.0:
        errors.append(result)
thread.join()
assert not errors, errors[:3]
'''
    result = subprocess.run([sys.executable, "-c", program], text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr


def test_defined_unit_preserves_component_shape_values() -> None:
    from casars.tools import componentlist

    qa = quanta()
    assert qa.define("casaShapeUnit", "2arcsec")
    cl = componentlist()
    assert cl.addcomponent(shape="Gaussian", majoraxis="1casaShapeUnit", minoraxis="1arcsec", positionangle="0deg")
    shape = cl.getshape(0)
    assert qa.convert(shape["majoraxis"], "arcsec")["value"] == 2.0
    assert qa.convert(shape["minoraxis"], "arcsec")["value"] == 1.0


def test_component_shape_uses_one_definition_during_redefinition() -> None:
    program = r'''
import threading
from casars.tools import componentlist, quanta
qa = quanta()
assert qa.define("casaThreadAngle", "2arcsec")
start = threading.Barrier(2)
def redefine():
    start.wait()
    for i in range(8000):
        qa.define("casaThreadAngle", "2arcsec" if i % 2 else "3arcsec")
thread = threading.Thread(target=redefine)
thread.start()
start.wait()
for _ in range(800):
    cl = componentlist()
    assert cl.addcomponent(shape="Gaussian", majoraxis="1casaThreadAngle",
                           minoraxis="1casaThreadAngle", positionangle="0deg")
    shape = cl.getshape(0)
    assert abs(shape["majoraxis"]["value"] - shape["minoraxis"]["value"]) < 1e-12, shape
thread.join()
'''
    result = subprocess.run([sys.executable, "-c", program], text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr


def test_simutil_pointing_size_and_spacing_use_one_definition() -> None:
    program = r'''
import threading
from casars import simutil
from casars.tools import quanta
qa = quanta()
assert qa.define("casaMapUnit", "2arcsec")
tool = simutil.simutil("J2000 19h00m00 -23d00m00")
def pointings():
    return tool.calc_pointings2("1casaMapUnit", ["4casaMapUnit", "4casaMapUnit"], "square")
expected_count = len(pointings())
start = threading.Barrier(2)
def redefine():
    start.wait()
    for i in range(2000):
        qa.define("casaMapUnit", "2arcsec" if i % 2 else "3arcsec")
thread = threading.Thread(target=redefine)
thread.start()
start.wait()
for _ in range(200):
    assert len(pointings()) == expected_count
thread.join()
'''
    result = subprocess.run([sys.executable, "-c", program], text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr


def test_simutil_quantity_retry_does_not_repeat_python_extraction() -> None:
    from casars import simutil

    qa = quanta()
    calls = []

    class DefiningString(str):
        def __len__(self) -> int:
            calls.append(1)
            assert qa.define("casaLengthOnce", "2m")
            return super().__len__()

    assert simutil.simutil().isquantity([DefiningString("1casaLengthOnce")])
    assert calls == [1]
