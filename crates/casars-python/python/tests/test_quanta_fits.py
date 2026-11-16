"""CASA comparisons for process-wide FITS unit registration."""
import subprocess
import sys
from pathlib import Path


def test_fits_registers_casa_units_across_quanta_instances() -> None:
    fixture = Path(__file__).parent / "data" / "casa_quanta_fits.json"
    # qa.fits changes a process-wide registry, so exercise both states in a
    # fresh interpreter without changing other tests' unit environment.
    program = r'''
import json, sys
from casars.tools import quanta
j = json.load(open(sys.argv[1]))
qa = quanta()
def read(unit, target, tool):
    try:
        value = tool.quantity(1, unit)
        conformant = tool.compare(value, tool.quantity(1, target))
        result = {"quantity": value, "conformant": conformant}
        if conformant:
            result["converted"] = tool.convert(value, target)
        return result
    except Exception as error:
        return {"error": {"type": type(error).__name__, "message": str(error)}}
before = {unit: read(unit, target, qa) for unit, target in j["aliases"].items()}
assert before == j["before"], (before, j["before"])
assert qa.fits() is j["first"]
after = {unit: read(unit, target, qa) for unit, target in j["aliases"].items()}
assert after == j["after"], (after, j["after"])
another = quanta()
assert read("DEGREES", "deg", another) == j["other_instance"]
assert another.fits() is j["second"]
'''
    result = subprocess.run([sys.executable, "-c", program, str(fixture)], text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_concurrent_fits_registration_does_not_mix_unit_meanings() -> None:
    # The long expression gives registration a chance to occur between its
    # two S fields. Both before and after registration it is S squared.
    program = r'''
import threading, time
from casars.tools import quanta
qa = quanta()
unit = "S." + "m/m." * 2000000 + "S"
before = qa.canonical({"value": 1, "unit": "S.S"})
started = threading.Event()
result = []
def parse():
    started.set()
    result.append(qa.canonical({"value": 1, "unit": unit}))
worker = threading.Thread(target=parse)
worker.start()
started.wait()
time.sleep(0.8)
qa.fits()
worker.join()
after = qa.canonical({"value": 1, "unit": "S.S"})
assert result[0] in (before, after), (before, result[0], after)
'''
    result = subprocess.run([sys.executable, "-c", program], text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
