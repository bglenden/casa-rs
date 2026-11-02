# SPDX-License-Identifier: LGPL-3.0-or-later
"""Shared helpers for the tclean CASA comparison drivers (#980).

Evidence tooling, not a runtime dependency: run under CASA 6.7.6.14's Python.
`run_native` calls casa-rs's public Python `casars.tasks.tclean` in a separate
interpreter; `compare_product` compares one image product with CASA's under
the T1.5 limits (NRMS <= 1e-3, `.sumwt` within 5e-5 of CASA's peak, WCS, rest
frequency, units, Stokes, frames, beam axes 1e-3 and position angle 0.01 deg,
exact masks).
"""

import hashlib
import importlib.util
import json
import math
import os
import subprocess
import time
from pathlib import Path

# Runs in the casars interpreter: tclean's keyword arguments on stdin, its
# record or its refusal on stdout.
CHILD = r"""
import json, sys
values = json.load(sys.stdin)
def plain(value):
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if hasattr(value, "tolist"):
        return value.tolist()
    return value
from casars import tasks
try:
    record = tasks.tclean(**values)
except Exception as error:
    print(json.dumps({"refused": {"type": type(error).__name__, "message": str(error)}}))
else:
    print(json.dumps({"result": plain(record)}))
"""


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_reader():
    source = Path(__file__).resolve().parents[2] / "tools/imaging/t15_imager_additions.py"
    spec = importlib.util.spec_from_file_location("t15_imager_additions", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def members(folder, prefix="image"):
    """The image products under `folder` whose names start with `prefix` and a dot."""
    folder = Path(folder)
    if not folder.is_dir():
        return []
    return sorted(
        path.name
        for path in folder.iterdir()
        if path.name.startswith(prefix + ".") and path.is_dir()
    )


def metadata(path, casatools):
    image = casatools.image()
    image.open(str(path))
    try:
        coords = image.coordsys()
        try:
            record = coords.torecord()
            return {
                "unit": image.brightnessunit(),
                "stokes": list(coords.stokes()),
                "units": list(coords.units()),
                "codes": list(coords.referencecode()),
                "referencevalue": coords.referencevalue()["numeric"].tolist(),
                "referencepixel": coords.referencepixel()["numeric"].tolist(),
                "increment": coords.increment()["numeric"].tolist(),
                "restfreq": next(
                    value["restfreq"] for key, value in record.items() if key.startswith("spectral")
                ),
            }
        finally:
            coords.done()
    finally:
        image.done()


def compare_product(native, casa, name, reader, casatools, numpy):
    a, am, ab = reader.read(str(native))
    b, bm, bb = reader.read(str(casa))
    entry = {"shape_equal": a.shape == b.shape}
    if not entry["shape_equal"]:
        return entry | {"passed": False}
    valid = am & bm
    finite = bool(valid.any() and numpy.isfinite(a[am]).all() and numpy.isfinite(b[bm]).all())
    entry.update(mask_mismatch=int((am != bm).sum()), finite=finite)
    if not finite:
        return entry | {"passed": False}
    av, bv = a[valid], b[valid]
    power = float(numpy.sum(bv * bv))
    error = float(numpy.sum((av - bv) ** 2))
    entry["nrms"] = math.sqrt(error / power) if power else (0.0 if error == 0 else math.inf)
    if name == "image.sumwt":
        numeric = bool(numpy.max(numpy.abs(av - bv)) <= 5e-5 * numpy.max(numpy.abs(bv)))
    else:
        numeric = entry["nrms"] <= 1e-3
    left, right = metadata(native, casatools), metadata(casa, casatools)
    checks = {key: left[key] == right[key] for key in ("unit", "stokes", "units", "codes")}
    for key, rtol, atol in (
        ("referencevalue", 2e-10, 2e-10),
        ("referencepixel", 0, 1e-8),
        ("increment", 2e-10, 2e-10),
        ("restfreq", 2e-10, 2e-10),
    ):
        checks[key] = bool(numpy.allclose(left[key], right[key], rtol=rtol, atol=atol))
    beam = True
    if ab or bb:
        beam = bool(ab and bb) and all(abs(x - y) <= 1e-3 * abs(y) for x, y in zip(ab[:2], bb[:2]))
        if beam and bb[0] / bb[1] > 1.001:
            beam = abs((ab[2] - bb[2] + 90) % 180 - 90) <= 0.01
    entry.update(numeric_passed=numeric, metadata=checks, beam=[ab, bb], beam_passed=beam)
    entry["passed"] = bool(numeric and all(checks.values()) and beam and entry["mask_mismatch"] == 0)
    return entry


def run_native(casars_python, imager_binary, values, folder, log_name="stderr.log", timeout=600):
    """Call `casars.tasks.tclean(**values)` from `folder` in the casars interpreter.

    The reply holds `result` (tclean's record) or `refused` (the exception);
    `failed` instead when the interpreter timed out or did not reply.
    """
    folder = Path(folder)
    environment = {
        name: value for name, value in os.environ.items() if name != "CASA_RS_IMAGER_RUN_SUMMARY"
    }
    environment["CASARS_IMAGER_BIN"] = str(imager_binary)
    started = time.monotonic()
    try:
        run = subprocess.run(
            [str(casars_python), "-I", "-c", CHILD],
            input=json.dumps(values),
            cwd=folder,
            env=environment,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"failed": f"timed out after {timeout} s",
                "driver_elapsed_seconds": time.monotonic() - started}
    (folder / log_name).write_text(run.stderr)
    try:
        reply = json.loads(run.stdout.strip().splitlines()[-1])
    except (IndexError, ValueError):
        reply = {"failed": "no reply", "stdout_tail": run.stdout[-2000:]}
    reply["exit_code"] = run.returncode
    reply["driver_elapsed_seconds"] = time.monotonic() - started
    return reply
