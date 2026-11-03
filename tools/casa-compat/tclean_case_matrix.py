#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run a matrix of tclean calls through CASA and through casa-rs, and compare.

Evidence driver for #980, not a runtime dependency. Run it with CASA 6.7.6.14's
Python (`-I`). A case file names inputs (relative to `--data-root`), parameters
common to every case, and the cases:

    {"inputs": {"point": "unittest/tclean/refim_point.ms"},
     "common": {"imsize": 100, "cell": "8.0arcsec"},
     "cases": {"superuniform": {"vis": "point", "imagename": "tst",
                                "params": {"weighting": "superuniform"}}}}

A case is one call (`vis`, `imagename`, `params`) or an ordered `sequence` of
steps of that shape, replayed in one working directory (restarts, start
models, masks naming an earlier product). `vis` names an input or a list of
inputs. Optional keys: `stage` (inputs copied into the working directory under
their names, which the parameters use as relative paths), `mutates_vis` (each
implementation gets its own MeasurementSet copies), `expect_error` (CASA's
test expects an exception), `tolerates_error`, `casa_skip` (CASA's suite skips
the test; the case is skipped unless `--include-skipped`), `casa_environment`
(variables set for CASA's call), `skip_scalars` and `note`.
`resources/casa-compat/tclean-casa-test-cases.json` documents the format.

Both CASA's `casatasks.tclean` and casa-rs's public Python `casars.tasks.tclean`
(in the `--casars-python` interpreter, `CASARS_IMAGER_BIN` naming the binary)
run every step with the same keyword arguments in their own working directory.
Each case's CASA steps run, in order, in a fresh CASA Python process of their
own (`--casa-steps`, its log beside the case): CASA keeps state between calls
in one process, and a case run before another changed CASA's results for it
(test_iterbot_cube_restart_updatedmask before test_modelvis_16: 120 iterations
against 170).
A step CASA refuses must be one its test expects to fail (`expect_error`,
`tolerates_error`; any other refusal fails the case) and must be refused by
casa-rs too, and an image name only refused steps wrote is not compared;
every other step must
succeed with exact iterdone, nmajordone and stopcode, and, for the return
values CASA reports, maxpsfsidelobe and cyclethreshold within 1e-4 relative
plus 1e-8 absolute (`SCALAR_RTOL`). Each image name's products must match
CASA's inventory and each product the T1.5 limits
(`tclean_compare.compare_product`).
"""

import argparse
import contextlib
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tclean_compare import compare_product, load_reader, members, run_native, sha256  # noqa: E402

SCALARS = ("maxpsfsidelobe", "cyclethreshold")
# The owner's 1e-4 (2026-11-02): each scalar is one pixel of a PSF or
# residual image, whose rounding follows summation order (CASA grids in
# single precision); rounding moved them by up to 5e-5 relative, and every
# real difference seen was 1e-3 or more.
SCALAR_RTOL = 1e-4
COUNTS = ("iterdone", "nmajordone", "stopcode")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, required=True, help="case-matrix JSON")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--casars-python", type=Path, required=True)
    parser.add_argument("--imager-binary", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="40-character source revision")
    parser.add_argument("--backend", default="cpu", choices=("cpu", "gpu"))
    parser.add_argument("--precision", default="f64", choices=("f32", "f64"))
    parser.add_argument("--output", type=Path, required=True, help="new directory")
    parser.add_argument("--case", action="append", help="run only these cases")
    parser.add_argument("--case-prefix", action="append", help="run cases with this prefix")
    parser.add_argument("--include-skipped", action="store_true")
    parser.add_argument("--timeout", type=float, default=900.0, help="per native call, s")
    return parser.parse_args()


def plain(value):
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


@contextlib.contextmanager
def working_directory(path, environment):
    previous_directory = Path.cwd()
    previous = {name: os.environ.get(name) for name in environment}
    os.chdir(path)
    os.environ.update({name: str(value) for name, value in environment.items()})
    try:
        yield
    finally:
        os.chdir(previous_directory)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def run_casa(casatasks, values, folder, environment):
    started = time.monotonic()
    with working_directory(folder, environment):
        try:
            record = casatasks.tclean(**values)
        except Exception as error:  # CASA's refusals are exceptions of several types
            reply = {"refused": {"type": type(error).__name__, "message": str(error)}}
        else:
            reply = {"result": plain(record)}
    reply["driver_elapsed_seconds"] = time.monotonic() - started
    return reply


def run_casa_steps(steps_values, folder, environment, log):
    """One case's CASA calls, in order, in a fresh CASA Python process: the
    replies of `run_casa`, one per call."""
    request = folder.parent / "casa-steps.json"
    reply = folder.parent / "casa-steps-reply.json"
    request.write_text(json.dumps({
        "steps": steps_values,
        "folder": str(folder),
        "environment": environment,
        "log": str(log),
    }))
    result = subprocess.run(
        [sys.executable, "-I", str(Path(__file__).resolve()), "--casa-steps", str(request)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not reply.exists():
        raise RuntimeError(
            f"the CASA worker exited {result.returncode}: {result.stderr[-2000:]}"
        )
    return json.loads(reply.read_text())


def casa_worker(request_path):
    """`--casa-steps`: run the CASA calls a request file names, in this
    process, and write their replies beside it."""
    request = json.loads(Path(request_path).read_text())
    import casatasks

    casatasks.casalog.setlogfile(request["log"])
    folder = Path(request["folder"])
    replies = [
        run_casa(casatasks, values, folder, request["environment"])
        for values in request["steps"]
    ]
    (Path(request_path).parent / "casa-steps-reply.json").write_text(json.dumps(replies))
    return 0


def steps(case):
    if "sequence" in case:
        return case["sequence"]
    return [{"call": 1, "vis": case["vis"], "imagename": case.get("imagename", "image"),
             "params": case["params"]}]


def copy_input(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, target, symlinks=True)
    else:
        shutil.copy2(source, target)


def prepare(folder, case, staged):
    """The working directory of one implementation, and the paths of its inputs."""
    folder.mkdir(parents=True)
    for name in case.get("stage", []):
        copy_input(staged[name], folder / name)
    paths = dict(staged)
    if case.get("mutates_vis"):
        names = set()
        for step in steps(case):
            names.update(step["vis"] if isinstance(step["vis"], list) else [step["vis"]])
        for name in sorted(names):
            paths[name] = folder / "vis" / name
            copy_input(staged[name], paths[name])
    for step in steps(case):
        (folder / step["imagename"]).parent.mkdir(parents=True, exist_ok=True)
    return paths


def call_values(common, step, paths):
    vis = step["vis"]
    vis = [str(paths[name]) for name in vis] if isinstance(vis, list) else str(paths[vis])
    return dict(common, **step["params"], vis=vis, imagename=step["imagename"])


def compare_step(casa, native, case):
    if "refused" in casa:
        # A refusal CASA's test does not expect is the environment's (missing
        # data, a path CASA cannot read), not agreement.
        expected = bool(case.get("expect_error") or case.get("tolerates_error"))
        return {
            "passed": expected and "refused" in native,
            "casa_refused": True,
            "casa_refusal_expected": expected,
        }
    if "result" not in native:
        return {"passed": False}
    expected, actual = casa["result"], native["result"]
    row = {"counts": {key: actual.get(key) == expected.get(key) for key in COUNTS}}
    row["scalars"] = {
        key: {
            "native": actual.get(key),
            "casa": expected[key],
            "passed": actual.get(key) is not None
            and abs(actual[key] - expected[key]) <= 1e-8 + SCALAR_RTOL * abs(expected[key]),
        }
        for key in SCALARS
        if expected.get(key) is not None
    }
    scalars = case.get("skip_scalars", False) or all(
        entry["passed"] for entry in row["scalars"].values()
    )
    row["passed"] = bool(all(row["counts"].values()) and scalars)
    return row


def compare_products(imagename, casa_folder, native_folder, tools):
    casa_dir = casa_folder / Path(imagename).parent
    native_dir = native_folder / Path(imagename).parent
    prefix = Path(imagename).name
    expected, actual = members(casa_dir, prefix), members(native_dir, prefix)
    row = {"inventory": {"casa": expected, "native": actual, "passed": expected == actual}}
    row["products"] = {
        product: compare_product(native_dir / product, casa_dir / product, product, *tools)
        for product in expected
        if (native_dir / product).is_dir()
    }
    row["passed"] = bool(
        row["inventory"]["passed"] and all(entry["passed"] for entry in row["products"].values())
    )
    return row


def run_case(args, name, case, common, staged, tools):
    folder = args.output / "cases" / name.replace("/", "_")
    casa_folder, native_folder = folder / "casa", folder / "native"
    casa_paths = prepare(casa_folder, case, staged)
    native_paths = prepare(native_folder, case, staged)
    environment = case.get("casa_environment") or {}
    row = {"note": case.get("note"), "steps": []}
    case_steps = steps(case)
    casa_replies = run_casa_steps(
        [call_values(common, step, casa_paths) for step in case_steps],
        casa_folder,
        environment,
        folder / "casa.log",
    )
    for index, (step, casa) in enumerate(zip(case_steps, casa_replies)):
        values = call_values(common, step, casa_paths)
        native_values = call_values(common, step, native_paths)
        native_values.update(backend=args.backend, gridprecision=args.precision)
        native = run_native(args.casars_python, args.imager_binary, native_values, native_folder,
                            log_name=f"stderr-{index + 1}.log", timeout=args.timeout)
        entry = {"call": step.get("call"), "parameters": values, "casa": casa, "native": native}
        entry |= compare_step(casa, native, case)
        row["steps"].append(entry)
    # What a refused call leaves behind is not a product: CASA's own tests
    # check only the exception (`test_error_gridding` leaves a `.psf`).
    imagenames = sorted({
        entry["parameters"]["imagename"]
        for entry in row["steps"]
        if "refused" not in entry["casa"]
    })
    row["products"] = {
        imagename: compare_products(imagename, casa_folder, native_folder, tools)
        for imagename in imagenames
    }
    row["passed"] = bool(
        all(entry["passed"] for entry in row["steps"])
        and all(entry["passed"] for entry in row["products"].values())
    )
    if case.get("expect_error"):
        row["expect_error"] = case["expect_error"]
    return row


def selected(args, name, case):
    if case.get("casa_skip") and not args.include_skipped:
        return False
    if args.case and name not in args.case:
        return False
    if args.case_prefix and not any(name.startswith(prefix) for prefix in args.case_prefix):
        return False
    return True


def main():
    args = parse_args()
    import casatasks
    import casatools
    import numpy

    assert casatasks.version_string() == "6.7.6.14", casatasks.version_string()
    assert casatools.version_string() == "6.7.6-14", casatools.version_string()
    matrix = json.loads(args.cases.read_text())
    cases = {name: case for name, case in matrix["cases"].items() if selected(args, name, case)}
    # Absolute, not resolved: CASA cuts a path at a space ("Table /Volumes/Extra
    # does not exist") in lists of MeasurementSets and in mosaic and Taylor
    # runs, and a refusal on both sides would pass. A resolved symbolic link
    # may introduce one, and a path that holds one is refused here.
    args.output = args.output.absolute()
    for path in (args.output, args.data_root.absolute()):
        if any(character.isspace() for character in str(path)):
            raise SystemExit(f"{path}: CASA cannot run under a path with whitespace")
    args.output.mkdir(parents=True)
    needed = set()
    for case in cases.values():
        needed.update(case.get("stage", []))
        for step in steps(case):
            needed.update(step["vis"] if isinstance(step["vis"], list) else [step["vis"]])
    staged = {}
    for name in sorted(needed):
        source = (args.data_root / matrix["inputs"][name]).absolute()
        if not source.exists() or (source.is_dir() and not any(source.iterdir())):
            raise SystemExit(f"{source}: input is missing or empty")
        staged[name] = args.output / "inputs" / name
        copy_input(source, staged[name])
    casatasks.casalog.setlogfile(str(args.output / "casa.log"))
    tools = (load_reader(), casatools, numpy)
    receipt = {
        "driver": "tools/casa-compat/tclean_case_matrix.py",
        "case_file": str(args.cases),
        "case_file_sha256": sha256(args.cases),
        "casa_tasks": casatasks.version_string(),
        "casa_tools": casatools.version_string(),
        "native_revision": args.revision,
        "imager_binary_sha256": sha256(args.imager_binary),
        "backend": args.backend,
        "precision": args.precision,
        "host": platform.platform(),
        "cases": {},
    }

    def write():
        (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")

    for name, case in cases.items():
        try:
            row = run_case(args, name, case, matrix.get("common", {}), staged, tools)
        except Exception as error:  # a driver failure on one case must not stop the matrix
            row = {
                "passed": False,
                "driver_error": f"{type(error).__name__}: {error}",
                "traceback": traceback.format_exc(),
            }
        receipt["cases"][name] = row
        write()
        print("case", name, row["passed"], flush=True)
    rows = list(receipt["cases"].values())
    receipt["passed"] = sum(bool(row["passed"]) for row in rows)
    receipt["total"] = len(rows)
    write()
    print(f"{receipt['passed']}/{receipt['total']} passed", flush=True)
    return 0 if receipt["passed"] == receipt["total"] else 1


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--casa-steps":
        raise SystemExit(casa_worker(sys.argv[2]))
    raise SystemExit(main())
