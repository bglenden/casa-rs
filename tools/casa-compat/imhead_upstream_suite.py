# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run the upstream imhead assertions against CASA or the native executable."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest

import numpy as np
from casatasks import casalog, version_string


def unpack(value):
    kind = value["kind"]
    if kind == "none":
        return None
    if kind == "dict":
        return {entry["key"]: unpack(entry["value"]) for entry in value["entries"]}
    if kind == "list":
        return [unpack(item) for item in value["items"]]
    if kind.endswith("_array"):
        dtype = value.get("dtype", {"bool_array": "bool", "string_array": "str"}.get(kind))
        return np.asarray(value["data"], dtype=dtype).reshape(value["shape"], order="F")
    if kind == "complex":
        return complex(value["real"], value["imag"])
    return value["value"]


def parameter_json(value):
    if isinstance(value, (np.ndarray, np.generic)):
        return value.tolist()
    raise TypeError(type(value))


def native_task(executable, integration_errors):
    def imhead(imagename="", mode="summary", hdkey="", hdvalue="", verbose=False):
        values = dict(imagename=imagename, mode=mode, hdkey=hdkey, hdvalue=hdvalue, verbose=verbose)
        try:
            child = subprocess.run(
                [executable, "imhead", "--json-run", "-"],
                input=json.dumps({"surface": "imhead", "values": values}, default=parameter_json),
                capture_output=True,
                text=True,
                timeout=60,
            )
            # The original logging assertions read the file selected by casalog.
            if child.stderr:
                with open(casalog.logfile(), "a", encoding="utf-8") as logfile:
                    logfile.write(child.stderr)
            reply = json.loads(child.stdout)
            if reply["surface"] != "imhead":
                raise ValueError("unexpected task surface")
            if reply["status"] == "succeeded" and child.returncode == 0:
                return unpack(reply["value"])
            failure_code = {"execution": 1, "validation": 2}.get(reply.get("kind"))
            message = reply.get("message")
            if (reply["status"] != "failed" or child.returncode != failure_code
                    or not isinstance(message, str) or not message):
                raise ValueError(f"exit {child.returncode} and invalid task reply {reply!r}")
        except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as error:
            # Some upstream tests catch any Exception for an expected refusal.
            integration_errors.append(f"imhead({mode}, {hdkey}): {error}")
            raise
        raise RuntimeError(message)

    return imhead


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="CASA's test_task_imhead.py")
    parser.add_argument("fixtures", type=Path, help="casatestdata's unittest/imhead directory")
    parser.add_argument("--native-bin", type=Path, help="native imexplore executable")
    parser.add_argument("--scratch", type=Path, required=True, help="directory for disposable outputs")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"

    source = args.source.read_text(encoding="utf-8")
    fixture_line = "datapath = ctsys_resolve('unittest/imhead/')"
    assert source.count(fixture_line) == 1
    source = source.replace(fixture_line, f"datapath = {str(args.fixtures.resolve())!r}", 1)
    executable = args.native_bin.resolve() if args.native_bin else None
    scratch_root = args.scratch.resolve()
    scratch_root.mkdir(parents=True, exist_ok=True)
    original = Path.cwd()
    with tempfile.TemporaryDirectory(dir=scratch_root) as scratch:
        os.chdir(scratch)
        try:
            module = types.ModuleType("upstream_imhead")
            casalog.setlogfile(str(Path(scratch) / "suite.log"))
            exec(compile(source, str(args.source), "exec"), module.__dict__)
            integration_errors = []
            if executable:
                module.imhead = native_task(executable, integration_errors)
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(module.imhead_test)
            result = unittest.TextTestRunner(verbosity=2).run(suite)
            for error in integration_errors:
                print(f"Native integration failure: {error}", file=sys.stderr)
            return result.wasSuccessful() and not integration_errors
        finally:
            os.chdir(original)


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
