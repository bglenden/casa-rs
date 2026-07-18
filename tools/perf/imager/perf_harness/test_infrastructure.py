# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for canonical imaging evidence process and comparator boundaries."""

from __future__ import annotations

import pathlib
import py_compile
import sys
import tempfile
import unittest
from unittest import mock

from perf_harness import ms_compare
from perf_harness.casa_protocol import CasaProtocolResult, run_json_file_protocol
from perf_harness.ms_compare import compare_measurement_set_pairs, compare_measurement_sets
from perf_harness.provenance import capture_provenance, executable_path
from perf_harness.subprocesses import run_command


PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent
CASA_PROGRAMS = tuple(sorted(PACKAGE_ROOT.glob("casa_*.py")))


class CasaProtocolTests(unittest.TestCase):
    def test_provenance_preserves_virtual_environment_invocation_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            interpreter = root / "casa-python"
            interpreter.symlink_to(sys.executable)

            provenance = capture_provenance(
                repo_root=PACKAGE_ROOT,
                executables={"casa_python": interpreter},
                datasets={},
                storage_label="test",
            )

            self.assertEqual(str(interpreter), executable_path(provenance, "casa_python"))
            self.assertEqual(
                str(pathlib.Path(sys.executable).resolve()),
                provenance["executables"]["casa_python"]["resolved_path"],
            )

    def test_checked_in_casa_programs_are_independently_syntax_valid(self) -> None:
        self.assertTrue(CASA_PROGRAMS)
        for path in CASA_PROGRAMS:
            py_compile.compile(str(path), doraise=True)

    def test_protocol_distinguishes_unavailable_execution_failure_and_success(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            common = {
                "request": {"schema_version": 1},
                "request_path": root / "request.json",
                "output_path": root / "output.json",
                "log_path": root / "run.log",
                "cwd": root,
            }
            unavailable = run_json_file_protocol(
                casa_python=None,
                script=root / "missing.py",
                **common,
            )
            self.assertEqual("unavailable", unavailable.status)

            failed = run_json_file_protocol(
                casa_python=sys.executable,
                script=root / "missing.py",
                **common,
            )
            self.assertEqual("failed_execution", failed.status)

            script = root / "protocol_program.py"
            script.write_text(
                "import json, pathlib, sys\n"
                "request = json.loads(pathlib.Path(sys.argv[1]).read_text())\n"
                "pathlib.Path(sys.argv[2]).write_text(json.dumps({'echo': request}))\n",
                encoding="utf-8",
            )
            completed = run_json_file_protocol(
                casa_python=sys.executable,
                script=script,
                **common,
            )
            self.assertEqual("completed", completed.status)
            self.assertEqual({"schema_version": 1}, completed.output["echo"])

    def test_shared_process_runner_supports_stdin_and_separate_stderr(self) -> None:
        completed = run_command(
            [
                sys.executable,
                "-c",
                "import sys; print(sys.stdin.read().upper()); print('diagnostic', file=sys.stderr)",
            ],
            input_text="payload",
            merge_stderr=False,
            check=True,
        )
        self.assertEqual("PAYLOAD", completed.stdout.strip())
        self.assertEqual("diagnostic", completed.stderr.strip())

    def test_ms_comparator_exposes_full_and_sampled_modes(self) -> None:
        protocol = CasaProtocolResult(
            status="completed",
            return_code=0,
            output={"status": "passed"},
            reason=None,
            request_path=pathlib.Path("request.json"),
            output_path=pathlib.Path("result.json"),
            log_path=pathlib.Path("compare.log"),
        )
        with mock.patch(
            "perf_harness.ms_compare.run_json_file_protocol", return_value=protocol
        ) as run_protocol:
            for mode in ("full", "sampled"):
                result = compare_measurement_sets(
                    casa_python=sys.executable,
                    native_path="native.ms",
                    casa_path="casa.ms",
                    mode=mode,
                    uvw_atol=1.0e-5,
                    data_atol=5.0e-2,
                    data_rtol=1.0e-2,
                    artifact_prefix=pathlib.Path("comparison"),
                    cwd=pathlib.Path("."),
                )
                self.assertEqual(mode, result["comparison_mode"])
                self.assertEqual(mode, run_protocol.call_args.kwargs["request"]["mode"])

            pairs = compare_measurement_set_pairs(
                casa_python=sys.executable,
                pairs=[{"id": "aca", "native_ms": "native.ms", "casa_ms": "casa.ms"}],
                artifact_prefix=pathlib.Path("aca-comparison"),
                cwd=pathlib.Path("."),
            )
            self.assertEqual("aca_pairs", pairs["comparison_mode"])
            self.assertEqual(ms_compare.CASA_MS_COMPARATOR, run_protocol.call_args.kwargs["script"])
            self.assertEqual("aca_pairs", run_protocol.call_args.kwargs["request"]["mode"])


if __name__ == "__main__":
    unittest.main()
