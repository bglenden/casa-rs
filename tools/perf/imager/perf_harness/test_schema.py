# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for the canonical imaging evidence contracts."""

from __future__ import annotations

import json
import pathlib
import tempfile
import unittest

from perf_harness import (
    ContractError,
    RUN_RESULT_SCHEMA_VERSION,
    atomic_write_json,
    load_run_result,
    load_workload_manifest,
)


class SchemaTests(unittest.TestCase):
    def test_workload_rejects_unknown_and_unversioned_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "workload.json"
            path.write_text(json.dumps({"id": "old"}), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "schema_version"):
                load_workload_manifest(path)
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "id": "test",
                        "mode_id": "test",
                        "dataset": {},
                        "imaging": {},
                        "legacy_alias": True,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ContractError, "unknown workload"):
                load_workload_manifest(path)

    def test_workload_rejects_unknown_nested_fields_and_wrong_types(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "workload.json"
            base = {
                "schema_version": 1,
                "id": "test",
                "mode_id": "test",
                "dataset": {"key": "fixture", "path": "/tmp/fixture.ms"},
                "imaging": {
                    "specmode": "mfs",
                    "gridder": "standard",
                    "mode": "dirty",
                },
            }
            invalid = json.loads(json.dumps(base))
            invalid["imaging"]["private_alias"] = True
            atomic_write_json(path, invalid)
            with self.assertRaisesRegex(ContractError, "unknown field"):
                load_workload_manifest(path)
            invalid = json.loads(json.dumps(base))
            invalid["imaging"]["imsize"] = "1024"
            atomic_write_json(path, invalid)
            with self.assertRaisesRegex(ContractError, "imsize must be an integer"):
                load_workload_manifest(path)

    def test_result_rejects_legacy_version_and_nonfinite_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            result = root / "result.json"
            atomic_write_json(
                result,
                {
                    "schema_version": RUN_RESULT_SCHEMA_VERSION,
                    "kind": "test",
                    "status": "completed",
                    "run_id": "test-run",
                    "created_at": "2026-07-18T00:00:00Z",
                    "environment": {},
                    "artifacts": {},
                    "results": {},
                },
            )
            self.assertEqual("completed", load_run_result(result)["status"])
            result.write_text(
                '{"schema_version": 1, "status": "completed", "results": {}}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ContractError, "schema_version"):
                load_run_result(result)
            with self.assertRaises(ValueError):
                atomic_write_json(result, {"value": float("nan")})

    def test_failure_states_require_typed_failure_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "failure.json"
            base = {
                "schema_version": RUN_RESULT_SCHEMA_VERSION,
                "kind": "test",
                "status": "failed_execution",
                "run_id": "test-run",
                "created_at": "2026-07-18T00:00:00Z",
                "environment": {},
                "artifacts": {},
                "results": {},
            }
            atomic_write_json(path, base)
            with self.assertRaisesRegex(ContractError, "results.failure"):
                load_run_result(path)
            base["results"] = {
                "failure": {"kind": "execution", "reason": "process exited 2"}
            }
            atomic_write_json(path, base)
            self.assertEqual("failed_execution", load_run_result(path)["status"])


if __name__ == "__main__":
    unittest.main()
