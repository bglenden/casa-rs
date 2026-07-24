#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for the explicit checked-in evidence v2-to-v3 migration."""

from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from perf_harness import RUN_RESULT_SCHEMA_VERSION, load_run_result
from perf_harness.schema import LEGACY_RUN_RESULT_SCHEMA_VERSION
from test_support import canonical_workload_result

import migrate_evidence_v2_to_v3 as migration


class EvidenceV3MigrationTests(unittest.TestCase):
    def test_v2_artifacts_and_manifest_hashes_migrate_once_to_v3(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            evidence = root / "evidence" / "artifacts"
            evidence.mkdir(parents=True)
            artifact = evidence / "run.json"
            artifact.write_text(
                json.dumps(legacy_result()),
                encoding="utf-8",
            )
            manifest = write_manifest(root, "evidence/artifacts/run.json")

            migration.migrate_manifest(manifest)
            first_bytes = artifact.read_bytes()
            migration.migrate_manifest(manifest)

            self.assertEqual(first_bytes, artifact.read_bytes())
            result = load_run_result(artifact)
            self.assertEqual(RUN_RESULT_SCHEMA_VERSION, result["schema_version"])
            updated = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(
                hashlib.sha256(first_bytes).hexdigest(),
                updated["artifacts"][0]["sha256"],
            )

    def test_migration_rejects_a_source_hash_mismatch_without_writing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            evidence = root / "evidence" / "artifacts"
            evidence.mkdir(parents=True)
            artifact = evidence / "run.json"
            artifact.write_text(json.dumps(legacy_result()), encoding="utf-8")
            manifest = write_manifest(
                root,
                "evidence/artifacts/run.json",
                sha256_value="0" * 64,
            )
            artifact_before = artifact.read_bytes()
            manifest_before = manifest.read_bytes()

            with self.assertRaisesRegex(
                ValueError, "refusing to bless modified evidence"
            ):
                migration.migrate_manifest(manifest)

            self.assertEqual(artifact_before, artifact.read_bytes())
            self.assertEqual(manifest_before, manifest.read_bytes())

    def test_preflight_failure_does_not_partially_migrate_earlier_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            evidence = root / "evidence" / "artifacts"
            evidence.mkdir(parents=True)
            first = evidence / "first.json"
            second = evidence / "second.json"
            first.write_text(json.dumps(legacy_result()), encoding="utf-8")
            second.write_text(json.dumps(legacy_result()), encoding="utf-8")
            manifest = write_manifest(
                root,
                "evidence/artifacts/first.json",
                additional_paths=["evidence/artifacts/second.json"],
            )
            first_before = first.read_bytes()
            manifest_before = manifest.read_bytes()
            second.write_text(json.dumps({**legacy_result(), "run_id": "tampered"}))

            with self.assertRaisesRegex(
                ValueError, "refusing to bless modified evidence"
            ):
                migration.migrate_manifest(manifest)

            self.assertEqual(first_before, first.read_bytes())
            self.assertEqual(manifest_before, manifest.read_bytes())

    def test_write_failure_rolls_back_already_migrated_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            evidence = root / "evidence" / "artifacts"
            evidence.mkdir(parents=True)
            first = evidence / "first.json"
            second = evidence / "second.json"
            first.write_text(json.dumps(legacy_result()), encoding="utf-8")
            second.write_text(json.dumps(legacy_result()), encoding="utf-8")
            manifest = write_manifest(
                root,
                "evidence/artifacts/first.json",
                additional_paths=["evidence/artifacts/second.json"],
            )
            first_before = first.read_bytes()
            second_before = second.read_bytes()
            manifest_before = manifest.read_bytes()
            real_atomic_write = migration.atomic_write_json

            def fail_on_second(path: pathlib.Path, value: object) -> None:
                if path.name == "second.json":
                    raise OSError("simulated second artifact write failure")
                real_atomic_write(path, value)

            with mock.patch.object(
                migration, "atomic_write_json", side_effect=fail_on_second
            ):
                with self.assertRaisesRegex(OSError, "simulated second artifact"):
                    migration.migrate_manifest(manifest)

            self.assertEqual(first_before, first.read_bytes())
            self.assertEqual(second_before, second.read_bytes())
            self.assertEqual(manifest_before, manifest.read_bytes())

    def test_migration_rejects_non_v2_input(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            evidence = root / "evidence" / "artifacts"
            evidence.mkdir(parents=True)
            artifact = evidence / "run.json"
            value = legacy_result()
            value["schema_version"] = 1
            artifact.write_text(json.dumps(value), encoding="utf-8")
            manifest = write_manifest(root, "evidence/artifacts/run.json")

            with self.assertRaisesRegex(ValueError, "expected run-result schema"):
                migration.migrate_manifest(manifest)

    def test_migration_rejects_manifest_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "evidence").mkdir()
            outside = root / "outside.json"
            outside.write_text(json.dumps(legacy_result()), encoding="utf-8")
            manifest = write_manifest(root, "../outside.json", sha256_value="0" * 64)

            with self.assertRaisesRegex(ValueError, "must remain beneath"):
                migration.migrate_manifest(manifest)


def legacy_result() -> dict[str, object]:
    result = canonical_workload_result()
    result["schema_version"] = LEGACY_RUN_RESULT_SCHEMA_VERSION
    result["environment"]["migration"] = {
        "source_schema_version": 1,
        "method": "synthetic v2 migration fixture",
    }
    return result


def write_manifest(
    root: pathlib.Path,
    checked_in_path: str,
    *,
    sha256_value: str | None = None,
    additional_paths: list[str] | None = None,
) -> pathlib.Path:
    manifest = root / "evidence" / "manifest.json"
    paths = [checked_in_path, *(additional_paths or [])]
    artifacts = []
    for index, path in enumerate(paths):
        digest = sha256_value
        if digest is None:
            digest = hashlib.sha256((root / path).read_bytes()).hexdigest()
        artifacts.append(
            {
                "artifact_id": f"run-{index}",
                "artifact_role": "baseline",
                "workload_id": "workload",
                "checked_in_path": path,
                "sha256": digest,
            }
        )
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifacts": artifacts,
            }
        ),
        encoding="utf-8",
    )
    return manifest


if __name__ == "__main__":
    unittest.main()
