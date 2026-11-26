from __future__ import annotations

import json
import pathlib
import sys
import tarfile
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import stage_vlass_fragment as stage


class StageVlassFragmentTests(unittest.TestCase):
    def test_minimum_free_space_override_can_only_raise_the_one_tib_floor(self) -> None:
        self.assertEqual(
            stage.MINIMUM_FREE_BYTES,
            stage.validated_minimum_free_bytes(stage.MINIMUM_FREE_BYTES),
        )
        self.assertEqual(
            2 << 40,
            stage.validated_minimum_free_bytes(2 << 40),
        )
        with self.assertRaisesRegex(stage.StagingError, "cannot be lower"):
            stage.validated_minimum_free_bytes(stage.MINIMUM_FREE_BYTES - 1)

    def test_member_validator_accepts_only_frozen_tree(self) -> None:
        members = [
            _member(f"./{stage.MS_MEMBER}/", directory=True),
            _member(f"./{stage.MS_MEMBER}/table.dat"),
            _member(stage.RECIPE_MEMBER),
        ]
        stage._validate_members(members)

    def test_member_validator_rejects_path_traversal(self) -> None:
        members = [
            _member(f"./{stage.MS_MEMBER}/"),
            _member("../tclean.last"),
        ]
        with self.assertRaisesRegex(stage.StagingError, "unsafe archive member"):
            stage._validate_members(members)

    def test_member_validator_rejects_unexpected_payload(self) -> None:
        members = [
            _member(f"./{stage.MS_MEMBER}/"),
            _member(stage.RECIPE_MEMBER),
            _member("other.txt"),
        ]
        with self.assertRaisesRegex(stage.StagingError, "unexpected archive member"):
            stage._validate_members(members)

    def test_tree_identity_is_stable_and_content_sensitive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "sub").mkdir()
            (root / "sub" / "a").write_bytes(b"one")
            first = stage.tree_identity(root)
            self.assertEqual(first["file_count"], 1)
            self.assertEqual(first["size_bytes"], 3)
            self.assertEqual(first, stage.tree_identity(root))
            (root / "sub" / "a").write_bytes(b"two")
            self.assertNotEqual(
                first["tree_sha256"], stage.tree_identity(root)["tree_sha256"]
            )

    def test_external_issue_root_allows_only_issue_owned_descendants(self) -> None:
        stage._require_external_issue_root(
            pathlib.Path("/Volumes/GLENDENNING/casa-rs-vlass/issue-446/staging")
        )
        with self.assertRaisesRegex(stage.StagingError, "destination must be"):
            stage._require_external_issue_root(pathlib.Path("/tmp/vlass"))

    def test_existing_stage_accepts_matching_receipt_and_tree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _existing_stage_fixture(pathlib.Path(directory))
            stage.validate_frozen_archive_dataset_receipt(fixture.receipt)
            with mock.patch.object(stage, "RECIPE_SHA256", fixture.recipe_sha256):
                stage._validate_existing_stage(
                    fixture.receipt_path,
                    fixture.ms_path,
                    fixture.recipe_path,
                    archive_path=fixture.archive_path,
                    root=fixture.root,
                )

    def test_receipt_contract_rejects_workload_specific_kind(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _existing_stage_fixture(pathlib.Path(directory))
            fixture.receipt["kind"] = "vlass_fragment_dataset_receipt"
            with self.assertRaisesRegex(stage.StagingError, "receipt kind must be"):
                stage.validate_frozen_archive_dataset_receipt(fixture.receipt)

    def test_receipt_contract_rejects_suffix_lookalike_kind(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _existing_stage_fixture(pathlib.Path(directory))
            fixture.receipt["kind"] = "vlass_frozen_archive_dataset_receipt"
            with self.assertRaisesRegex(stage.StagingError, "receipt kind must be"):
                stage.validate_frozen_archive_dataset_receipt(fixture.receipt)

    def test_existing_stage_rejects_unknown_receipt_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _existing_stage_fixture(pathlib.Path(directory))
            fixture.receipt["dataset"]["untrusted"] = True
            fixture.write_receipt()
            with (
                mock.patch.object(stage, "RECIPE_SHA256", fixture.recipe_sha256),
                self.assertRaisesRegex(stage.StagingError, "fields do not match"),
            ):
                stage._validate_existing_stage(
                    fixture.receipt_path,
                    fixture.ms_path,
                    fixture.recipe_path,
                    archive_path=fixture.archive_path,
                    root=fixture.root,
                )

    def test_existing_stage_rejects_non_integer_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _existing_stage_fixture(pathlib.Path(directory))
            fixture.receipt["schema_version"] = 1.0
            fixture.write_receipt()
            with (
                mock.patch.object(stage, "RECIPE_SHA256", fixture.recipe_sha256),
                self.assertRaisesRegex(stage.StagingError, "schema_version"),
            ):
                stage._validate_existing_stage(
                    fixture.receipt_path,
                    fixture.ms_path,
                    fixture.recipe_path,
                    archive_path=fixture.archive_path,
                    root=fixture.root,
                )

    def test_existing_stage_rejects_receipt_bound_to_another_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _existing_stage_fixture(pathlib.Path(directory))
            fixture.receipt["dataset"]["path"] = str(fixture.root / "other.ms")
            fixture.write_receipt()
            with (
                mock.patch.object(stage, "RECIPE_SHA256", fixture.recipe_sha256),
                self.assertRaisesRegex(stage.StagingError, "receipt.dataset.path"),
            ):
                stage._validate_existing_stage(
                    fixture.receipt_path,
                    fixture.ms_path,
                    fixture.recipe_path,
                    archive_path=fixture.archive_path,
                    root=fixture.root,
                )

    def test_existing_stage_rejects_dataset_mutated_after_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _existing_stage_fixture(pathlib.Path(directory))
            (fixture.ms_path / "table.dat").write_bytes(b"mutated")
            with (
                mock.patch.object(stage, "RECIPE_SHA256", fixture.recipe_sha256),
                self.assertRaisesRegex(stage.StagingError, "tree identity"),
            ):
                stage._validate_existing_stage(
                    fixture.receipt_path,
                    fixture.ms_path,
                    fixture.recipe_path,
                    archive_path=fixture.archive_path,
                    root=fixture.root,
                )


def _member(name: str, *, directory: bool = False) -> tarfile.TarInfo:
    value = tarfile.TarInfo(name)
    value.type = tarfile.DIRTYPE if directory else tarfile.REGTYPE
    value.size = 0
    return value


class _ExistingStageFixture:
    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self.archive_path = root / "vlass_test.tgz"
        self.archive_path.write_bytes(b"frozen archive")
        final_root = root / "data" / stage.ARCHIVE_SHA256
        self.ms_path = final_root / stage.MS_MEMBER
        self.ms_path.mkdir(parents=True)
        (self.ms_path / "table.dat").write_bytes(b"visibility data")
        self.recipe_path = final_root / stage.RECIPE_MEMBER
        self.recipe_path.write_bytes(b"tclean recipe")
        self.recipe_sha256 = stage.sha256_file(self.recipe_path)
        self.receipt_path = root / "receipts" / f"dataset-{stage.ARCHIVE_SHA256}.json"
        identity = stage.tree_identity(self.ms_path)
        self.receipt: dict[str, object] = {
            "schema_version": 1,
            "kind": stage.DATASET_RECEIPT_KIND,
            "archive": {
                "path": str(self.archive_path),
                "size_bytes": self.archive_path.stat().st_size,
                "sha256": stage.ARCHIVE_SHA256,
                "gzip_integrity": "verified",
            },
            "dataset": {
                "path": str(self.ms_path),
                **identity,
            },
            "recipe": {
                "path": str(self.recipe_path),
                "size_bytes": self.recipe_path.stat().st_size,
                "sha256": self.recipe_sha256,
            },
            "storage": {
                "root": str(self.root),
                "device": self.root.stat().st_dev,
                "free_bytes_before": 1,
                "same_device_atomic_promotion": True,
            },
            "elapsed_seconds": 0.25,
        }
        self.write_receipt()

    def write_receipt(self) -> None:
        self.receipt_path.parent.mkdir(parents=True, exist_ok=True)
        self.receipt_path.write_text(
            json.dumps(self.receipt, sort_keys=True), encoding="utf-8"
        )


def _existing_stage_fixture(root: pathlib.Path) -> _ExistingStageFixture:
    return _ExistingStageFixture(root)


if __name__ == "__main__":
    unittest.main()
