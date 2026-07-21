# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for deterministic CASA runtime and data/model identities."""

from __future__ import annotations

import copy
import pathlib
import tempfile
import types
import unittest
from unittest import mock

from perf_harness import casa_runtime_identity as identity


class CasaRuntimeIdentityTests(unittest.TestCase):
    def test_result_digest_and_casa_version_are_fail_closed(self) -> None:
        payload = valid_runtime_identity()
        result = {
            "schema_version": 1,
            "kind": identity.RESULT_KIND,
            "status": "completed",
            "identity": payload,
            "identity_sha256": identity.stable_identity_sha256(payload),
        }
        identity.validate_result(result, expected_casa_version="6.7.5.9")

        changed = copy.deepcopy(result)
        changed["identity"]["data_trees"]["vla"]["tree_sha256"] = "b" * 64
        with self.assertRaisesRegex(identity.IdentityError, "digest"):
            identity.validate_result(changed)

        with self.assertRaisesRegex(identity.IdentityError, "version mismatch"):
            identity.validate_result(result, expected_casa_version="6.8.0.0")

    def test_result_rejects_dropped_or_mutated_frozen_identity_fields(self) -> None:
        payload = valid_runtime_identity()

        def result_for(value):
            return {
                "schema_version": 1,
                "kind": identity.RESULT_KIND,
                "status": "completed",
                "identity": value,
                "identity_sha256": identity.stable_identity_sha256(value),
            }

        dropped = copy.deepcopy(payload)
        del dropped["modules"]["casatools"]["code_tree"]
        with self.assertRaisesRegex(identity.IdentityError, "missing=.*code_tree"):
            identity.validate_result(
                {
                    "schema_version": 1,
                    "kind": identity.RESULT_KIND,
                    "status": "completed",
                    "identity": dropped,
                    "identity_sha256": "0" * 64,
                }
            )

        mutated = copy.deepcopy(payload)
        mutated["data_trees"]["vla"]["file_count"] = True
        with self.assertRaisesRegex(
            identity.IdentityError, "file_count must be an integer"
        ):
            identity.validate_result(
                {
                    "schema_version": 1,
                    "kind": identity.RESULT_KIND,
                    "status": "completed",
                    "identity": mutated,
                    "identity_sha256": "0" * 64,
                }
            )

        valid = result_for(payload)
        valid["unbound"] = True
        with self.assertRaisesRegex(identity.IdentityError, "unknown=.*unbound"):
            identity.validate_result(valid)

    def test_required_tree_records_exclusion_policy_in_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "model.dat").write_bytes(b"science")
            (root / "table.lock").write_bytes(b"volatile")

            first = identity._required_tree(root, "fixture")
            self.assertEqual(1, first["excluded_count"])
            (root / "table.lock").write_bytes(b"changed")
            second = identity._required_tree(root, "fixture")
            self.assertEqual(first["tree_sha256"], second["tree_sha256"])

            (root / "model.dat").write_bytes(b"changed science")
            third = identity._required_tree(root, "fixture")
            self.assertNotEqual(second["tree_sha256"], third["tree_sha256"])

    def test_config_parser_does_not_execute_values(self) -> None:
        parsed = identity._parse_config(
            [
                "measurespath = '/data'",
                "datapath = ['/data', '/models']",
                "unsafe = make_value()",
            ]
        )
        self.assertEqual("/data", parsed["measurespath"])
        self.assertEqual(["/data", "/models"], parsed["datapath"])
        self.assertNotIn("unsafe", parsed)

    def test_stable_identity_ignores_host_locators_but_not_code_or_data(self) -> None:
        first = valid_runtime_identity(root="/host-a")
        relocated = copy.deepcopy(first)
        relocated["python"]["executable"] = "/host-b/python"
        for name in relocated["modules"]:
            relocated["modules"][name]["module_file"] = f"/host-b/{name}/__init__.py"
        relocated["configuration"]["measurespath"] = "/host-b/casa-data"
        relocated["configuration"]["datapath"] = ["/host-b/casa-data"]
        relocated["data_trees"]["geodetic"]["path"] = "/host-b/casa-data/geodetic"
        relocated["data_trees"]["vla"]["path"] = "/host-b/casa-data/nrao/VLA"
        self.assertEqual(
            identity.stable_identity_sha256(first),
            identity.stable_identity_sha256(relocated),
        )

        relocated["modules"]["casatasks"]["code_tree"]["tree_sha256"] = "d" * 64
        self.assertNotEqual(
            identity.stable_identity_sha256(first),
            identity.stable_identity_sha256(relocated),
        )
        relocated = copy.deepcopy(first)
        relocated["data_trees"]["vla"]["tree_sha256"] = "e" * 64
        self.assertNotEqual(
            identity.stable_identity_sha256(first),
            identity.stable_identity_sha256(relocated),
        )

    def test_distribution_identity_hashes_non_init_code_and_native_libraries(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            files = [
                pathlib.PurePosixPath("casatasks/__init__.py"),
                pathlib.PurePosixPath("casatasks/tclean.py"),
                pathlib.PurePosixPath("casatasks/_imager.so"),
                pathlib.PurePosixPath("casatasks/__pycache__/tclean.pyc"),
            ]
            for relative in files:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(relative.as_posix().encode("utf-8"))
            distribution = types.SimpleNamespace(
                files=files, locate_file=lambda relative: root / relative
            )
            module = types.SimpleNamespace(__name__="casatasks")
            with mock.patch.object(
                identity.importlib.metadata,
                "distribution",
                return_value=distribution,
            ):
                before = identity._distribution_code_identity(
                    module, distribution="casatasks"
                )
                (root / "casatasks/tclean.py").write_bytes(b"changed task code")
                after_task_change = identity._distribution_code_identity(
                    module, distribution="casatasks"
                )
                self.assertNotEqual(
                    before["tree_sha256"], after_task_change["tree_sha256"]
                )
                (root / "casatasks/_imager.so").write_bytes(b"changed native code")
                after_native_change = identity._distribution_code_identity(
                    module, distribution="casatasks"
                )
                self.assertNotEqual(
                    after_task_change["tree_sha256"],
                    after_native_change["tree_sha256"],
                )
                (root / "casatasks/__pycache__/tclean.pyc").write_bytes(b"volatile")
                after_bytecode_change = identity._distribution_code_identity(
                    module, distribution="casatasks"
                )
                self.assertEqual(
                    after_native_change["tree_sha256"],
                    after_bytecode_change["tree_sha256"],
                )


def valid_runtime_identity(*, root: str = "/runtime") -> dict:
    modules = {}
    for index, name in enumerate(("casatasks", "casatools", "casaconfig", "casadata")):
        modules[name] = {
            "distribution_version": (
                "6.7.5.9" if name in {"casatasks", "casatools"} else "1.0"
            ),
            "reported_version": "6.7.5.9" if name == "casatasks" else None,
            "module_file": f"{root}/{name}/__init__.py",
            "module_file_sha256": f"{index + 1:x}" * 64,
            "code_tree": {
                "tree_sha256": f"{index + 5:x}" * 64,
                "file_count": index + 1,
                "size_bytes": 100 + index,
                "policy": "package_files_without_bytecode_v1",
            },
        }
    return {
        "schema_version": 1,
        "python": {
            "version": "3.14.6",
            "implementation": "CPython",
            "executable": f"{root}/python",
            "executable_sha256": "a" * 64,
        },
        "modules": modules,
        "configuration": {
            "measurespath": f"{root}/casa-data",
            "datapath": [f"{root}/casa-data"],
        },
        "data_versions": {
            "casarundata": {"version": "2026.02", "date": "2026-02-19"},
            "measures": {"version": "unknown", "date": "", "site": "unknown"},
        },
        "data_trees": {
            "geodetic": {
                "path": f"{root}/casa-data/geodetic",
                "tree_sha256": "b" * 64,
                "file_count": 3,
                "size_bytes": 1000,
                "excluded_names": ["data_update.lock", "table.lock"],
                "excluded_count": 1,
            },
            "vla": {
                "path": f"{root}/casa-data/nrao/VLA",
                "tree_sha256": "c" * 64,
                "file_count": 4,
                "size_bytes": 2000,
                "excluded_names": ["data_update.lock", "table.lock"],
                "excluded_count": 2,
            },
        },
    }


if __name__ == "__main__":
    unittest.main()
