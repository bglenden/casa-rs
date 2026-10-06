# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA-free tests for the retained MeasurementSet equivalence proof."""

from __future__ import annotations

import copy
import contextlib
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from perf_harness import measurement_set_equivalence
from perf_harness.errors import HarnessError
import test_run_workload


CASA_PYTHON = "/configured/casa/python"
OWNER_KEYWORD = "CASA_RS_IMAGING_OWNER_MANIFEST"


def write_measurement_set(root: pathlib.Path) -> None:
    (root / "ANTENNA").mkdir(parents=True)
    (root / "HISTORY").mkdir()
    for relative, payload in {
        "table.dat": b"main table metadata",
        "table.f0": b"visibility and flag payload",
        "table.info": b"Measurement Set",
        "ANTENNA/table.dat": b"antenna metadata and values",
        "HISTORY/table.dat": b"history schema",
        "HISTORY/table.f0": b"history row storage",
    }.items():
        (root / relative).write_bytes(payload)


def metadata(root: pathlib.Path) -> dict:
    keywords = {"ANTENNA": f"Table: {root / 'ANTENNA'}", "MS_VERSION": 2.0}
    return {
        "main": {
            "rows": 2,
            "columns": ["DATA", "FLAG"],
            "description": {
                "DATA": {"valueType": "complex", "ndim": 2},
                "FLAG": {"valueType": "boolean", "ndim": 2},
                "_keywords_": copy.deepcopy(keywords),
            },
            "keywords": keywords,
            "data_managers": {"*1": {"TYPE": "TiledShapeStMan", "SPEC": {}}},
        },
        "history": {
            "metadata": {
                "rows": 1,
                "columns": ["TIME", "MESSAGE"],
                "description": {
                    "TIME": {"valueType": "double"},
                    "MESSAGE": {"valueType": "string"},
                    "_keywords_": {},
                },
                "keywords": {},
                "data_managers": {
                    "*1": {"TYPE": "StandardStMan", "SPEC": {"IndexLength": 16}}
                },
            },
            "rows": [{"TIME": 1.0, "MESSAGE": "original acquisition"}],
        },
    }


class MeasurementSetEquivalenceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = pathlib.Path(temporary.name)
        self.current = root / "current.ms"
        self.historical = root / "historical.ms"
        for path in (self.current, self.historical):
            write_measurement_set(path)
        self.current_metadata = metadata(self.current)
        self.historical_metadata = metadata(self.historical)
        reader = mock.patch.object(
            measurement_set_equivalence, "_read_metadata", side_effect=self.read_metadata
        )
        reader.start()
        self.addCleanup(reader.stop)

    def read_metadata(self, path: pathlib.Path, casa_python: str) -> dict:
        self.assertEqual(casa_python, CASA_PYTHON)
        self.assertIn(path, (self.current, self.historical))
        return copy.deepcopy(
            self.current_metadata if path == self.current else self.historical_metadata
        )

    def validate(self) -> dict:
        return measurement_set_equivalence.validate_measurement_set_equivalence(
            self.current, self.historical, casa_python=CASA_PYTHON
        )

    def test_identical_payload_and_root_bound_table_references_prove_equivalence(self):
        result = self.validate()
        self.assertEqual(result["current_path"], str(self.current))
        self.assertEqual(result["historical_path"], str(self.historical))
        self.assertEqual(result["history_original_rows"], 1)
        self.assertEqual(result["history_current_rows"], 1)
        self.assertEqual(result["history_appended_rows"], [])
        self.assertEqual(result["scientific_payload_files"], 4)

    def test_only_exact_main_owner_annotation_may_differ(self):
        owner = {"generation": "owned-current-copy", "revision": 7}
        self.current_metadata["main"]["keywords"][OWNER_KEYWORD] = owner
        self.current_metadata["main"]["description"]["_keywords_"][OWNER_KEYWORD] = owner
        (self.current / "table.dat").write_bytes(b"main rewritten for owner annotation")
        self.validate()

    def test_nested_table_locks_do_not_change_equivalence(self):
        (self.current / "table.lock").write_bytes(b"current process")
        (self.historical / "HISTORY/table.lock").write_bytes(b"historical process")
        self.validate()

    def test_changed_payload_bytes_are_rejected_even_with_equal_metadata(self):
        (self.current / "table.f0").write_bytes(b"Visibility and flag payload")
        with self.assertRaises(HarnessError):
            self.validate()

    def test_non_lock_inventory_additions_are_rejected_on_either_side(self):
        for root in (self.current, self.historical):
            with self.subTest(root=root.name):
                extra = root / "ANTENNA/unknown.table"
                extra.write_bytes(b"unaccounted payload")
                try:
                    with self.assertRaises(HarnessError):
                        self.validate()
                finally:
                    extra.unlink()

    def test_main_schema_and_unknown_metadata_changes_are_rejected(self):
        baseline = copy.deepcopy(self.current_metadata)
        changes = (
            (("main", "rows"), 3),
            (("main", "columns"), ["DATA", "FLAG", "EXTRA"]),
            (("main", "description", "DATA", "valueType"), "dcomplex"),
            (("main", "keywords", "UNKNOWN_SCIENCE"), "changed"),
            (("main", "keywords", OWNER_KEYWORD + "_EXTRA"), "not the owner annotation"),
            (("main", "description", "_keywords_", "UNKNOWN_SCIENCE"), "changed"),
            (("main", "description", "DATA", OWNER_KEYWORD), "not a table keyword"),
            (("main", "data_managers", "*1", "SPEC", "BucketSize"), 8192),
        )
        for path, value in changes:
            with self.subTest(path=path):
                self.current_metadata = copy.deepcopy(baseline)
                target = self.current_metadata
                for component in path[:-1]:
                    target = target[component]
                target[path[-1]] = value
                with self.assertRaises(HarnessError):
                    self.validate()

    def test_whole_root_and_root_child_table_references_normalize(self):
        for root, value in (
            (self.current, self.current_metadata),
            (self.historical, self.historical_metadata),
        ):
            value["main"]["keywords"]["ROOT"] = f"Table: {root}"
            value["main"]["keywords"]["TABLE_PATH"] = f"Table: {root / 'ANTENNA'}"
        self.validate()

    def test_root_substrings_and_sibling_lookalikes_do_not_normalize(self):
        for template in (
            "note Table: {root}/ANTENNA",
            "Table: {root}-backup/ANTENNA",
            "{root}/ANTENNA",
        ):
            with self.subTest(template=template):
                self.current_metadata["main"]["keywords"]["ANNOTATION"] = template.format(
                    root=self.current
                )
                self.historical_metadata["main"]["keywords"]["ANNOTATION"] = template.format(
                    root=self.historical
                )
                with self.assertRaises(HarnessError):
                    self.validate()

    def test_non_keyword_descriptor_strings_are_literal_not_table_references(self):
        for root, value in (
            (self.current, self.current_metadata),
            (self.historical, self.historical_metadata),
        ):
            value["main"]["description"]["DATA"]["comment"] = f"Table: {root / 'ANTENNA'}"
        with self.assertRaises(HarnessError):
            self.validate()

    def test_history_row_strings_are_literal_even_when_they_look_like_table_references(self):
        for root, value in (
            (self.current, self.current_metadata),
            (self.historical, self.historical_metadata),
        ):
            value["history"]["rows"][0]["MESSAGE"] = f"Table: {root / 'ANTENNA'}"
        with self.assertRaises(HarnessError):
            self.validate()

    def test_table_references_cannot_escape_the_inventoried_measurement_set(self):
        for suffix in ("/../external.table", "/ANTENNA/../../external.table"):
            with self.subTest(suffix=suffix):
                for root, value in (
                    (self.current, self.current_metadata),
                    (self.historical, self.historical_metadata),
                ):
                    value["main"]["keywords"]["ESCAPED"] = f"Table: {root}{suffix}"
                with self.assertRaises(HarnessError):
                    self.validate()

    def append_history(self) -> dict:
        row = {"TIME": 2.0, "MESSAGE": "bounded owner initialized"}
        self.current_metadata["history"]["rows"].append(row)
        self.current_metadata["history"]["metadata"]["rows"] = 2
        (self.current / "HISTORY/table.f0").write_bytes(b"storage with appended row")
        return row

    def test_history_append_is_reported_and_standard_index_may_grow(self):
        appended = self.append_history()
        managers = self.current_metadata["history"]["metadata"]["data_managers"]
        managers["*1"]["SPEC"]["IndexLength"] = 32
        result = self.validate()
        self.assertEqual(result["history_original_rows"], 1)
        self.assertEqual(result["history_current_rows"], 2)
        self.assertEqual(result["history_appended_rows"], [appended])

    def test_common_history_rows_must_match_even_with_append(self):
        self.append_history()
        self.current_metadata["history"]["rows"][0]["MESSAGE"] = "rewritten acquisition"
        with self.assertRaises(HarnessError):
            self.validate()

    def test_history_cannot_be_truncated_or_misreport_its_row_count(self):
        baseline = copy.deepcopy(self.current_metadata)
        for rows, count in (([], 0), (baseline["history"]["rows"], 2)):
            with self.subTest(rows=rows, count=count):
                self.current_metadata = copy.deepcopy(baseline)
                self.current_metadata["history"]["rows"] = rows
                self.current_metadata["history"]["metadata"]["rows"] = count
                with self.assertRaises(HarnessError):
                    self.validate()

    def test_history_schema_unknown_keywords_and_other_manager_changes_fail(self):
        self.append_history()
        baseline = copy.deepcopy(self.current_metadata)
        changes = (
            (("columns",), ["TIME", "MESSAGE", "EXTRA"]),
            (("description", "MESSAGE", "valueType"), "double"),
            (("keywords", "UNKNOWN"), "changed"),
            (("keywords", OWNER_KEYWORD), "owner exemption is MAIN only"),
            (("data_managers", "*1", "SPEC", "BucketSize"), 8192),
            (("data_managers", "*1", "SPEC", "IndexLength"), 8),
        )
        for path, value in changes:
            with self.subTest(path=path):
                self.current_metadata = copy.deepcopy(baseline)
                target = self.current_metadata["history"]["metadata"]
                for component in path[:-1]:
                    target = target[component]
                target[path[-1]] = value
                with self.assertRaises(HarnessError):
                    self.validate()

    def test_history_index_cannot_grow_without_appended_rows(self):
        managers = self.current_metadata["history"]["metadata"]["data_managers"]
        managers["*1"]["SPEC"]["IndexLength"] = 32
        with self.assertRaises(HarnessError):
            self.validate()

    def test_index_growth_exemption_is_only_for_standard_stman(self):
        self.append_history()
        for value in (self.current_metadata, self.historical_metadata):
            value["history"]["metadata"]["data_managers"]["*1"]["TYPE"] = "OtherStMan"
        self.current_metadata["history"]["metadata"]["data_managers"]["*1"]["SPEC"]["IndexLength"] = 32
        with self.assertRaises(HarnessError):
            self.validate()


class ReusedCasaMeasurementSetTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = pathlib.Path(temporary.name)
        self.plan, self.prefix = (
            test_run_workload.FrozenCasaRecipeExecutionTests._reused_bundle(root)
        )
        self.historical = root / "input.ms"
        self.current = root / "relocated.ms"
        for path in (self.current, self.historical):
            write_measurement_set(path)
        self.plan["command"]["casa"]["python"] = CASA_PYTHON
        self.plan["command"]["casa"]["base_overrides"]["vis"] = str(self.current)

    def validate(self) -> str:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            test_run_workload.run_workload.casa_tclean_workflow.validate_reused_casa_prefix(
                self.plan, self.prefix
            )
        return output.getvalue()

    def read_metadata(self, path: pathlib.Path, casa_python: str) -> dict:
        self.assertEqual(casa_python, CASA_PYTHON)
        return metadata(path)

    def test_relocated_vis_requires_real_equivalence_proof_and_reports_it(self):
        with mock.patch.object(
            measurement_set_equivalence, "_read_metadata", side_effect=self.read_metadata
        ):
            output = self.validate()
        prefix = "reused_casa_ms_equivalence "
        self.assertTrue(output.startswith(prefix))
        evidence = json.loads(output[len(prefix):])
        self.assertEqual(evidence["current_path"], str(self.current))
        self.assertEqual(evidence["historical_path"], str(self.historical))
        self.assertEqual(evidence["scientific_payload_files"], 4)

    def test_relocated_vis_with_changed_payload_is_not_ignored(self):
        (self.current / "table.f0").write_bytes(b"different visibility data")
        with self.assertRaisesRegex(HarnessError, "scientific payload"):
            self.validate()

    def test_matching_vis_does_not_require_relocation_proof(self):
        self.plan["command"]["casa"]["base_overrides"]["vis"] = str(self.historical)
        with mock.patch.object(
            measurement_set_equivalence,
            "_read_metadata",
            side_effect=AssertionError("same-path reuse must not invoke CASA metadata"),
        ):
            self.assertEqual(self.validate(), "")

    def test_equivalent_ms_does_not_excuse_other_effective_parameter_changes(self):
        self.plan["command"]["casa"]["base_overrides"]["niter"] = 0
        with mock.patch.object(
            measurement_set_equivalence, "_read_metadata", side_effect=self.read_metadata
        ):
            with self.assertRaisesRegex(HarnessError, "effective parameters.*niter"):
                self.validate()


if __name__ == "__main__":
    unittest.main()
