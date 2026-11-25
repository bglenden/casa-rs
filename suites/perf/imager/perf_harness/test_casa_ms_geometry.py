# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused CASA-free tests for the MS geometry receipt protocol."""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

from perf_harness import casa_ms_geometry as casa_vlass_dataset


CASA_VERSION = "6.7.5.9"


class CasaMsGeometryRequestTests(unittest.TestCase):
    def test_plan_freezes_vlass_selectors_and_expected_counts(self) -> None:
        request = make_request(action="plan")
        plan = casa_vlass_dataset.process_request(request)

        self.assertEqual("planned", plan["status"])
        self.assertEqual(
            [10_400, 655_200],
            [item["expected_selected_rows"] for item in plan["selections"]],
        )
        self.assertEqual(
            "1107~1127,1512~1532,1542~1562",
            plan["selections"][1]["field"],
        )
        self.assertEqual("2~17", plan["selections"][0]["spw"])
        self.assertEqual("<12km", plan["selections"][0]["uvrange"])
        self.assertEqual("OBSERVE_TARGET#UNSPECIFIED", plan["selections"][0]["intent"])
        self.assertEqual(
            casa_vlass_dataset.canonical_sha256(request), plan["request_sha256"]
        )

    def test_schema_rejects_unknown_fields_duplicate_ids_and_relative_paths(
        self,
    ) -> None:
        unknown = make_request()
        unknown["best_effort"] = True
        with self.assertRaisesRegex(
            casa_vlass_dataset.ProtocolError, "unknown=.*best_effort"
        ):
            casa_vlass_dataset.validate_request(unknown)

        duplicate = make_request()
        duplicate["selections"][1]["id"] = duplicate["selections"][0]["id"]
        with self.assertRaisesRegex(casa_vlass_dataset.ProtocolError, "duplicates"):
            casa_vlass_dataset.validate_request(duplicate)

        relative = make_request()
        relative["dataset"]["path"] = "relative.ms"
        with self.assertRaisesRegex(casa_vlass_dataset.ProtocolError, "absolute"):
            casa_vlass_dataset.validate_request(relative)

    def test_limits_and_expected_counts_are_positive_and_bounded(self) -> None:
        for path, value, pattern in (
            (("limits", "table_block_rows"), 0, "positive"),
            (("limits", "max_representative_rows"), 4097, "<= 4096"),
            (("limits", "ms_range_block_mb"), True, "positive"),
            (("selections", 0, "expected_selected_rows"), 0, "positive"),
        ):
            with self.subTest(path=path):
                request = make_request()
                target = request
                for component in path[:-1]:
                    target = target[component]
                target[path[-1]] = value
                with self.assertRaisesRegex(casa_vlass_dataset.ProtocolError, pattern):
                    casa_vlass_dataset.validate_request(request)


class CasaMsGeometryExecutionTests(unittest.TestCase):
    def test_injected_inspector_completes_without_importing_casa(self) -> None:
        request = make_request(action="inspect")
        calls = []

        def inspector(path, selections, limits):
            calls.append((path, selections, limits))
            return make_inspection([10_400, 655_200])

        result = casa_vlass_dataset.process_request(
            request, inspector=inspector, casa_version=CASA_VERSION
        )

        self.assertEqual("completed", result["status"])
        self.assertEqual(CASA_VERSION, result["casa"]["actual_version"])
        self.assertEqual(1, len(calls))
        self.assertEqual(request["dataset"]["path"], calls[0][0])
        self.assertEqual(
            [10_400, 655_200],
            [item["actual_selected_rows"] for item in result["selection_results"]],
        )
        casa_vlass_dataset.validate_result(result)

    def test_row_count_mismatch_is_a_typed_postcondition_failure(self) -> None:
        result = casa_vlass_dataset.process_request(
            make_request(action="inspect"),
            inspector=lambda *_: make_inspection([10_399, 655_200]),
            casa_version=CASA_VERSION,
        )

        self.assertEqual("failed_postcondition", result["status"])
        self.assertEqual("selected_row_count", result["failure"]["kind"])
        self.assertEqual(
            [
                {
                    "selection_id": "single-field-1525",
                    "expected_selected_rows": 10_400,
                    "actual_selected_rows": 10_399,
                }
            ],
            result["failure"]["mismatches"],
        )

    def test_inspector_exception_and_version_drift_are_typed(self) -> None:
        def broken(*_):
            raise OSError("unreadable table")

        execution = casa_vlass_dataset.process_request(
            make_request(action="inspect"),
            inspector=broken,
            casa_version=CASA_VERSION,
        )
        self.assertEqual("failed_execution", execution["status"])
        self.assertEqual("inspection", execution["failure"]["kind"])

        drift = casa_vlass_dataset.process_request(
            make_request(action="inspect"),
            inspector=lambda *_: make_inspection([10_400, 655_200]),
            casa_version="7.0.0",
        )
        self.assertEqual("failed_validation", drift["status"])
        self.assertEqual("casa_version", drift["failure"]["kind"])

    def test_inconsistent_inspector_receipt_is_rejected(self) -> None:
        inspection = make_inspection([10_400, 655_200])
        inspection["selection_results"][0]["matches_expected"] = False
        result = casa_vlass_dataset.process_request(
            make_request(action="inspect"),
            inspector=lambda *_: inspection,
            casa_version=CASA_VERSION,
        )
        self.assertEqual("failed_validation", result["status"])
        self.assertEqual("inspection_protocol", result["failure"]["kind"])


class CasaMsGeometryBoundedHelperTests(unittest.TestCase):
    def test_representative_indices_are_bounded_deterministic_and_keep_endpoints(
        self,
    ) -> None:
        self.assertEqual([], casa_vlass_dataset.representative_indices(0, 8))
        self.assertEqual([0, 1, 2], casa_vlass_dataset.representative_indices(3, 8))
        first = casa_vlass_dataset.representative_indices(1_000_000, 7)
        second = casa_vlass_dataset.representative_indices(1_000_000, 7)
        self.assertEqual(first, second)
        self.assertEqual(7, len(first))
        self.assertEqual(0, first[0])
        self.assertEqual(999_999, first[-1])

    def test_direction_summary_keeps_shape_and_only_first_polynomial_term(self) -> None:
        summary = casa_vlass_dataset.first_direction(
            [[1.25, 99.0, 101.0], [-0.5, 88.0, 102.0]]
        )
        self.assertEqual([2, 3], summary["shape"])
        self.assertEqual([1.25, -0.5], summary["first_direction_rad"])

        with self.assertRaisesRegex(casa_vlass_dataset.ProtocolError, "coordinate"):
            casa_vlass_dataset.first_direction([[1.0]])

    def test_result_schema_rejects_unknown_top_level_fields(self) -> None:
        result = casa_vlass_dataset.build_inspection_plan(make_request(action="plan"))
        result["private_table_dump"] = []
        with self.assertRaisesRegex(
            casa_vlass_dataset.ProtocolError, "private_table_dump"
        ):
            casa_vlass_dataset.validate_result(result)


class CasaMsGeometryCliTests(unittest.TestCase):
    def test_plan_cli_writes_result_without_casa(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request_path = root / "request.json"
            result_path = root / "result.json"
            request_path.write_text(
                json.dumps(make_request(action="plan")), encoding="utf-8"
            )
            script = pathlib.Path(casa_vlass_dataset.__file__)
            completed = subprocess.run(
                [sys.executable, str(script), str(request_path), str(result_path)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual("planned", result["status"])

    def test_invalid_cli_request_writes_typed_validation_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request_path = root / "request.json"
            result_path = root / "result.json"
            request = make_request(action="plan")
            request["dataset"]["path"] = "relative.ms"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            exit_code = casa_vlass_dataset.main([str(request_path), str(result_path)])
            self.assertEqual(0, exit_code)
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual("failed_validation", result["status"])
            self.assertEqual("protocol", result["failure"]["kind"])


def make_request(*, action: str = "plan") -> dict:
    return {
        "schema_version": 1,
        "kind": "casa_ms_geometry_request",
        "request_id": "vlass-fragment-geometry-001",
        "action": action,
        "expected_casa_version": CASA_VERSION,
        "dataset": {"path": "/Volumes/GLENDENNING/casa-rs-vlass/data/input.ms"},
        "selections": [
            {
                "id": "single-field-1525",
                "field": "1525",
                "spw": "2~17",
                "uvrange": "<12km",
                "intent": "OBSERVE_TARGET#UNSPECIFIED",
                "expected_selected_rows": 10_400,
            },
            {
                "id": "all-63-fields",
                "field": "1107~1127,1512~1532,1542~1562",
                "spw": "2~17",
                "uvrange": "<12km",
                "intent": "OBSERVE_TARGET#UNSPECIFIED",
                "expected_selected_rows": 655_200,
            },
        ],
        "limits": {
            "table_block_rows": 65_536,
            "max_representative_rows": 128,
            "ms_range_block_mb": 32,
        },
    }


def make_inspection(actual_counts: list[int]) -> dict:
    request = make_request(action="inspect")
    geometry = {
        "main": {
            "row_count": 655_200,
            "column_names": ["DATA", "DATA_DESC_ID", "FIELD_ID", "TIME", "UVW"],
            "scan": {"block_rows": 65_536, "blocks_read": 10},
            "time_seconds": {"count": 655_200, "min": 5.0e9, "max": 5.0e9 + 1.0},
            "interval_seconds": {"count": 655_200, "min": 0.05, "max": 0.05},
            "field_id": {
                "count": 655_200,
                "min": 1107,
                "max": 1562,
                "representative_values": [1107, 1525, 1562],
                "representative_values_truncated": True,
            },
            "data_desc_id": {
                "count": 655_200,
                "min": 2,
                "max": 17,
                "representative_values": list(range(2, 18)),
                "representative_values_truncated": False,
            },
            "scan_number": {
                "count": 655_200,
                "min": 1,
                "max": 1,
                "representative_values": [1],
                "representative_values_truncated": False,
            },
            "uvw_meters": {
                "u": {"count": 655_200, "min": -11_000.0, "max": 11_000.0},
                "v": {"count": 655_200, "min": -11_500.0, "max": 11_500.0},
                "w": {"count": 655_200, "min": -10_000.0, "max": 10_000.0},
            },
        },
        "field": {
            **empty_sampled_table(1563, ["NAME", "PHASE_DIR"]),
            "direction_reference": direction_reference("J2000"),
        },
        "spectral_window": empty_sampled_table(
            18, ["CHAN_FREQ", "CHAN_WIDTH", "NUM_CHAN"]
        ),
        "data_description": empty_sampled_table(
            18, ["POLARIZATION_ID", "SPECTRAL_WINDOW_ID"]
        ),
        "polarization": empty_sampled_table(1, ["CORR_TYPE", "NUM_CORR"]),
        "pointing": {
            **empty_sampled_table(40_000, ["ANTENNA_ID", "DIRECTION", "TIME"]),
            "scan": {"block_rows": 65_536, "blocks_read": 1},
            "time_seconds": {
                "count": 40_000,
                "min": 5.0e9,
                "max": 5.0e9 + 1.0,
            },
            "interval_seconds": {"count": 40_000, "min": 0.05, "max": 0.05},
            "antenna_id": {
                "count": 40_000,
                "min": 0,
                "max": 26,
                "representative_values": list(range(27)),
                "representative_values_truncated": False,
            },
            "direction_reference": direction_reference("AZELGEO"),
        },
    }
    results = []
    for selection, actual in zip(request["selections"], actual_counts):
        results.append(
            {
                "id": selection["id"],
                "expressions": {
                    "field": selection["field"],
                    "spw": selection["spw"],
                    "uvrange": selection["uvrange"],
                    "intent": selection["intent"],
                },
                "expected_selected_rows": selection["expected_selected_rows"],
                "actual_selected_rows": actual,
                "matches_expected": actual == selection["expected_selected_rows"],
                "selected_indices": {
                    "field_ids": [1525],
                    "spw_ids": list(range(2, 18)),
                    "data_desc_ids": list(range(2, 18)),
                    "channel_selection": {
                        "shape": [16, 4],
                        "rows": [],
                        "truncated": False,
                    },
                },
                "range": {
                    "time_seconds": [5.0e9, 5.0e9 + 1.0],
                    "uvdistance_meters": [0.0, 11_999.0],
                },
                "channels_and_correlations": [
                    {
                        "data_desc_id": 2,
                        "spectral_window_id": 2,
                        "polarization_id": 0,
                        "num_channels": 64,
                        "num_correlations": 4,
                        "correlation_types": [9, 10, 11, 12],
                        "correlation_names": ["XX", "XY", "YX", "YY"],
                    }
                ],
            }
        )
    return {"geometry": geometry, "selection_results": results}


def empty_sampled_table(row_count: int, columns: list[str]) -> dict:
    return {
        "row_count": row_count,
        "column_names": columns,
        "sampling": {
            "method": "selected-focus-plus-evenly-spaced-endpoints",
            "limit": 128,
            "sample_count": 0,
            "row_count": row_count,
            "focus_row_count": 0,
            "truncated": row_count > 0,
        },
        "rows": [],
    }


def direction_reference(name: str) -> dict:
    return {
        "type": "direction",
        "fixed_reference": name,
        "variable_reference_column": None,
        "reference_codes": [],
        "quantum_units": ["rad", "rad"],
    }


if __name__ == "__main__":
    unittest.main()
