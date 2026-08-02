# SPDX-License-Identifier: LGPL-3.0-or-later
"""Adversarial tests for generic frozen dataset geometry identities."""

from __future__ import annotations

import unittest

from perf_harness.dataset_selection_identity import (
    bind_frozen_selection,
    validate_frozen_dataset_geometry_identity,
)
from perf_harness.errors import HarnessError


class FrozenSelectionTests(unittest.TestCase):
    def test_exact_selection_binds_rows_channels_and_correlations(self) -> None:
        identity = bind_frozen_selection(
            _document(),
            selection_name="single_field",
            imaging=_imaging(),
            spw_ids=[2, 3],
        )

        self.assertEqual(1300, identity["selected_rows"])
        self.assertEqual(["RR", "RL", "LR", "LL"], identity["correlations"])

    def test_every_selection_expression_is_frozen(self) -> None:
        for name, value in (
            ("field", "1526"),
            ("spw", "2"),
            ("intent", "OBSERVE_OTHER"),
            ("uvrange", "<10km"),
            ("channel_start", 1),
            ("channel_count", 32),
        ):
            with self.subTest(name=name):
                imaging = _imaging()
                imaging[name] = value
                with self.assertRaisesRegex(HarnessError, f"imaging {name}"):
                    bind_frozen_selection(
                        _document(),
                        selection_name="single_field",
                        imaging=imaging,
                        spw_ids=[2, 3],
                    )

    def test_spw_and_correlation_facts_cannot_mutate(self) -> None:
        bad_spw = _document()
        bad_spw["selections"]["single_field"]["spw_ids"] = [2]
        with self.assertRaisesRegex(HarnessError, "spw_ids"):
            bind_frozen_selection(
                bad_spw,
                selection_name="single_field",
                imaging=_imaging(),
                spw_ids=[2, 3],
            )

        bad_correlations = _document()
        bad_correlations["selections"]["single_field"]["correlations"] = ["RR"]
        with self.assertRaisesRegex(HarnessError, "correlations"):
            bind_frozen_selection(
                bad_correlations,
                selection_name="single_field",
                imaging=_imaging(),
                spw_ids=[2, 3],
            )

    def test_channel_window_must_fit_every_selected_spw(self) -> None:
        document = _document()
        document["geometry"]["spectral_windows"][1]["channels"] = 32
        with self.assertRaisesRegex(HarnessError, "exceed SPW 3"):
            bind_frozen_selection(
                document,
                selection_name="single_field",
                imaging=_imaging(),
                spw_ids=[2, 3],
            )


class FrozenDatasetGeometryContractTests(unittest.TestCase):
    def test_complete_generic_identity_is_valid(self) -> None:
        validate_frozen_dataset_geometry_identity(_complete_document())

    def test_unknown_kind_is_rejected(self) -> None:
        document = _complete_document()
        document["kind"] = "other_identity"
        with self.assertRaisesRegex(HarnessError, "kind must be"):
            validate_frozen_dataset_geometry_identity(document)

    def test_suffix_lookalike_kind_is_rejected(self) -> None:
        document = _complete_document()
        document["kind"] = "vlass_dataset_geometry_identity"
        with self.assertRaisesRegex(HarnessError, "kind must be"):
            validate_frozen_dataset_geometry_identity(document)

    def test_unknown_nested_field_is_rejected(self) -> None:
        document = _complete_document()
        document["geometry"]["spectral_windows"][0]["extra"] = True
        with self.assertRaisesRegex(HarnessError, r"unknown=\['extra'\]"):
            validate_frozen_dataset_geometry_identity(document)

    def test_missing_nested_field_is_rejected(self) -> None:
        document = _complete_document()
        del document["selections"]["single_field"]["selected_rows"]
        with self.assertRaisesRegex(HarnessError, r"missing=\['selected_rows'\]"):
            validate_frozen_dataset_geometry_identity(document)

    def test_wrong_nested_type_is_rejected(self) -> None:
        document = _complete_document()
        document["source_receipts"]["geometry_receipts"]["full"]["request_sha256"] = 7
        with self.assertRaisesRegex(HarnessError, "lowercase SHA-256"):
            validate_frozen_dataset_geometry_identity(document)


def _imaging() -> dict[str, object]:
    return {
        "field": "1525",
        "spw": "2~3",
        "uvrange": "<12km",
        "intent": "OBSERVE_TARGET#UNSPECIFIED",
        "channel_start": 0,
        "channel_count": 64,
    }


def _document() -> dict[str, object]:
    return {
        "geometry": {
            "correlations": ["RR", "RL", "LR", "LL"],
            "spectral_windows": [
                {"id": 2, "channels": 64},
                {"id": 3, "channels": 64},
            ],
        },
        "selections": {
            "single_field": {
                "field": "1525",
                "spw": "2~3",
                "uvrange": "<12km",
                "intent": "OBSERVE_TARGET#UNSPECIFIED",
                "selected_rows": 1300,
                "spw_ids": [2, 3],
                "channel_start": 0,
                "channel_count": 64,
                "correlations": ["RR", "RL", "LR", "LL"],
            }
        },
    }


def _complete_document() -> dict[str, object]:
    document = _document()
    document.update(
        {
            "schema_version": 1,
            "kind": "frozen_dataset_geometry_identity",
            "dataset": {
                "archive_sha256": "a" * 64,
                "tree_sha256": "b" * 64,
                "file_count": 12,
                "size_bytes": 34,
            },
            "source_receipts": {
                "dataset_receipt_sha256": "c" * 64,
                "geometry_receipts": {
                    "full": {
                        "request_sha256": "d" * 64,
                        "result_sha256": "e" * 64,
                    },
                    "smoke": {
                        "request_sha256": "f" * 64,
                        "result_sha256": "0" * 64,
                    },
                },
            },
        }
    )
    document["geometry"].update(
        {
            "data_description_ids": [2, 3],
            "field_row_count": 2,
            "main_row_count": 1300,
            "pointing_row_count": 20,
            "field_groups": [{"id": "row_1", "field": "1~2"}],
        }
    )
    for index, row in enumerate(document["geometry"]["spectral_windows"]):
        row.update(
            {
                "first_hz": 1.0e9 + index * 1.0e8,
                "last_hz": 1.1e9 + index * 1.0e8,
                "width_hz": 2.0e6,
            }
        )
    return document


if __name__ == "__main__":
    unittest.main()
