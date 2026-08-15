#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import vlass_aw_datagrid_tt0_arithmetic_compat_compare as subject


def write_envelope(
    path: Path,
    *,
    envelope_schema: str,
    member: str,
    embedded: dict[str, object],
    earlier_nested_decoy: bool = False,
) -> str:
    embedded_json = json.dumps(embedded, indent=2, sort_keys=True)
    digest = hashlib.sha256(embedded_json.encode()).hexdigest()
    decoy = (
        f'  "earlier_decoy": {{{json.dumps(member)}: {{"schema": "decoy"}}}},\n'
        if earlier_nested_decoy
        else ""
    )
    envelope = (
        "{\n"
        f'  "schema": {json.dumps(envelope_schema)},\n'
        f"{decoy}"
        '  "content_address": {\n'
        '    "algorithm": "sha256",\n'
        f'    "scope": "embedded-{member}-json-utf8",\n'
        f'    "digest": "{digest}"\n'
        "  },\n"
        f'  "{member}": {embedded_json}\n'
        "}\n"
    )
    path.write_text(envelope)
    return digest


def v4_evidence() -> dict[str, object]:
    return {
        "schema": "casa-rs-aw-datagrid-bracket-v4",
        "status": "completed-before-finalize",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "products": "not-entered",
        "expected_grid_nxy": 4096,
        "completed_calls": 2,
        "completed_blocks": 1,
        "selection": copy.deepcopy(subject.EXPECTED_SELECTION),
        "observed_first_buffer": copy.deepcopy(subject.EXPECTED_FIRST_BUFFER),
        "absolute_main_rows": copy.deepcopy(subject.EXPECTED_ABSOLUTE_ROWS),
        "direct_raw_input_hash": subject.DIRECT_COMPACT_INPUT_HASH,
        "compact_input_hash": subject.DIRECT_COMPACT_INPUT_HASH,
        "direct_compact_exact_match": True,
        "portable_hash_contract": subject.PORTABLE_HASH_CONTRACT,
        "calls": [
            {
                "call": 0,
                "block": 0,
                "term": 0,
                "source_count": subject.SOURCE_COUNT,
                "portable_geometry_hash": subject.PORTABLE_GEOMETRY_HASH,
                "portable_input_hash": subject.PORTABLE_INPUT_HASH,
            },
            {"term": 1, "source_count": subject.SOURCE_COUNT},
        ],
        "block_boundaries": [
            {
                "terms": [
                    {
                        "term": 0,
                        "grid_hash": subject.CASA_RS_V4_TT0_GRID_HASH,
                        "sumwt_hash": subject.CASA_RS_V4_TT0_SUMWT_HASH,
                        "sumwt_value_bits": subject.CASA_RS_V4_TT0_SUMWT_BITS,
                        "grid_values_hashed": subject.GRID_VALUES,
                    },
                    {
                        "term": 1,
                        "grid_hash": 1,
                        "sumwt_hash": 2,
                        "grid_values_hashed": subject.GRID_VALUES,
                    },
                ]
            }
        ],
    }


def casa_v5_receipt() -> dict[str, object]:
    first_buffer = copy.deepcopy(subject.EXPECTED_FIRST_BUFFER)
    first_buffer["row_ids"] = list(range(325))
    return {
        "schema": "casa-aw-datagrid-bracket-v1",
        "status": "completed-before-finalize",
        "casa_source_commit": subject.CASA_SOURCE_COMMIT,
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "expected_grid_nxy": 4096,
        "completed_calls": 2,
        "completed_blocks": 1,
        "native_first_vb": first_buffer,
        "calls": [
            {"term": 0, "source_count": subject.SOURCE_COUNT},
            {"term": 1, "source_count": subject.SOURCE_COUNT},
        ],
        "block_boundaries": [
            {
                "terms": [
                    {
                        "term": 0,
                        "grid_hash": subject.CASA_TT0_GRID_HASH,
                        "sumwt_hash": subject.CASA_TT0_SUMWT_HASH,
                        "grid_values_hashed": subject.GRID_VALUES,
                    },
                    {
                        "term": 1,
                        "grid_hash": 3,
                        "sumwt_hash": 4,
                        "grid_values_hashed": subject.GRID_VALUES,
                    },
                ]
            }
        ],
    }


def candidate_evidence(
    *,
    matching_ordinals: tuple[int, ...] = (),
) -> dict[str, object]:
    variants = []
    for ordinal, contract in enumerate(subject.EXPECTED_VARIANTS):
        grid_hash = subject.CASA_RS_V4_TT0_GRID_HASH if ordinal == 0 else 100 + ordinal
        if ordinal in matching_ordinals:
            grid_hash = subject.CASA_TT0_GRID_HASH
        variants.append(
            {
                "ordinal": ordinal,
                **copy.deepcopy(contract),
                "traversal_contract": subject.TRAVERSAL_CONTRACT,
                "grid_hash_contract": subject.GRID_HASH_CONTRACT,
                "grid_hash": grid_hash,
                "grid_values_hashed": subject.GRID_VALUES,
                "nonfinite_grid_value_count": 0,
                "source_count": subject.SOURCE_COUNT,
                "logical_role_count": subject.ROLE_COUNT,
                "tap_count": 55_555,
                "matches_frozen_production_baseline": (
                    grid_hash == subject.CASA_RS_V4_TT0_GRID_HASH
                ),
                "matches_casa_target": (grid_hash == subject.CASA_TT0_GRID_HASH),
            }
        )
    matches = [
        variant["name"] for variant in variants if variant["matches_casa_target"]
    ]
    result = {
        0: "completed-no-tested-variant-matched-casa",
        1: "completed-single-tested-variant-matched-casa",
    }.get(len(matches), "completed-multiple-tested-variants-matched-casa")
    roles = copy.deepcopy(subject.EXPECTED_CORRELATION_MUELLER_ROLES)
    role_words = [
        item
        for role in roles
        for item in (
            role["ordinal"],
            role["selected_corr_index"],
            role["selected_corr_code"],
            role["logical_mueller"],
        )
    ]
    return {
        "schema": subject.CANDIDATE_EVIDENCE_SCHEMA,
        "status": "completed-controlled-stop",
        "result": result,
        "result_taxonomy": copy.deepcopy(subject.RESULT_TAXONOMY),
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "producer": "casa-rs",
        "production_path_changed": False,
        "production_dispatch": "not-entered",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "products": "not-entered",
        "tt1": False,
        "terms_evaluated": [0],
        "sumwt": "not-controlled",
        "phase_application": ("inherited-prephased-production-bundles-not-controlled"),
        "grid_dispatch": ("serial-host-f64-exact-source-order-tt0-arithmetic-variants"),
        "traversal_contract": subject.TRAVERSAL_CONTRACT,
        "correlation_mueller_role_order": {
            "contract": subject.CORRELATION_MUELLER_CONTRACT,
            "count": 2,
            "hash_contract": subject.CORRELATION_MUELLER_HASH_CONTRACT,
            "hash": subject._fnv1a64_usize_words(role_words),
            "roles": roles,
        },
        "grid_hash_contract": subject.TOP_LEVEL_GRID_HASH_CONTRACT,
        "portable_hash_contract": subject.PORTABLE_HASH_CONTRACT,
        "expected_grid_nxy": 4096,
        "target_blocks": 1,
        "diagnostic_terms": 1,
        "request_nterms": 2,
        "replay_block_ordinal": 0,
        "replay_window_ordinal": 0,
        "last_window_in_replay_block": True,
        "frozen_production_baseline_hash": subject.CASA_RS_V4_TT0_GRID_HASH,
        "casa_target_hash": subject.CASA_TT0_GRID_HASH,
        "baseline_gate": "matched-frozen-production-baseline",
        "casa_target_matching_variants": matches,
        "selection": copy.deepcopy(subject.EXPECTED_SELECTION),
        "observed_first_buffer": copy.deepcopy(subject.EXPECTED_FIRST_BUFFER),
        "absolute_main_rows": copy.deepcopy(subject.EXPECTED_ABSOLUTE_ROWS),
        "input_hashes": {
            "direct_raw": subject.DIRECT_COMPACT_INPUT_HASH,
            "compact": subject.DIRECT_COMPACT_INPUT_HASH,
            "direct_compact_exact_match": True,
            "portable_geometry": subject.PORTABLE_GEOMETRY_HASH,
            "portable_input": subject.PORTABLE_INPUT_HASH,
        },
        "portable_call": {
            "call": 0,
            "block": 0,
            "term": 0,
            "source_count": subject.SOURCE_COUNT,
        },
        "counts": {
            "source": subject.SOURCE_COUNT,
            "logical_role": subject.ROLE_COUNT,
            "tap": 55_555,
            "grid_values_per_variant": subject.GRID_VALUES,
            "variant_count": len(subject.EXPECTED_VARIANTS),
            "nonfinite_grid_value": 0,
            "out_of_grid_support_attempt": 0,
        },
        "nonfinite_grid_value_contract": (subject.NONFINITE_GRID_VALUE_CONTRACT),
        "out_of_grid_support_attempt_contract": (
            subject.OUT_OF_GRID_SUPPORT_ATTEMPT_CONTRACT
        ),
        "traversal_hash": 8_888,
        "variants": variants,
    }


class ReceiptFixture:
    def __init__(self, root: Path) -> None:
        self.v4 = root / "v4.json"
        self.v5 = root / "v5.json"
        self.candidate = root / "candidate.json"
        v4_digest = write_envelope(
            self.v4,
            envelope_schema="casa-rs-aw-datagrid-bracket-envelope-v4",
            member="evidence",
            embedded=v4_evidence(),
        )
        self.v5.write_text(json.dumps(casa_v5_receipt(), indent=2) + "\n")
        self.frozen = subject.FrozenReceipts(
            casa_rs_v4_sha256=subject.sha256_path(self.v4),
            casa_rs_v4_evidence_sha256=v4_digest,
            casa_rs_v4_revision=subject.CASA_RS_V4_REVISION,
            casa_v5_sha256=subject.sha256_path(self.v5),
            casa_source_commit=subject.CASA_SOURCE_COMMIT,
        )

    def write_candidate(
        self,
        *,
        matching_ordinals: tuple[int, ...] = (),
        earlier_nested_decoy: bool = False,
    ) -> None:
        write_envelope(
            self.candidate,
            envelope_schema=subject.CANDIDATE_ENVELOPE_SCHEMA,
            member="evidence",
            embedded=candidate_evidence(
                matching_ordinals=matching_ordinals,
            ),
            earlier_nested_decoy=earlier_nested_decoy,
        )


class ArithmeticCompatibilityComparisonTests(unittest.TestCase):
    def test_earlier_nested_member_key_does_not_shadow_top_level_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            fixture.write_candidate(earlier_nested_decoy=True)
            value = subject.build_comparison(
                candidate_path=fixture.candidate,
                casa_rs_v4_path=fixture.v4,
                casa_v5_path=fixture.v5,
                frozen=fixture.frozen,
            )
            self.assertEqual(
                value["comparison"]["classification"],
                "valid-negative-no-exact-casa-tt0-grid-hash-match",
            )

    def test_duplicate_top_level_evidence_is_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            embedded_json = json.dumps(candidate_evidence(), indent=2, sort_keys=True)
            digest = hashlib.sha256(embedded_json.encode()).hexdigest()
            fixture.candidate.write_text(
                "{\n"
                f'  "schema": {json.dumps(subject.CANDIDATE_ENVELOPE_SCHEMA)},\n'
                '  "content_address": {\n'
                '    "algorithm": "sha256",\n'
                '    "scope": "embedded-evidence-json-utf8",\n'
                f'    "digest": "{digest}"\n'
                "  },\n"
                '  "evidence": {"schema": "earlier-duplicate"},\n'
                f'  "evidence": {embedded_json}\n'
                "}\n"
            )
            with self.assertRaisesRegex(
                subject.ContractError,
                "ambiguous duplicate top-level",
            ):
                subject.build_comparison(
                    candidate_path=fixture.candidate,
                    casa_rs_v4_path=fixture.v4,
                    casa_v5_path=fixture.v5,
                    frozen=fixture.frozen,
                )

    def test_json_type_identity_is_enforced_recursively(self) -> None:
        mutations = [
            ("boolean for integer", "target_blocks", True),
            ("integer for boolean", "production_path_changed", 0),
            ("nested boolean for integer", "terms_evaluated", [False]),
        ]
        for label, field, replacement in mutations:
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as temporary:
                    fixture = ReceiptFixture(Path(temporary))
                    candidate = candidate_evidence()
                    candidate[field] = replacement
                    write_envelope(
                        fixture.candidate,
                        envelope_schema=subject.CANDIDATE_ENVELOPE_SCHEMA,
                        member="evidence",
                        embedded=candidate,
                    )
                    with self.assertRaises(subject.ContractError):
                        subject.build_comparison(
                            candidate_path=fixture.candidate,
                            casa_rs_v4_path=fixture.v4,
                            casa_v5_path=fixture.v5,
                            frozen=fixture.frozen,
                        )

    def test_valid_negative_is_a_successful_classification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            fixture.write_candidate()
            value = subject.build_comparison(
                candidate_path=fixture.candidate,
                casa_rs_v4_path=fixture.v4,
                casa_v5_path=fixture.v5,
                frozen=fixture.frozen,
            )
            comparison = value["comparison"]
            self.assertEqual(comparison["status"], "valid-classification")
            self.assertEqual(
                comparison["classification"],
                "valid-negative-no-exact-casa-tt0-grid-hash-match",
            )
            self.assertTrue(comparison["claims"]["valid_negative_classification"])
            self.assertFalse(comparison["claims"]["exact_casa_tt0_grid_hash_match"])
            self.assertFalse(comparison["claims"]["production_tt0_promoted"])

    def test_single_exact_casa_grid_match_is_distinct_from_negative(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            fixture.write_candidate(matching_ordinals=(3,))
            value = subject.build_comparison(
                candidate_path=fixture.candidate,
                casa_rs_v4_path=fixture.v4,
                casa_v5_path=fixture.v5,
                frozen=fixture.frozen,
            )
            self.assertEqual(
                value["comparison"]["classification"],
                "exact-casa-tt0-grid-hash-match-single-variant",
            )
            self.assertEqual(
                value["comparison"]["candidate"][
                    "exact_casa_tt0_grid_matching_variants"
                ],
                [subject.EXPECTED_VARIANTS[3]["name"]],
            )
            self.assertFalse(
                value["comparison"]["claims"]["exact_casa_tt0_sumwt_match"]
            )

    def test_multiple_exact_casa_grid_matches_are_classified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            fixture.write_candidate(matching_ordinals=(3, 4))
            value = subject.build_comparison(
                candidate_path=fixture.candidate,
                casa_rs_v4_path=fixture.v4,
                casa_v5_path=fixture.v5,
                frozen=fixture.frozen,
            )
            self.assertEqual(
                value["comparison"]["classification"],
                "exact-casa-tt0-grid-hash-match-multiple-variants",
            )
            self.assertFalse(value["comparison"]["claims"]["production_tt0_promoted"])

    def test_baseline_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            candidate = candidate_evidence()
            candidate["variants"][0]["grid_hash"] = 7
            candidate["variants"][0]["matches_frozen_production_baseline"] = False
            write_envelope(
                fixture.candidate,
                envelope_schema=subject.CANDIDATE_ENVELOPE_SCHEMA,
                member="evidence",
                embedded=candidate,
            )
            with self.assertRaisesRegex(
                subject.ContractError, "did not reproduce frozen casa-rs v4"
            ):
                subject.build_comparison(
                    candidate_path=fixture.candidate,
                    casa_rs_v4_path=fixture.v4,
                    casa_v5_path=fixture.v5,
                    frozen=fixture.frozen,
                )

    def test_declared_result_must_match_variant_classification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            candidate = candidate_evidence(matching_ordinals=(3,))
            candidate["result"] = "completed-no-tested-variant-matched-casa"
            write_envelope(
                fixture.candidate,
                envelope_schema=subject.CANDIDATE_ENVELOPE_SCHEMA,
                member="evidence",
                embedded=candidate,
            )
            with self.assertRaisesRegex(
                subject.ContractError,
                "result does not match",
            ):
                subject.build_comparison(
                    candidate_path=fixture.candidate,
                    casa_rs_v4_path=fixture.v4,
                    casa_v5_path=fixture.v5,
                    frozen=fixture.frozen,
                )

    def test_correlation_mueller_role_order_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            candidate = candidate_evidence()
            candidate["correlation_mueller_role_order"]["roles"][0][
                "selected_corr_index"
            ] = 3
            write_envelope(
                fixture.candidate,
                envelope_schema=subject.CANDIDATE_ENVELOPE_SCHEMA,
                member="evidence",
                embedded=candidate,
            )
            with self.assertRaisesRegex(
                subject.ContractError,
                "selected_corr_index changed",
            ):
                subject.build_comparison(
                    candidate_path=fixture.candidate,
                    casa_rs_v4_path=fixture.v4,
                    casa_v5_path=fixture.v5,
                    frozen=fixture.frozen,
                )

    def test_nonfinite_variant_grid_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            candidate = candidate_evidence()
            candidate["variants"][2]["nonfinite_grid_value_count"] = 1
            candidate["counts"]["nonfinite_grid_value"] = 1
            write_envelope(
                fixture.candidate,
                envelope_schema=subject.CANDIDATE_ENVELOPE_SCHEMA,
                member="evidence",
                embedded=candidate,
            )
            with self.assertRaisesRegex(
                subject.ContractError,
                "nonfinite_grid_value",
            ):
                subject.build_comparison(
                    candidate_path=fixture.candidate,
                    casa_rs_v4_path=fixture.v4,
                    casa_v5_path=fixture.v5,
                    frozen=fixture.frozen,
                )

    def test_tampered_candidate_content_address_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            fixture.write_candidate()
            payload = fixture.candidate.read_text().replace(
                f'"grid_hash": {subject.CASA_RS_V4_TT0_GRID_HASH}',
                '"grid_hash": 7',
            )
            fixture.candidate.write_text(payload)
            with self.assertRaisesRegex(subject.ContractError, "digest mismatch"):
                subject.build_comparison(
                    candidate_path=fixture.candidate,
                    casa_rs_v4_path=fixture.v4,
                    casa_v5_path=fixture.v5,
                    frozen=fixture.frozen,
                )

    def test_tampered_parent_whole_file_hash_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ReceiptFixture(Path(temporary))
            fixture.write_candidate()
            fixture.v5.write_text(fixture.v5.read_text() + " ")
            with self.assertRaisesRegex(subject.ContractError, "whole-file"):
                subject.build_comparison(
                    candidate_path=fixture.candidate,
                    casa_rs_v4_path=fixture.v4,
                    casa_v5_path=fixture.v5,
                    frozen=fixture.frozen,
                )

    def test_atomic_writer_refuses_to_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "comparison.json"
            output.write_text("preserve me")
            with self.assertRaisesRegex(subject.ContractError, "overwrite"):
                subject.atomic_write_json(output, {"schema": "example"})
            self.assertEqual(output.read_text(), "preserve me")


if __name__ == "__main__":
    unittest.main()
