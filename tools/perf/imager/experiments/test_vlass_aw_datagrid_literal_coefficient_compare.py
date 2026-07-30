#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import vlass_aw_datagrid_literal_coefficient_compare as subject


def write_candidate_json(
    path: Path,
    embedded: str,
    *,
    extra_top_level: str = "",
    extra_content_address: str = "",
) -> str:
    digest = hashlib.sha256(embedded.encode()).hexdigest()
    path.write_text(
        "{\n"
        f"{extra_top_level}"
        f'  "schema": {json.dumps(subject.CANDIDATE_ENVELOPE_SCHEMA)},\n'
        '  "content_address": {\n'
        '    "algorithm": "sha256",\n'
        '    "scope": "embedded-evidence-json-utf8",\n'
        f'    "digest": "{digest}"{extra_content_address}\n'
        "  },\n"
        f'  "evidence": {embedded}\n'
        "}\n"
    )
    return digest


def write_candidate(path: Path, evidence: dict[str, object]) -> str:
    return write_candidate_json(
        path,
        json.dumps(evidence, indent=2, sort_keys=True),
    )


def first_mismatch() -> dict[str, object]:
    return {
        "source_ordinal": 12,
        "source_sample_index": 34,
        "pointing_group_index": 5,
        "logical_role": 1,
        "tap_bundle": 56,
        "tap_ordinal": 78,
        "iy": -1,
        "ix": 2,
        "grid_y": 2000,
        "grid_x": 2001,
        "cell": {
            "frequency_bits": 4_740_000_000_000_000_000,
            "w_bits": 4_620_000_000_000_000_000,
            "mueller": 15,
            "parallactic_angle_bits": 4_620_000_000_000_000_000,
        },
        "conjugate_for_grid": True,
        "raw_cf_bits": [1, 2],
        "post_w_sign_bits": [3, 4],
        "pointing_phase_bits": [5, 6],
        "literal_coefficient_bits": [7, 8],
        "packed_coefficient_bits": [9, 10],
    }


def candidate_evidence(
    result: str = "completed-literal-packed-exact-no-grid",
) -> dict[str, object]:
    mismatch_count = 0 if result == "completed-literal-packed-exact-no-grid" else 1
    grid_hash = {
        "completed-literal-packed-mismatch-grid-matches-casa": (
            subject.CASA_TT0_GRID_HASH
        ),
        "completed-literal-packed-mismatch-grid-matches-rust": (
            subject.CASA_RS_V4_TT0_GRID_HASH
        ),
        "completed-literal-packed-mismatch-grid-matches-neither": 123_456_789,
    }.get(result)
    conditional_grid = (
        None
        if grid_hash is None
        else {
            "allocated": True,
            "allocation_count": 1,
            "replay_count": 1,
            "grid_hash": grid_hash,
            "matches_frozen_rust": (grid_hash == subject.CASA_RS_V4_TT0_GRID_HASH),
            "matches_frozen_casa": grid_hash == subject.CASA_TT0_GRID_HASH,
            "grid_values_hashed": subject.GRID_VALUES,
            "grid_bytes": subject.GRID_BYTES,
            "nonfinite_grid_value_count": 0,
            "source_count": subject.SOURCE_COUNT,
            "logical_role_count": subject.ROLE_COUNT,
            "tap_count": subject.TAP_COUNT,
        }
    )
    literal_hash = 101
    packed_hash = literal_hash if mismatch_count == 0 else 102
    return {
        "schema": subject.CANDIDATE_EVIDENCE_SCHEMA,
        "status": "completed-controlled-stop",
        "result": result,
        "result_taxonomy": copy.deepcopy(subject.RESULT_TAXONOMY),
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "producer": "casa-rs",
        "diagnostic_hook_added": True,
        "normal_execution_behavior_changed": False,
        "production_science_arithmetic_changed": False,
        "production_dispatch": "not-entered",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "products": "not-entered",
        "tt1": False,
        "terms_evaluated": [0],
        "sumwt": "not-controlled",
        "cross_producer_reference": (
            "conditional-whole-TT0-grid-hash-only-from-frozen-CASA-v5"
        ),
        "coefficient_reference": (
            "source-exact-casa-6.7.5.18-replay-over-frozen-casa-rs-CF-pixels"
        ),
        "casa_source": copy.deepcopy(subject.EXPECTED_CASA_SOURCE),
        "literal_arithmetic": copy.deepcopy(subject.EXPECTED_LITERAL_ARITHMETIC),
        "phase_path": ("prephased-direct-projector-no-phase-table-no-tapless-replay"),
        "traversal_contract": subject.TRAVERSAL_CONTRACT,
        "expected_grid_nxy": 4096,
        "target_blocks": 1,
        "diagnostic_terms": 1,
        "request_nterms": 2,
        "replay_block_ordinal": 0,
        "replay_window_ordinal": 0,
        "last_window_in_replay_block": True,
        "frozen_parent_receipts": {
            "casa_rs_v4_sha256": subject.CASA_RS_V4_RECEIPT_SHA256,
            "casa_rs_v4_embedded_evidence_sha256": (subject.CASA_RS_V4_EVIDENCE_SHA256),
            "casa_rs_v4_revision": subject.CASA_RS_V4_REVISION,
            "casa_v5_sha256": subject.CASA_V5_RECEIPT_SHA256,
            "arithmetic_v1_sha256": subject.ARITHMETIC_V1_RECEIPT_SHA256,
            "arithmetic_v1_embedded_evidence_sha256": (
                subject.ARITHMETIC_V1_EVIDENCE_SHA256
            ),
            "arithmetic_v1_comparison_sha256": (
                subject.ARITHMETIC_V1_COMPARISON_SHA256
            ),
            "arithmetic_v1_comparison_embedded_evidence_sha256": (
                subject.ARITHMETIC_V1_COMPARISON_EVIDENCE_SHA256
            ),
            "arithmetic_v1_revision": subject.ARITHMETIC_V1_REVISION,
        },
        "frozen_grid_hashes": {
            "rust_tt0": subject.CASA_RS_V4_TT0_GRID_HASH,
            "casa_tt0": subject.CASA_TT0_GRID_HASH,
        },
        "selection": copy.deepcopy(subject.arithmetic.EXPECTED_SELECTION),
        "observed_first_buffer": copy.deepcopy(
            subject.arithmetic.EXPECTED_FIRST_BUFFER
        ),
        "absolute_main_rows": copy.deepcopy(subject.arithmetic.EXPECTED_ABSOLUTE_ROWS),
        "correlation_mueller_role_order": copy.deepcopy(subject.EXPECTED_ROLE_ORDER),
        "input_hashes": {
            "direct_raw": subject.arithmetic.DIRECT_COMPACT_INPUT_HASH,
            "compact": subject.arithmetic.DIRECT_COMPACT_INPUT_HASH,
            "direct_compact_exact_match": True,
            "portable_geometry": subject.arithmetic.PORTABLE_GEOMETRY_HASH,
            "portable_input": subject.arithmetic.PORTABLE_INPUT_HASH,
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
            "tap": subject.TAP_COUNT,
            "tap_request": 2_000,
            "unique_bundle": 1_234,
            "nonfinite_operand": 0,
            "out_of_grid_support_attempt": 0,
        },
        "memory": {
            "literal_operand_bytes": 2_000_000,
            "conditional_grid_bytes": (
                0 if conditional_grid is None else subject.GRID_BYTES
            ),
        },
        "ordered_hashes": {
            "contracts": copy.deepcopy(subject.ORDERED_HASH_CONTRACTS),
            "destination": 11,
            "selected_cell": 12,
            "raw_cf": 13,
            "post_w_sign": 14,
            "pointing_phase": 15,
            "literal_coefficient": literal_hash,
            "packed_coefficient": packed_hash,
        },
        "coefficient_comparison": {
            "mismatch_count": mismatch_count,
            "first_mismatch": None if mismatch_count == 0 else first_mismatch(),
        },
        "conditional_grid": conditional_grid,
        "traversal_hash": subject.TRAVERSAL_HASH,
    }


class LiteralCoefficientCandidateTests(unittest.TestCase):
    def validate(self, evidence: dict[str, object]) -> tuple[dict[str, object], str]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            digest = write_candidate(path, evidence)
            observed, _, observed_digest = subject.validate_candidate(path)
            return observed, observed_digest or digest

    def assert_rejected(self, evidence: dict[str, object], message: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            write_candidate(path, evidence)
            with self.assertRaisesRegex(
                (subject.ContractError, subject.arithmetic.ContractError),
                message,
            ):
                subject.validate_candidate(path)

    def assert_raw_rejected(
        self,
        embedded: str,
        message: str,
        *,
        extra_top_level: str = "",
        extra_content_address: str = "",
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            write_candidate_json(
                path,
                embedded,
                extra_top_level=extra_top_level,
                extra_content_address=extra_content_address,
            )
            with self.assertRaisesRegex(
                (subject.ContractError, subject.arithmetic.ContractError),
                message,
            ):
                subject.validate_candidate(path)

    def test_matching_internal_coefficients_skip_grid(self) -> None:
        evidence = candidate_evidence()
        observed, digest = self.validate(evidence)
        self.assertEqual(
            subject.classify_candidate(observed),
            "valid-negative-literal-coefficient-boundary-excluded",
        )
        self.assertEqual(
            digest,
            hashlib.sha256(
                json.dumps(evidence, indent=2, sort_keys=True).encode()
            ).hexdigest(),
        )

    def test_conditional_grid_results_have_distinct_classifications(self) -> None:
        cases = {
            "completed-literal-packed-mismatch-grid-matches-casa": (
                "candidate-localization-literal-grid-matches-frozen-casa"
            ),
            "completed-literal-packed-mismatch-grid-matches-rust": (
                "valid-negative-literal-grid-reproduces-frozen-rust"
            ),
            "completed-literal-packed-mismatch-grid-matches-neither": (
                "valid-unanchored-literal-grid-hard-stop"
            ),
        }
        for result, classification in cases.items():
            with self.subTest(result=result):
                observed, _ = self.validate(candidate_evidence(result))
                self.assertEqual(subject.classify_candidate(observed), classification)

    def test_grid_is_forbidden_when_coefficients_match(self) -> None:
        evidence = candidate_evidence()
        evidence["conditional_grid"] = {
            "allocated": True,
            "grid_hash": subject.CASA_TT0_GRID_HASH,
        }
        self.assert_rejected(evidence, "conditional_grid must be null")

    def test_grid_is_required_when_coefficients_differ(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-casa"
        )
        evidence["conditional_grid"] = None
        self.assert_rejected(evidence, "conditional_grid must be an object")

    def test_grid_flags_must_follow_whole_grid_hash(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-casa"
        )
        grid = evidence["conditional_grid"]
        assert isinstance(grid, dict)
        grid["matches_frozen_casa"] = False
        self.assert_rejected(evidence, "conditional_grid.matches_frozen_casa changed")

    def test_conditional_grid_is_allocated_and_replayed_exactly_once(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-casa"
        )
        grid = evidence["conditional_grid"]
        assert isinstance(grid, dict)
        grid["replay_count"] = 2
        self.assert_rejected(
            evidence,
            "conditional_grid.replay_count changed",
        )

    def test_result_must_follow_conditional_grid_hash(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-casa"
        )
        evidence["result"] = "completed-literal-packed-mismatch-grid-matches-rust"
        self.assert_rejected(evidence, "candidate result changed")

    def test_mismatch_count_requires_first_mismatch(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-neither"
        )
        comparison = evidence["coefficient_comparison"]
        assert isinstance(comparison, dict)
        comparison["first_mismatch"] = None
        self.assert_rejected(
            evidence, "coefficient_comparison.first_mismatch must be an object"
        )

    def test_first_mismatch_must_record_different_coefficient_bits(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-neither"
        )
        comparison = evidence["coefficient_comparison"]
        assert isinstance(comparison, dict)
        mismatch = comparison["first_mismatch"]
        assert isinstance(mismatch, dict)
        mismatch["packed_coefficient_bits"] = mismatch["literal_coefficient_bits"]
        self.assert_rejected(
            evidence,
            "first_mismatch literal and packed coefficient bits must differ",
        )

    def test_first_mismatch_requires_pointing_group_index(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-neither"
        )
        comparison = evidence["coefficient_comparison"]
        assert isinstance(comparison, dict)
        mismatch = comparison["first_mismatch"]
        assert isinstance(mismatch, dict)
        del mismatch["pointing_group_index"]
        self.assert_rejected(
            evidence,
            r"first_mismatch key set changed: missing=\['pointing_group_index'\]",
        )

    def test_matching_hashes_must_agree_with_mismatch_count(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-neither"
        )
        hashes = evidence["ordered_hashes"]
        assert isinstance(hashes, dict)
        hashes["packed_coefficient"] = hashes["literal_coefficient"]
        self.assert_rejected(
            evidence,
            "literal/packed ordered hashes disagree with mismatch_count",
        )

    def test_ordered_hashes_and_conditional_grid_hash_are_u64(self) -> None:
        ordered_fields = (
            "destination",
            "selected_cell",
            "raw_cf",
            "post_w_sign",
            "pointing_phase",
            "literal_coefficient",
            "packed_coefficient",
        )
        for field in ordered_fields:
            with self.subTest(ordered_hash=field):
                evidence = candidate_evidence()
                hashes = evidence["ordered_hashes"]
                assert isinstance(hashes, dict)
                hashes[field] = 1 << 64
                self.assert_rejected(
                    evidence,
                    rf"ordered_hashes\.{field} is not a u64",
                )

        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-neither"
        )
        grid = evidence["conditional_grid"]
        assert isinstance(grid, dict)
        grid["grid_hash"] = 1 << 64
        self.assert_rejected(
            evidence,
            r"conditional_grid\.grid_hash is not a u64",
        )

    def test_first_mismatch_tap_bundle_is_bounded_by_request_census(self) -> None:
        evidence = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-neither"
        )
        comparison = evidence["coefficient_comparison"]
        assert isinstance(comparison, dict)
        mismatch = comparison["first_mismatch"]
        assert isinstance(mismatch, dict)
        counts = evidence["counts"]
        assert isinstance(counts, dict)
        mismatch["tap_bundle"] = counts["tap_request"]
        self.assert_rejected(
            evidence,
            "first_mismatch.tap_bundle exceeds the audited tap-request census",
        )

    def test_conditional_grid_byte_count_is_biconditional(self) -> None:
        exact = candidate_evidence()
        exact_memory = exact["memory"]
        assert isinstance(exact_memory, dict)
        exact_memory["conditional_grid_bytes"] = subject.GRID_BYTES
        self.assert_rejected(
            exact,
            "conditional_grid_bytes must be zero when coefficients match",
        )

        mismatch = candidate_evidence(
            "completed-literal-packed-mismatch-grid-matches-neither"
        )
        mismatch_memory = mismatch["memory"]
        assert isinstance(mismatch_memory, dict)
        mismatch_memory["conditional_grid_bytes"] = 0
        self.assert_rejected(
            mismatch,
            "conditional_grid_bytes must equal one 4096-square complex64 grid",
        )

    def test_frozen_parent_identity_mutations_are_rejected(self) -> None:
        base = candidate_evidence()
        base_parents = base["frozen_parent_receipts"]
        assert isinstance(base_parents, dict)
        parent_fields = tuple(base_parents)
        for field in parent_fields:
            with self.subTest(parent=field):
                evidence = candidate_evidence()
                parents = evidence["frozen_parent_receipts"]
                assert isinstance(parents, dict)
                original = parents[field]
                assert isinstance(original, str)
                parents[field] = "0" * len(original)
                self.assert_rejected(
                    evidence,
                    "candidate.frozen_parent_receipts changed",
                )

    def test_exact_json_types_reject_boolean_count(self) -> None:
        evidence = candidate_evidence()
        counts = evidence["counts"]
        assert isinstance(counts, dict)
        counts["tap"] = True
        self.assert_rejected(evidence, "candidate counts.tap JSON type changed")

    def test_source_commit_and_runtime_cf_area_are_frozen(self) -> None:
        evidence = candidate_evidence()
        source = evidence["casa_source"]
        assert isinstance(source, dict)
        source["casacore_commit"] = "0" * 40
        self.assert_rejected(evidence, "casa_source changed")

        evidence = candidate_evidence()
        literal = evidence["literal_arithmetic"]
        assert isinstance(literal, dict)
        literal["runtime_cf_area_division"] = True
        self.assert_rejected(evidence, "literal_arithmetic changed")

    def test_embedded_content_address_is_checked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            write_candidate(path, candidate_evidence())
            payload = path.read_text().replace('"destination": 11', '"destination": 99')
            path.write_text(payload)
            with self.assertRaisesRegex(
                subject.ContractError,
                "content_address.digest changed",
            ):
                subject.validate_candidate(path)

    def test_duplicate_top_level_members_are_rejected(self) -> None:
        embedded = json.dumps(candidate_evidence(), indent=2, sort_keys=True)
        duplicates = {
            "schema": '  "schema": "duplicate",\n',
            "content_address": '  "content_address": {},\n',
            "evidence": '  "evidence": {},\n',
        }
        for member, extra in duplicates.items():
            with self.subTest(member=member):
                self.assert_raw_rejected(
                    embedded,
                    rf"duplicate JSON object key '{member}'",
                    extra_top_level=extra,
                )

    def test_duplicate_nested_member_is_rejected_with_fresh_digest(self) -> None:
        embedded = json.dumps(candidate_evidence(), indent=2, sort_keys=True)
        duplicate = embedded.replace(
            '"source": 12359,',
            '"source": 12359,\n    "source": 12359,',
            1,
        )
        self.assertNotEqual(embedded, duplicate)
        self.assert_raw_rejected(
            duplicate,
            r"duplicate JSON object key 'source'",
        )

    def test_nonstandard_json_constants_are_rejected(self) -> None:
        embedded = json.dumps(candidate_evidence(), indent=2, sort_keys=True)
        for constant in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(constant=constant):
                changed = embedded.replace(
                    '"destination": 11',
                    f'"destination": {constant}',
                    1,
                )
                self.assertNotEqual(embedded, changed)
                self.assert_raw_rejected(
                    changed,
                    f"nonstandard JSON constant '{constant}'",
                )

    def test_unknown_top_level_and_evidence_decoys_are_rejected(self) -> None:
        embedded = json.dumps(candidate_evidence(), indent=2, sort_keys=True)
        self.assert_raw_rejected(
            embedded,
            r"envelope key set changed: .*unexpected=\['decoy'\]",
            extra_top_level='  "decoy": true,\n',
        )

        evidence = candidate_evidence()
        evidence["decoy"] = {"looks": "plausible"}
        self.assert_rejected(
            evidence,
            r"evidence key set changed: .*unexpected=\['decoy'\]",
        )

    def test_unknown_dynamic_object_decoys_are_rejected(self) -> None:
        paths = [
            ("counts",),
            ("memory",),
            ("ordered_hashes",),
            ("coefficient_comparison",),
            ("coefficient_comparison", "first_mismatch"),
            ("coefficient_comparison", "first_mismatch", "cell"),
            ("conditional_grid",),
        ]
        result = "completed-literal-packed-mismatch-grid-matches-neither"
        for path in paths:
            with self.subTest(path=path):
                evidence = candidate_evidence(result)
                value: object = evidence
                for member in path:
                    assert isinstance(value, dict)
                    value = value[member]
                assert isinstance(value, dict)
                value["decoy"] = 1
                self.assert_rejected(
                    evidence,
                    r"key set changed: .*unexpected=\['decoy'\]",
                )

        embedded = json.dumps(candidate_evidence(), indent=2, sort_keys=True)
        self.assert_raw_rejected(
            embedded,
            r"content_address key set changed: .*unexpected=\['decoy'\]",
            extra_content_address=',\n    "decoy": true',
        )


class ComparisonTests(unittest.TestCase):
    def test_comparison_disclaims_cross_producer_stage_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "receipt.json"
            write_candidate(candidate, candidate_evidence())
            with mock.patch.object(
                subject, "validate_frozen_parents", return_value=None
            ):
                envelope = subject.build_comparison(
                    candidate_path=candidate,
                    casa_rs_v4_path=Path("/frozen/casa-rs-v4.json"),
                    casa_v5_path=Path("/frozen/casa-v5.json"),
                    arithmetic_v1_path=Path("/frozen/arithmetic-v1.json"),
                    arithmetic_v1_comparison_path=Path(
                        "/frozen/arithmetic-v1-comparison.json"
                    ),
                )
        comparison = envelope["comparison"]
        self.assertTrue(comparison["claims"]["valid_structural_classification"])
        self.assertTrue(comparison["claims"]["diagnostic_hook_added"])
        self.assertFalse(comparison["claims"]["normal_execution_behavior_changed"])
        self.assertFalse(comparison["claims"]["production_science_arithmetic_changed"])
        self.assertTrue(comparison["claims"]["valid_negative_boundary_excluded"])
        self.assertFalse(comparison["claims"]["production_promotion_authorized"])
        self.assertFalse(
            comparison["claims"]["ordered_coefficient_hashes_cross_producer"]
        )
        self.assertTrue(
            comparison["claims"]["whole_grid_hash_is_only_cross_producer_comparison"]
        )
        digest = hashlib.sha256(subject._canonical_json(comparison)).hexdigest()
        self.assertEqual(envelope["content_address"]["digest"], digest)

    def test_each_result_has_an_explicit_nonpromotion_disposition(self) -> None:
        cases = {
            "completed-literal-packed-exact-no-grid": (
                "boundary-excluded-continue-with-next-approved-localization",
                "valid_negative_boundary_excluded",
            ),
            "completed-literal-packed-mismatch-grid-matches-casa": (
                "candidate-localization-requires-integrated-promotion-gates",
                "candidate_localization_matches_frozen_casa",
            ),
            "completed-literal-packed-mismatch-grid-matches-rust": (
                "boundary-excluded-no-production-promotion",
                "valid_negative_reproduces_frozen_rust",
            ),
            "completed-literal-packed-mismatch-grid-matches-neither": (
                "unanchored-hard-stop-no-production-promotion",
                "unanchored_hard_stop",
            ),
        }
        semantic_claims = {
            "valid_negative_boundary_excluded",
            "valid_negative_reproduces_frozen_rust",
            "candidate_localization_matches_frozen_casa",
            "unanchored_hard_stop",
        }
        for result, (disposition, true_claim) in cases.items():
            with self.subTest(result=result):
                with tempfile.TemporaryDirectory() as directory:
                    candidate = Path(directory) / "receipt.json"
                    write_candidate(candidate, candidate_evidence(result))
                    with mock.patch.object(
                        subject,
                        "validate_frozen_parents",
                        return_value=None,
                    ):
                        envelope = subject.build_comparison(
                            candidate_path=candidate,
                            casa_rs_v4_path=Path("/frozen/casa-rs-v4.json"),
                            casa_v5_path=Path("/frozen/casa-v5.json"),
                            arithmetic_v1_path=Path("/frozen/arithmetic-v1.json"),
                            arithmetic_v1_comparison_path=Path(
                                "/frozen/arithmetic-v1-comparison.json"
                            ),
                        )
                comparison = envelope["comparison"]
                self.assertEqual(comparison["disposition"], disposition)
                self.assertFalse(
                    comparison["claims"]["production_promotion_authorized"]
                )
                for claim in semantic_claims:
                    self.assertEqual(
                        comparison["claims"][claim],
                        claim == true_claim,
                    )

    def test_atomic_output_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "comparison.json"
            subject.atomic_write_json(path, {"schema": "test"})
            with self.assertRaisesRegex(
                subject.ContractError, "refusing to overwrite comparison"
            ):
                subject.atomic_write_json(path, {"schema": "replacement"})


if __name__ == "__main__":
    unittest.main()
