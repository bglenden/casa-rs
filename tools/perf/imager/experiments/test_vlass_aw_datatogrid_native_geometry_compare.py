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

import vlass_aw_datatogrid_native_geometry_compare as subject


def hypothesis(
    use_conjugate_frequency_cf: bool,
    *,
    exact_stream: bool,
    exact_geometry: bool,
    source_count: int = subject.SOURCE_COUNT,
) -> dict[str, object]:
    calls: list[dict[str, object]] = []
    for ordinal, expected in enumerate(subject.EXPECTED_CASA_CALLS):
        stream_hash = (
            expected["stream_hash"]
            if exact_stream
            else expected["stream_hash"] + 10 + ordinal
        )
        geometry_hash = (
            expected["geometry_hash"]
            if exact_geometry
            else expected["geometry_hash"] + 10 + ordinal
        )
        calls.append(
            {
                "call": ordinal,
                "block": 0,
                "term": ordinal,
                "source_count": source_count,
                "stream_hash": stream_hash,
                "geometry_hash": geometry_hash,
            }
        )
    return {
        "use_conjugate_frequency_cf": use_conjugate_frequency_cf,
        "calls": calls,
    }


def candidate_evidence(
    result: str = "completed-native-stream-and-geometry-exact",
    *,
    exact_hypothesis: bool = False,
) -> dict[str, object]:
    if result == "completed-source-count-mismatch":
        hypotheses = [
            hypothesis(False, exact_stream=False, exact_geometry=False, source_count=7),
            hypothesis(True, exact_stream=False, exact_geometry=False, source_count=7),
        ]
    elif result == "completed-native-stream-mismatch":
        hypotheses = [
            hypothesis(False, exact_stream=False, exact_geometry=False),
            hypothesis(True, exact_stream=False, exact_geometry=False),
        ]
    elif result == "completed-native-stream-exact-geometry-mismatch":
        matching = hypothesis(
            exact_hypothesis,
            exact_stream=True,
            exact_geometry=False,
        )
        other = hypothesis(
            not exact_hypothesis,
            exact_stream=False,
            exact_geometry=False,
        )
        hypotheses = [matching, other] if not exact_hypothesis else [other, matching]
    elif result == "completed-native-stream-and-geometry-exact":
        matching = hypothesis(
            exact_hypothesis,
            exact_stream=True,
            exact_geometry=True,
        )
        other = hypothesis(
            not exact_hypothesis,
            exact_stream=False,
            exact_geometry=False,
        )
        hypotheses = [matching, other] if not exact_hypothesis else [other, matching]
    else:
        raise AssertionError(f"unsupported fixture result {result}")
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
        "cf_selection": "not-entered",
        "placement": "not-entered",
        "tap_processing": "not-entered",
        "grid_dispatch": "not-entered",
        "sumwt": "not-entered",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "products": "not-entered",
        "terms_evaluated": [0, 1],
        "hash_reference": subject.HASH_REFERENCE,
        "hash_contracts": copy.deepcopy(subject.HASH_CONTRACTS),
        "uvw_hypothesis": subject.UVW_HYPOTHESIS,
        "expected_grid_shape": copy.deepcopy(subject.EXPECTED_GRID_SHAPE),
        "target_blocks": 1,
        "request_nterms": 2,
        "selection": copy.deepcopy(subject.arithmetic.EXPECTED_SELECTION),
        "observed_first_buffer": copy.deepcopy(
            subject.arithmetic.EXPECTED_FIRST_BUFFER
        ),
        "absolute_main_rows": copy.deepcopy(subject.arithmetic.EXPECTED_ABSOLUTE_ROWS),
        "im_ref_freq_bits": subject.IM_REF_FREQ_BITS,
        "frozen_parent_receipts": copy.deepcopy(subject.FROZEN_PARENT_RECEIPTS),
        "hypotheses": hypotheses,
    }


def write_candidate(path: Path, evidence: dict[str, object]) -> str:
    evidence_json = json.dumps(evidence, indent=2, sort_keys=True)
    digest = hashlib.sha256(evidence_json.encode()).hexdigest()
    path.write_text(
        "{\n"
        f'  "schema": {json.dumps(subject.CANDIDATE_ENVELOPE_SCHEMA)},\n'
        '  "content_address": {\n'
        '    "algorithm": "sha256",\n'
        '    "scope": "embedded-evidence-json-utf8",\n'
        f'    "digest": "{digest}"\n'
        "  },\n"
        f'  "evidence": {evidence_json}\n'
        "}\n"
    )
    return digest


def write_raw_candidate(
    path: Path,
    embedded: str,
    *,
    extra_top_level: str = "",
    extra_content_address: str = "",
) -> None:
    digest = hashlib.sha256(embedded.encode()).hexdigest()
    path.write_text(
        "{\n"
        f'  "schema": {json.dumps(subject.CANDIDATE_ENVELOPE_SCHEMA)},\n'
        f"{extra_top_level}"
        '  "content_address": {\n'
        '    "algorithm": "sha256",\n'
        '    "scope": "embedded-evidence-json-utf8",\n'
        f'    "digest": "{digest}"{extra_content_address}\n'
        "  },\n"
        f'  "evidence": {embedded}\n'
        "}\n"
    )


class CandidateTests(unittest.TestCase):
    def validate(
        self,
        evidence: dict[str, object],
    ) -> tuple[dict[str, object], bool | None, list[dict[str, object]]]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            write_candidate(path, evidence)
            observed, _, _, selected, matches = subject.validate_candidate(path)
            return observed, selected, matches

    def assert_rejected(self, evidence: dict[str, object], message: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            write_candidate(path, evidence)
            with self.assertRaisesRegex(
                (
                    subject.ContractError,
                    subject.arithmetic.ContractError,
                    subject.literal.ContractError,
                ),
                message,
            ):
                subject.validate_candidate(path)

    def test_all_four_results_are_independently_classified(self) -> None:
        for result in subject.RESULT_TAXONOMY:
            with self.subTest(result=result):
                observed, selected, _ = self.validate(candidate_evidence(result))
                self.assertEqual(observed["result"], result)
                expected_selected = (
                    False
                    if result
                    in {
                        "completed-native-stream-exact-geometry-mismatch",
                        "completed-native-stream-and-geometry-exact",
                    }
                    else None
                )
                self.assertEqual(selected, expected_selected)

    def test_true_conjugate_frequency_hypothesis_can_match(self) -> None:
        evidence = candidate_evidence(
            "completed-native-stream-and-geometry-exact",
            exact_hypothesis=True,
        )
        _, selected, matches = self.validate(evidence)
        self.assertIs(selected, True)
        self.assertFalse(matches[0]["stream_exact"])
        self.assertTrue(matches[1]["geometry_exact"])

    def test_result_must_follow_deepest_complete_hypothesis_prefix(self) -> None:
        evidence = candidate_evidence("completed-native-stream-mismatch")
        evidence["result"] = "completed-native-stream-and-geometry-exact"
        self.assert_rejected(evidence, "candidate.result changed")

    def test_both_terms_must_match_within_one_hypothesis(self) -> None:
        evidence = candidate_evidence("completed-native-stream-mismatch")
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list)
        first = hypotheses[0]
        second = hypotheses[1]
        assert isinstance(first, dict) and isinstance(second, dict)
        first_calls = first["calls"]
        second_calls = second["calls"]
        assert isinstance(first_calls, list) and isinstance(second_calls, list)
        assert isinstance(first_calls[0], dict) and isinstance(second_calls[1], dict)
        first_calls[0]["stream_hash"] = subject.EXPECTED_CASA_CALLS[0]["stream_hash"]
        second_calls[1]["stream_hash"] = subject.EXPECTED_CASA_CALLS[1]["stream_hash"]
        self.validate(evidence)

    def test_multiple_full_stream_matches_are_ambiguous(self) -> None:
        evidence = candidate_evidence(
            "completed-native-stream-and-geometry-exact",
        )
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list)
        for hypothesis_value in hypotheses:
            assert isinstance(hypothesis_value, dict)
            calls = hypothesis_value["calls"]
            assert isinstance(calls, list)
            for call, expected in zip(calls, subject.EXPECTED_CASA_CALLS, strict=True):
                assert isinstance(call, dict)
                call["stream_hash"] = expected["stream_hash"]
        self.assert_rejected(evidence, "multiple conjugate-frequency hypotheses")

    def test_source_count_must_not_change_between_hypotheses(self) -> None:
        evidence = candidate_evidence("completed-source-count-mismatch")
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list)
        second = hypotheses[1]
        assert isinstance(second, dict)
        calls = second["calls"]
        assert (
            isinstance(calls, list)
            and isinstance(calls[0], dict)
            and isinstance(calls[1], dict)
        )
        calls[0]["source_count"] = 8
        calls[1]["source_count"] = 8
        self.assert_rejected(evidence, "source_count changed between")

    def test_source_count_must_not_change_between_terms_in_each_hypothesis(
        self,
    ) -> None:
        evidence = candidate_evidence("completed-source-count-mismatch")
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list)
        for hypothesis_value in hypotheses:
            assert isinstance(hypothesis_value, dict)
            calls = hypothesis_value["calls"]
            assert isinstance(calls, list) and isinstance(calls[1], dict)
            calls[1]["source_count"] = 8
        self.assert_rejected(evidence, "different TT0/TT1 source counts")

    def test_hypothesis_and_call_order_are_exact(self) -> None:
        evidence = candidate_evidence()
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list)
        hypotheses.reverse()
        self.assert_rejected(evidence, "use_conjugate_frequency_cf changed")

        evidence = candidate_evidence()
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list) and isinstance(hypotheses[0], dict)
        calls = hypotheses[0]["calls"]
        assert isinstance(calls, list)
        calls.reverse()
        self.assert_rejected(evidence, r"calls\[0\]\.call changed")

    def test_u64_and_exact_json_types_are_enforced(self) -> None:
        evidence = candidate_evidence()
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list) and isinstance(hypotheses[0], dict)
        calls = hypotheses[0]["calls"]
        assert isinstance(calls, list) and isinstance(calls[0], dict)
        calls[0]["stream_hash"] = 1 << 64
        self.assert_rejected(evidence, "stream_hash is not a u64")

        evidence = candidate_evidence()
        evidence["target_blocks"] = True
        self.assert_rejected(evidence, "target_blocks JSON type changed")

    def test_static_scope_and_parent_bindings_are_exact(self) -> None:
        mutations = {
            "im_ref_freq_bits": subject.IM_REF_FREQ_BITS + 1,
            "placement": "entered",
            "expected_grid_shape": [8192, 8192, 1, 1],
            "uvw_hypothesis": "casa-awproject-direct-internal-uvw",
        }
        for field, value in mutations.items():
            with self.subTest(field=field):
                evidence = candidate_evidence()
                evidence[field] = value
                self.assert_rejected(evidence, f"{field} changed")

        evidence = candidate_evidence()
        parents = evidence["frozen_parent_receipts"]
        assert isinstance(parents, dict)
        parents["casa_v5_sha256"] = "0" * 64
        self.assert_rejected(evidence, "frozen_parent_receipts changed")

    def test_v2_contract_cannot_be_satisfied_by_v1_receipt(self) -> None:
        evidence = candidate_evidence()
        evidence["schema"] = subject.NATIVE_GEOMETRY_V1_EVIDENCE_SCHEMA
        self.assert_rejected(evidence, "evidence.schema changed")

    def test_uvw_hypothesis_is_bound_in_scope_and_hash_contract(self) -> None:
        evidence = candidate_evidence()
        contracts = evidence["hash_contracts"]
        assert isinstance(contracts, dict)
        contracts["uvw_hypothesis"] = "unspecified"
        self.assert_rejected(evidence, "hash_contracts changed")

        evidence = candidate_evidence()
        del evidence["uvw_hypothesis"]
        self.assert_rejected(evidence, "evidence key set changed")

    def test_unknown_fields_are_rejected_at_each_dynamic_level(self) -> None:
        evidence = candidate_evidence()
        evidence["decoy"] = 1
        self.assert_rejected(evidence, "evidence key set changed")

        evidence = candidate_evidence()
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list) and isinstance(hypotheses[0], dict)
        hypotheses[0]["decoy"] = 1
        self.assert_rejected(evidence, r"hypotheses\[0\] key set changed")

        evidence = candidate_evidence()
        hypotheses = evidence["hypotheses"]
        assert isinstance(hypotheses, list) and isinstance(hypotheses[0], dict)
        calls = hypotheses[0]["calls"]
        assert isinstance(calls, list) and isinstance(calls[0], dict)
        calls[0]["decoy"] = 1
        self.assert_rejected(evidence, r"calls\[0\] key set changed")

    def test_content_address_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            write_candidate(path, candidate_evidence())
            path.write_text(
                path.read_text().replace(
                    f'"im_ref_freq_bits": {subject.IM_REF_FREQ_BITS}',
                    '"im_ref_freq_bits": 7',
                )
            )
            with self.assertRaisesRegex(subject.ContractError, "digest changed"):
                subject.validate_candidate(path)

    def test_duplicate_keys_and_nonstandard_constants_are_rejected(self) -> None:
        embedded = json.dumps(candidate_evidence(), indent=2, sort_keys=True)
        duplicate = embedded.replace(
            '"source_count": 12359,',
            '"source_count": 12359,\n          "source_count": 12359,',
            1,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "duplicate.json"
            write_raw_candidate(path, duplicate)
            with self.assertRaisesRegex(subject.ContractError, "duplicate JSON"):
                subject.validate_candidate(path)

            nonstandard = embedded.replace(
                f'"im_ref_freq_bits": {subject.IM_REF_FREQ_BITS}',
                '"im_ref_freq_bits": NaN',
            )
            path = Path(directory) / "nan.json"
            write_raw_candidate(path, nonstandard)
            with self.assertRaisesRegex(subject.ContractError, "nonstandard JSON"):
                subject.validate_candidate(path)

    def test_unknown_envelope_and_content_address_fields_are_rejected(self) -> None:
        embedded = json.dumps(candidate_evidence(), indent=2, sort_keys=True)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "top.json"
            write_raw_candidate(path, embedded, extra_top_level='  "decoy": true,\n')
            with self.assertRaisesRegex(subject.ContractError, "envelope key set"):
                subject.validate_candidate(path)

            path = Path(directory) / "address.json"
            write_raw_candidate(
                path,
                embedded,
                extra_content_address=',\n    "decoy": true',
            )
            with self.assertRaisesRegex(
                subject.ContractError,
                "content_address key set",
            ):
                subject.validate_candidate(path)


class ComparisonTests(unittest.TestCase):
    def build(self, result: str) -> dict[str, object]:
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.json"
            write_candidate(candidate, candidate_evidence(result))
            with mock.patch.object(
                subject,
                "validate_frozen_parents",
                return_value={},
            ):
                return subject.build_comparison(
                    candidate_path=candidate,
                    casa_rs_v4_path=Path("/frozen/casa-rs-v4.json"),
                    casa_v5_path=Path("/frozen/casa-v5.json"),
                    arithmetic_v1_path=Path("/frozen/arithmetic-v1.json"),
                    arithmetic_v1_comparison_path=Path(
                        "/frozen/arithmetic-v1-comparison.json"
                    ),
                    literal_v1_path=Path("/frozen/literal-v1.json"),
                    literal_v1_comparison_path=Path(
                        "/frozen/literal-v1-comparison.json"
                    ),
                    native_geometry_v1_path=Path("/frozen/native-geometry-v1.json"),
                    native_geometry_v1_comparison_path=Path(
                        "/frozen/native-geometry-v1-comparison.json"
                    ),
                    native_geometry_v1_provenance_path=Path(
                        "/frozen/native-geometry-v1-provenance.tsv"
                    ),
                )

    def test_classifications_and_dispositions_are_distinct(self) -> None:
        for result in subject.RESULT_TAXONOMY:
            with self.subTest(result=result):
                envelope = self.build(result)
                comparison = envelope["comparison"]
                self.assertEqual(
                    comparison["classification"],
                    subject.CLASSIFICATIONS[result],
                )
                self.assertEqual(
                    comparison["disposition"],
                    subject.DISPOSITIONS[result],
                )

    def test_stream_mismatch_leaves_girar_refocus_unresolved_not_defective(
        self,
    ) -> None:
        envelope = self.build("completed-native-stream-mismatch")
        comparison = envelope["comparison"]
        self.assertEqual(
            comparison["disposition"],
            (
                "stop-resolve-actual-casa-girar-refocus-uvw-dphase-bits-or-"
                "flags-no-production-defect-identified"
            ),
        )
        self.assertFalse(
            comparison["claims"][
                "actual_casa_girar_refocus_uvw_dphase_and_flags_equivalence_proven"
            ]
        )
        self.assertFalse(comparison["claims"]["production_defect_identified"])

    def test_exact_geometry_still_denies_downstream_claims(self) -> None:
        envelope = self.build("completed-native-stream-and-geometry-exact")
        comparison = envelope["comparison"]
        self.assertEqual(
            comparison["scope"]["uvw_hypothesis"],
            subject.UVW_HYPOTHESIS,
        )
        self.assertEqual(
            comparison["scope"]["phase_hypothesis"],
            subject.PHASE_HYPOTHESIS,
        )
        self.assertEqual(
            comparison["parents"]["native_geometry_v1"]["sha256"],
            subject.NATIVE_GEOMETRY_V1_RECEIPT_SHA256,
        )
        claims = comparison["claims"]
        self.assertTrue(claims["source_count_exact"])
        self.assertTrue(claims["hypothesis_stream_exact"])
        self.assertTrue(claims["hypothesis_geometry_exact"])
        for claim in (
            "native_input_payload_exact",
            "cf_selection_equivalence_proven",
            "placement_equivalence_proven",
            "tap_stream_equivalence_proven",
            "whole_grid_equivalence_proven",
            "actual_casa_girar_refocus_uvw_dphase_and_flags_equivalence_proven",
            "production_defect_identified",
            "production_path_changed",
            "production_promotion_authorized",
            "integrated_4096_row_promoted",
            "performance_evidence",
        ):
            self.assertFalse(claims[claim], claim)
        digest = hashlib.sha256(subject._canonical_json(comparison)).hexdigest()
        self.assertEqual(envelope["content_address"]["digest"], digest)

    def test_atomic_output_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "comparison.json"
            subject.atomic_write_json(path, {"schema": "first"})
            with self.assertRaisesRegex(subject.ContractError, "overwrite"):
                subject.atomic_write_json(path, {"schema": "replacement"})
            self.assertEqual(json.loads(path.read_text()), {"schema": "first"})


if __name__ == "__main__":
    unittest.main()
