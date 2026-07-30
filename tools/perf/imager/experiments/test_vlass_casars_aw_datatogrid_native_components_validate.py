#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

from contextlib import contextmanager
import copy
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock

import vlass_casars_aw_datatogrid_native_components_validate as subject


def f32_bits(value: float) -> int:
    return int.from_bytes(struct.pack("<f", value), "little")


def f64_bits(value: float) -> int:
    return int.from_bytes(struct.pack("<d", value), "little")


def fixture_header() -> dict[str, object]:
    return {
        "use_conjugate_frequency_cf": False,
        "begin_row": 0,
        "end_row": subject.ROW_COUNT,
        "n_row": subject.ROW_COUNT,
        "spw_id": 2,
        "im_ref_freq_bits": subject.IM_REF_FREQ_BITS,
        "grid_shape": copy.deepcopy(subject.GRID_SHAPE),
        "channel_map": [0] * subject.CHANNEL_COUNT,
        "polarization_map": [0, -1, -1, 0],
        "row_ids": list(range(subject.ROW_COUNT)),
        "frequency_bits": [
            f64_bits(2_000_000_000.0 + channel * 2_000_000.0)
            for channel in range(subject.CHANNEL_COUNT)
        ],
    }


def fixture_casa_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in range(subject.ROW_COUNT):
        flagged = row % 17 == 0
        rows.append(
            {
                "row": row,
                "row_flag": flagged,
                "uvw_bits": [
                    f64_bits(row + 0.125),
                    f64_bits(-row - 0.25),
                    f64_bits(row * 0.5 + 0.375),
                ],
                "dphase_bits": f64_bits(row / 1024.0),
                "flag_masks": [
                    (row + channel) & 0xF for channel in range(subject.CHANNEL_COUNT)
                ],
                "imaging_weight_bits": [
                    f32_bits(0.0)
                    if flagged or (row + channel) % 7 == 0
                    else f32_bits(0.25 + ((row + channel) % 11) / 8.0)
                    for channel in range(subject.CHANNEL_COUNT)
                ],
            }
        )
    return rows


def candidate_rows_from(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    candidate: list[dict[str, object]] = []
    for row in rows:
        uvw = list(row["uvw_bits"])
        candidate.append(
            {
                **copy.deepcopy(row),
                "admitted": [False] * subject.CHANNEL_COUNT,
                "auxiliary": {
                    "absolute_main_row": 353_600 + row["row"],
                    "raw_uvw_bits": list(uvw),
                    "gridft_density_uvw_bits": list(uvw),
                    "casa_rs_internal_uvw_bits": subject._negated_raw_uvw(uvw),
                    "negated_uv_transform_uvw_bits": list(uvw),
                    "first_parallel_hand_natural_weight_bits": [f32_bits(1.0)]
                    * subject.CHANNEL_COUNT,
                    "second_parallel_hand_natural_weight_bits": [f32_bits(1.0)]
                    * subject.CHANNEL_COUNT,
                    "collapsed_natural_weight_bits": [f32_bits(1.0)]
                    * subject.CHANNEL_COUNT,
                },
            }
        )
    return candidate


def write_envelope(
    path: Path,
    envelope_schema: str,
    evidence: dict[str, object],
) -> str:
    embedded = json.dumps(evidence, indent=2, sort_keys=True)
    digest = hashlib.sha256(embedded.encode()).hexdigest()
    path.write_text(
        "{\n"
        f'  "schema": {json.dumps(envelope_schema)},\n'
        '  "content_address": {\n'
        '    "algorithm": "sha256",\n'
        '    "scope": "embedded-evidence-json-utf8",\n'
        f'    "digest": "{digest}"\n'
        "  },\n"
        f'  "evidence": {embedded}\n'
        "}\n"
    )
    return digest


def casa_evidence(
    header: dict[str, object],
    rows: list[dict[str, object]],
) -> dict[str, object]:
    components, counts, calls, _, _ = subject.recompute(header, rows)
    return {
        "schema": subject.CASA_EVIDENCE_SCHEMA,
        "status": "completed-controlled-stop",
        "result": "completed-native-components-exact-frozen-v5",
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "producer": "CASA",
        "casa_version": "6.7.5.18",
        "casa_version_string": "6.7.5-18",
        "casa_source_commit": "418bb1a26df7c4aba663ff123b038b75a6fa0295",
        "casacore_source_commit": "25b653f6963a78a1dcfc8e16954081e091a50fbe",
        "datatogrid_symbol": "fixture-symbol",
        "symbol_owner": "libcasacpp_synthesis.6.dylib",
        "observed_dispatch": "first-dcomplex-non-psf",
        "diagnostic_hook_added": True,
        "normal_execution_behavior_changed": False,
        "production_science_arithmetic_changed": False,
        "original_datatogrid": "not-invoked",
        "grid_storage": "received-not-read-or-written",
        "grid_dispatch": "not-entered",
        "sumwt": "not-read-or-written",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "products": "not-entered",
        "completed_calls": 1,
        "terms_observed": [0],
        "hash_contracts": copy.deepcopy(subject.HASH_CONTRACT),
        "frozen_parent_receipts": {"fixture": True},
        "header": copy.deepcopy(header),
        "component_hashes": components,
        "counts": counts,
        "recomputed_frozen_hashes": subject._expected_claimed_calls(calls),
        "rows": copy.deepcopy(rows),
    }


def refresh_candidate(
    evidence: dict[str, object],
    frozen_components: dict[str, int],
    frozen_calls: list[dict[str, int]],
) -> None:
    header = evidence["header"]
    rows = evidence["rows"]
    assert isinstance(header, dict) and isinstance(rows, list)
    components, counts, calls, checkpoints, admission = subject.recompute(header, rows)
    for row, decisions in zip(rows, admission, strict=True):
        row["admitted"] = decisions
    evidence["component_hashes"] = components
    evidence["component_comparison"] = {
        name: {
            "actual": components[name],
            "expected_casa": frozen_components[name],
            "exact": components[name] == frozen_components[name],
        }
        for name in subject.COMPONENT_NAMES
    }
    evidence["mismatched_components"] = [
        name
        for name in subject.COMPONENT_NAMES
        if components[name] != frozen_components[name]
    ]
    evidence["counts"] = counts
    evidence["recomputed_frozen_hashes"] = subject._expected_claimed_calls(calls)
    evidence["row_checkpoints"] = checkpoints
    evidence["result"] = (
        "completed-native-components-exact-frozen-casa"
        if not evidence["mismatched_components"] and calls == frozen_calls
        else "completed-native-components-mismatch"
    )


def candidate_evidence(
    header: dict[str, object],
    rows: list[dict[str, object]],
    *,
    casa_sha: str,
    frozen_components: dict[str, int],
    frozen_calls: list[dict[str, int]],
) -> dict[str, object]:
    evidence: dict[str, object] = {
        "schema": subject.CANDIDATE_EVIDENCE_SCHEMA,
        "status": "completed-controlled-stop",
        "result": "pending-refresh",
        "result_taxonomy": [
            "completed-native-components-mismatch",
            "completed-native-components-exact-frozen-casa",
        ],
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "producer": "casa-rs",
        "diagnostic_hook_added": True,
        "normal_execution_behavior_changed": False,
        "production_science_arithmetic_changed": False,
        "density_pass": "diagnostic-only-completed-with-production-weighting-plan",
        "density_source_blocks": 32,
        "production_dispatch": "not-entered",
        "cf_cache": "not-opened",
        "cf_selection": "not-entered",
        "grid_storage": "not-allocated",
        "grid_dispatch": "not-entered",
        "sumwt": "not-entered",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "products": "not-entered",
        "completed_calls": 0,
        "terms_observed": [],
        "hash_contracts": copy.deepcopy(subject.CANDIDATE_HASH_CONTRACT),
        "frozen_parent_receipts": {
            "casa_native_components_v1": {
                "schema": subject.CASA_EVIDENCE_SCHEMA,
                "receipt_sha256": casa_sha,
            }
        },
        "header": copy.deepcopy(header),
        "component_hashes": {},
        "component_comparison": {},
        "mismatched_components": [],
        "counts": {},
        "recomputed_frozen_hashes": [],
        "row_checkpoints": [],
        "rows": copy.deepcopy(rows),
    }
    refresh_candidate(evidence, frozen_components, frozen_calls)
    return evidence


class NativeComponentsV5ValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.header = fixture_header()
        self.casa_rows = fixture_casa_rows()
        (
            self.casa_components,
            self.casa_counts,
            self.casa_calls,
            self.casa_checkpoints,
            self.casa_admission,
        ) = subject.recompute(self.header, self.casa_rows)
        self.casa_path = self.directory / "casa.json"
        write_envelope(
            self.casa_path,
            subject.CASA_ENVELOPE_SCHEMA,
            casa_evidence(self.header, self.casa_rows),
        )
        self.casa_sha = subject.sha256_path(self.casa_path)

    @contextmanager
    def frozen_targets(self):
        with mock.patch.multiple(
            subject,
            CASA_RECEIPT_SHA256=self.casa_sha,
            FROZEN_COMPONENT_HASHES=self.casa_components,
            FROZEN_COUNTS=self.casa_counts,
            FROZEN_CALLS=self.casa_calls,
        ):
            yield

    def evidence(
        self,
        rows: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        candidate_rows = candidate_rows_from(self.casa_rows) if rows is None else rows
        return candidate_evidence(
            self.header,
            candidate_rows,
            casa_sha=self.casa_sha,
            frozen_components=self.casa_components,
            frozen_calls=self.casa_calls,
        )

    def write_candidate(self, evidence: dict[str, object]) -> Path:
        path = self.directory / "candidate.json"
        path.unlink(missing_ok=True)
        write_envelope(path, subject.CANDIDATE_ENVELOPE_SCHEMA, evidence)
        return path

    def compare(self, evidence: dict[str, object]) -> dict[str, object]:
        candidate = self.write_candidate(evidence)
        with self.frozen_targets():
            return subject.build_comparison(
                candidate_path=candidate,
                casa_path=self.casa_path,
            )

    def assert_candidate_rejected(
        self, evidence: dict[str, object], message: str
    ) -> None:
        candidate = self.write_candidate(evidence)
        with (
            self.frozen_targets(),
            self.assertRaisesRegex(subject.ContractError, message),
        ):
            subject.validate_candidate(candidate)

    def test_exact_receipt_recomputes_all_components_slots_and_checkpoints(
        self,
    ) -> None:
        envelope = self.compare(self.evidence())
        comparison = envelope["comparison"]
        self.assertEqual(
            comparison["classification"], "exact-frozen-casa-native-components"
        )
        self.assertEqual(comparison["scope"]["raw_slots_compared"], 20_800)
        self.assertEqual(comparison["row_checkpoints"]["count"], 325)
        self.assertEqual(len(comparison["row_checkpoints"]["comparison"]), 325)
        self.assertTrue(
            all(
                component["exact"]
                for component in comparison["component_comparison"].values()
            )
        )

    def test_valid_uvw_mismatch_reports_owner_and_first_row(self) -> None:
        evidence = self.evidence()
        row = evidence["rows"][9]
        row["uvw_bits"][1] ^= 1
        row["auxiliary"]["negated_uv_transform_uvw_bits"] = list(row["uvw_bits"])
        row["auxiliary"]["casa_rs_internal_uvw_bits"] = subject._negated_raw_uvw(
            row["uvw_bits"]
        )
        refresh_candidate(evidence, self.casa_components, self.casa_calls)
        comparison = self.compare(evidence)["comparison"]
        self.assertEqual(
            comparison["classification"], "valid-native-component-mismatch"
        )
        uvw = comparison["component_comparison"]["uvw_dphase"]
        self.assertEqual(uvw["owner"], "uvw-reprojection-and-phase-rotation")
        self.assertEqual(uvw["first_difference"]["row"], 9)
        self.assertIn(uvw["first_difference"]["axis"], {"u", "v", "w"})

    def test_matching_admission_counts_do_not_prove_membership(self) -> None:
        evidence = self.evidence()
        rows = evidence["rows"]
        admitted_slot = None
        rejected_slot = None
        for row_index, row in enumerate(rows):
            if row["row_flag"]:
                continue
            for channel, bits in enumerate(row["imaging_weight_bits"]):
                if subject._weight_nonzero(bits) and admitted_slot is None:
                    admitted_slot = (row_index, channel)
                if not subject._weight_nonzero(bits) and rejected_slot is None:
                    rejected_slot = (row_index, channel)
        assert admitted_slot is not None and rejected_slot is not None
        left = rows[admitted_slot[0]]["imaging_weight_bits"]
        right = rows[rejected_slot[0]]["imaging_weight_bits"]
        left[admitted_slot[1]], right[rejected_slot[1]] = (
            right[rejected_slot[1]],
            left[admitted_slot[1]],
        )
        refresh_candidate(evidence, self.casa_components, self.casa_calls)
        comparison = self.compare(evidence)["comparison"]
        self.assertTrue(comparison["admission"]["count_exact"])
        self.assertFalse(comparison["admission"]["membership_exact"])
        self.assertFalse(comparison["component_comparison"]["admission"]["exact"])
        self.assertIsNotNone(comparison["admission"]["first_difference"])

    def test_any_pol_broadcast_hypothesis_is_evaluated_independently(self) -> None:
        broadcast_casa_rows = copy.deepcopy(self.casa_rows)
        for row in broadcast_casa_rows:
            row["flag_masks"] = [15 if mask else 0 for mask in row["flag_masks"]]
        self.casa_rows = broadcast_casa_rows
        (
            self.casa_components,
            self.casa_counts,
            self.casa_calls,
            self.casa_checkpoints,
            self.casa_admission,
        ) = subject.recompute(self.header, self.casa_rows)
        write_envelope(
            self.casa_path,
            subject.CASA_ENVELOPE_SCHEMA,
            casa_evidence(self.header, self.casa_rows),
        )
        self.casa_sha = subject.sha256_path(self.casa_path)
        raw_rows = fixture_casa_rows()
        evidence = self.evidence(candidate_rows_from(raw_rows))
        comparison = self.compare(evidence)["comparison"]
        self.assertFalse(
            comparison["flag_hypotheses"]["internal_four_polarization_masks"][
                "component_exact"
            ]["flag_masks"]
        )
        self.assertTrue(
            comparison["flag_hypotheses"]["casa_any_polarization_broadcast"][
                "component_exact"
            ]["flag_masks"]
        )

    def test_raw_negated_raw_and_internal_uvw_hypotheses_are_distinct(
        self,
    ) -> None:
        raw_rows = fixture_casa_rows()
        negated_casa_rows = copy.deepcopy(raw_rows)
        for row in negated_casa_rows:
            row["uvw_bits"] = subject._negated_raw_uvw(row["uvw_bits"])
        self.casa_rows = negated_casa_rows
        (
            self.casa_components,
            self.casa_counts,
            self.casa_calls,
            self.casa_checkpoints,
            self.casa_admission,
        ) = subject.recompute(self.header, self.casa_rows)
        write_envelope(
            self.casa_path,
            subject.CASA_ENVELOPE_SCHEMA,
            casa_evidence(self.header, self.casa_rows),
        )
        self.casa_sha = subject.sha256_path(self.casa_path)
        evidence = self.evidence(candidate_rows_from(raw_rows))
        comparison = self.compare(evidence)["comparison"]
        self.assertFalse(
            comparison["uvw_hypotheses"]["published_casa_convention"][
                "component_exact"
            ]["uvw_dphase"]
        )
        self.assertFalse(
            comparison["uvw_hypotheses"]["raw"]["component_exact"]["uvw_dphase"]
        )
        self.assertTrue(
            comparison["uvw_hypotheses"]["negated_raw"]["component_exact"]["uvw_dphase"]
        )
        self.assertTrue(
            comparison["uvw_hypotheses"]["casa_rs_internal"]["component_exact"][
                "uvw_dphase"
            ]
        )
        self.assertFalse(
            comparison["uvw_hypotheses"]["negated_internal"]["component_exact"][
                "uvw_dphase"
            ]
        )

    def test_component_claim_tampering_is_rejected(self) -> None:
        evidence = self.evidence()
        evidence["component_hashes"]["imaging_weights"] ^= 1
        self.assert_candidate_rejected(evidence, "component hashes")

    def test_admission_membership_claim_tampering_is_rejected(self) -> None:
        evidence = self.evidence()
        evidence["rows"][1]["admitted"][1] ^= True
        self.assert_candidate_rejected(evidence, "exact admission membership")

    def test_negated_internal_auxiliary_tampering_is_rejected(self) -> None:
        evidence = self.evidence()
        evidence["rows"][3]["auxiliary"]["negated_uv_transform_uvw_bits"][0] ^= 1
        self.assert_candidate_rejected(evidence, "published CASA-convention UVW")

    def test_row_checkpoint_tampering_is_rejected(self) -> None:
        evidence = self.evidence()
        evidence["row_checkpoints"][100]["stream_hash"] ^= 1
        self.assert_candidate_rejected(evidence, "row checkpoints")

    def test_valid_mismatch_does_not_require_exact_counts(self) -> None:
        evidence = self.evidence()
        row = evidence["rows"][1]
        row["imaging_weight_bits"][1] = f32_bits(0.0)
        refresh_candidate(evidence, self.casa_components, self.casa_calls)
        comparison = self.compare(evidence)["comparison"]
        self.assertEqual(
            comparison["classification"], "valid-native-component-mismatch"
        )
        self.assertFalse(comparison["counts"]["exact"])

    def test_raw_embedded_digest_mutation_is_rejected(self) -> None:
        candidate = self.write_candidate(self.evidence())
        candidate.write_text(
            candidate.read_text().replace('"status":', '"status" :', 1)
        )
        with (
            self.frozen_targets(),
            self.assertRaisesRegex(subject.ContractError, "content_address.digest"),
        ):
            subject.validate_candidate(candidate)

    def test_shape_and_exact_key_contracts_are_enforced(self) -> None:
        evidence = self.evidence()
        evidence["rows"][0]["flag_masks"].pop()
        self.assert_candidate_rejected(evidence, "must contain 64")
        evidence = self.evidence()
        evidence["rows"][0]["auxiliary"]["unexpected"] = 1
        self.assert_candidate_rejected(evidence, "key set changed")

    def test_comparison_publication_never_clobbers(self) -> None:
        output = (self.directory / "comparison.json").resolve()
        subject.atomic_write_json(output, {"result": "first"})
        before = output.read_bytes()
        with self.assertRaisesRegex(subject.ContractError, "refusing to overwrite"):
            subject.atomic_write_json(output, {"result": "second"})
        self.assertEqual(output.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
