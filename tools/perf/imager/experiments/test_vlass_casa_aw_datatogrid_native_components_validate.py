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

import vlass_casa_aw_datatogrid_native_components_validate as subject


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
            f64_bits(1_900_000_000.0 + 2_000_000.0 * channel)
            for channel in range(subject.CHANNEL_COUNT)
        ],
    }


def fixture_rows() -> list[dict[str, object]]:
    remaining_sources = subject.SOURCE_COUNT
    rows: list[dict[str, object]] = []
    for row in range(subject.ROW_COUNT):
        flagged = row < 48
        weights: list[int] = []
        for _channel in range(subject.CHANNEL_COUNT):
            admitted = not flagged and remaining_sources > 0
            weights.append(f32_bits(1.0) if admitted else f32_bits(0.0))
            if admitted:
                remaining_sources -= 1
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
                "imaging_weight_bits": weights,
            }
        )
    if remaining_sources != 0:
        raise AssertionError("fixture could not place the frozen source count")
    return rows


def frozen_first_vb(
    header: dict[str, object],
    components: dict[str, int],
    counts: dict[str, int],
) -> dict[str, object]:
    frequencies = header["frequency_bits"]
    assert isinstance(frequencies, list)
    return {
        "begin_row": 0,
        "end_row": subject.ROW_COUNT,
        "n_row": subject.ROW_COUNT,
        "spw_id": 2,
        "row_ids_count": subject.ROW_COUNT,
        "row_ids_hash": components["row_ids"],
        "row_id_first": 0,
        "row_id_last": subject.ROW_COUNT - 1,
        "row_ids": list(range(subject.ROW_COUNT)),
        "n_data_chan": subject.CHANNEL_COUNT,
        "n_data_pol": subject.POLARIZATION_COUNT,
        "chan_map_count": subject.CHANNEL_COUNT,
        "chan_map_hash": components["channel_map"],
        "pol_map_count": subject.POLARIZATION_COUNT,
        "pol_map_hash": components["polarization_map"],
        "freq_count": subject.CHANNEL_COUNT,
        "freq_hash": components["frequencies"],
        "freq_first_bits": frequencies[0],
        "freq_last_bits": frequencies[-1],
        "row_flags_count": subject.ROW_COUNT,
        "row_flags_hash": components["row_flags"],
        "flagged_rows": counts["flagged_rows"],
    }


def frozen_calls(calls: list[dict[str, int]]) -> list[dict[str, int]]:
    input_hashes = [149_759_612_434_800_605, 6_114_644_945_970_596_433]
    return [
        {**call, "input_hash": input_hashes[ordinal]}
        for ordinal, call in enumerate(calls)
    ]


def frozen_boundaries(calls: list[dict[str, int]]) -> list[dict[str, object]]:
    return [
        {
            "block": 0,
            "stream_hash": calls[0]["stream_hash"],
            "input_stream_hash": 9_670_878_879_986_980_654,
            "terms": [
                {
                    "term": 0,
                    "grid_hash": 9_328_098_071_914_194_885,
                    "sumwt_hash": 5_773_668_711_911_205_477,
                    "grid_values_hashed": 16_777_216,
                    "sumwt_values_hashed": 1,
                },
                {
                    "term": 1,
                    "grid_hash": 9_296_706_034_202_754_823,
                    "sumwt_hash": 6_979_414_366_695_050_184,
                    "grid_values_hashed": 16_777_216,
                    "sumwt_values_hashed": 1,
                },
            ],
        }
    ]


def write_frozen_v5(
    path: Path,
    first_vb: dict[str, object],
    calls: list[dict[str, int]],
) -> str:
    receipt = {
        "schema": "casa-aw-datagrid-bracket-v1",
        "status": "completed-before-finalize",
        "reason": "configured-block-boundary",
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "casa_version": "6.7.5.18",
        "casa_version_string": "6.7.5-18",
        "casa_source_commit": subject.CASA_SOURCE_COMMIT,
        "exit_code": 86,
        "original_invocation": "two-level-bound-exact-DComplex-specialization",
        "dispatch_identity": "stable-grid-storage-and-source-stream",
        "probe_serialization": "global-mutex",
        "formed_image": False,
        "normalization": "not-entered",
        "fft": "not-entered",
        "expected_grid_nxy": 4096,
        "target_blocks": 1,
        "terms_per_block": 2,
        "completed_calls": 2,
        "completed_blocks": 1,
        "input_stream_hash": 9_670_878_879_986_980_654,
        "native_first_vb": first_vb,
        "calls": frozen_calls(calls),
        "block_boundaries": frozen_boundaries(calls),
    }
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return subject.sha256_path(path)


def candidate_evidence(
    header: dict[str, object],
    rows: list[dict[str, object]],
    *,
    frozen_sha: str,
) -> dict[str, object]:
    components, counts, calls, _ = subject.recompute_components(header, rows)
    return {
        "schema": subject.CANDIDATE_EVIDENCE_SCHEMA,
        "status": "completed-controlled-stop",
        "result": "completed-native-components-exact-frozen-v5",
        "role": "bounded-correctness-oracle-not-performance-evidence",
        "producer": "CASA",
        "casa_version": "6.7.5.18",
        "casa_version_string": "6.7.5-18",
        "casa_source_commit": subject.CASA_SOURCE_COMMIT,
        "casacore_source_commit": subject.CASACORE_SOURCE_COMMIT,
        "datatogrid_symbol": subject.DATATOGRID_DCOMPLEX_SYMBOL,
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
        "hash_contracts": copy.deepcopy(subject.HASH_CONTRACTS),
        "frozen_parent_receipts": {
            "casa_v5": {
                "schema": "casa-aw-datagrid-bracket-v1",
                "receipt_sha256": frozen_sha,
            }
        },
        "header": copy.deepcopy(header),
        "component_hashes": components,
        "counts": counts,
        "recomputed_frozen_hashes": [
            {"origin": subject.EXPECTED_ORIGINS[ordinal], **call}
            for ordinal, call in enumerate(calls)
        ],
        "rows": copy.deepcopy(rows),
    }


def write_candidate(path: Path, evidence: dict[str, object]) -> str:
    embedded = json.dumps(evidence, indent=2, sort_keys=True)
    digest = hashlib.sha256(embedded.encode()).hexdigest()
    path.write_text(
        "{\n"
        f'  "schema": {json.dumps(subject.CANDIDATE_ENVELOPE_SCHEMA)},\n'
        '  "content_address": {\n'
        '    "algorithm": "sha256",\n'
        '    "scope": "embedded-evidence-json-utf8",\n'
        f'    "digest": "{digest}"\n'
        "  },\n"
        f'  "evidence": {embedded}\n'
        "}\n"
    )
    return digest


class NativeComponentsValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.header = fixture_header()
        self.rows = fixture_rows()
        (
            self.components,
            self.counts,
            self.calls,
            self.checkpoints,
        ) = subject.recompute_components(self.header, self.rows)
        self.first_vb = frozen_first_vb(self.header, self.components, self.counts)
        self.v5_path = self.directory / "casa-v5.json"
        self.v5_sha = write_frozen_v5(
            self.v5_path,
            self.first_vb,
            self.calls,
        )

    @contextmanager
    def frozen_targets(self):
        parents = {
            "casa_v5": {
                "schema": "casa-aw-datagrid-bracket-v1",
                "receipt_sha256": self.v5_sha,
            }
        }
        with mock.patch.multiple(
            subject,
            CASA_V5_RECEIPT_SHA256=self.v5_sha,
            FROZEN_PARENT_RECEIPTS=parents,
            STREAM_HASH=self.calls[0]["stream_hash"],
            GEOMETRY_HASHES=[
                self.calls[0]["geometry_hash"],
                self.calls[1]["geometry_hash"],
            ],
            EXPECTED_FIRST_VB=self.first_vb,
            EXPECTED_V5_CALLS=frozen_calls(self.calls),
            EXPECTED_V5_BOUNDARIES=frozen_boundaries(self.calls),
        ):
            yield

    def evidence(self) -> dict[str, object]:
        return candidate_evidence(
            self.header,
            self.rows,
            frozen_sha=self.v5_sha,
        )

    def validate(self, evidence: dict[str, object]) -> tuple:
        candidate = self.directory / "candidate.json"
        write_candidate(candidate, evidence)
        with self.frozen_targets():
            return subject.validate_candidate(candidate)

    def assert_rejected(
        self,
        evidence: dict[str, object],
        message: str,
    ) -> None:
        candidate = self.directory / "candidate.json"
        write_candidate(candidate, evidence)
        with (
            self.frozen_targets(),
            self.assertRaisesRegex(subject.ContractError, message),
        ):
            subject.validate_candidate(candidate)

    def refresh_claims(self, evidence: dict[str, object]) -> None:
        header = evidence["header"]
        rows = evidence["rows"]
        assert isinstance(header, dict) and isinstance(rows, list)
        components, counts, calls, _ = subject.recompute_components(header, rows)
        evidence["component_hashes"] = components
        evidence["counts"] = counts
        evidence["recomputed_frozen_hashes"] = [
            {"origin": subject.EXPECTED_ORIGINS[ordinal], **call}
            for ordinal, call in enumerate(calls)
        ]

    def test_valid_receipt_recomposes_all_frozen_targets(self) -> None:
        (
            evidence,
            _,
            _,
            components,
            counts,
            calls,
            checkpoints,
        ) = self.validate(self.evidence())
        self.assertEqual(
            evidence["result"], "completed-native-components-exact-frozen-v5"
        )
        self.assertEqual(components, self.components)
        self.assertEqual(counts["admitted_channels"], subject.SOURCE_COUNT)
        self.assertEqual(calls, self.calls)
        self.assertEqual(len(checkpoints), subject.ROW_COUNT)
        self.assertEqual(checkpoints[-1]["stream_hash"], calls[0]["stream_hash"])
        self.assertEqual(
            checkpoints[-1]["tt1_geometry_hash"], calls[1]["geometry_hash"]
        )

    def test_comparison_is_content_addressed_with_325_checkpoints(self) -> None:
        candidate = self.directory / "candidate.json"
        write_candidate(candidate, self.evidence())
        with self.frozen_targets():
            envelope = subject.build_comparison(
                candidate_path=candidate,
                casa_v5_path=self.v5_path,
            )
        comparison = envelope["comparison"]
        self.assertEqual(
            comparison["classification"], "exact-frozen-v5-native-components"
        )
        self.assertEqual(len(comparison["row_checkpoints"]), subject.ROW_COUNT)
        expected_digest = hashlib.sha256(
            subject._canonical_json(comparison)
        ).hexdigest()
        self.assertEqual(envelope["content_address"]["digest"], expected_digest)
        self.assertFalse(comparison["claims"]["tt1_observed"])

    def test_raw_embedded_digest_mutation_is_rejected(self) -> None:
        candidate = self.directory / "candidate.json"
        write_candidate(candidate, self.evidence())
        payload = candidate.read_text()
        candidate.write_text(payload.replace('"status":', '"status" :', 1))
        with (
            self.frozen_targets(),
            self.assertRaisesRegex(subject.ContractError, "content_address.digest"),
        ):
            subject.validate_candidate(candidate)

    def test_exact_key_sets_and_json_types_are_enforced(self) -> None:
        evidence = self.evidence()
        evidence["unexpected"] = None
        self.assert_rejected(evidence, "key set changed")

        evidence = self.evidence()
        evidence["completed_calls"] = True
        self.assert_rejected(evidence, "completed_calls changed")

        evidence = self.evidence()
        header = evidence["header"]
        assert isinstance(header, dict)
        header["unexpected"] = 1
        self.assert_rejected(evidence, "header key set changed")

    def test_all_required_array_shapes_are_enforced(self) -> None:
        mutations = [
            ("rows", lambda evidence: evidence["rows"].pop(), "contain 325 rows"),
            (
                "frequency",
                lambda evidence: evidence["header"]["frequency_bits"].pop(),
                "must contain exactly 64",
            ),
            (
                "uvw",
                lambda evidence: evidence["rows"][0]["uvw_bits"].pop(),
                "must contain three",
            ),
            (
                "flags",
                lambda evidence: evidence["rows"][0]["flag_masks"].pop(),
                "must contain 64",
            ),
            (
                "weights",
                lambda evidence: evidence["rows"][0]["imaging_weight_bits"].pop(),
                "must contain 64",
            ),
        ]
        for name, mutate, message in mutations:
            with self.subTest(name=name):
                evidence = self.evidence()
                mutate(evidence)
                self.assert_rejected(evidence, message)
                (self.directory / "candidate.json").unlink(missing_ok=True)

    def test_flag_mask_rejects_bits_outside_four_polarizations(self) -> None:
        evidence = self.evidence()
        rows = evidence["rows"]
        assert isinstance(rows, list) and isinstance(rows[0], dict)
        masks = rows[0]["flag_masks"]
        assert isinstance(masks, list)
        masks[0] = 16
        self.assert_rejected(evidence, "uses bits outside 0..3")

    def test_weight_bits_are_independently_hashed(self) -> None:
        evidence = self.evidence()
        rows = evidence["rows"]
        assert isinstance(rows, list) and isinstance(rows[48], dict)
        weights = rows[48]["imaging_weight_bits"]
        assert isinstance(weights, list)
        weights[0] = f32_bits(2.0)
        self.assert_rejected(evidence, "component_hashes")

    def test_admission_mutation_cannot_self_consistently_change_target(self) -> None:
        evidence = self.evidence()
        rows = evidence["rows"]
        assert isinstance(rows, list)
        row = rows[-1]
        assert isinstance(row, dict)
        weights = row["imaging_weight_bits"]
        assert isinstance(weights, list)
        zero_channel = weights.index(f32_bits(0.0))
        weights[zero_channel] = f32_bits(1.0)
        self.refresh_claims(evidence)
        self.assert_rejected(evidence, "source count")

    def test_recomposition_catches_mutation_after_claims_are_refreshed(self) -> None:
        evidence = self.evidence()
        rows = evidence["rows"]
        assert isinstance(rows, list)
        row = rows[48]
        assert isinstance(row, dict)
        uvw = row["uvw_bits"]
        assert isinstance(uvw, list)
        uvw[0] ^= 1
        self.refresh_claims(evidence)
        self.assert_rejected(evidence, "STREAM")

    def test_claimed_target_hash_cannot_differ_from_independent_recomposition(
        self,
    ) -> None:
        evidence = self.evidence()
        hashes = evidence["recomputed_frozen_hashes"]
        assert isinstance(hashes, list) and isinstance(hashes[0], dict)
        hashes[0]["stream_hash"] ^= 1
        self.assert_rejected(evidence, "independently recomputed frozen hashes")

    def test_source_lineage_and_no_grid_claims_are_exact(self) -> None:
        for key, mutation, message in [
            ("source", "casa_source_commit", "casa_source_commit changed"),
            ("original", "original_datatogrid", "original_datatogrid changed"),
            ("grid", "grid_dispatch", "grid_dispatch changed"),
            ("image", "formed_image", "formed_image changed"),
        ]:
            with self.subTest(key=key):
                evidence = self.evidence()
                if mutation == "formed_image":
                    evidence[mutation] = True
                else:
                    evidence[mutation] = "mutated"
                self.assert_rejected(evidence, message)
                (self.directory / "candidate.json").unlink(missing_ok=True)

    def test_frozen_v5_hash_schema_calls_and_header_are_all_checked(self) -> None:
        mutations = [
            ("schema", lambda receipt: receipt.__setitem__("schema", "changed")),
            (
                "calls",
                lambda receipt: receipt["calls"][0].__setitem__(
                    "stream_hash", receipt["calls"][0]["stream_hash"] ^ 1
                ),
            ),
            (
                "header",
                lambda receipt: receipt["native_first_vb"].__setitem__("spw_id", 7),
            ),
        ]
        for name, mutate in mutations:
            with self.subTest(name=name):
                receipt = json.loads(self.v5_path.read_text())
                mutate(receipt)
                path = self.directory / f"v5-{name}.json"
                path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
                changed_sha = subject.sha256_path(path)
                with (
                    mock.patch.object(subject, "CASA_V5_RECEIPT_SHA256", changed_sha),
                    self.assertRaises(subject.ContractError),
                ):
                    subject.validate_frozen_casa_v5(path)

    def test_frozen_v5_whole_file_digest_is_immutable(self) -> None:
        changed = self.directory / "v5-whitespace-mutated.json"
        changed.write_bytes(self.v5_path.read_bytes() + b"\n")
        with (
            mock.patch.object(subject, "CASA_V5_RECEIPT_SHA256", self.v5_sha),
            self.assertRaisesRegex(subject.ContractError, "whole-file SHA-256"),
        ):
            subject.validate_frozen_casa_v5(changed)

    def test_comparison_publication_never_clobbers(self) -> None:
        output = (self.directory / "comparison.json").resolve()
        value = {"result": "first"}
        subject.atomic_write_json(output, value)
        before = output.read_bytes()
        with self.assertRaisesRegex(subject.ContractError, "refusing to overwrite"):
            subject.atomic_write_json(output, {"result": "second"})
        self.assertEqual(output.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
