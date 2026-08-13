#!/usr/bin/env python3
"""Build and validate the bounded VLASS CASA AW division call-site trace."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np

import vlass_casa_aw_division_codegen_audit as codegen
import vlass_casa_aw_divsc3_direct_probe as direct_probe
import vlass_casa_mtmfs_term_degrid_compare as term_oracle


EXPECTED_NPZ_SHA256 = "a801635e7d9529cc4dbd3f462abd10bdcd66b8283bb5894a85da419a95899b7d"
EXPECTED_SOURCE_TRACE_SHA256 = (
    "0f5c3a7ee8e546aa5291ed6d97ff3057e80dd87b7d0ffca8bc9ecf998d599725"
)
EXPECTED_TERM_ORACLE_SHA256 = (
    "6b314935bd0a0ef072c1abcbb00fb3513b327010c047db1a1c61cb8f4b79fc13"
)
EXPECTED_SOURCE_ORDINALS = (0, 1446)
EXPECTED_IDENTITIES = {
    0: {
        "row_id": 353600,
        "ddid": 2,
        "spw_id": 2,
        "channel": 11,
        "row_in_vb": 0,
        "helper_call_index": 0,
        "numerator": [0x3DA00F0F, 0x3DC30CDE],
        "normalizer": [0x3F6E1694, 0xBD1ED44B],
        "official_result": direct_probe.EXPECTED_SOURCE_ZERO,
    },
    1446: {
        "row_id": 353635,
        "ddid": 2,
        "spw_id": 2,
        "channel": 19,
        "row_in_vb": 35,
        "helper_call_index": 2892,
        "numerator": [0x39C7D0F4, 0xBE8D50A9],
        "normalizer": [0x3F7A5C92, 0x3C71A8AE],
        "official_result": direct_probe.EXPECTED_SOURCE_1446,
    },
}
EXPECTED_CALLSITE_RETURN_VMADDR = direct_probe.EXPECTED_CALLSITE_VMADDR + 4


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def f32_bits(value: np.float32) -> int:
    return int(np.asarray(value, dtype=np.float32).view(np.uint32))


def _load_pinned(path: Path, expected_sha256: str, label: str) -> str:
    actual = sha256_file(path)
    if actual != expected_sha256:
        raise RuntimeError(f"frozen {label} SHA-256 changed")
    return actual


def enumerate_helper_calls(
    *,
    row_ids: np.ndarray,
    spw_ids: np.ndarray,
    uv_selected: np.ndarray,
    flags: np.ndarray,
    source_samples: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if (
        row_ids.ndim != 1
        or spw_ids.shape != row_ids.shape
        or uv_selected.shape != row_ids.shape
        or flags.shape != (row_ids.size, 4, 64)
    ):
        raise RuntimeError("frozen CASA selection topology changed")
    if np.any(flags[:, 0, :] != flags[:, 3, :]):
        raise RuntimeError("parallel-hand flag symmetry changed")

    targets: list[dict[str, Any]] = []
    source_ordinal = 0
    helper_call_index = 0
    for selected_row_index, row_id_value in enumerate(row_ids):
        if not bool(uv_selected[selected_row_index]):
            continue
        for channel in range(flags.shape[2]):
            if bool(flags[selected_row_index, 0, channel]):
                continue
            if source_ordinal >= len(source_samples):
                raise RuntimeError(
                    "CASA selection contains more sources than the trace"
                )
            sample = source_samples[source_ordinal]
            identity = (
                int(row_id_value),
                int(spw_ids[selected_row_index]),
                channel,
            )
            source_identity = (
                int(sample["row_id"]),
                int(sample["spw_id"]),
                int(sample["channel"]),
            )
            if identity != source_identity:
                raise RuntimeError(
                    f"source-order identity differs at ordinal {source_ordinal}"
                )
            if int(sample["source_ordinal"]) != source_ordinal:
                raise RuntimeError("source trace ordinal is not contiguous")
            if source_ordinal in EXPECTED_SOURCE_ORDINALS:
                targets.append(
                    {
                        "source_ordinal": source_ordinal,
                        "selected_row_index": selected_row_index,
                        "row_id": int(row_id_value),
                        "ddid": int(sample["ddid"]),
                        "spw_id": int(spw_ids[selected_row_index]),
                        "channel": channel,
                        "role": "rr",
                        "term": "tt0",
                        "helper_call_index": helper_call_index,
                        "ll_helper_call_index": helper_call_index + 1,
                    }
                )
            source_ordinal += 1
            helper_call_index += 2
    if source_ordinal != len(source_samples):
        raise RuntimeError("source trace contains sources outside the CASA selection")
    return targets, {
        "source_count": source_ordinal,
        "parallel_hand_helper_call_count": helper_call_index,
        "asymmetric_parallel_hand_flag_count": 0,
    }


def build_manifest(
    *,
    npz_path: Path,
    source_trace_path: Path,
    term_oracle_path: Path,
    casa_source_path: Path,
    output: Path,
) -> dict[str, Any]:
    if output.exists():
        raise RuntimeError(f"refusing to overwrite manifest: {output}")
    npz_sha256 = _load_pinned(npz_path, EXPECTED_NPZ_SHA256, "CASA prediction NPZ")
    source_trace_sha256 = _load_pinned(
        source_trace_path,
        EXPECTED_SOURCE_TRACE_SHA256,
        "casa-rs source trace",
    )
    term_oracle_sha256 = _load_pinned(
        term_oracle_path,
        EXPECTED_TERM_ORACLE_SHA256,
        "CASA term oracle",
    )

    source_trace = json.loads(source_trace_path.read_text(encoding="utf-8"))
    source_samples = source_trace.get("samples")
    if not isinstance(source_samples, list):
        raise RuntimeError("source trace lacks samples")
    with np.load(npz_path, allow_pickle=False) as npz:
        targets, census = enumerate_helper_calls(
            row_ids=np.asarray(npz["row_id"]),
            spw_ids=np.asarray(npz["spectral_window_id"]),
            uv_selected=np.asarray(npz["uv_range_selected"]),
            flags=np.asarray(npz["flag"]),
            source_samples=source_samples,
        )

    records = np.fromfile(term_oracle_path, dtype=term_oracle.CASA_DTYPE)
    first_call = records[records["call"] == 0]
    if first_call.size != 20_800:
        raise RuntimeError("frozen first CASA MT-MFS get call changed extent")
    term_keys: dict[tuple[int, int, int], np.void] = {}
    for record in first_call:
        key = (
            int(record["spw_id"]),
            int(record["row_id"]),
            int(record["channel"]),
        )
        if key in term_keys:
            raise RuntimeError(f"duplicate term-oracle key {key}")
        term_keys[key] = record

    if len(targets) != len(EXPECTED_SOURCE_ORDINALS):
        raise RuntimeError("call-order census did not find both fixed targets")
    for target in targets:
        ordinal = target["source_ordinal"]
        expected = EXPECTED_IDENTITIES[ordinal]
        for key in (
            "row_id",
            "ddid",
            "spw_id",
            "channel",
            "helper_call_index",
        ):
            if target[key] != expected[key]:
                raise RuntimeError(
                    f"source {ordinal} {key} changed: {target[key]} != {expected[key]}"
                )
        target["row_in_vb"] = expected["row_in_vb"]
        target["asserted_numerator"] = expected["numerator"]
        target["asserted_normalizer"] = expected["normalizer"]
        target["official_result"] = expected["official_result"]
        record = term_keys[
            (expected["spw_id"], expected["row_in_vb"], expected["channel"])
        ]
        term_bits = [
            f32_bits(record["tt0_rr_re"]),
            f32_bits(record["tt0_rr_im"]),
        ]
        if term_bits != expected["official_result"]:
            raise RuntimeError(f"source {ordinal} term-oracle bits changed")
        target["term_oracle_call"] = int(record["call"])
        target["term_oracle_row_id"] = int(record["row_id"])
        target["term_oracle_result"] = term_bits

    casa_source_sha256 = sha256_file(casa_source_path)
    manifest = {
        "schema": "casa-rs-vlass-casa-aw-divsc3-callsite-manifest-v1",
        "role": "frozen-call-order-and-term-identity-gate",
        "loop_contract": (
            "AWVisResampler::GridToData row-channel-ipol order; symmetric "
            "parallel-hand flags; RR then LL; first sub-FTM call is TT0"
        ),
        "inputs": {
            "prediction_npz": str(npz_path),
            "prediction_npz_sha256": npz_sha256,
            "source_trace": str(source_trace_path),
            "source_trace_sha256": source_trace_sha256,
            "term_oracle": str(term_oracle_path),
            "term_oracle_sha256": term_oracle_sha256,
            "casa_awvisresampler_source": str(casa_source_path),
            "casa_awvisresampler_source_sha256": casa_source_sha256,
        },
        "callsite": {
            "vmaddr": f"0x{direct_probe.EXPECTED_CALLSITE_VMADDR:016x}",
            "return_vmaddr": f"0x{EXPECTED_CALLSITE_RETURN_VMADDR:016x}",
            "helper_vmaddr": f"0x{direct_probe.EXPECTED_HELPER_VMADDR:016x}",
        },
        "census": census,
        "targets": targets,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def classify_target(
    captured_operands: list[int],
    captured_result: list[int],
    asserted_operands: list[int],
    official_result: list[int],
) -> str:
    if captured_operands != asserted_operands:
        if captured_result == official_result:
            return "operands-differ-at-callsite"
        return "operands-differ-and-result-unexpected"
    if captured_result == direct_probe.EXPECTED_RUST_SOURCE_1446:
        return "operands-match-and-result-matches-rust-wide"
    if captured_result == official_result:
        return "operands-match-and-result-matches-official"
    return "operands-match-and-result-unexpected"


def analyze_trace(
    *,
    manifest_path: Path,
    raw_trace_path: Path,
    library: Path,
    output: Path,
) -> dict[str, Any]:
    if output.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {output}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_trace = json.loads(raw_trace_path.read_text(encoding="utf-8"))
    if (
        manifest.get("schema") != "casa-rs-vlass-casa-aw-divsc3-callsite-manifest-v1"
        or raw_trace.get("schema")
        != "casa-rs-vlass-casa-aw-divsc3-callsite-raw-trace-v1"
        or raw_trace.get("status") != "completed-at-source1446-return"
    ):
        raise RuntimeError("call-site trace inputs are incomplete")

    audit = codegen.audit(library)
    if audit["library"]["sha256"] != direct_probe.EXPECTED_LIBRARY_SHA256:
        raise RuntimeError("installed CASA synthesis library changed")
    if raw_trace["library_uuid"] != direct_probe.EXPECTED_IMAGE_UUID:
        raise RuntimeError("live CASA image UUID changed")
    if raw_trace["callsite_vmaddr"] != manifest["callsite"]["vmaddr"]:
        raise RuntimeError("live call-site VM address changed")
    if raw_trace["return_vmaddr"] != manifest["callsite"]["return_vmaddr"]:
        raise RuntimeError("live return VM address changed")
    if raw_trace["helper_vmaddr"] != manifest["callsite"]["helper_vmaddr"]:
        raise RuntimeError("live helper VM address changed")

    expected_targets = {
        int(target["source_ordinal"]): target for target in manifest["targets"]
    }
    captured_targets = {
        int(target["source_ordinal"]): target for target in raw_trace.get("targets", [])
    }
    if set(captured_targets) != set(EXPECTED_SOURCE_ORDINALS):
        raise RuntimeError("live trace did not capture both fixed targets")
    for ordinal in EXPECTED_SOURCE_ORDINALS:
        expected = expected_targets[ordinal]
        captured = captured_targets[ordinal]
        if (
            int(captured["helper_call_index"]) != int(expected["helper_call_index"])
            or captured["role"] != "rr"
            or captured["term"] != "tt0"
        ):
            raise RuntimeError(f"live source {ordinal} binding differs")
        if (
            captured["fpcr_before"] != captured["fpcr_after"]
            or captured["thread_id"] != raw_trace["trace_thread_id"]
        ):
            raise RuntimeError(f"live source {ordinal} FP/thread identity changed")

    control = captured_targets[0]
    control_expected = expected_targets[0]
    if control["pre_bits"] != (
        control_expected["asserted_numerator"] + control_expected["asserted_normalizer"]
    ):
        raise RuntimeError("source-0 live operands do not match the frozen control")
    if control["post_bits"] != control_expected["official_result"]:
        raise RuntimeError("source-0 live result does not match the frozen control")

    target = captured_targets[1446]
    target_expected = expected_targets[1446]
    asserted_operands = (
        target_expected["asserted_numerator"] + target_expected["asserted_normalizer"]
    )
    classification = classify_target(
        target["pre_bits"],
        target["post_bits"],
        asserted_operands,
        target_expected["official_result"],
    )
    result = {
        "schema": "casa-rs-vlass-casa-aw-divsc3-callsite-comparison-v1",
        "classification": classification,
        "valid": True,
        "manifest": {
            "path": str(manifest_path),
            "sha256": sha256_file(manifest_path),
        },
        "raw_trace": {
            "path": str(raw_trace_path),
            "sha256": sha256_file(raw_trace_path),
        },
        "library": audit["library"],
        "callsite": raw_trace["callsite_disassembly"],
        "source_zero_control": {
            "pre_bits": control["pre_bits"],
            "post_bits": control["post_bits"],
            "expected_pre_bits": (
                control_expected["asserted_numerator"]
                + control_expected["asserted_normalizer"]
            ),
            "expected_post_bits": control_expected["official_result"],
        },
        "source_1446": {
            "pre_bits": target["pre_bits"],
            "post_bits": target["post_bits"],
            "asserted_pre_bits": asserted_operands,
            "frozen_official_result": target_expected["official_result"],
            "rust_wide_result": direct_probe.EXPECTED_RUST_SOURCE_1446,
        },
        "authorization": (
            "bounded-exact-callsite-operand-correction-only"
            if classification == "operands-differ-at-callsite"
            else "no-production-change-audit-frozen-oracle-binding"
            if classification == "operands-match-and-result-matches-rust-wide"
            else "no-production-change"
        ),
        "stop_boundary": raw_trace["stop_boundary"],
        "prohibited_after_stop": {
            "tt1_degrid_completed": False,
            "prediction_finalized": False,
            "residual_grid_dispatched": False,
            "products_formed": False,
            "clean_entered": False,
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    manifest_parser = subparsers.add_parser("manifest")
    manifest_parser.add_argument("--npz", required=True, type=Path)
    manifest_parser.add_argument("--source-trace", required=True, type=Path)
    manifest_parser.add_argument("--term-oracle", required=True, type=Path)
    manifest_parser.add_argument("--casa-source", required=True, type=Path)
    manifest_parser.add_argument("--output", required=True, type=Path)
    analyze_parser = subparsers.add_parser("analyze")
    analyze_parser.add_argument("--manifest", required=True, type=Path)
    analyze_parser.add_argument("--raw-trace", required=True, type=Path)
    analyze_parser.add_argument("--library", required=True, type=Path)
    analyze_parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    if args.command == "manifest":
        result = build_manifest(
            npz_path=args.npz,
            source_trace_path=args.source_trace,
            term_oracle_path=args.term_oracle,
            casa_source_path=args.casa_source,
            output=args.output,
        )
    else:
        result = analyze_trace(
            manifest_path=args.manifest,
            raw_trace_path=args.raw_trace,
            library=args.library,
            output=args.output,
        )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
