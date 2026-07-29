#!/usr/bin/env python3
"""Compare two bounded casa-rs AW prediction traces for the same model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from vlass_prediction_trace_compare import complex_metrics, parse_rust_trace


def sample_key(record: dict[str, str]) -> tuple[str, ...]:
    return (
        record["frequency_hz"],
        record["u_lambda"],
        record["v_lambda"],
        record["w_lambda"],
        record["group"],
    )


def prediction(records: list[dict[str, str]], hand: str) -> dict[tuple[str, ...], complex]:
    return {
        sample_key(record): complex(
            float(record[f"{hand}_prediction_re"]),
            float(record[f"{hand}_prediction_im"]),
        )
        for record in records
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-log", required=True, type=Path)
    parser.add_argument("--reference-log", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    candidate_records = parse_rust_trace(args.candidate_log)
    reference_records = parse_rust_trace(args.reference_log)
    result = {
        "kind": "vlass_awproject_prediction_trace_pair_comparison",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "candidate_log": str(args.candidate_log),
        "reference_log": str(args.reference_log),
    }
    common_keys: set[tuple[str, ...]] | None = None
    for hand in ("rr", "ll"):
        candidate = prediction(candidate_records, hand)
        reference = prediction(reference_records, hand)
        keys = set(candidate) & set(reference)
        common_keys = keys if common_keys is None else common_keys & keys
        ordered = sorted(keys)
        result[hand] = complex_metrics(
            np.asarray([candidate[key] for key in ordered], dtype=np.complex128),
            np.asarray([reference[key] for key in ordered], dtype=np.complex128),
        )
    result["matched_samples"] = len(common_keys or ())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
