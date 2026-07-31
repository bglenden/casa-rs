from __future__ import annotations

import importlib.util
import json
import pathlib


MODULE_PATH = pathlib.Path(__file__).with_name(
    "vlass_ordered_response_full_matrix_proof.py"
)
SPEC = importlib.util.spec_from_file_location(
    "vlass_ordered_response_full_matrix_proof", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def synthetic_source_contract() -> dict[str, object]:
    graph = MODULE.load_graph_module()
    pairs: list[dict[str, object]] = []
    for pair_index in range(27):
        imaging_frequency_hz = 2_000_000_000 + (pair_index % 14) * 128_000_000
        prediction_frequency_hz = (
            2_000_000_000 + (pair_index % 16) * 128_000_000
        )
        for imaging_mueller, prediction_mueller in [(0, 15), (15, 0)]:
            pairs.append(
                {
                    "imaging_frequency_hz": imaging_frequency_hz,
                    "imaging_mueller_element": imaging_mueller,
                    "prediction_frequency_hz": prediction_frequency_hz,
                    "prediction_mueller_element": prediction_mueller,
                }
            )
    return {
        "schema": graph.SOURCE_SCHEMA,
        "aw_screen_selection": {
            "imaging_prediction_state_pairs": pairs,
        },
    }


def test_full_matrix_proof_matches_rows_and_rejects_wrong_semantics(
    tmp_path: pathlib.Path,
) -> None:
    source_path = tmp_path / "source.json"
    source_path.write_text(
        json.dumps(synthetic_source_contract()) + "\n", encoding="utf-8"
    )

    proof = MODULE.prove(source_path)

    metrics = proof["metrics"]
    assert (
        metrics["explicit_contraction_vs_direct_polynomial"]["relative_l2"]
        <= MODULE.ALGEBRA_L2_LIMIT
    )
    assert (
        metrics["fft_embedding_vs_explicit_contraction"]["relative_l2"]
        <= MODULE.FFT_L2_LIMIT
    )
    assert (
        metrics["rhs_contraction_vs_direct_polynomial"]["relative_l2"]
        <= MODULE.ALGEBRA_L2_LIMIT
    )
    assert all(
        control["error_ratio"] >= MODULE.CONTROL_RATIO_MIN
        for control in proof["adversarial_controls"].values()
    )
