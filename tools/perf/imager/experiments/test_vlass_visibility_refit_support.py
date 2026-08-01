from __future__ import annotations

import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("vlass_visibility_refit_support.py")
SPEC = importlib.util.spec_from_file_location(
    "vlass_visibility_refit_support", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_parse_support_coalesces_unique_scale_position_atoms(tmp_path: Path) -> None:
    log = tmp_path / "run.log"
    log.write_text(
        "\n".join(
            [
                "unrelated line",
                "mtmfs_component_trace local_iteration=0 scale_index=0 "
                "scale_pixels=0 x=10 y=20 signed_score=1 biased_signed_score=1 "
                "coefficients=[1.0, -2.0] rhs=[0, 0]",
                "mtmfs_component_trace local_iteration=1 scale_index=0 "
                "scale_pixels=0 x=10 y=20 signed_score=1 biased_signed_score=1 "
                "coefficients=[3.0, 4.0] rhs=[0, 0]",
                "mtmfs_component_trace local_iteration=0 scale_index=2 "
                "scale_pixels=12 x=11 y=21 signed_score=1 biased_signed_score=1 "
                "coefficients=[5.0, 6.0] rhs=[0, 0]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = MODULE.parse_support(log, gain=0.1, nterms=2)

    assert result["trace_components"] == 3
    assert result["unique_atoms"] == 2
    assert result["scale_update_counts"] == {"0": 2, "2": 1}
    assert result["scale_atom_counts"] == {"0": 1, "2": 1}
    first = result["atoms"][0]
    assert (first["scale_index"], first["x"], first["y"]) == (0, 10, 20)
    assert first["updates"] == 2
    assert first["coalesced_term_deltas_f32_sequential"] == [
        MODULE.f32(MODULE.f32(0.1) + MODULE.f32(0.3)),
        MODULE.f32(MODULE.f32(-0.2) + MODULE.f32(0.4)),
    ]
    assert result["source_log"]["sha256"] == MODULE.sha256_file(log)


def test_write_json_exclusive_refuses_to_replace_receipt(tmp_path: Path) -> None:
    output = tmp_path / "receipt.json"
    MODULE.write_json_exclusive(output, {"value": 1})
    assert json.loads(output.read_text(encoding="utf-8")) == {"value": 1}

    try:
        MODULE.write_json_exclusive(output, {"value": 2})
    except FileExistsError:
        pass
    else:
        raise AssertionError("expected exclusive receipt publication to fail")
