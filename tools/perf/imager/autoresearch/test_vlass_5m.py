from __future__ import annotations

import copy
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from autoresearch import vlass_5m
from autoresearch.vlass_5m_core import (
    comparison_request,
    evaluate_receipt,
    load_contract,
    parse_runtime_log,
    runtime_command,
)


HERE = pathlib.Path(__file__).resolve().parent
CONTRACT_PATH = HERE / "vlass_5m_contract.json"
PRODUCTS = [
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual.tt0",
    ".residual.tt1",
    ".model.tt0",
    ".model.tt1",
    ".image.tt0",
    ".image.tt1",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".weight.tt0",
    ".weight.tt1",
    ".weight.tt2",
    ".alpha",
    ".alpha.error",
    ".mask",
    ".pb.tt0",
]


def synthetic_log() -> str:
    product_lines = "\n".join(
        f"image_product_write suffix={suffix} role=test "
        f"shape={'1x1x1x1' if suffix.startswith('.sumwt') else '4096x4096x1x1'} "
        "elements=1 elapsed_ms=1.0"
        for suffix in PRODUCTS
    )
    return f"""
mfs_ddid_execution_plan ddids=2,7,12,17 spws=2,7,12,17 rows=100 selected_channel_visits=2400 bounded_row_groups=true
mosaic_mtmfs_stream_replay invocation=0 pass=WeightingDensity load_visibility_values=false blocks=4 samples=1000000 elapsed_ms=10.0
mosaic_mtmfs_stream_replay invocation=1 pass=InitialDirty load_visibility_values=true blocks=4 samples=1000000 elapsed_ms=100000.0
awproject_metal_resident_tile_chain built=true program_bytes=6000000000 build_ms=10.0 dispatch_wait_ms=2.0 total_ms=13.0
awproject_metal_resident_tile_chain built=true program_bytes=6000000000 build_ms=11.0 dispatch_wait_ms=3.0 total_ms=15.0
awproject_compact_source_order windows=30 largest_window_samples=100 peak_tap_bytes=268435000 routed_samples=500000 spatial_tile_side=0 plan_ms=10.0 materialize_ms=30.0 cache_load_worker_ms=20.0 tap_pack_ms=9.0 prepare_ms=1.0 grid_including_tile_plan_ms=5.0
awproject_compact_source_order windows=30 largest_window_samples=101 peak_tap_bytes=268435000 routed_samples=500000 spatial_tile_side=0 plan_ms=11.0 materialize_ms=31.0 cache_load_worker_ms=21.0 tap_pack_ms=9.0 prepare_ms=1.0 grid_including_tile_plan_ms=5.0
mosaic_mtmfs_stream_replay invocation=2 pass=ResidualRefresh load_visibility_values=true blocks=4 samples=2204617 elapsed_ms=300000.0
awproject_metal_grid_summary pass=residual_refresh calls=100 samples=2204617 dispatch_wait_ms=5000.0 total_ms=6000.0
awproject_metal_compensated_residual_readback products=2 fft_precision=f64 resident_bytes=536870912 readback_ms=70.0
awproject_cache residency_budget_bytes=268435456 resident_cells=59 resident_bytes=258975232 loads=100000 hits=1000 evictions=99941 attempted_samples=2204617 accepted_samples=2204617 rejected_not_gridable=0 rejected_invalid_input=0
awproject_plan implementation=test projection=SIN image_shape=4096x4096 wplanes=32 aterm=true psterm=false wbawp=true conjbeams=true usepointing=true cf_metadata_key=f9427a9611b99dc6 cf_mueller=[0, 15]
awproject_frozen_model_refresh prefix=/frozen terms=2 image_shape=4096x4096
awproject_frozen_model_support positions=193 source=imported-nonzero-union
mosaic_mtmfs_final_residual_refresh reported_iterations=2000 refreshed_peak=0.1 model_flux=1.0
{product_lines}
frontend stage=run_summary total_ms=400000.0 write_products_ms=1000.0
core stage=run_summary total_ms=399000.0 residual_degrid_grid_ms=300000.0
Wrote CASA-compatible products at prefix /tmp/rust (2204617 gridded samples, 1 major cycles, 2000 minor iterations, stop=Some(IterationLimitReached))
"""


def split_total(total: int, keys: list[str]) -> dict[str, int]:
    quotient, remainder = divmod(total, len(keys))
    return {key: quotient + int(index < remainder) for index, key in enumerate(keys)}


def valid_selection_accounting(contract: dict) -> dict:
    spw_keys = [str(spw) for spw in contract["workload"]["spw_ids"]]
    field_keys = [str(field) for field in contract["workload"]["field_ids"]]
    accepted_by_spw = split_total(
        contract["baseline"]["qualification"]["accepted_samples"], spw_keys
    )
    return {
        spw: {
            "schema_version": 2,
            "samples": {
                "attempted_stokes_i_samples": accepted_by_spw[spw],
                "accepted_stokes_i_samples": accepted_by_spw[spw],
                "by_field_attempted_stokes_i_samples": split_total(
                    accepted_by_spw[spw], field_keys
                ),
                "by_field_accepted_stokes_i_samples": split_total(
                    accepted_by_spw[spw], field_keys
                ),
            },
        }
        for spw in spw_keys
    }


def valid_receipt(contract: dict) -> dict:
    runtime = parse_runtime_log(synthetic_log())
    runtime["compact_programs"]["builds"] = 100
    return {
        "schema_version": 1,
        "workload_id": contract["workload_id"],
        "source": {"state_sha256": "source"},
        "build": {
            "profile": "release",
            "command": contract["build"]["command"],
            "binary": "/tmp/target/release/casars-imager",
            "timed_build_seconds": 0.0,
            "completed_before_timed_region": True,
            "binary_sha256": "a" * 64,
        },
        "process": {
            "exit_code": 0,
            "command": ["/tmp/target/release/casars-imager"],
            "wall_seconds": 400.0,
        },
        "runtime": runtime,
        "selection": {
            "field_ids": contract["workload"]["field_ids"],
            "spw_ids": contract["workload"]["spw_ids"],
            "accounting_sha256": contract["dataset"]["selection_accounting_sha256"],
            "by_spw": valid_selection_accounting(contract),
        },
        "comparison": None,
        "host_telemetry": {
            "status": "measured",
            "summary": {
                "process_physical_footprint_bytes_peak": 4 * 1024**3,
                "swapin_bytes_delta": 0,
                "swapout_bytes_delta": 0,
            },
        },
    }


class VlassFiveMinuteContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)
        self.contract["baseline"]["status"] = "qualification"

    def test_runtime_command_preserves_scientific_shape(self) -> None:
        command = runtime_command(
            self.contract,
            binary=pathlib.Path("/tmp/target/release/casars-imager"),
            output=pathlib.Path("/tmp/output"),
        )
        joined = " ".join(command)
        self.assertIn("/release/casars-imager", command[0])
        self.assertIn("--imsize 4096", joined)
        self.assertIn("--channel-count 24", joined)
        self.assertIn("--field 1107~1127,1512~1532,1542~1562", joined)
        self.assertIn("--usepointing", command)
        self.assertIn("--wprojplanes 32", joined)
        self.assertIn("--nterms 2", joined)

    def test_parser_captures_timed_refresh_and_miss_pressure(self) -> None:
        parsed = parse_runtime_log(synthetic_log())
        self.assertEqual(300.0, parsed["metric"]["seconds"])
        self.assertEqual(1, parsed["replay"]["residual_refresh_count"])
        self.assertEqual(
            12_000_000_000, parsed["compact_programs"]["logical_program_bytes"]
        )
        self.assertEqual(60, parsed["source_order"]["windows"])
        self.assertAlmostEqual(1000 / 101000, parsed["application_cache"]["hit_rate"])
        self.assertGreater(parsed["application_cache"]["eviction_load_ratio"], 0.99)
        self.assertEqual(0, parsed["application_cache"]["rejected_samples"])
        self.assertEqual([0, 15], parsed["awproject_plan"]["cf_mueller"])
        self.assertEqual(PRODUCTS, parsed["products"]["inventory"])

    def test_qualification_guard_accepts_release_receipt(self) -> None:
        receipt = valid_receipt(self.contract)
        errors = evaluate_receipt(
            self.contract,
            receipt,
            expected_receipt_sha256="receipt",
            actual_receipt_sha256="receipt",
            current_source_state_sha256="source",
        )
        self.assertEqual([], errors)

    def test_guard_rejects_debug_or_timed_build(self) -> None:
        receipt = valid_receipt(self.contract)
        receipt["build"]["profile"] = "debug"
        receipt["build"]["timed_build_seconds"] = 1.0
        errors = evaluate_receipt(
            self.contract,
            receipt,
            expected_receipt_sha256="receipt",
            actual_receipt_sha256="receipt",
            current_source_state_sha256="source",
        )
        self.assertIn("timed executable was not release optimized", errors)
        self.assertIn("build time entered timed region", errors)

    def test_guard_is_pure_and_rejects_relaxed_cache_pressure(self) -> None:
        receipt = valid_receipt(self.contract)
        receipt["runtime"]["application_cache"]["hit_rate"] = 0.9
        before = json.dumps(receipt, sort_keys=True)
        errors = evaluate_receipt(
            self.contract,
            receipt,
            expected_receipt_sha256="receipt",
            actual_receipt_sha256="receipt",
            current_source_state_sha256="source",
        )
        self.assertEqual(before, json.dumps(receipt, sort_keys=True))
        self.assertIn(
            "application-cache hit rate no longer reproduces the miss-heavy regime",
            errors,
        )

    def test_guard_rejects_missing_field_sample_accounting(self) -> None:
        receipt = valid_receipt(self.contract)
        del receipt["selection"]["by_spw"]["2"]["samples"][
            "by_field_accepted_stokes_i_samples"
        ]["1107"]
        errors = evaluate_receipt(
            self.contract,
            receipt,
            expected_receipt_sha256="receipt",
            actual_receipt_sha256="receipt",
            current_source_state_sha256="source",
        )
        self.assertIn(
            "SPW 2 sample accounting does not cover all 63 fields",
            errors,
        )

    def test_minimum_improvement_guard_reads_retained_controller_metric(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            results = root / "autoresearch-results"
            results.mkdir()
            (results / "run.json").write_text(
                json.dumps(
                    {
                        "metric": {
                            "name": self.contract["metric"]["name"],
                            "direction": "lower",
                        }
                    }
                ),
                encoding="utf-8",
            )
            (results / "events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"event": "baseline", "metric": 76.5}),
                        json.dumps(
                            {
                                "event": "iteration",
                                "retained_metric": 70.25,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with mock.patch.object(vlass_5m, "REPO_ROOT", root):
                retained = vlass_5m.autoresearch_retained_metric(self.contract)
        self.assertEqual(70.25, retained)

    def test_proxy_comparison_uses_full_topology_and_normalized_rms(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            request = comparison_request(
                self.contract,
                candidate_prefix=root / "candidate",
                baseline_prefix=root / "baseline",
                run_root=root,
            )
        self.assertEqual("full", request["mode"])
        self.assertTrue(request["tolerances"]["require_full_array"])
        self.assertEqual(
            0.001,
            request["tolerances"]["default"]["diff_rms_over_right_rms"],
        )
        self.assertTrue(request["tolerances"]["default"]["require_topology_parity"])
        self.assertTrue(request["require_metadata_parity"])

    def test_frozen_mode_requires_comparison_pass(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["baseline"]["status"] = "frozen"
        receipt = valid_receipt(contract)
        errors = evaluate_receipt(
            contract,
            receipt,
            expected_receipt_sha256="receipt",
            actual_receipt_sha256="receipt",
            current_source_state_sha256="source",
        )
        self.assertIn("proxy output comparison failed", errors)


if __name__ == "__main__":
    unittest.main()
