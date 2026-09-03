#!/usr/bin/env python3
"""Focused tests for the T51 frozen-VLASS acceptance gate."""

from __future__ import annotations

import copy
import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("t51_aw_vlass_acceptance.py")
SPEC = importlib.util.spec_from_file_location("t51_aw_vlass_acceptance", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


PRODUCTS = [".image.tt0", ".residual.tt0", ".psf.tt0", ".weight.tt0"]
PREFIX = pathlib.Path("/frozen/casa")


def workload() -> dict:
    return {
        "id": "t51-test",
        "imaging": {
            "gridder": "awproject",
            "wterm": "wproject",
            "wprojplanes": 32,
            "field": "1107~1127,1512~1532,1542~1562",
            "spw": "2~17",
            "channel_count": 64,
            "imsize": 4096,
            "nterms": 2,
            "aterm": True,
            "psterm": False,
            "wbawp": True,
            "conjbeams": True,
            "usepointing": True,
            "computepastep": 360.0,
            "rotatepastep": 360.0,
            "pointingoffsetsigdev": 0.0,
            "normtype": "flatnoise",
            "mosweight": False,
            "psfphasecenter": "",
            "vptable": "",
            "facets": 1,
            "uvrange": "<12km",
            "intent": "OBSERVE_TARGET#UNSPECIFIED",
            "smallscalebias": 0.0,
            "restoringbeam": "common",
        },
        "comparison": {"products": PRODUCTS},
        "run": {"cf_cache_role": "cold"},
    }


def receipt() -> dict:
    product = {
        "status": "compared",
        "metadata": {"status": "matched"},
        "topology_parity": True,
        "full_array": {
            "diff_rms_over_right_rms": 0.0009,
            "topology": {
                "finite_equal": True,
                "mask_equal": True,
                "nonfinite_kind_equal": True,
            },
        },
    }
    return {
        "status": "completed",
        "exit_code": 0,
        "workload": {"id": "t51-test"},
        "mode": {"image_shape": [4096, 4096], "gridder": "awproject", "nterms": 2},
        "run": {
            "skip_casa": "1",
            "skip_rust": "0",
            "reuse_casa_prefix": str(PREFIX),
            "cf_cache_role": "cold",
        },
        "benchmark_features": {
            "visibility": {
                "selected_rows": 20_000,
                "selected_channels": 64,
                "correlations": 1,
                "visibility_work": 1_280_000,
            },
            "resources": {"peak_rss_bytes": 8 * 1024**3},
        },
        "results": {
            "rust": {"status": "ran"},
            "casa": {"status": "reused"},
            "product_comparison": {
                "status": "completed",
                "product_inventory": {"status": "matched", "observed_match": True},
                "products": {suffix: copy.deepcopy(product) for suffix in PRODUCTS},
                "tolerance_evaluation": {"status": "passed"},
            },
        },
    }


class T51AwVlassAcceptanceTests(unittest.TestCase):
    def test_preflight_requires_runnable_aw_route(self) -> None:
        candidate = receipt()
        candidate["status"] = "dry_run"
        candidate["run_support"] = {
            "status": "runnable",
            "targets": {"rust": {"status": "runnable", "reason": None}},
        }
        result = GATE.validate_preflight(
            candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
        )
        self.assertEqual("runnable", result["status"])

    def test_complete_production_receipt_passes(self) -> None:
        result = GATE.validate_receipt(
            receipt(), expected_workload=workload(), expected_casa_prefix=PREFIX
        )
        self.assertEqual(1_280_000, result["selected_samples"])
        self.assertEqual(len(PRODUCTS), result["product_count"])

    def test_exact_native_aw_command_binding_passes(self) -> None:
        candidate = receipt()
        prepared = pathlib.Path("/validated/cfs")
        rust = pathlib.Path("/fresh/shared/dirty")
        candidate["command"] = {
            "argv": [str(GATE.ROOT / "scripts/bench-imager-vs-casa.sh"), "/data.ms"],
            "env": {
                "IMAGER_BENCH_PREPARED_AW_CASA_CACHE": str(prepared),
                "IMAGER_BENCH_RUST_OUTPUT_PREFIX": str(rust),
                "IMAGER_BENCH_AW_CF_RESIDENT_MB": "384",
                "IMAGER_BENCH_ATERM": "1",
                "IMAGER_BENCH_PSTERM": "0",
                "IMAGER_BENCH_WBAWP": "1",
                "IMAGER_BENCH_CONJBEAMS": "1",
                "IMAGER_BENCH_USEPOINTING": "1",
                "IMAGER_BENCH_COMPUTEPASTEP": "360.0",
                "IMAGER_BENCH_ROTATEPASTEP": "360.0",
                "IMAGER_BENCH_POINTINGOFFSETSIGDEV": "0.0",
                "IMAGER_BENCH_NORMTYPE": "flatnoise",
                "IMAGER_BENCH_MOSWEIGHT": "0",
                "IMAGER_BENCH_PSFPHASECENTER": "",
                "IMAGER_BENCH_VPTABLE": "",
                "IMAGER_BENCH_GRIDDER": "awproject",
                "IMAGER_BENCH_WTERM": "wproject",
                "IMAGER_BENCH_WPROJPLANES": "32",
                "IMAGER_BENCH_FACETS": "1",
                "IMAGER_BENCH_UVRANGE": "<12km",
                "IMAGER_BENCH_INTENT": "OBSERVE_TARGET#UNSPECIFIED",
                "IMAGER_BENCH_MASK_IMAGE": "",
                "IMAGER_BENCH_SMALL_SCALE_BIAS": "0.0",
                "IMAGER_BENCH_RESTORING_BEAM": "common",
            },
        }
        candidate["products"] = {"rust_prefix": str(rust)}
        candidate["results"]["product_paths"] = {
            "rust_prefix": str(rust),
            "casa_prefix": str(PREFIX),
        }
        GATE.validate_receipt(
            candidate,
            expected_workload=workload(),
            expected_casa_prefix=PREFIX,
            expected_prepared_aw_casa_cache=prepared,
            expected_rust_prefix=rust,
        )

    def test_prepared_manifest_snapshot_is_content_bound(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            entry = root / "cell"
            entry.mkdir()
            manifest = entry / "manifest.json"
            manifest.write_text('{"identity":"cold"}\n', encoding="utf-8")
            cold = GATE.prepared_store_snapshot(root)
            self.assertEqual(["cell/manifest.json"], list(cold))
            manifest.write_text('{"identity":"mutated"}\n', encoding="utf-8")
            self.assertNotEqual(cold, GATE.prepared_store_snapshot(root))

    def test_missing_measured_rss_fails_closed(self) -> None:
        candidate = receipt()
        candidate["benchmark_features"]["resources"]["peak_rss_bytes"] = None
        with self.assertRaisesRegex(GATE.GateError, "peak_rss_bytes"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

    def test_topology_mismatch_fails_closed(self) -> None:
        candidate = receipt()
        candidate["results"]["product_comparison"]["products"][".psf.tt0"][
            "topology_parity"
        ] = False
        with self.assertRaisesRegex(GATE.GateError, "validity topology differs"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

    def test_exact_32_gib_is_not_under_the_ceiling(self) -> None:
        candidate = receipt()
        candidate["benchmark_features"]["resources"]["peak_rss_bytes"] = 32 * 1024**3
        with self.assertRaisesRegex(GATE.GateError, "not below 32 GiB"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

    def test_rms_above_contract_fails(self) -> None:
        candidate = receipt()
        candidate["results"]["product_comparison"]["products"][".image.tt0"][
            "full_array"
        ]["diff_rms_over_right_rms"] = 0.0010001
        with self.assertRaisesRegex(GATE.GateError, "normalized RMS exceeds"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )


if __name__ == "__main__":
    unittest.main()
