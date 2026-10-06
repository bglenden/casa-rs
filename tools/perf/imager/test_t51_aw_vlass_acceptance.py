#!/usr/bin/env python3
"""Focused tests for the T51 frozen-VLASS acceptance gate."""

from __future__ import annotations

import copy
import importlib.util
import pathlib
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("t51_aw_vlass_acceptance.py")
SPEC = importlib.util.spec_from_file_location("t51_aw_vlass_acceptance", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


PRODUCTS = [".image.tt0", ".residual.tt0", ".psf.tt0", ".weight.tt0"]
PREFIX = pathlib.Path("/frozen/casa")
WORKLOAD_ID = "t51-test-dirty"


def workload() -> dict:
    return {
        "id": WORKLOAD_ID,
        "imaging": {
            "mode": "dirty",
            "specmode": "mfs",
            "gridder": "awproject",
            "casa_gridder": "awproject",
            "wterm": "wproject",
            "wprojplanes": 32,
            "field": "1107~1127,1512~1532,1542~1562",
            "phasecenter_field": 1525,
            "spw": "2~17",
            "channel_start": 0,
            "channel_count": 64,
            "imsize": 4096,
            "cell_arcsec": 0.6,
            "datacolumn": "data",
            "stokes": "I",
            "projection": "SIN",
            "interpolation": "linear",
            "uvrange": "<12km",
            "intent": "OBSERVE_TARGET#UNSPECIFIED",
            "weighting": "briggs",
            "robust": 1.0,
            "perchanweightdensity": True,
            "deconvolver": "mtmfs",
            "nterms": 2,
            "scales": [0, 5, 12],
            "smallscalebias": 0.0,
            "niter": 0,
            "gain": 0.1,
            "threshold_jy": 0.0,
            "nsigma": 5.0,
            "minor_cycle_length": 2000,
            "cyclefactor": 3.0,
            "min_psf_fraction": 0.05,
            "max_psf_fraction": 0.8,
            "facets": 1,
            "aterm": True,
            "psterm": False,
            "wbawp": True,
            "conjbeams": True,
            "usepointing": True,
            "computepastep": 360.0,
            "rotatepastep": 360.0,
            "pointingoffsetsigdev": 0.0,
            "pblimit": 0.0001,
            "normtype": "flatnoise",
            "write_pb": True,
            "pbcor": False,
            "restoration": True,
            "restoringbeam": "common",
            "interactive": False,
            "usemask": "user",
            "restart": False,
            "savemodel": "none",
            "calcres": True,
            "calcpsf": True,
            "parallel": False,
            "standard_mfs_acceleration": "cpu",
            "imaging_fft_precision": "auto",
            "imaging_fft_backend": "rustfft",
            "mosweight": False,
            "psfphasecenter": "",
            "vptable": "",
        },
        "comparison": {"products": PRODUCTS},
        "run": {
            "repeats": 1,
            "warmups": 0,
            "ms_staging": "direct",
            "skip_profile": "1",
            "cf_cache_role": "cold",
        },
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
        "workload": {"id": WORKLOAD_ID},
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
            "backend_plan_logs": {
                "aw_cache_inventory": [
                    {
                        "name": "imaging_aw_cache_inventory_summary",
                        "fields": {
                            "paired_cells": 1024,
                            "frequencies": 16,
                            "w_values": 32,
                            "mueller_elements": 2,
                            "parallactic_angles": 1,
                            "prepared_cache_bytes": 9_663_676_416,
                            "decoded_resident_ceiling_bytes": 384 * 1024**2,
                        },
                    }
                ],
                "prepared_artifact_readers": [
                    {
                        "name": "imaging_prepared_artifact_reader_summary",
                        "fields": {
                            "catalog": "a" * 64,
                            "logical_bytes": 9_646_899_200,
                            "decoded_ceiling_bytes": 384 * 1024**2,
                            "decoder_workspace_ceiling_bytes": 512 * 1024,
                            "total_ceiling_bytes": 392 * 1024**2,
                            "reads": 4,
                            "read_bytes": 1_048_576,
                            "read_operations": 32,
                            "resident_peak_bytes": 262_144,
                            "decoder_workspace_peak_bytes": 65_536,
                            "total_peak_resident_bytes": 327_680,
                            "pinned_peak_bytes": 131_072,
                            "hits": 7,
                            "loads": 4,
                            "evicted_bytes": 0,
                            "copied_bytes": 262_144,
                            "aborted": False,
                        },
                    }
                ],
            },
            "product_comparison": {
                "status": "completed",
                "product_inventory": {"status": "matched", "observed_match": True},
                "products": {suffix: copy.deepcopy(product) for suffix in PRODUCTS},
                "tolerance_evaluation": {"status": "passed"},
            },
        },
    }


class T51AwVlassAcceptanceTests(unittest.TestCase):
    def test_checked_in_manifests_use_current_serial_cli_contract(self) -> None:
        for path in (GATE.DIRTY_WORKLOAD, GATE.CLEAN_WORKLOAD):
            candidate = GATE._load_json(path)
            imaging = GATE.validate_manifest_contract(candidate)
            self.assertEqual("cpu", imaging["standard_mfs_acceleration"])
            self.assertEqual("auto", imaging["imaging_fft_precision"])
            self.assertEqual("rustfft", imaging["imaging_fft_backend"])
            self.assertIs(imaging["parallel"], False)
            self.assertTrue(
                all(name not in imaging for name in GATE.FORBIDDEN_RUNTIME_OVERRIDES)
            )

    def test_manifest_rejects_retired_runtime_controls(self) -> None:
        for name, value in (
            ("chanchunks", 1),
            ("standard_mfs_grid_threads", 2),
            ("imaging_memory_target_mb", 16_384),
        ):
            with self.subTest(name=name):
                candidate = workload()
                candidate["imaging"][name] = value
                with self.assertRaisesRegex(GATE.GateError, name):
                    GATE.validate_manifest_contract(candidate)

    def test_manifest_rejects_non_serial_acceleration(self) -> None:
        candidate = workload()
        candidate["imaging"]["standard_mfs_acceleration"] = "metal"
        with self.assertRaisesRegex(GATE.GateError, "standard_mfs_acceleration"):
            GATE.validate_manifest_contract(candidate)

    def test_gate_rejects_unmeasured_storage_profile(self) -> None:
        with self.assertRaisesRegex(GATE.GateError, "SPILL_READ"):
            GATE.validate_storage_profile_environment({})

    def test_gate_accepts_bound_storage_profile(self) -> None:
        GATE.validate_storage_profile_environment(GATE.STORAGE_PROFILE_ENV)

    @mock.patch.object(GATE.subprocess, "run")
    def test_cli_preflight_requires_checked_shell_marker(self, run: mock.Mock) -> None:
        run.return_value = mock.Mock(
            returncode=0,
            stdout=GATE.CLI_PREFLIGHT_MARKER + "\n",
            stderr="",
        )
        candidate = {"command": {"argv": ["/runner", "/missing.ms"], "env": {}}}

        GATE.run_rust_cli_preflight(candidate, base_environment={"BASE": "1"})

        self.assertEqual(["/runner", "/missing.ms"], run.call_args.args[0])
        self.assertEqual(
            "1",
            run.call_args.kwargs["env"]["IMAGER_BENCH_VALIDATE_RUST_CLI_ONLY"],
        )

    @mock.patch.object(GATE.subprocess, "run")
    def test_cli_preflight_fails_without_checked_shell_marker(
        self, run: mock.Mock
    ) -> None:
        run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
        candidate = {"command": {"argv": ["/runner", "/missing.ms"], "env": {}}}
        with self.assertRaisesRegex(GATE.GateError, "validation marker"):
            GATE.run_rust_cli_preflight(candidate, base_environment={})

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
        self.assertEqual(1024, result["prepared_aw"]["inventory"]["paired_cells"])
        self.assertEqual(4, result["prepared_aw"]["reader_sessions"][0]["reads"])

    def test_scientific_beam_roundoff_uses_frozen_contract(self) -> None:
        expected = workload()
        expected["comparison"]["tolerances"] = {
            "contract_version": 2,
            "require_full_array": False,
            "default": {"require_topology_parity": True},
            "products": {
                ".image.tt0": {
                    "beam_kernel_nrmse": 0.001,
                    "beam_area_relative": 0.001,
                }
            },
        }
        candidate = receipt()
        products = candidate["results"]["product_comparison"]["products"]
        for product in products.values():
            product["shape"] = [4096, 4096]
            product["metadata_parity_required"] = True
            left = {
                "status": "complete",
                "shape": [4096, 4096],
                "unit": "Jy/beam",
                "coordinates": {},
                "masks": [],
                "errors": [],
                "restoring_beam": {
                    "major": {"value": 2.9601199626922607, "unit": "arcsec"},
                    "minor": {"value": 2.088184356689453, "unit": "arcsec"},
                    "positionangle": {"value": 71.21408081054688, "unit": "deg"},
                },
            }
            right = copy.deepcopy(left)
            right["restoring_beam"]["major"]["value"] = 2.960120439529419
            product["metadata"] = {
                "status": "mismatch",
                "parity": False,
                "left": left,
                "right": right,
                "field_parity": {
                    "shape": True,
                    "unit": True,
                    "coordinates": True,
                    "restoring_beam": False,
                    "masks": True,
                },
            }
        GATE.validate_receipt(
            candidate, expected_workload=expected, expected_casa_prefix=PREFIX
        )

        for defect in (
            "coordinates",
            "unlinked_beam",
            "missing_beam",
            "failed_beam",
            "no_contract",
        ):
            with self.subTest(defect=defect):
                broken = copy.deepcopy(candidate)
                contract = copy.deepcopy(expected)
                metadata = broken["results"]["product_comparison"]["products"][
                    ".psf.tt0"
                ]["metadata"]
                if defect == "coordinates":
                    metadata["left"]["coordinates"] = {"wrong": 1}
                    metadata["field_parity"]["coordinates"] = False
                elif defect == "unlinked_beam":
                    metadata["left"]["restoring_beam"]["major"]["value"] += 0.01
                elif defect == "missing_beam":
                    metadata["left"]["restoring_beam"] = {}
                elif defect == "failed_beam":
                    for product in broken["results"]["product_comparison"][
                        "products"
                    ].values():
                        product["metadata"]["left"]["restoring_beam"]["major"][
                            "value"
                        ] = 5.0
                else:
                    del contract["comparison"]["tolerances"]
                with self.assertRaises(GATE.GateError):
                    GATE.validate_receipt(
                        broken, expected_workload=contract, expected_casa_prefix=PREFIX
                    )

    def test_missing_aw_inventory_receipt_fails_closed(self) -> None:
        candidate = receipt()
        candidate["results"]["backend_plan_logs"]["aw_cache_inventory"] = []
        with self.assertRaisesRegex(GATE.GateError, "one AW cache inventory"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

    def test_incomplete_aw_catalog_receipt_fails_closed(self) -> None:
        candidate = receipt()
        inventory = candidate["results"]["backend_plan_logs"]["aw_cache_inventory"][0][
            "fields"
        ]
        inventory["paired_cells"] = 1023
        with self.assertRaisesRegex(GATE.GateError, "paired_cells must be 1024"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

    def test_reader_without_real_transfer_fails_closed(self) -> None:
        candidate = receipt()
        reader = candidate["results"]["backend_plan_logs"]["prepared_artifact_readers"][
            0
        ]["fields"]
        reader["reads"] = 0
        with self.assertRaisesRegex(GATE.GateError, "reads must be a positive"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

    def test_reader_residency_above_ceiling_fails_closed(self) -> None:
        candidate = receipt()
        reader = candidate["results"]["backend_plan_logs"]["prepared_artifact_readers"][
            0
        ]["fields"]
        reader["resident_peak_bytes"] = 384 * 1024**2 + 1
        reader["total_peak_resident_bytes"] = 384 * 1024**2 + 1
        with self.assertRaisesRegex(GATE.GateError, "decoded residency ceiling"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

    def test_reader_decoder_workspace_above_ceiling_fails_closed(self) -> None:
        candidate = receipt()
        reader = candidate["results"]["backend_plan_logs"]["prepared_artifact_readers"][
            0
        ]["fields"]
        reader["decoder_workspace_peak_bytes"] = (
            reader["decoder_workspace_ceiling_bytes"] + 1
        )
        reader["total_peak_resident_bytes"] += 1
        with self.assertRaisesRegex(GATE.GateError, "decoder workspace ceiling"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

    def test_aborted_reader_receipt_fails_closed(self) -> None:
        candidate = receipt()
        reader = candidate["results"]["backend_plan_logs"]["prepared_artifact_readers"][
            0
        ]["fields"]
        reader["aborted"] = True
        with self.assertRaisesRegex(GATE.GateError, "was aborted"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )

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
                "IMAGER_BENCH_SPECMODE": "mfs",
                "IMAGER_BENCH_FIELD": "1107~1127,1512~1532,1542~1562",
                "IMAGER_BENCH_PHASECENTER_FIELD": "1525",
                "IMAGER_BENCH_SPW": "2~17",
                "IMAGER_BENCH_CHANNEL_START": "0",
                "IMAGER_BENCH_CHANNEL_COUNT": "64",
                "IMAGER_BENCH_IMSIZE": "4096",
                "IMAGER_BENCH_CELL_ARCSEC": "0.6",
                "IMAGER_BENCH_STOKES": "I",
                "IMAGER_BENCH_INTERPOLATION": "linear",
                "IMAGER_BENCH_WEIGHTING": "briggs",
                "IMAGER_BENCH_ROBUST": "1.0",
                "IMAGER_BENCH_PERCHANWEIGHTDENSITY": "1",
                "IMAGER_BENCH_DECONVOLVER": "mtmfs",
                "IMAGER_BENCH_NTERMS": "2",
                "IMAGER_BENCH_SCALES": "0,5,12",
                "IMAGER_BENCH_NITER": "0",
                "IMAGER_BENCH_NMAJOR": "-1",
                "IMAGER_BENCH_GAIN": "0.1",
                "IMAGER_BENCH_THRESHOLD_JY": "0.0",
                "IMAGER_BENCH_NSIGMA": "5.0",
                "IMAGER_BENCH_MINOR_CYCLE_LENGTH": "2000",
                "IMAGER_BENCH_CYCLEFACTOR": "3.0",
                "IMAGER_BENCH_MIN_PSFFRACTION": "0.05",
                "IMAGER_BENCH_MAX_PSFFRACTION": "0.8",
                "IMAGER_BENCH_PBLIMIT": "0.0001",
                "IMAGER_BENCH_WRITE_PB": "1",
                "IMAGER_BENCH_PBCOR": "0",
                "IMAGER_BENCH_USEMASK": "user",
                "IMAGER_BENCH_SAVEMODEL": "none",
                "IMAGER_BENCH_STANDARD_MFS_ACCELERATION": "cpu",
                "IMAGER_BENCH_IMAGING_FFT_PRECISION": "auto",
                "IMAGER_BENCH_IMAGING_FFT_BACKEND": "rustfft",
                "IMAGER_BENCH_PARALLEL": "0",
                "IMAGER_BENCH_WTERM": "wproject",
                "IMAGER_BENCH_WPROJPLANES": "32",
                "IMAGER_BENCH_FACETS": "1",
                "IMAGER_BENCH_UVRANGE": "<12km",
                "IMAGER_BENCH_INTENT": "OBSERVE_TARGET#UNSPECIFIED",
                "IMAGER_BENCH_MASK_IMAGE": "",
                "IMAGER_BENCH_SMALL_SCALE_BIAS": "0.0",
                "IMAGER_BENCH_RESTORING_BEAM": "common",
                "IMAGER_BENCH_MS_STAGING": "direct",
                "IMAGER_BENCH_SKIP_CASA": "1",
                "IMAGER_BENCH_SKIP_RUST": "0",
                "IMAGER_BENCH_SKIP_PROFILE": "1",
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

    def test_cold_checkpoint_survives_clean_failure(self) -> None:
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            output = root / "receipts"
            snapshot = {"cell/manifest.json": {"sha256": "cold", "size": 3}}
            argv = [str(SCRIPT)]
            for flag, path in {
                "dirty-casa-prefix": root / "dirty",
                "clean-casa-prefix": root / "clean",
                "output-dir": output,
                "artifact-root": root / "artifacts",
                "cf-cache-root": root / "oracle",
                "prepared-aw-casa-cache": root,
                "prepared-aw-shared-parent": root / "shared",
            }.items():
                argv.extend(["--" + flag, str(path)])

            def run_workload(**kwargs):
                if kwargs["workload_path"] == GATE.CLEAN_WORKLOAD:
                    self.assertEqual(
                        snapshot,
                        json.loads((output / "t51-cold-store-checkpoint.json").read_text()),
                    )
                    raise GATE.GateError("injected CLEAN failure")
                return output / "dirty.json"

            with (
                mock.patch("sys.argv", argv),
                mock.patch.object(GATE, "validate_storage_profile_environment"),
                mock.patch.object(GATE, "validate_manifest_contract"),
                mock.patch.object(GATE, "_load_json", return_value={}),
                mock.patch.object(GATE, "validate_receipt", return_value={}),
                mock.patch.object(GATE, "EXPECTED_PREPARED_AW_CELLS", 1),
                mock.patch.object(GATE, "prepared_store_snapshot", return_value=snapshot),
                mock.patch.object(GATE, "run_workload", side_effect=run_workload),
            ):
                with self.assertRaisesRegex(GATE.GateError, "injected CLEAN failure"):
                    GATE.main()
            self.assertEqual(
                snapshot,
                json.loads((output / "t51-cold-store-checkpoint.json").read_text()),
            )

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

    def test_non_finite_rms_fails(self) -> None:
        candidate = receipt()
        candidate["results"]["product_comparison"]["products"][".image.tt0"][
            "full_array"
        ]["diff_rms_over_right_rms"] = float("nan")
        with self.assertRaisesRegex(GATE.GateError, "normalized RMS exceeds"):
            GATE.validate_receipt(
                candidate, expected_workload=workload(), expected_casa_prefix=PREFIX
            )


if __name__ == "__main__":
    unittest.main()
