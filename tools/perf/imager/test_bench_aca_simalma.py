#!/usr/bin/env python3
"""Focused tests for the ACA/simalma simulation breadth harness."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_aca_simalma


class AcaSimalmaHarnessTests(unittest.TestCase):
    def test_preflight_accepts_sparse_lfs_targets_and_real_configs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "casatestdata"
            config_root = Path(tmp) / "simmos"
            (root / "fits").mkdir(parents=True)
            (root / "fits" / "M51ha.fits").write_text("fits", encoding="utf-8")
            (root / "image" / "m51ha.model").mkdir(parents=True)
            (root / "image" / "m51ha.model" / "table.dat").write_text(
                "model", encoding="utf-8"
            )
            (root / "regression" / "sim_multi_arrays_and_TP" / "m51c_reference").mkdir(
                parents=True
            )
            config_root.mkdir()
            for name in (
                "alma.cycle6.3.cfg",
                "aca.cycle6.cfg",
                "alma.out07.cfg",
                "aca.i.cfg",
                "aca.tp.cfg",
            ):
                (config_root / name).write_text("0 0 0 12 DA01\n", encoding="utf-8")

            preflight = bench_aca_simalma.build_preflight(
                ["simalma", "aca"],
                root,
                config_root,
            )

            self.assertEqual(preflight["simalma"]["status"], "available")
            self.assertEqual(preflight["aca"]["status"], "available")
            self.assertEqual(
                preflight["simalma"]["inputs"]["m51ha_fits"]["selected_relative_path"],
                "fits/M51ha.fits",
            )
            self.assertEqual(preflight["aca"]["configs"]["aca.tp.cfg"]["status"], "available")

    def test_preflight_uses_regression_symlink_fallback_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "casatestdata"
            config_root = Path(tmp) / "simmos"
            (root / "regression" / "simalma_12m_ACA_combination").mkdir(
                parents=True
            )
            (root / "regression" / "simalma_12m_ACA_combination" / "M51ha.fits").write_text(
                "fits", encoding="utf-8"
            )
            config_root.mkdir()
            for name in ("alma.cycle6.3.cfg", "aca.cycle6.cfg", "aca.tp.cfg"):
                (config_root / name).write_text("0 0 0 12 DA01\n", encoding="utf-8")

            preflight = bench_aca_simalma.build_preflight(
                ["simalma"],
                root,
                config_root,
            )

            self.assertEqual(preflight["simalma"]["status"], "available")
            self.assertEqual(
                preflight["simalma"]["inputs"]["m51ha_fits"]["selected_relative_path"],
                "regression/simalma_12m_ACA_combination/M51ha.fits",
            )

    def test_stage_inputs_records_missing_optional_reference_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "casatestdata"
            config_root = Path(tmp) / "simmos"
            stage_root = Path(tmp) / "stage"
            (root / "fits").mkdir(parents=True)
            (root / "fits" / "M51ha.fits").write_text("fits", encoding="utf-8")
            (root / "image" / "m51ha.model").mkdir(parents=True)
            (root / "image" / "m51ha.model" / "table.dat").write_text(
                "model", encoding="utf-8"
            )
            config_root.mkdir()
            for name in ("alma.out07.cfg", "aca.i.cfg", "aca.tp.cfg"):
                (config_root / name).write_text("0 0 0 7 CM01\n", encoding="utf-8")
            preflight = bench_aca_simalma.build_preflight(["aca"], root, config_root)

            staged = bench_aca_simalma.stage_all_inputs(preflight, stage_root)

            self.assertEqual(preflight["aca"]["status"], "available")
            self.assertEqual(staged["aca"]["inputs"]["m51ha_fits"]["status"], "staged")
            self.assertEqual(staged["aca"]["inputs"]["m51c_reference"]["status"], "missing")
            self.assertTrue(Path(staged["aca"]["configs"]["aca.i.cfg"]["path"]).exists())

    def test_native_family_plan_uses_family_json_and_real_config_path(self) -> None:
        staged = {
            "inputs": {
                "m51ha_fits": {
                    "status": "staged",
                    "path": "/tmp/M51ha.fits",
                }
            },
            "configs": {
                "aca.i.cfg": {
                    "status": "staged",
                    "path": "/tmp/aca.i.cfg",
                }
            },
        }

        plan = bench_aca_simalma.native_family_plan(
            "aca-7m-interferometric",
            staged,
            Path("/tmp/native"),
            model_key="m51ha_fits",
            config_name="aca.i.cfg",
            telescope="ACA",
            imaging_mode="mosaic",
            observation_mode="interferometric",
            target_gib=0.5,
            pointing_count=7,
            polarizations=1,
            row_workers=4,
            channel_workers=8,
        )

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["request"]["kind"], "family")
        request = plan["request"]["request"]
        self.assertEqual(request["source_model"]["kind"], "fits_image")
        self.assertEqual(request["array_config"], "/tmp/aca.i.cfg")
        self.assertEqual(request["observation_mode"], "interferometric")
        self.assertEqual(request["pointing_count"], 7)
        self.assertEqual(request["worker_policy"], "auto")
        self.assertEqual(request["row_workers"], 4)
        self.assertEqual(request["channel_workers"], 8)

    def test_closeout_gate_blocks_without_casa_native_comparisons(self) -> None:
        preflight = {
            "simalma": {
                "status": "available",
                "missing_required": [],
            }
        }
        casa = {"status": "not_run"}
        native = {"status": "not_run"}
        comparisons = {
            "simalma": {
                "status": "not_available",
                "reason": "same-input CASA/native end-to-end comparison has not run",
            }
        }

        gate = bench_aca_simalma.evaluate_closeout_gate(
            ["simalma"],
            preflight,
            casa,
            native,
            comparisons,
            required_native_throughput_mb_s=500.0,
        )

        self.assertEqual(gate["status"], "blocked")
        reasons = [blocker["reason"] for blocker in gate["blockers"]]
        self.assertIn("CASA oracle did not run successfully", reasons)
        self.assertIn("native implemented slices did not run successfully", reasons)
        self.assertIn("same-input CASA/native end-to-end comparison has not run", reasons)

    def test_run_assessment_preflight_only_writes_scripts_and_returns_blocked_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "casatestdata"
            config_root = Path(tmp) / "simmos"
            output_dir = Path(tmp) / "out"
            (root / "fits").mkdir(parents=True)
            (root / "fits" / "M51ha.fits").write_text("fits", encoding="utf-8")
            config_root.mkdir()
            for name in ("alma.cycle6.3.cfg", "aca.cycle6.cfg", "aca.tp.cfg"):
                (config_root / name).write_text("0 0 0 12 DA01\n", encoding="utf-8")
            args = argparse.Namespace(
                scenario="simalma",
                output_dir=output_dir,
                testdata_root=root,
                config_root=config_root,
                casars_binary=Path("/missing/simobserve"),
                casars_imager_binary=Path("/missing/casars-imager"),
                casa_python=sys.executable,
                preflight_only=True,
                skip_casa=False,
                skip_native=False,
                run_casa=False,
                run_native=False,
                native_target_gib=0.001,
                native_repeats=1,
                row_workers=None,
                channel_workers=None,
                require_native_throughput_mb_s=500.0,
                allow_incomplete=True,
            )

            result = bench_aca_simalma.run_assessment(args)
            details = result["results"]["aca_simalma"]

            self.assertEqual(details["inputs"]["preflight"]["simalma"]["status"], "available")
            self.assertEqual(details["casa"]["status"], "not_run")
            self.assertEqual(details["native"]["status"], "not_run")
            self.assertEqual(details["closeout_gate"]["status"], "blocked")
            script = Path(details["casa"]["programs"]["simalma"]["script"])
            self.assertTrue(script.exists())


if __name__ == "__main__":
    unittest.main()
