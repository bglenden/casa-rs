#!/usr/bin/env python3
"""Tests for Wave 1 dataset staging policy."""

from __future__ import annotations

import copy
import pathlib
import sys
import unittest


TOOL_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import stage_wave1_datasets as stage  # noqa: E402


class StageWave1DatasetsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = stage.read_json(stage.REGISTRY_PATH)
        self.data_root = pathlib.Path("/Volumes/GLENDENNING/casa-rs-imperformance")

    def test_full_plan_uses_one_mosaic_large_dataset(self) -> None:
        specs = stage.select_datasets(
            self.registry,
            dataset_ids=None,
            tiers=None,
            instruments=None,
        )

        plan = stage.build_plan(
            self.registry,
            specs,
            self.data_root,
            allow_non_external_large_root=False,
        )

        large = [dataset for dataset in plan["datasets"] if dataset["tier"] == "large"]
        self.assertEqual(["wave1-alma-mosaic-large"], [dataset["id"] for dataset in large])
        self.assertEqual(
            [
                "standard-mfs-dirty-control",
                "standard-mfs-clean-current",
                "standard-cube-line",
                "mosaic-mfs-clean-primary",
                "mosaic-cube-bounded",
                "mtmfs-wideband-sentinel",
            ],
            large[0]["selected_modes"],
        )

    def test_mosaic_large_workloads_select_standard_or_mosaic_gridder(self) -> None:
        spec = stage.select_datasets(
            self.registry,
            dataset_ids=["wave1-alma-mosaic-large"],
            tiers=None,
            instruments=None,
        )
        dataset = stage.build_plan(
            self.registry,
            spec,
            self.data_root,
            allow_non_external_large_root=False,
        )["datasets"][0]

        standard = stage.build_workload_manifest(dataset, "standard-cube-line")
        mfs = stage.build_workload_manifest(dataset, "standard-mfs-dirty-control")
        mosaic = stage.build_workload_manifest(dataset, "mosaic-cube-bounded")
        all_channels = dataset["shape"]["channels"]

        self.assertEqual("standard", standard["imaging"]["gridder"])
        self.assertEqual("0", standard["imaging"]["field"])
        self.assertEqual(all_channels, standard["imaging"]["channel_count"])
        self.assertEqual("mfs", mfs["imaging"]["specmode"])
        self.assertEqual(all_channels, mfs["imaging"]["channel_count"])
        self.assertEqual("mosaic", mosaic["imaging"]["gridder"])
        self.assertEqual("", mosaic["imaging"]["field"])
        self.assertEqual(0, mosaic["imaging"]["phasecenter_field"])
        self.assertEqual(32, mosaic["imaging"]["channel_count"])

    def test_large_tier_policy_rejects_multiple_large_datasets(self) -> None:
        registry = copy.deepcopy(self.registry)
        duplicate = copy.deepcopy(registry["datasets"][-1])
        duplicate["id"] = "wave1-extra-large"
        registry["datasets"].append(duplicate)
        specs = stage.select_datasets(
            registry,
            dataset_ids=None,
            tiers=["large"],
            instruments=None,
        )

        with self.assertRaisesRegex(stage.DatasetError, "large tier policy expects 1"):
            stage.build_plan(
                registry,
                specs,
                self.data_root,
                allow_non_external_large_root=False,
            )


if __name__ == "__main__":
    unittest.main()
