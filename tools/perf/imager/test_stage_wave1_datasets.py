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
import generate_wave1_casa_datasets as generate  # noqa: E402


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

    def test_alma_loc_conversion_matches_casa_simutil(self) -> None:
        position = stage.alma_loc_to_itrf(15.93453511, -700.6757482, -2.32967552)

        self.assertAlmostEqual(position[0], 2_225_052.37659287, places=7)
        self.assertAlmostEqual(position[1], -5_440_045.71553472, places=7)
        self.assertAlmostEqual(position[2], -2_481_673.80672726, places=7)

    def test_single_field_request_uses_casa_pointing_phase_center(self) -> None:
        spec = stage.select_datasets(
            self.registry,
            dataset_ids=["wave1-alma-single-small"],
            tiers=None,
            instruments=None,
        )
        dataset = stage.build_plan(
            self.registry,
            spec,
            self.data_root,
            allow_non_external_large_root=False,
        )["datasets"][0]

        request = stage.build_casars_simobserve_request(dataset)["request"]

        self.assertAlmostEqual(request["phase_center_rad"][0], -1.5707940753201612)
        self.assertAlmostEqual(request["phase_center_rad"][1], -0.4014237906432261)

    def test_fits_string_cards_start_in_standard_value_field(self) -> None:
        card = stage.format_card("CTYPE1", "'RA---SIN'")

        self.assertEqual("CTYPE1  = 'RA---SIN'", card[:20])
        self.assertEqual(80, len(card))

    def test_source_model_is_continuous_across_negative_ra_axis(self) -> None:
        above = stage.source_pixel(148, 255, 512, "single")
        below = stage.source_pixel(148, 256, 512, "single")

        self.assertLess(abs(above - below) / max(above, below), 0.01)

    def test_casa_generator_reuses_staging_source_model(self) -> None:
        staged = stage.source_plane(32, "single")
        casa = generate.structured_plane(32, "single")

        self.assertEqual(staged.shape, casa.shape)
        self.assertEqual(float(staged.max()), float(casa.max()))
        self.assertEqual(
            [stage.spectral_total_scale(8, channel) for channel in range(8)],
            generate.spectral_profile(8),
        )


if __name__ == "__main__":
    unittest.main()
