#!/usr/bin/env python3
"""Focused unit tests for the T44 frozen-product comparator."""

from __future__ import annotations

import copy
import importlib.util
import math
import sys
import unittest
from pathlib import Path

import numpy as np


MODULE_PATH = Path(__file__).with_name("t44_mtmfs_products_compare.py")
SPEC = importlib.util.spec_from_file_location("t44_mtmfs_products_compare", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


COMMON_BEAM = {
    "major_rad": 4.0e-4,
    "minor_rad": 2.0e-4,
    "position_angle_rad": 0.4,
}


def fixture() -> tuple[dict[str, MODULE.Product], dict]:
    casa: dict[str, MODULE.Product] = {}
    members = []
    for ordinal, name in enumerate(sorted(MODULE.EXPECTED_PRODUCTS), start=1):
        shape = MODULE.STATE_SHAPE if name in MODULE.STATE_PRODUCTS else MODULE.IMAGE_SHAPE
        size = math.prod(shape)
        values = np.arange(1, size + 1, dtype=np.float64).reshape(shape) * ordinal / 10.0
        validity = np.ones(shape, dtype=bool)
        if name in MODULE.ALPHA_PRODUCTS:
            validity.reshape(-1)[-1] = False
            values.reshape(-1)[-1] = 0.0
        beam = COMMON_BEAM if name in MODULE.COMMON_BEAM_PRODUCTS else None
        if name == ".psf.tt0":
            beam = COMMON_BEAM
        casa[name] = MODULE.Product(values, validity, shape, "", beam)
        members.append(
            {
                "name": name,
                "shape": list(shape),
                "unit": MODULE.PRODUCT_UNITS[name],
                "payload": values.reshape(-1).tolist(),
                "validity": validity.reshape(-1).tolist(),
                "beam": beam,
            }
        )

    by_name = {member["name"]: member for member in members}
    i0 = np.asarray(by_name[".image.tt0"]["payload"])
    i1 = np.asarray(by_name[".image.tt1"]["payload"])
    pb = np.full_like(i0, 0.8)
    by_name[".pb.tt0"]["payload"] = pb.tolist()
    by_name[".image.tt0.pbcor"]["payload"] = (i0 / pb).tolist()
    by_name[".image.tt1.pbcor"]["payload"] = (i1 / pb).tolist()
    alpha_validity = np.asarray(by_name[".alpha"]["validity"], dtype=bool)
    alpha = np.zeros_like(i0)
    alpha[alpha_validity] = (i1 / i0)[alpha_validity]
    by_name[".alpha"]["payload"] = alpha.tolist()

    for name, member in by_name.items():
        casa[name] = MODULE.Product(
            np.asarray(member["payload"], dtype=np.float64).reshape(member["shape"]),
            np.asarray(member["validity"], dtype=bool).reshape(member["shape"]),
            tuple(member["shape"]),
            "",
            member["beam"],
        )
    return casa, {"schema": MODULE.RUST_SCHEMA, "members": members}


class T44ComparatorTests(unittest.TestCase):
    def test_matching_inventory_products_masks_units_beams_and_algebra_pass(self) -> None:
        casa, rust = fixture()
        summary = MODULE.compare_documents(casa, rust)
        self.assertTrue(summary["pass"], summary["failures"])
        self.assertEqual(summary["inventory"]["casa_rs"], sorted(MODULE.EXPECTED_PRODUCTS))

    def test_weight_member_and_alpha_pbcor_are_forbidden_by_exact_inventory(self) -> None:
        casa, rust = fixture()
        extra = copy.deepcopy(rust["members"][0])
        extra["name"] = ".weight.tt0"
        rust["members"].append(extra)
        summary = MODULE.compare_documents(casa, rust)
        self.assertFalse(summary["pass"])
        self.assertIn("inventory", summary["failures"])

    def test_alpha_mask_and_zero_blanking_are_exact(self) -> None:
        casa, rust = fixture()
        alpha = next(member for member in rust["members"] if member["name"] == ".alpha")
        alpha["validity"][-1] = True
        alpha["payload"][-1] = 4.0
        summary = MODULE.compare_documents(casa, rust)
        self.assertFalse(summary["pass"])
        self.assertIn(".alpha_validity", summary["failures"])
        self.assertIn(".alpha_zero_blanking", summary["failures"])

    def test_product_unit_and_pbcor_algebra_fail_closed(self) -> None:
        casa, rust = fixture()
        model = next(member for member in rust["members"] if member["name"] == ".model.tt0")
        model["unit"] = "jy_per_beam"
        pbcor = next(
            member for member in rust["members"] if member["name"] == ".image.tt1.pbcor"
        )
        pbcor["payload"] = [value * 2.0 for value in pbcor["payload"]]
        summary = MODULE.compare_documents(casa, rust)
        self.assertFalse(summary["pass"])
        self.assertIn(".model.tt0_unit", summary["failures"])
        self.assertIn("pbcor_tt1_algebra", summary["failures"])


if __name__ == "__main__":
    unittest.main()
