#!/usr/bin/env python3
"""Create the reduced all-63-fields deterministic VLASS mask once."""

from __future__ import annotations

import json
import pathlib
import shutil

import numpy as np
from casatools import image


ROOT = pathlib.Path(
    "/Volumes/Extra Storage (not encrypted)/SoftwareProjects/"
    "casa-rs-vlass/issue-446"
)
SOURCE = ROOT / "masks/vlass-single-field-peak-box-4096.mask"
OUTPUT = ROOT / "masks/vlass-all-fields-peak-box-4096.mask"
RECEIPT = ROOT / "receipts/masks/vlass-all-fields-peak-box-4096.json"
BOX_BLC = (2185, 1945)
BOX_TRC = (2248, 2008)


def main() -> None:
    if not SOURCE.is_dir():
        raise RuntimeError(f"source mask is missing: {SOURCE}")
    if OUTPUT.exists():
        raise RuntimeError(f"refusing to overwrite mask: {OUTPUT}")
    if RECEIPT.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {RECEIPT}")

    shutil.copytree(SOURCE, OUTPUT)
    tool = image()
    try:
        if not tool.open(str(OUTPUT)):
            raise RuntimeError(f"failed to open copied mask: {OUTPUT}")
        pixels = np.asarray(tool.getchunk(), dtype=np.float32)
        if pixels.shape != (4096, 4096, 1, 1):
            raise RuntimeError(f"unexpected copied mask shape: {pixels.shape}")
        pixels.fill(0.0)
        pixels[
            BOX_BLC[0] : BOX_TRC[0] + 1,
            BOX_BLC[1] : BOX_TRC[1] + 1,
            :,
            :,
        ] = 1.0
        tool.putchunk(pixels)
        verified = np.asarray(tool.getchunk(), dtype=np.float32)
    finally:
        tool.done()

    selected = np.argwhere(verified > 0.5)
    if selected.shape != (4096, 4):
        raise RuntimeError(
            f"mask selected {selected.shape[0]} pixels instead of exactly 4096"
        )
    actual_blc = tuple(int(value) for value in selected.min(axis=0)[:2])
    actual_trc = tuple(int(value) for value in selected.max(axis=0)[:2])
    if actual_blc != BOX_BLC or actual_trc != BOX_TRC:
        raise RuntimeError(
            f"mask bounds {actual_blc}..{actual_trc} do not match "
            f"{BOX_BLC}..{BOX_TRC}"
        )

    RECEIPT.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "kind": "vlass_reduced_deterministic_clean_mask",
        "role": "reduced_turnaround_only",
        "source": str(SOURCE),
        "output": str(OUTPUT),
        "shape": list(verified.shape),
        "inclusive_blc": list(BOX_BLC),
        "inclusive_trc": list(BOX_TRC),
        "selected_pixels": int(selected.shape[0]),
        "derivation": {
            "full_geometry_side": 12150,
            "reduced_geometry_side": 4096,
            "full_inclusive_blc": [6243, 6003],
            "full_inclusive_trc": [6306, 6066],
            "phasecenter_field": 1525,
            "cell_arcsec": 0.6,
        },
    }
    RECEIPT.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
