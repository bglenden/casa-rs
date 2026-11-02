#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Re-evaluate a saved T1.5 receipt with the approved weak-WEIGHT rule.

Run with CASA Python. Reads existing products only: never runs tclean or native
imaging. Output is a new, versioned rule evaluation bound to the raw receipt;
it is not full tclean acceptance or a replacement for missing scientific gates.
"""
from contextlib import ExitStack
import argparse
import hashlib
import json
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "perf" / "imager"))
from perf_harness.taylor_moment_comparison import MAX_CHUNK_CELLS, TaylorMomentAccumulator
from perf_harness.taylor_weight_gate import RULE, apply_to_run, evaluate, reference_pblimit


def open_image(stack, path, factory):
    image = factory()
    stack.callback(image.done)
    if not image.open(str(path)):
        raise ValueError("cannot open image: " + str(path))
    return image


def geometry(image):
    coords = image.coordsys()
    try:
        # CASA's record includes projection/frame/PC matrix as well as WCS values.
        record = coords.torecord()
        for key in ("obsdate", "observer", "telescope", "telescopeposition"):
            record.pop(key, None)
        # Rest frequency and velocity display settings do not place pixels in
        # singleton FREQ images. Full metadata acceptance remains a separate gate.
        spectral = record.get("spectral2", {})
        if spectral.get("wcs", {}).get("ctype") != "FREQ":
            raise ValueError("need native FREQ coordinates")
        for key in ("restfreq", "restfreqs", "velType", "velUnit", "waveUnit", "formatUnit"):
            spectral.pop(key, None)
        require_finite_coordinates(record)
        return record
    finally:
        coords.done()


def require_finite_coordinates(value):
    """Reject nonfinite numeric WCS metadata before any equality comparison."""
    if isinstance(value, dict):
        for item in value.values():
            require_finite_coordinates(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            require_finite_coordinates(item)
    else:
        array = np.asarray(value)
        if array.dtype.kind in "fciu" and not np.all(np.isfinite(array)):
            raise ValueError("coordinate metadata must be finite")
        if array.dtype.kind == "O" and isinstance(value, np.ndarray):
            for item in value.flat:
                require_finite_coordinates(item)


def equal_geometry(left, right):
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(equal_geometry(left[k], right[k]) for k in left)
    if isinstance(left, (np.ndarray, list)) or isinstance(right, (np.ndarray, list)):
        a, b = np.asarray(left), np.asarray(right)
        if a.shape != b.shape:
            return False
        if a.dtype.kind in "fiu" and b.dtype.kind in "fiu":
            return bool(np.allclose(a, b, rtol=16*np.finfo(float).eps,
                                    atol=16*np.finfo(float).eps))
        return np.array_equal(a, b)
    if isinstance(left, (float, np.floating)) and isinstance(right, (float, np.floating)):
        return bool(np.isclose(left, right, rtol=16*np.finfo(float).eps,
                               atol=16*np.finfo(float).eps))
    return left == right


def measure_images(reference, candidate, parameters, factory):
    """Stream aligned [RA, Dec, I, singleton-frequency] images in bounded tiles."""
    with ExitStack() as stack:
        families = [[open_image(stack, folder / f"image.weight.tt{t}", factory)
                     for t in range(3)] for folder in (reference, candidate)]
        pb = open_image(stack, reference / "image.pb.tt0", factory)
        shape = tuple(families[0][0].shape())
        if len(shape) != 4 or shape[2:] != (1, 1) or min(shape) <= 0:
            raise ValueError("unsupported weight shape; need [RA, Dec, I, frequency=1]")
        images = [*families[0], *families[1], pb]
        coord = families[0][0].coordsys()
        try:
            if coord.names() != ["Right Ascension", "Declination", "Stokes", "Frequency"] or coord.stokes() != ["I"]:
                raise ValueError("unsupported coordinate axes or Stokes")
        finally:
            coord.done()
        reference_geometry = geometry(images[0])
        for image in images:
            if tuple(image.shape()) != shape or not equal_geometry(reference_geometry, geometry(image)):
                raise ValueError("weight/PB shape or coordinate alignment mismatch")
        sums = []
        for folder in (reference, candidate):
            image = open_image(stack, folder / "image.sumwt.tt0", factory)
            if tuple(image.shape()) != (1, 1, 1, 1):
                raise ValueError("sumwt.tt0 must contain exactly one scalar")
            if not bool(image.getchunk(getmask=True).item()):
                raise ValueError("sumwt.tt0 is masked")
            sums.append(image.getchunk().item())
        acc = TaylorMomentAccumulator(*sums)
        limit = reference_pblimit(parameters)
        # One y row per tile also bounds widths larger than MAX_CHUNK_CELLS.
        for y in range(shape[1]):
            for x in range(0, shape[0], MAX_CHUNK_CELLS):
                blc = [x, y, 0, 0]
                trc = [min(x + MAX_CHUNK_CELLS, shape[0]) - 1, y, 0, 0]
                values = [i.getchunk(blc=blc, trc=trc).ravel() for i in images]
                masks = [i.getchunk(blc=blc, trc=trc, getmask=True).ravel() for i in images]
                if not np.all(np.isfinite(values[6][masks[6]])):
                    raise ValueError("reference PB valid payload must be finite")
                support = masks[6] & (values[6] >= limit)
                acc.add(values[:3], values[3:6], masks[:3], masks[3:6], support)
        result = acc.result()
        rest_frequencies = []
        for image in (families[0][0], families[1][0]):
            coords = image.coordsys()
            try:
                rest_frequencies.append(float(coords.torecord()["spectral2"]["restfreq"]))
            finally:
                coords.done()
        result["metadata_assessment"] = {
            "full_metadata": "unassessed; not waived by this numeric gate",
            "reference_restfreq_hz": rest_frequencies[0],
            "candidate_restfreq_hz": rest_frequencies[1],
            "observed_restfreq_mismatch": rest_frequencies[0] != rest_frequencies[1],
            "alignment_only": "active FREQ pixel grid; 16 float64 eps relative/absolute tolerance",
        }
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.resolve() == args.receipt.resolve() or args.output.exists():
        parser.error("output must be a new file distinct from the original receipt")
    from casatools import image, version_string
    raw_bytes = args.receipt.read_bytes()
    raw = json.loads(raw_bytes)
    if version_string() != raw["casa_tools"]:
        raise ValueError("CASA tools version differs from the pinned receipt")
    files = [Path(__file__), Path(__file__).resolve().parents[1] / "perf/imager/perf_harness/taylor_weight_gate.py",
             Path(__file__).resolve().parents[1] / "perf/imager/perf_harness/taylor_moment_comparison.py"]
    output = dict(rule=RULE, raw_receipt=str(args.receipt.resolve()),
                  raw_receipt_sha256=hashlib.sha256(raw_bytes).hexdigest(),
                  source_sha256={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
                  casa_tools=version_string(), native_revision=raw["casars_revision"],
                  command=sys.argv, full_metadata="unassessed; observed discrepancies are retained per row",
                  scope="Only recorded T1.5 checks; not full metadata, return, or T2 acceptance",
                  cases={})
    for name, case in raw["cases"].items():
        rows = output["cases"][name] = {}
        for label, run in case["runs"].items():
            reference = Path(case["casa_reference"])
            candidate = args.receipt.parent / name / label
            gate = evaluate(case["parameters"], run["products"]["image.weight.tt1"]["passed_recorded_checks"],
                            lambda: measure_images(reference, candidate, case["parameters"], image))
            rows[label] = dict(apply_to_run(run, gate), reference=str(reference), candidate=str(candidate),
                               original_raw_nrms=run["products"]["image.weight.tt1"]["nrms"])
    if args.receipt.read_bytes() != raw_bytes:
        raise ValueError("raw receipt changed during evaluation")
    args.output.write_text(json.dumps(output, indent=2, allow_nan=False) + "\n")
    return 0 if all(r["passed_recorded_checks"] for c in output["cases"].values() for r in c.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
