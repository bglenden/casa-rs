# SPDX-License-Identifier: LGPL-3.0-or-later
"""Bounded T52 native-CF DIRTY/CLEAN acceptance against retained CASA products."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from .image_compare import normalize_comparison_request, validate_comparison_output
from .tolerances import evaluate_comparison_tolerances
from .t51_pair_driver import (
    CASA_SITE, REPO, command, records, save, sha256_file, source_identity, stable_tree,
)
from .t51_pair_guard import run_pair_pipeline

TEST = "continuum_request::tests::source_bind_probe::native_acceptance::t52_native_evla_representative_full_products"


def accept_comparison(output, comparison, tolerances):
    validate_comparison_output(output, comparison)
    if output["status"] != "completed":
        raise ValueError(f"full-product comparison failed: {output.get('reason')}")
    evaluation = evaluate_comparison_tolerances(output, tolerances)
    if evaluation["status"] != "passed":
        raise ValueError("unchanged full-product tolerances failed")
    return evaluation


def pipeline(args):
    root = args.output
    root.mkdir(exist_ok=False)
    (root / "logs").mkdir()
    if shutil.disk_usage(root).free < 16 << 30:
        raise ValueError("insufficient acceptance disk headroom")
    binary = root / "native-acceptance-test"
    shutil.copy2(args.binary, binary)
    sources = source_identity()
    inputs = {"surface_sha256": sha256_file(args.surface), "binary_sha256": sha256_file(binary)}
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    manifests = {role: json.loads(getattr(args, role + "_workload").read_text())
                 for role in ("dirty", "clean")}
    for role, manifest in manifests.items():
        imaging = manifest["imaging"]
        required = {"imsize": 512, "field": "1516~1524", "spw": "2,7,12,17",
                    "channel_count": 64, "niter": 0 if role == "dirty" else 30,
                    "wprojplanes": 32, "nterms": 2, "usepointing": True,
                    "aterm": True, "psterm": False, "wbawp": True, "conjbeams": True}
        if any(imaging.get(key) != value for key, value in required.items()):
            raise ValueError("workload does not match the frozen native request")
    input_paths = {"measurement_set": args.ms}
    input_paths.update({
        f"{role}{product}": Path(str(getattr(args, role + "_casa_prefix")) + product)
        for role, manifest in manifests.items()
        for product in manifest["comparison"]["products"]
    })
    input_trees = {name: stable_tree(path) for name, path in input_paths.items()}
    save(root / "request.json", {"inputs": inputs, "source_sha256": sources,
         "tested_commit": revision,
         "input_trees": input_trees,
         "measurement_set": str(args.ms), "surface": str(args.surface),
         "workloads": manifests, "wall_cap_seconds": 1800, "rss_cap_bytes": 32 << 30,
         "admission_bytes": 16 << 30, "native_cache": str(args.resume_native_cache or root / "native-cf"),
         "resume_native_cache": args.resume_native_cache is not None,
         "scope": "T52 representative correctness, no performance claim"})
    native_cache = args.resume_native_cache or root / "native-cf"
    environment = os.environ.copy()
    environment.update(CASA_RS_T52_MS=str(args.ms), CASA_RS_T52_SURFACE=str(args.surface),
                       CASA_RS_T52_NATIVE_CACHE=str(native_cache),
                       CASA_RS_T52_EXPECT_COLD="0" if args.resume_native_cache else "1",
                       CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND="3000000000",
                       CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND="3000000000")
    environment.pop("CASA_RS_IMAGING_SCIENCE_PROBE", None)
    comparison_environment = environment | {
        "PYTHONPATH": os.pathsep.join((str(CASA_SITE), str(REPO / "tools/perf/imager"))),
        "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
    }
    before_cache = stable_tree(native_cache) if args.resume_native_cache else None
    acceptance = {}
    for role, manifest in manifests.items():
        print(f"t52_stage native_{role}", flush=True)
        environment.update(CASA_RS_T52_ACCEPTANCE_ROLE=role,
                           CASA_RS_T52_OUTPUT_PREFIX=str(root / role))
        log = root / "logs" / f"{role}.log"
        command([str(binary), TEST, "--exact", "--ignored", "--nocapture"], log, environment)
        completed, = records(log, "t52_native_full_products_complete ")
        if completed["selected_correlation_channel_samples"] != 5_990_400:
            raise ValueError("native representative selection changed")
        if set(item["name"] for item in completed["products"]) != set(manifest["comparison"]["products"]):
            raise ValueError("native product inventory differs")
        cache = stable_tree(native_cache)
        if before_cache is not None and before_cache != cache:
            raise ValueError("warm reuse changed native cache payload")
        before_cache = cache
        save(root / f"{role}-cache.json", cache)
        reference = getattr(args, role + "_casa_prefix")
        raw = dict(manifest["comparison"])
        raw.update(left_prefix=str(root / role), right_prefix=str(reference),
                   left_label="native CF casa-rs", right_label="frozen CASA",
                   panel_dir=str(root / f"{role}-panels"),
                   structure_workspace_dir=str(root / f"{role}-structure"),
                   require_direction_wcs_parity=True)
        comparison = normalize_comparison_request(raw)
        save(root / f"{role}-comparison-request.json", comparison)
        print(f"t52_stage compare_{role}", flush=True)
        command([str(args.casa_python), "-m", "perf_harness.casa_image_compare",
                 str(root / f"{role}-comparison-request.json"), str(root / f"{role}-comparison.json")],
                root / f"{role}-comparison.log", comparison_environment)
        output = json.loads((root / f"{role}-comparison.json").read_text())
        evaluation = accept_comparison(output, comparison, manifest["comparison"]["tolerances"])
        acceptance[role] = {"native": completed, "tolerances": evaluation}
        save(root / f"{role}-acceptance.json", acceptance[role])
    if inputs != {"surface_sha256": sha256_file(args.surface), "binary_sha256": sha256_file(binary)}:
        raise ValueError("native binary or surface changed during acceptance")
    if sources != source_identity():
        raise ValueError("source changed during acceptance")
    if input_trees != {name: stable_tree(path) for name, path in input_paths.items()}:
        raise ValueError("MeasurementSet or frozen CASA products changed during acceptance")
    save(root / "result.json", {"completed": True, "acceptance": acceptance,
         "visual_review_required": True, "source_sha256": sources, "tested_commit": revision})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--resume-native-cache", type=Path,
                        help="explicitly reuse a completed native catalog from a retained attempt")
    for name in ("output", "binary", "ms", "surface", "casa-python", "dirty-workload",
                 "clean-workload", "dirty-casa-prefix", "clean-casa-prefix"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    if any(not value.is_absolute() for value in vars(args).values() if isinstance(value, Path)):
        parser.error("all paths must be absolute")
    if args.pipeline:
        pipeline(args)
    else:
        environment = os.environ.copy()
        environment.update(PYTHONPATH=str(REPO / "tools/perf/imager"), PYTHONDONTWRITEBYTECODE="1")
        result = run_pair_pipeline([sys.executable, "-m", "perf_harness.t52_native_acceptance",
                                   "--pipeline", *sys.argv[1:]], cwd=REPO, environment=environment,
                                  log=sys.stdout, wall_seconds=1800)
        if args.output.is_dir():
            save(args.output / "guard.json", result)
        print(json.dumps(result), flush=True)
        sys.exit(0 if result["complete"] else 1)
