#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Bounded, paired T55 serial metric and regression guard for autoresearch.

The approved local run supplies CASA_RS_T55_AUTORESEARCH_ROOT/config.json.
Only generated artifacts are written there; the controller owns Git and its
own state. Builds, warmups, profiles, and comparisons are outside task timing.
"""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys

from perf_harness import t51_pair_guard
from perf_harness.image_compare import compare_products
from perf_harness.tree_identity import sha256_file, tree_identity

REPO = Path(__file__).resolve().parents[3]
TEST = "t55_real_cube::t55_intermediate_clark_cube_worker_scaling"
PRODUCTS = [".image", ".residual", ".psf", ".sumwt", ".model", ".pb", ".mask"]


def save(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def command(argv, log, *, env=None):
    with log.open("x") as stream:
        subprocess.run(argv, cwd=REPO, env=env, stdout=stream,
                       stderr=subprocess.STDOUT, check=True)


def current_head():
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()


def controller_context(config):
    if not (REPO / "autoresearch-results/run.json").exists():
        return None
    sys.path.insert(0, config["controller_scripts"])
    from autoresearch_core import load_context
    return load_context(REPO)


def build_application(directory):
    environment = os.environ.copy()
    environment["CARGO_INCREMENTAL"] = "0"
    argv = ["cargo", "test", "-p", "casa-imaging-application", "--release",
            "--test", "continuum_application", "--no-run", "--message-format=json"]
    with (directory / "build.stderr.log").open("x") as log:
        result = subprocess.run(argv, cwd=REPO, env=environment, stdout=subprocess.PIPE,
                                stderr=log, text=True, check=False)
    (directory / "build.stdout.jsonl").write_text(result.stdout)
    result.check_returncode()
    executables = [record["executable"] for line in result.stdout.splitlines()
                   if (record := json.loads(line)).get("reason") == "compiler-artifact"
                   and record["target"]["name"] == "continuum_application"
                   and record.get("executable")]
    assert len(executables) == 1, executables
    binary = directory / "application"
    shutil.copy2(executables[0], binary)
    return binary


def image_case(binary, directory, config, *, profile=False):
    environment = os.environ.copy()
    environment.update(RUST_TEST_THREADS="1", RUST_MIN_STACK="16777216",
        CASA_RS_T55_REAL_MS=config["measurement_set"],
        CASA_RS_T55_ARTIFACT_ROOT=str(directory),
        CASA_RS_T55_NATIVE_MEMORY_BYTES=str(4 << 30),
        CASA_RS_T55_TIMING_WORKERS="1", CASA_RS_T55_TIMING_REPETITIONS="1",
        CASA_RS_TRACE_IMAGING_STAGE_TIMING="1")
    argv = [str(binary), "--ignored", "--exact", TEST, "--nocapture"]
    log_path = directory.with_suffix(".log")
    with log_path.open("x") as log:
        child = subprocess.Popen(argv, cwd=REPO, env=environment,
                                 stdout=log, stderr=subprocess.STDOUT)
        if profile:
            with directory.with_suffix(".sample-command.log").open("x") as sample_log:
                sampled = subprocess.run(["/usr/bin/sample", str(child.pid), "18", "1",
                    "-file", str(directory.with_suffix(".sample.txt"))],
                    stdout=sample_log, stderr=subprocess.STDOUT)
        code = child.wait()
    assert code == 0, f"native test failed: {log_path}"
    if profile:
        assert sampled.returncode == 0
    product_directory = directory / "natural-w1"
    assert (product_directory / "accepted.txt").is_file()
    result = json.loads((product_directory / "summary.json").read_text())
    assert result["major_cycles"] == 3 and result["actual_minor_iterations"] == 19
    assert result["requested_workers"] == result["actual_minor_workers"] == 1
    assert result["native_memory_bytes"] == 4 << 30
    lines = log_path.read_text().splitlines()
    initial = [line for line in lines if line.startswith("imaging_source_read_ahead_summary ")]
    assert len(initial) == 1
    return {"task_seconds": result["task_wall_seconds"],
            "initial_seconds": int(re.search(r"\bwall_nanos=(\d+)", initial[0])[1]) / 1e9,
            "products": str(product_directory / "image"), "log": str(log_path)}


def measure(root, config, directory):
    before = tree_identity(config["measurement_set"], excluded_names={"table.lock"})
    binary = build_application(directory)
    context = controller_context(config)
    if context is None:
        parent = binary
        parent_head = current_head()
    else:
        _, _, events, state = context
        assert subprocess.check_output(["git", "rev-parse", "HEAD^"], cwd=REPO,
                                       text=True).strip() == state.head
        retained = [event for event in events if event["event"] == "baseline"
                    or (event["event"] == "iteration" and event["outcome"] == "keep")][-1]
        parent_head = retained["head"]
        parent = root / "commits" / parent_head / "application"
        parent_record = json.loads((parent.parent / "measurement.json").read_text())
        assert sha256_file(parent) == parent_record["candidate_sha256"]
    hashes = {"parent": sha256_file(parent), "candidate": sha256_file(binary)}
    print(json.dumps({"head": current_head(), "parent_head": parent_head,
                      "binary_sha256": hashes, "timing": "one-worker execute_continuum through publication"}), flush=True)
    for role, executable in [("parent", parent), ("candidate", binary)]:
        image_case(executable, directory / f"warmup-{role}", config)
    pairs = []
    for index in range(3):
        order = [("parent", parent), ("candidate", binary)]
        if index % 2:
            order.reverse()
        pair = {}
        for role, executable in order:
            pair[role] = image_case(executable, directory / f"{role}-{index}", config)
            print(json.dumps({"pair": index, "role": role, **pair[role]}), flush=True)
        pairs.append(pair)
    result = {"head": current_head(), "parent_head": parent_head,
        "candidate_sha256": hashes["candidate"], "parent_sha256": hashes["parent"],
        "pairs": pairs, "seconds": statistics.median(pair["candidate"]["task_seconds"] for pair in pairs),
        "parent_seconds": statistics.median(pair["parent"]["task_seconds"] for pair in pairs),
        "input_identity": before,
        "cache_policy": "OS cache not purged; both binaries warmed; fresh outputs and backings per call"}
    assert hashes == {"parent": sha256_file(parent), "candidate": sha256_file(binary)}
    assert before == tree_identity(config["measurement_set"], excluded_names={"table.lock"})
    save(directory / "measurement.json", result)


def guard(config, directory):
    result = json.loads((directory / "measurement.json").read_text())
    assert result["head"] == current_head()
    assert sha256_file(directory / "application") == result["candidate_sha256"]
    context = controller_context(config)
    if context is not None:
        _, _, _, state = context
        assert result["seconds"] <= float(state.metric) * 0.99, "less than 1% gain over retained metric"
        assert all(pair["candidate"]["task_seconds"] <= pair["parent"]["task_seconds"] * 0.99
                   for pair in result["pairs"]), "not a consistent >=1% win in all three pairs"
    comparison = compare_products(casa_python=config["casa_python"], cwd=directory,
        artifact_prefix=directory / "products", request={
            "left_prefix": result["pairs"][-1]["candidate"]["products"], "left_label": "candidate",
            "right_prefix": config["reference_products"], "right_label": "pre-autoresearch reference",
            "mode": "full", "products": PRODUCTS, "max_elements_per_product": 1000000,
            "full_chunk_elements": 1000000, "require_exact_product_inventory": True,
            "require_direction_wcs_parity": True, "require_metadata_parity": True,
            "panel_dir": str(directory / "panels"),
            "structure_workspace_dir": str(directory / "structure-workspace"),
            "tolerances": {"contract_version": 2, "require_full_array": True, "products": {},
                           "default": {"diff_rms_over_right_rms": 0.0}}})
    save(directory / "product-comparison.json", comparison)
    assert comparison["status"] == "completed", comparison.get("reason")
    assert comparison["tolerance_evaluation"]["status"] == "passed"
    assert len(comparison["products"]) == 7
    assert all(value["full_array"]["diff_abs_max"] == 0.0
               and value["metadata"]["status"] == "matched"
               and value["direction_wcs"]["status"] == "matched"
               and value["topology_parity"] for value in comparison["products"].values())
    environment = os.environ.copy()
    environment.update(CARGO_INCREMENTAL="0", RUST_TEST_THREADS="1")
    for index, test_filter in enumerate(["specification_metadata_tests::", "polarization",
                                        "spectral_operator::tests::", "gridded_normal_operator::"]):
        command(["cargo", "test", "-p", "casa-imaging-reconstruction", "--release", "--lib",
                 test_filter, "--", "--test-threads=1"], directory / f"unit-{index}.log", env=environment)
    command(["cargo", "fmt", "--all", "--", "--check"], directory / "format.log")
    command(["git", "diff", "--check"], directory / "whitespace.log")
    print("PASS: exact seven-product regression, topology/WCS/metadata, focused tests, and paired noise guard", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["verify", "guard", "profile"])
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    root = Path(os.environ["CASA_RS_T55_AUTORESEARCH_ROOT"]).resolve()
    config = json.loads((root / "config.json").read_text())
    directory = args.directory or root / "commits" / current_head()
    if args.inside:
        if args.mode == "verify":
            measure(root, config, directory)
        elif args.mode == "guard":
            guard(config, directory)
        else:
            result = image_case(Path(config["profile_binary"]), directory / "profile", config, profile=True)
            save(directory / "profile-result.json", result)
        return
    if args.mode in ("verify", "profile"):
        directory.mkdir(parents=True, exist_ok=False)
    t51_pair_guard.RSS_BYTES = 8 << 30
    argv = [sys.executable, __file__, args.mode, "--inside", "--directory", str(directory)]
    with (directory / f"{args.mode}-pipeline.log").open("x") as log:
        receipt = t51_pair_guard.run_pair_pipeline(argv, cwd=REPO, environment=os.environ.copy(),
            log=log, wall_seconds=600 if args.mode != "profile" else 120)
    save(directory / f"{args.mode}-resource-guard.json", receipt)
    print(json.dumps({"artifacts": str(directory), "resource_guard": receipt}), flush=True)
    assert receipt["complete"], receipt
    if args.mode == "verify":
        result = json.loads((directory / "measurement.json").read_text())
        print(json.dumps({"seconds": result["seconds"]}), flush=True)


if __name__ == "__main__":
    main()
