#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Bounded, paired T55 serial metric and regression guard for autoresearch.

The approved local run supplies CASA_RS_T55_AUTORESEARCH_ROOT/config.json.
Only generated artifacts are written there; the controller owns Git and its
own state. Builds, warmups, profiles, and comparisons are outside task timing.
"""

import argparse
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import time

from perf_harness import t51_pair_guard
from perf_harness.image_compare import compare_products
from perf_harness.tree_identity import sha256_file, tree_identity

REPO = Path(__file__).resolve().parents[3]
TEST = "t55_real_cube::t55_intermediate_clark_cube_worker_scaling"
PRODUCTS = [".image", ".residual", ".psf", ".sumwt", ".model", ".pb", ".mask"]
PAIR_COUNT = 5


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


def input_identity(path):
    identity = tree_identity(Path(path), excluded_names={"table.lock"})
    identity.pop("excluded_count")
    return identity


def check_frozen_controls(root):
    frozen = json.loads((root / "frozen-controls.json").read_text())
    assert sha256_file(root / "config.json") == frozen["config_sha256"]
    for relative, expected in frozen["source_sha256"].items():
        assert sha256_file(REPO / relative) == expected, f"frozen benchmark control changed: {relative}"
    return frozen


def paired_statistics(numerators, denominators):
    assert len(numerators) == len(denominators) == PAIR_COUNT
    assert all(math.isfinite(value) and value > 0 for value in [*numerators, *denominators])
    log_ratios = [math.log(left / right) for left, right in zip(numerators, denominators)]
    mean = statistics.mean(log_ratios)
    # Two-sided Student t interval, four degrees of freedom, on paired log ratios.
    half_width = 2.7764451051977987 * statistics.stdev(log_ratios) / math.sqrt(PAIR_COUNT)
    return {"ratio": math.exp(mean),
            "approximate_95pct_ratio_interval": [math.exp(mean - half_width), math.exp(mean + half_width)]}


def pair_order(index, *, has_parent):
    order = ["parent", "candidate", "casa"] if has_parent else ["candidate", "casa"]
    return list(reversed(order)) if index % 2 else order


def casa_case(config, directory):
    import casatasks
    from casatasks import casalog, tclean

    version = str(casatasks.version_string())
    assert version == config["casa_version"], (version, config["casa_version"])
    casalog.setlogfile(str(directory / "casa-task.log"))
    parameters = dict(config["casa_kwargs"], vis=config["casa_measurement_set"],
                      imagename=str(directory / "image"))
    started = time.perf_counter()
    returned = tclean(**parameters)
    elapsed = time.perf_counter() - started
    result = {"task_seconds": elapsed, "products": parameters["imagename"],
              "casa_version": version, "kwargs": parameters,
              "major_cycles": int(returned["nmajordone"]),
              "actual_minor_iterations": int(returned["iterdone"])}
    save(directory / "result.json", result)
    assert result["major_cycles"] == 3 and result["actual_minor_iterations"] == 19, result


def measure_casa(config, directory):
    directory.mkdir()
    environment = os.environ.copy()
    environment.update(PYTHONDONTWRITEBYTECODE="1", MPLBACKEND="Agg")
    with (directory / "process.log").open("x") as log:
        subprocess.run([config["casa_python"], str(Path(__file__).resolve()), "casa-case",
                        "--inside", "--directory", str(directory)], cwd=directory, env=environment,
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    result = json.loads((directory / "result.json").read_text())
    assert result["major_cycles"] == 3 and result["actual_minor_iterations"] == 19
    assert math.isfinite(result["task_seconds"]) and result["task_seconds"] > 0
    return result


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
    frozen = check_frozen_controls(root)
    before = input_identity(config["measurement_set"])
    assert before["tree_sha256"] == config["input_tree_sha256"]
    assert input_identity(config["casa_measurement_set"]) == before
    binary = build_application(directory)
    context = controller_context(config)
    if context is None:
        parent = None
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
    hashes = {"parent": sha256_file(parent) if parent else None, "candidate": sha256_file(binary)}
    print(json.dumps({"head": current_head(), "parent_head": parent_head,
                      "binary_sha256": hashes, "timing": "one-worker execute_continuum through publication"}), flush=True)
    for role, executable in ([("parent", parent)] if parent else []) + [("candidate", binary)]:
        image_case(executable, directory / f"warmup-{role}", config)
    measure_casa(config, directory / "warmup-casa")
    pairs = []
    for index in range(PAIR_COUNT):
        pair = {}
        for role in pair_order(index, has_parent=parent is not None):
            destination = directory / f"{role}-{index}"
            pair[role] = (measure_casa(config, destination) if role == "casa" else
                          image_case(parent if role == "parent" else binary, destination, config))
            print(json.dumps({"pair": index, "role": role, **pair[role]}), flush=True)
        pairs.append(pair)
    candidate_seconds = [pair["candidate"]["task_seconds"] for pair in pairs]
    casa_seconds = [pair["casa"]["task_seconds"] for pair in pairs]
    result = {"head": current_head(), "parent_head": parent_head, "baseline": parent is None,
        "candidate_sha256": hashes["candidate"], "parent_sha256": hashes["parent"],
        "pairs": pairs, "seconds": statistics.median(candidate_seconds),
        "casa_seconds": statistics.median(casa_seconds),
        **paired_statistics(candidate_seconds, casa_seconds),
        "parent_comparison": paired_statistics(candidate_seconds,
            [pair["parent"]["task_seconds"] for pair in pairs]) if parent else None,
        "input_identity": before, "frozen_controls": frozen,
        "cache_policy": "OS cache not purged; all implementations warmed; fresh outputs/backings per call",
        "thread_environment": {key: os.environ.get(key) for key in
            ["OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "RAYON_NUM_THREADS"]}}
    assert hashes == {"parent": sha256_file(parent) if parent else None, "candidate": sha256_file(binary)}
    assert before == input_identity(config["measurement_set"])
    assert before == input_identity(config["casa_measurement_set"])
    assert frozen == check_frozen_controls(root)
    save(directory / "measurement.json", result)


def guard(root, config, directory):
    frozen = check_frozen_controls(root)
    result = json.loads((directory / "measurement.json").read_text())
    assert result["head"] == current_head()
    assert result["frozen_controls"] == frozen
    assert sha256_file(directory / "application") == result["candidate_sha256"]
    context = controller_context(config)
    if context is not None:
        _, _, _, state = context
        assert not result["baseline"]
        assert result["ratio"] < float(state.metric), "no improvement in the matched CASA ratio"
        assert result["parent_comparison"]["approximate_95pct_ratio_interval"][1] < 1.0, \
            "paired parent/candidate timing interval does not establish an improvement"
    else:
        assert result["baseline"]
    comparison_contract = json.loads(
        (REPO / "tools/perf/imager/workloads/t55-clark-cube-development.json").read_text())["comparison"]
    for label, reference in [("CASA", result["pairs"][-1]["casa"]["products"]),
                             ("preserved-native", config["reference_products"])]:
        comparison = compare_products(casa_python=config["casa_python"], cwd=directory,
            artifact_prefix=directory / f"products-{label}", request={
                **comparison_contract,
                "left_prefix": result["pairs"][-1]["candidate"]["products"], "left_label": "casa-rs",
                "right_prefix": reference, "right_label": label,
                "panel_dir": str(directory / f"panels-{label}"),
                "structure_workspace_dir": str(directory / f"structure-{label}")})
        save(directory / f"product-comparison-{label}.json", comparison)
        assert comparison["status"] == "completed", comparison.get("reason")
        assert comparison["tolerance_evaluation"]["status"] == "passed"
        assert len(comparison["products"]) == 7
        assert all(value["direction_wcs"]["status"] == "matched" and value["topology_parity"]
                   for value in comparison["products"].values())
    environment = os.environ.copy()
    environment.update(CARGO_INCREMENTAL="0", RUST_TEST_THREADS="1")
    for index, test_filter in enumerate(["specification_metadata_tests::", "polarization",
                                        "spectral_operator::tests::", "gridded_normal_operator::"]):
        command(["cargo", "test", "-p", "casa-imaging-reconstruction", "--release", "--lib",
                 test_filter, "--", "--test-threads=1"], directory / f"unit-{index}.log", env=environment)
    command(["cargo", "test", "-p", "casa-imaging-runtime", "--release", "--lib",
             "paged_cube_state::tests::", "--", "--test-threads=1"], directory / "unit-storage.log", env=environment)
    paths = subprocess.check_output(["git", "diff", "--name-only", result["parent_head"],
                                     result["head"]], cwd=REPO, text=True).splitlines()
    for index, argv in enumerate(affected_tests(paths)):
        command(argv, directory / f"affected-{index}.log", env=environment)
    command(["cargo", "fmt", "--all", "--", "--check"], directory / "format.log")
    command(["git", "diff", "--check"], directory / "whitespace.log")
    assert frozen == check_frozen_controls(root)
    print("PASS: seven products at nRMS<=0.001 vs CASA and native reference, scientific metadata, focused tests, and paired timing guard", flush=True)


def affected_tests(paths):
    packages = sorted({path.split("/")[1] for path in paths if path.startswith("crates/")})
    commands = []
    for package in packages:
        if package == "casa-imaging-reconstruction":
            continue
        if (REPO / "crates" / package / "src/lib.rs").exists():
            commands.append(["cargo", "test", "-p", package, "--release", "--lib", "--", "--test-threads=1"])
    if "casa-tables" in packages:
        commands.append(["cargo", "test", "-p", "casa-tables", "--release", "--test", "selected_incremental_arrays", "--", "--test-threads=1"])
        commands.append(["cargo", "test", "-p", "casa-test-support", "--release", "--features", "cpp-interop-tests",
                         "--test", "tables_cross_matrix_tiled_stman", "--", "--test-threads=1"])
    if set(packages) & {"casa-images", "casa-lattices"}:
        commands.append(["cargo", "test", "-p", "casa-test-support", "--release", "--features", "cpp-interop-tests",
                         "--test", "images_interop", "--", "--test-threads=1"])
    if "casa-ms" in packages:
        commands.append(["cargo", "test", "-p", "casa-ms", "--release", "--features", "cpp-interop-tests",
                         "--test", "ms_data_interop", "--", "--test-threads=1"])
    return commands


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["verify", "guard", "profile", "casa-case"])
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    root = Path(os.environ["CASA_RS_T55_AUTORESEARCH_ROOT"]).resolve()
    config = json.loads((root / "config.json").read_text())
    directory = args.directory or root / "commits" / current_head()
    if args.inside:
        if args.mode == "verify":
            measure(root, config, directory)
        elif args.mode == "guard":
            guard(root, config, directory)
        elif args.mode == "casa-case":
            check_frozen_controls(root)
            casa_case(config, directory)
        else:
            assert args.binary is not None, "profile requires --binary"
            result = image_case(args.binary, directory / "profile", config, profile=True)
            save(directory / "profile-result.json", result)
        return
    if args.mode in ("verify", "profile"):
        directory.mkdir(parents=True, exist_ok=False)
    t51_pair_guard.RSS_BYTES = 8 << 30
    argv = [sys.executable, __file__, args.mode, "--inside", "--directory", str(directory)]
    if args.binary is not None:
        argv += ["--binary", str(args.binary)]
    with (directory / f"{args.mode}-pipeline.log").open("x") as log:
        receipt = t51_pair_guard.run_pair_pipeline(argv, cwd=REPO, environment=os.environ.copy(),
            log=log, wall_seconds=600 if args.mode != "profile" else 120)
    save(directory / f"{args.mode}-resource-guard.json", receipt)
    print(json.dumps({"artifacts": str(directory), "resource_guard": receipt}), flush=True)
    assert receipt["complete"], receipt
    if args.mode == "verify":
        result = json.loads((directory / "measurement.json").read_text())
        print(json.dumps({"ratio": result["ratio"]}), flush=True)


if __name__ == "__main__":
    main()
