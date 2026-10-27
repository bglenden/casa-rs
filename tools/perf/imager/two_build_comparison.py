# SPDX-License-Identifier: LGPL-3.0-or-later
"""Two-build blocking comparison helpers for ``run_alternating_comparison.py``.

The imaging port's baseline and blocking comparison
(``docs/measurement-set/imaging-port.md`` section 5, thresholds in decision 4)
runs one workload on two casa-rs checkouts in counterbalanced A B B A order
and judges the candidate (B) against the baseline (A) three ways:

- **wall time:** the median paired relative delta is at most
  ``max(wall_tolerance, noise_sigmas * sigma)``, where ``sigma`` is the
  baseline's own relative spread: the standard deviation of its measured
  walls over their median;
- **peak RSS:** the candidate's median peak RSS is at most
  ``(1 + rss_tolerance)`` times the baseline's;
- **products:** every image product's normalized RMS difference, the RMS of
  candidate minus baseline over the RMS of the baseline, is at most
  ``product_nrms``, over full arrays.

Missing evidence makes a verdict inconclusive, never a pass. The products are
compared by the one imaging-product comparator, ``perf_harness.image_compare``,
with the candidate as its left operand and the baseline as its right.
"""

from __future__ import annotations

import json
import math
import pathlib
import shutil
import statistics
import subprocess
from typing import Any, Callable, Mapping, Sequence

from perf_harness.provenance import capture_provenance
from perf_harness.tree_identity import sha256_file

RUN_WORKLOAD_RELATIVE = pathlib.Path("tools/perf/imager/run_workload.py")
BENCH_SCRIPT_RELATIVE = pathlib.Path("scripts/bench-imager-vs-casa.sh")
# `bench-imager-vs-casa.sh` runs this binary from the checkout it lives in.
IMAGER_BINARY_RELATIVE = pathlib.Path("target/release/casars-imager")
ROLES = ("baseline", "candidate")
GATES = ("slowdown", "blocking")
GATE_VERDICTS = {
    "slowdown": ("wall_time",),
    "blocking": ("wall_time", "peak_rss", "products"),
}
DEFAULT_WALL_TOLERANCE = 0.02
DEFAULT_NOISE_SIGMAS = 2.0
DEFAULT_RSS_TOLERANCE = 0.05
DEFAULT_PRODUCT_NRMS = 1.0e-6
DEFAULT_FULL_CHUNK_ELEMENTS = 1_000_000
RECORDED_ENVIRONMENT_PREFIXES = ("CASA_RS_BENCH_", "CASA_RS_IMPERF_")
RECORDED_ENVIRONMENT_NAMES = (
    "CARGO_TARGET_DIR",
    "CASA_RS_CASA_PYTHON",
    "PKG_CONFIG_PATH",
    "RUSTFLAGS",
)
# Product statuses that show the two builds disagree, as opposed to evidence
# the comparator could not produce.
DISAGREEING_PRODUCT_STATUSES = {"shape_mismatch"}

ProductComparer = Callable[..., dict[str, Any]]


class ComparisonError(Exception):
    """Error that should be shown without a traceback."""


# Role checkouts ------------------------------------------------------------


def validate_role_repos(
    repos: Mapping[str, pathlib.Path], environment: Mapping[str, str]
) -> None:
    """Check that each role's checkout can run and builds its own binary.

    Each run executes the role's own ``run_workload.py``, whose bench script
    runs ``cargo build --release`` and then ``target/release/casars-imager``
    in that checkout. Two different checkouts sharing one build directory
    would rebuild over each other between runs, or run the other's binary,
    so that is refused.
    """

    for role in ROLES:
        repo = repos[role]
        if not repo.is_dir():
            raise ComparisonError(f"--{role}-repo is not a directory: {repo}")
        for relative in (RUN_WORKLOAD_RELATIVE, BENCH_SCRIPT_RELATIVE):
            if not (repo / relative).is_file():
                raise ComparisonError(f"--{role}-repo {repo} has no {relative}")
    if repos["baseline"] == repos["candidate"]:
        return
    if environment.get("CARGO_TARGET_DIR"):
        raise ComparisonError(
            "CARGO_TARGET_DIR is set, so both checkouts would build into one "
            "directory while each run executes its checkout's own "
            f"{IMAGER_BINARY_RELATIVE}; unset it so each checkout builds its own"
        )
    targets = {role: (repos[role] / "target").resolve() for role in ROLES}
    if targets["baseline"] == targets["candidate"]:
        raise ComparisonError(
            f"the baseline and candidate checkouts share {targets['baseline']}; "
            "give each checkout its own target directory"
        )


def describe_role(repo: pathlib.Path) -> dict[str, Any]:
    """The checkout a role runs: its path, commit, branch and dirty flag."""

    provenance = capture_provenance(
        repo_root=repo,
        executables={
            "run_workload": repo / RUN_WORKLOAD_RELATIVE,
            "bench_script": repo / BENCH_SCRIPT_RELATIVE,
        },
        datasets={},
        storage_label=None,
    )
    repository = provenance["repository"]
    return {
        "repo": str(repo),
        "commit": repository.get("revision"),
        "branch": repository.get("branch"),
        "dirty": repository.get("dirty"),
        "run_workload": provenance["executables"]["run_workload"],
        "bench_script": provenance["executables"]["bench_script"],
    }


def recorded_environment(environment: Mapping[str, str]) -> dict[str, str]:
    """The environment variables that select inputs, builds and CASA reuse."""

    return {
        name: value
        for name, value in sorted(environment.items())
        if name in RECORDED_ENVIRONMENT_NAMES
        or name.startswith(RECORDED_ENVIRONMENT_PREFIXES)
    }


def describe_binary(
    path: pathlib.Path, cache: dict[tuple[str, int, int], dict[str, Any]]
) -> dict[str, Any]:
    """The imager binary a run used: its digest and linked FFTW codelets.

    ``fftw_n1fv_symbols`` counts FFTW's NEON codelet symbols
    (``nm BIN | grep -c n1fv_``, ``docs/agent-reference.md``): zero means a
    scalar FFTW. Results are cached by path, size and modification time.
    """

    try:
        stat = path.stat()
    except OSError:
        return {
            "path": str(path),
            "exists": False,
            "sha256": None,
            "fftw_n1fv_symbols": None,
        }
    key = (str(path), stat.st_size, stat.st_mtime_ns)
    if key not in cache:
        cache[key] = {
            "path": str(path),
            "exists": True,
            "sha256": sha256_file(path),
            "fftw_n1fv_symbols": count_fftw_codelets(path),
        }
    return dict(cache[key])


def count_fftw_codelets(path: pathlib.Path) -> int | None:
    try:
        completed = subprocess.run(
            ["nm", str(path)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return sum(1 for line in completed.stdout.splitlines() if "n1fv_" in line)


# Per-run evidence -----------------------------------------------------------


def lookup(value: Any, *keys: str) -> Any:
    """``value[keys[0]][keys[1]]...``, or ``None`` where the path breaks."""

    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def extract_peak_rss_bytes(result: dict[str, Any]) -> int | None:
    """The run's peak RSS: the ``wait4`` high-water mark of the
    ``casars-imager`` child, as ``run_workload.py`` records it.

    ``run_workload.py`` reads it from the bench log's
    ``imager_bench_process_resource`` line into
    ``results.backend_plan_logs.summary.peak_rss_bytes`` and mirrors it in
    ``benchmark_features.resources``. Like ``run_workload.py``, this does not
    fall back to a phase's ``process_peak_rss_bytes``.
    """

    for keys in (
        ("results", "backend_plan_logs", "summary", "peak_rss_bytes"),
        ("benchmark_features", "resources", "peak_rss_bytes"),
        ("results", "benchmark_features", "resources", "peak_rss_bytes"),
    ):
        value = positive_int(lookup(result, *keys))
        if value is not None:
            return value
    return None


def rust_prefix(result: dict[str, Any]) -> str | None:
    """The kept casa-rs product prefix of a ``run_workload.py`` result."""

    for keys in (
        ("results", "product_paths", "rust_prefix"),
        ("products", "rust_prefix"),
    ):
        value = lookup(result, *keys)
        if isinstance(value, str) and value:
            return value
    return None


def read_run_summary(prefix: str | None) -> dict[str, Any] | None:
    """What the receipt keeps from casars-imager's ``<imagename>.summary.json``:
    backend and precision as the run resolved them, its workers, and each
    phase's seconds, ``process_peak_rss_bytes`` and ``peak_reserved_bytes``.
    """

    if prefix is None:
        return None
    path = pathlib.Path(prefix + ".summary.json")
    try:
        summary = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"path": str(path), "status": "missing"}
    except (OSError, ValueError) as error:
        return {"path": str(path), "status": "unreadable", "reason": str(error)}
    if not isinstance(summary, dict):
        return {"path": str(path), "status": "unreadable", "reason": "not an object"}
    phases = [
        {
            "name": phase.get("name"),
            "completed": phase.get("completed"),
            "seconds": phase.get("seconds"),
            "process_peak_rss_bytes": phase.get("process_peak_rss_bytes"),
            "peak_reserved_bytes": phase.get("peak_reserved_bytes"),
        }
        for phase in summary.get("phases") or []
        if isinstance(phase, dict)
    ]
    products = [
        product for product in summary.get("products") or [] if isinstance(product, str)
    ]
    return {
        "path": str(path),
        "status": summary.get("status"),
        "backend": summary.get("backend"),
        "precision": summary.get("precision"),
        "workers": summary.get("workers"),
        "workers_policy": summary.get("workers_policy"),
        "workers_reason": summary.get("workers_reason"),
        "capacity_bytes": summary.get("capacity_bytes"),
        "process_peak_rss_bytes": max_positive(
            phase["process_peak_rss_bytes"] for phase in phases
        ),
        "peak_reserved_bytes": max_positive(
            phase["peak_reserved_bytes"] for phase in phases
        ),
        "phases": phases,
        "products": products,
    }


def max_positive(values: Any) -> int | None:
    numbers = [
        number for value in values if (number := positive_int(value)) is not None
    ]
    return max(numbers) if numbers else None


def prune_run_products(
    result: dict[str, Any], *, owned_root: pathlib.Path, protected: Sequence[str]
) -> list[str]:
    """Delete a run's kept products once its evidence has been read.

    ``run_workload.py`` keeps every run's products under
    ``<artifact-root>/products/<run-id>``. A blocking comparison needs one
    measured run's products per role, so the others go. Only a directory
    inside this comparison's own artifact root is removed, and never one that
    holds a protected path such as a reused CASA prefix.
    """

    root_value = lookup(result, "artifacts", "products_root")
    if not isinstance(root_value, str) or not root_value:
        return []
    root = pathlib.Path(root_value)
    if root.is_symlink() or not root.is_dir():
        return []
    resolved = root.resolve()
    if not resolved.is_relative_to(owned_root.resolve()):
        return []
    for path in protected:
        if path and pathlib.Path(path).resolve().is_relative_to(resolved):
            return []
    shutil.rmtree(root)
    return [str(root)]


# Products ------------------------------------------------------------------


def product_suffixes(
    prefix: str, run_summary: dict[str, Any] | None, result: dict[str, Any]
) -> tuple[list[str], str]:
    """The products to compare and where the list came from.

    The baseline's run summary lists every image it published; without one,
    the workload manifest's comparison products are used.
    """

    published = (run_summary or {}).get("products") or []
    # Products sit beside the prefix; names, not whole paths, are matched so
    # that a canonicalized directory cannot hide one.
    stem = pathlib.PurePath(prefix).name
    suffixes = sorted(
        {
            name[len(stem) :]
            for product in published
            if (name := pathlib.PurePath(product).name).startswith(stem + ".")
        }
    )
    if suffixes:
        return suffixes, "baseline_run_summary"
    manifest = lookup(result, "comparison", "products")
    if isinstance(manifest, list) and all(isinstance(item, str) for item in manifest):
        return sorted(set(manifest)), "workload_manifest"
    return [], "unavailable"


def product_comparison_request(
    *,
    baseline_prefix: str,
    candidate_prefix: str,
    suffixes: Sequence[str],
    exact_inventory: bool,
    product_nrms: float,
    full_chunk_elements: int,
    panel_dir: pathlib.Path,
    structure_workspace_dir: pathlib.Path,
    max_elements_per_product: int = DEFAULT_FULL_CHUNK_ELEMENTS,
) -> dict[str, Any]:
    """A full-array comparator request: candidate left, baseline right, so
    each product's ``diff_rms_over_right_rms`` is the decision-4 NRMS.
    ``max_elements_per_product`` only bounds the review panels' sampling."""

    return {
        "mode": "full",
        "left_prefix": candidate_prefix,
        "right_prefix": baseline_prefix,
        "left_label": "candidate",
        "right_label": "baseline",
        "products": list(suffixes),
        "max_elements_per_product": max_elements_per_product,
        "full_chunk_elements": full_chunk_elements,
        "require_exact_product_inventory": exact_inventory,
        "require_metadata_parity": False,
        "tolerances": {
            "contract_version": 2,
            "require_full_array": True,
            "default": {"diff_rms_over_right_rms": product_nrms},
            "products": {},
        },
        "panel_dir": str(panel_dir),
        "structure_workspace_dir": str(structure_workspace_dir),
    }


def compare_role_products(
    product_runs: Mapping[str, tuple[dict[str, Any], dict[str, Any] | None]],
    *,
    product_nrms: float,
    artifact_root: pathlib.Path,
    report_root: pathlib.Path,
    comparer: ProductComparer,
    casa_python: str | None,
) -> dict[str, Any]:
    """Compare the candidate's kept products with the baseline's.

    ``product_runs`` maps each role to its product run's record and result.
    Returns the receipt's ``product_comparison`` record.
    """

    prefixes: dict[str, str] = {}
    for role in ROLES:
        run, _ = product_runs.get(role, ({}, None))
        prefix = lookup(run, "products", "rust_prefix")
        if not lookup(run, "products", "retained") or not isinstance(prefix, str):
            return not_compared(f"the {role} product run kept no casa-rs products")
        prefixes[role] = prefix
    if prefixes["baseline"] == prefixes["candidate"]:
        return not_compared(
            "both roles kept their products at one prefix, so the later run "
            f"overwrote the earlier: {prefixes['baseline']}"
        )
    baseline_run, baseline_result = product_runs["baseline"]
    suffixes, source = product_suffixes(
        prefixes["baseline"], baseline_run.get("run_summary"), baseline_result or {}
    )
    if not suffixes:
        return not_compared("no product list: the baseline run summary lists none")

    def manifest_count(key: str) -> int:
        value = positive_int(lookup(baseline_result, "comparison", key))
        return value or DEFAULT_FULL_CHUNK_ELEMENTS

    request = product_comparison_request(
        baseline_prefix=prefixes["baseline"],
        candidate_prefix=prefixes["candidate"],
        suffixes=suffixes,
        exact_inventory=source == "baseline_run_summary",
        product_nrms=product_nrms,
        full_chunk_elements=manifest_count("full_chunk_elements"),
        max_elements_per_product=manifest_count("max_elements_per_product"),
        panel_dir=artifact_root / "product-comparison" / "panels",
        structure_workspace_dir=(
            artifact_root / "product-comparison" / "structure-workspace"
        ).resolve(),
    )
    report_root.mkdir(parents=True, exist_ok=True)
    comparison = comparer(
        request=request,
        artifact_prefix=report_root / "product-comparison",
        casa_python=casa_python,
    )
    summary = summarize_product_comparison(comparison)
    summary["product_list_source"] = source
    summary["candidate_sequence_index"] = product_runs["candidate"][0].get(
        "sequence_index"
    )
    summary["baseline_sequence_index"] = baseline_run.get("sequence_index")
    return summary


def not_compared(reason: str) -> dict[str, Any]:
    return {"status": "not_compared", "reason": reason, "products": {}}


def summarize_product_comparison(comparison: dict[str, Any]) -> dict[str, Any]:
    """The receipt's view of a comparator result; the full result stays in
    the comparator's own output file, named here."""

    products: dict[str, Any] = {}
    for suffix, product in sorted((comparison.get("products") or {}).items()):
        if not isinstance(product, dict):
            products[suffix] = {"status": None}
            continue
        products[suffix] = {
            "status": product.get("status"),
            "nrms": product.get("diff_rms_over_right_rms"),
            "diff_abs_max_over_right_peak": product.get("diff_abs_max_over_right_peak"),
            "shape": product.get("shape"),
            "left_exists": product.get("left_exists"),
            "right_exists": product.get("right_exists"),
        }
    inventory = comparison.get("product_inventory")
    evaluation = comparison.get("tolerance_evaluation")
    return {
        "status": comparison.get("status"),
        "reason": comparison.get("reason"),
        "left_label": comparison.get("left_label"),
        "right_label": comparison.get("right_label"),
        "left_prefix": comparison.get("left_prefix"),
        "right_prefix": comparison.get("right_prefix"),
        "product_inventory": (
            {
                key: inventory.get(key)
                for key in (
                    "status",
                    "left_missing",
                    "left_extra",
                    "right_missing",
                    "right_extra",
                )
            }
            if isinstance(inventory, dict)
            else None
        ),
        "tolerance_evaluation": (
            {
                key: evaluation.get(key)
                for key in ("status", "failed_checks", "incomplete_checks")
            }
            if isinstance(evaluation, dict)
            else None
        ),
        "input": comparison.get("input"),
        "output": comparison.get("output"),
        "log": comparison.get("log"),
        "products": products,
    }


# Verdicts ------------------------------------------------------------------


def relative_spread(values: Sequence[float]) -> dict[str, Any]:
    """Count, median, sample standard deviation and their ratio (sigma)."""

    finite = [float(value) for value in values if is_finite(value)]
    median = statistics.median(finite) if finite else None
    stdev = statistics.stdev(finite) if len(finite) >= 2 else None
    sigma = stdev / median if stdev is not None and median and median > 0 else None
    return {
        "count": len(finite),
        "median": median,
        "stdev": stdev,
        "relative_sigma": sigma,
    }


def is_finite(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def wall_time_verdict(
    relative_deltas: Sequence[float],
    baseline_walls: Sequence[float],
    *,
    wall_tolerance: float,
    noise_sigmas: float,
    evidence_complete: bool,
) -> dict[str, Any]:
    """Median paired relative delta against max(wall_tolerance, k * sigma).

    With ``noise_sigmas`` zero the allowance is ``wall_tolerance`` alone,
    which is the slowdown gate's ``--slowdown-tolerance`` check.
    """

    deltas = [float(value) for value in relative_deltas if is_finite(value)]
    observed = statistics.median(deltas) if deltas else None
    spread = relative_spread(baseline_walls)
    sigma = spread["relative_sigma"]
    if noise_sigmas == 0.0:
        allowed = wall_tolerance
    elif sigma is not None:
        allowed = max(wall_tolerance, noise_sigmas * sigma)
    else:
        allowed = None
    verdict = {
        "observed_median_relative_delta": observed,
        "wall_tolerance_fraction": wall_tolerance,
        "noise_sigmas": noise_sigmas,
        "baseline_walls": spread,
        "allowed_relative_delta": allowed,
        "basis": (
            "median of per-block candidate-versus-baseline relative deltas, at most "
            "max(wall_tolerance, noise_sigmas x baseline stdev/median)"
        ),
    }
    if not evidence_complete or observed is None:
        return {
            **verdict,
            "status": "inconclusive",
            "reason": "measured counterbalanced evidence is incomplete",
        }
    if allowed is None:
        return {
            **verdict,
            "status": "inconclusive",
            "reason": "the baseline's relative spread is unavailable",
        }
    passed = observed <= allowed
    return {
        **verdict,
        "status": "pass" if passed else "fail",
        "reason": (
            "candidate median paired slowdown is within the noise band"
            if passed
            else "candidate median paired slowdown exceeds the noise band"
        ),
    }


def peak_rss_verdict(
    baseline_rss: Sequence[int | None],
    candidate_rss: Sequence[int | None],
    *,
    rss_tolerance: float,
    evidence_complete: bool,
) -> dict[str, Any]:
    """Candidate median peak RSS at most (1 + rss_tolerance) x baseline's."""

    missing = sum(
        1 for value in (*baseline_rss, *candidate_rss) if positive_int(value) is None
    )
    medians = {
        role: (
            statistics.median([value for value in values if positive_int(value)])
            if any(positive_int(value) for value in values)
            else None
        )
        for role, values in (("baseline", baseline_rss), ("candidate", candidate_rss))
    }
    ratio = (
        medians["candidate"] / medians["baseline"]
        if medians["baseline"] and medians["candidate"]
        else None
    )
    verdict = {
        "baseline_median_bytes": medians["baseline"],
        "candidate_median_bytes": medians["candidate"],
        "observed_ratio": ratio,
        "allowed_ratio": 1.0 + rss_tolerance,
        "rss_tolerance_fraction": rss_tolerance,
        "missing_run_count": missing,
        "basis": "candidate median peak RSS over baseline median peak RSS",
    }
    if not evidence_complete or missing or ratio is None:
        return {
            **verdict,
            "status": "inconclusive",
            "reason": (
                f"peak RSS is missing for {missing} measured run(s)"
                if missing
                else "measured counterbalanced evidence is incomplete"
            ),
        }
    passed = ratio <= 1.0 + rss_tolerance
    return {
        **verdict,
        "status": "pass" if passed else "fail",
        "reason": (
            "candidate median peak RSS is within tolerance"
            if passed
            else "candidate median peak RSS exceeds tolerance"
        ),
    }


def product_verdict(
    comparison: dict[str, Any] | None, *, product_nrms: float
) -> dict[str, Any]:
    """Every compared product's NRMS at most ``product_nrms``.

    Disagreement (a product over the threshold, a shape mismatch, a product
    one build published and the other did not) fails; a comparison that did
    not run or could not measure a product is inconclusive.
    """

    verdict: dict[str, Any] = {
        "product_nrms": product_nrms,
        "basis": "RMS(candidate - baseline) / RMS(baseline) per product, full arrays",
        "products": {},
        "worst_nrms": None,
    }
    if comparison is None or comparison.get("status") in {
        "not_compared",
        "unavailable",
        "failed_execution",
        "failed_validation",
    }:
        reason = (comparison or {}).get("reason") or "products were not compared"
        return {**verdict, "status": "inconclusive", "reason": str(reason)}
    failures: list[str] = []
    incomplete: list[str] = []
    inventory = comparison.get("product_inventory") or {}
    if inventory.get("status") == "mismatch":
        failures.append(
            "the builds publish different products (candidate missing "
            f"{inventory.get('left_missing')}, extra {inventory.get('left_extra')})"
        )
    nrms_values: list[float] = []
    for suffix, product in sorted((comparison.get("products") or {}).items()):
        status, nrms = product.get("status"), product.get("nrms")
        if status == "compared" and is_finite(nrms):
            passed = float(nrms) <= product_nrms
            nrms_values.append(float(nrms))
            verdict["products"][suffix] = "pass" if passed else "fail"
            if not passed:
                failures.append(f"{suffix} NRMS {float(nrms):.3g} > {product_nrms:g}")
        elif status in DISAGREEING_PRODUCT_STATUSES or (
            status == "missing"
            and product.get("left_exists") is False
            and product.get("right_exists") is True
        ):
            verdict["products"][suffix] = "fail"
            failures.append(f"{suffix} {status}")
        else:
            verdict["products"][suffix] = "inconclusive"
            incomplete.append(
                f"{suffix} has no finite NRMS (is the baseline's RMS zero?)"
                if status == "compared"
                else f"{suffix} {status}"
            )
    verdict["worst_nrms"] = max(nrms_values) if nrms_values else None
    if failures:
        return {**verdict, "status": "fail", "reason": "; ".join(failures)}
    if incomplete or not verdict["products"]:
        reason = "; ".join(incomplete) or "the comparison measured no products"
        return {**verdict, "status": "inconclusive", "reason": reason}
    return {
        **verdict,
        "status": "pass",
        "reason": "every product is within the NRMS threshold",
    }


def not_evaluated(gate: str) -> dict[str, Any]:
    return {"status": "not_evaluated", "reason": f"not part of the {gate} gate"}


def overall_verdict(
    verdicts: Mapping[str, dict[str, Any]], required: Sequence[str]
) -> tuple[str, str]:
    """Pass only when every required verdict passes; any fail fails."""

    statuses = {name: verdicts[name]["status"] for name in required}
    for outcome in ("fail", "inconclusive"):
        named = [name for name in required if statuses[name] == outcome]
        if named:
            return outcome, "; ".join(
                f"{name}: {verdicts[name]['reason']}" for name in named
            )
    return "pass", "every verdict passes: " + ", ".join(required)
