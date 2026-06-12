#!/usr/bin/env python3
"""Generate Wave 3 #287 paired backend benchmark commands."""

from __future__ import annotations

import argparse
import json
import pathlib
import random
import shlex
import sys
from typing import Any


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = pathlib.Path("target/imperformance-wave3/worker-policy")
BACKENDS = ("cpu", "multi-cpu", "auto", "metal")

SCENARIOS: list[dict[str, Any]] = [
    {
        "id": "standard-mfs-hogbom-heavy",
        "mode_family": "standard_mfs_hogbom",
        "base_workload": "wave3-standard-mfs-single-term-heavy-wave2",
        "overrides": {"deconvolver": "hogbom"},
    },
    {
        "id": "standard-mfs-clark-heavy",
        "mode_family": "standard_mfs_clark",
        "base_workload": "wave3-standard-mfs-clark-heavy-wave2",
        "overrides": {"deconvolver": "clark"},
    },
    {
        "id": "mfs-multiscale-heavy",
        "mode_family": "mfs_multiscale",
        "base_workload": "wave3-mfs-ms-multiscale-heavy-wave2",
        "overrides": {"deconvolver": "multiscale", "scales": [0, 5, 15]},
    },
    {
        "id": "mtmfs-heavy",
        "mode_family": "mtmfs",
        "base_workload": "wave3-mtmfs-heavy-wave1-medium-serial",
        "overrides": {"deconvolver": "mtmfs", "nterms": 2},
    },
    {
        "id": "wprojection-hogbom-clean",
        "mode_family": "wprojection_hogbom",
        "base_workload": "wave3-wprojection-single-plane-heavy-wave1-medium-clean-auto",
        "overrides": {"deconvolver": "hogbom", "niter": 100},
    },
    {
        "id": "wprojection-clark-clean",
        "mode_family": "wprojection_clark",
        "base_workload": "wave3-wprojection-single-plane-heavy-wave1-medium-clean-auto",
        "overrides": {"deconvolver": "clark", "niter": 100},
    },
    {
        "id": "mosaic-mfs-hogbom-stress",
        "mode_family": "mosaic_mfs_hogbom",
        "base_workload": "wave3-mosaic-mfs-alma-large-stress-serial",
        "overrides": {"deconvolver": "hogbom", "scales": ""},
    },
    {
        "id": "mosaic-mfs-clark-stress",
        "mode_family": "mosaic_mfs_clark",
        "base_workload": "wave3-mosaic-mfs-alma-large-stress-serial",
        "overrides": {"deconvolver": "clark", "scales": ""},
    },
    {
        "id": "mosaic-mfs-multiscale-stress",
        "mode_family": "mosaic_mfs_multiscale",
        "base_workload": "wave3-mosaic-mfs-alma-large-stress-serial",
        "overrides": {"deconvolver": "multiscale", "scales": [0, 5, 15]},
    },
    {
        "id": "aw-widefield-hogbom-medium",
        "mode_family": "aw_widefield_hogbom",
        "base_workload": "wave3-aw-widefield-medium-serial",
        "overrides": {"deconvolver": "hogbom"},
    },
    {
        "id": "aw-widefield-clark-medium",
        "mode_family": "aw_widefield_clark",
        "base_workload": "wave3-aw-widefield-medium-serial",
        "overrides": {"deconvolver": "clark"},
    },
    {
        "id": "standard-cube-hogbom-one-channel",
        "mode_family": "standard_cube_hogbom",
        "base_workload": "wave3-standard-mfs-single-term-heavy-wave2",
        "overrides": {
            "specmode": "cube",
            "deconvolver": "hogbom",
            "channel_count": 1,
            "interpolation": "nearest",
            "width": "64",
        },
    },
    {
        "id": "standard-cube-clark-one-channel",
        "mode_family": "standard_cube_clark",
        "base_workload": "wave3-standard-mfs-clark-heavy-wave2",
        "overrides": {
            "specmode": "cube",
            "deconvolver": "clark",
            "channel_count": 1,
            "interpolation": "nearest",
            "width": "64",
        },
    },
    {
        "id": "cubedata-hogbom-one-channel",
        "mode_family": "cubedata_hogbom",
        "base_workload": "wave3-standard-mfs-single-term-heavy-wave2",
        "overrides": {
            "specmode": "cubedata",
            "deconvolver": "hogbom",
            "channel_count": 1,
            "interpolation": "nearest",
        },
    },
    {
        "id": "cubedata-clark-one-channel",
        "mode_family": "cubedata_clark",
        "base_workload": "wave3-standard-mfs-clark-heavy-wave2",
        "overrides": {
            "specmode": "cubedata",
            "deconvolver": "clark",
            "channel_count": 1,
            "interpolation": "nearest",
        },
    },
    {
        "id": "mosaic-cube-clark-width1024",
        "mode_family": "mosaic_cube_clark",
        "base_workload": "wave3-mosaic-cube-alma-large-stress-serial",
        "overrides": {"deconvolver": "clark", "width": "1024"},
    },
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--profile-repeats", type=int, default=1)
    parser.add_argument(
        "--skip-profile",
        action="store_true",
        help="set CASA_RS_BENCH_SKIP_PROFILE=1 for timing-only paired blocks",
    )
    parser.add_argument("--output-root", type=pathlib.Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--format", choices=("commands", "json"), default="commands")
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="scenario id to include; repeat for multiple scenarios",
    )
    parser.add_argument(
        "--backend",
        action="append",
        choices=BACKENDS,
        default=[],
        help="backend to include; repeat for multiple backends",
    )
    parser.add_argument(
        "--order",
        choices=("paired-blocks", "grouped-backends"),
        default="paired-blocks",
        help=(
            "paired-blocks runs one backend per command and rotates backend order "
            "inside each paired block; grouped-backends keeps the older one-command "
            "per backend layout"
        ),
    )
    parser.add_argument("--seed", type=int, default=287)
    parser.add_argument(
        "--include-casa",
        action="store_true",
        help="do not set CASA_RS_BENCH_SKIP_CASA=1 in generated commands",
    )
    args = parser.parse_args()

    rows = build_rows(
        args.repeats,
        args.profile_repeats,
        args.output_root,
        skip_casa=not args.include_casa,
        skip_profile=args.skip_profile,
        paired_blocks=args.order == "paired-blocks",
        seed=args.seed,
        scenario_filter=set(args.scenario),
        backend_filter=set(args.backend),
    )
    if args.format == "json":
        json.dump({"schema_version": 1, "rows": rows}, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        for row in rows:
            print(row["shell_command"])


def build_rows(
    repeats: int,
    profile_repeats: int,
    output_root: pathlib.Path,
    *,
    skip_casa: bool,
    skip_profile: bool = False,
    paired_blocks: bool = True,
    seed: int = 287,
    scenario_filter: set[str] | None = None,
    backend_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows = []
    rng = random.Random(seed)
    scenario_filter = scenario_filter or set()
    backend_filter = backend_filter or set()
    for scenario in SCENARIOS:
        if scenario_filter and scenario["id"] not in scenario_filter:
            continue
        selected_backends = [
            backend for backend in BACKENDS if not backend_filter or backend in backend_filter
        ]
        if paired_blocks:
            for block_index in range(1, repeats + 1):
                block_backends = list(selected_backends)
                rng.shuffle(block_backends)
                for block_order, backend in enumerate(block_backends, start=1):
                    rows.append(
                        build_row(
                            scenario,
                            backend,
                            output_root,
                            skip_casa=skip_casa,
                            skip_profile=skip_profile,
                            command_repeats=1,
                            profile_repeats=profile_repeats,
                            paired_block=block_index,
                            paired_block_order=block_order,
                            total_paired_blocks=repeats,
                        )
                    )
            continue
        for backend in selected_backends:
            rows.append(
                build_row(
                    scenario,
                    backend,
                    output_root,
                    skip_casa=skip_casa,
                    skip_profile=skip_profile,
                    command_repeats=repeats,
                    profile_repeats=profile_repeats,
                )
            )
    return rows


def build_row(
    scenario: dict[str, Any],
    backend: str,
    output_root: pathlib.Path,
    *,
    skip_casa: bool,
    skip_profile: bool,
    command_repeats: int,
    profile_repeats: int,
    paired_block: int | None = None,
    paired_block_order: int | None = None,
    total_paired_blocks: int | None = None,
) -> dict[str, Any]:
    overrides = dict(scenario["overrides"])
    overrides["standard_mfs_acceleration"] = backend
    output_dir = output_root / "screening" / scenario["id"] / backend
    run_label = f"wave3-287-{scenario['id']}-{backend}"
    if paired_block is not None:
        output_dir = output_dir / f"block-{paired_block:02d}"
        run_label = f"wave3-287-{scenario['id']}-block{paired_block:02d}-{backend}"
    command = [
        "python3",
        "tools/perf/imager/run_workload.py",
        scenario["base_workload"],
        "--repeats",
        str(command_repeats),
        "--run-label",
        run_label,
        "--output-dir",
        str(output_dir),
    ]
    for key, value in sorted(overrides.items()):
        command.extend(["--set-imaging", f"{key}={format_override(value)}"])
    shell_parts = []
    if skip_casa:
        shell_parts.append("CASA_RS_BENCH_SKIP_CASA=1")
    if skip_profile:
        shell_parts.append("CASA_RS_BENCH_SKIP_PROFILE=1")
    shell_parts.append(f"CASA_RS_BENCH_PROFILE_REPEATS={profile_repeats}")
    shell_parts.extend(shlex.quote(part) for part in command)
    row = {
        "scenario_id": scenario["id"],
        "mode_family": scenario["mode_family"],
        "backend": backend,
        "base_workload": scenario["base_workload"],
        "repeats": command_repeats,
        "profile_repeats": profile_repeats,
        "output_dir": str(output_dir),
        "overrides": overrides,
        "argv": command,
        "env": {
            **({"CASA_RS_BENCH_SKIP_CASA": "1"} if skip_casa else {}),
            **({"CASA_RS_BENCH_SKIP_PROFILE": "1"} if skip_profile else {}),
            "CASA_RS_BENCH_PROFILE_REPEATS": str(profile_repeats),
        },
        "shell_command": " ".join(shell_parts),
    }
    if paired_block is not None:
        row.update(
            {
                "paired_block": paired_block,
                "paired_block_order": paired_block_order,
                "total_paired_blocks": total_paired_blocks,
            }
        )
    return row


def format_override(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    return str(value)


if __name__ == "__main__":
    main()
