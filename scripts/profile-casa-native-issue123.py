#!/usr/bin/env python3
"""Native CASA profiling harness for Wave 4 issue #123.

The regular issue #123 parity script times CASA tasks from Python, but Python
profiling cannot see where the C++ extension spends time. This helper runs the
same applycal -> mstransform slice and attaches macOS `sample` after CASA task
imports are complete, so the resulting reports mostly reflect native CASA work.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path


def profile_enabled(value: str | None) -> bool:
    return value not in {None, "", "0", "false", "False", "FALSE", "off", "Off", "OFF"}


def default_casa_python() -> Path:
    configured = os.environ.get("CASA_RS_CASA_PYTHON")
    if configured:
        return Path(configured)
    return Path.home() / "SoftwareProjects/casa-build/venv/bin/python"


def wait_for(path: Path, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.025)
    raise RuntimeError(f"timed out waiting for {path}")


def write_child_script(
    path: Path,
    *,
    label: str,
    task_body: str,
) -> None:
    body = textwrap.dedent(task_body).strip()
    header = """import json
import os
import time
from pathlib import Path

from casatasks import applycal, mstransform

ready = Path(os.environ["CASA_RS_CHILD_READY"])
go = Path(os.environ["CASA_RS_CHILD_GO"])
timing = Path(os.environ["CASA_RS_CHILD_TIMING"])
ready.write_text("ready\\n")
while not go.exists():
    time.sleep(0.01)

started = time.perf_counter()
"""
    footer = f"""
elapsed = time.perf_counter() - started
timing.write_text(json.dumps({{"label": {label!r}, "elapsed_seconds": elapsed}}, indent=2))
"""
    path.write_text(f"{header}{body}\n{footer}")


def run_sampled_task(
    *,
    casa_python: Path,
    outdir: Path,
    label: str,
    task_body: str,
    duration_seconds: int,
    interval_ms: int,
    extra_env: dict[str, str],
) -> dict[str, float | str | int]:
    ready = outdir / f"{label}.ready"
    go = outdir / f"{label}.go"
    timing = outdir / f"{label}-timing.json"
    child_script = outdir / f"{label}.py"
    sample_file = outdir / f"{label}-native-sample.txt"
    stdout_file = outdir / f"{label}.stdout"
    stderr_file = outdir / f"{label}.stderr"
    for path in [ready, go, timing, sample_file, stdout_file, stderr_file, child_script]:
        if path.exists():
            path.unlink()

    write_child_script(child_script, label=label, task_body=task_body)
    env = os.environ.copy()
    env.update(extra_env)
    env.update(
        {
            "CASA_RS_CHILD_READY": str(ready),
            "CASA_RS_CHILD_GO": str(go),
            "CASA_RS_CHILD_TIMING": str(timing),
        }
    )
    with stdout_file.open("w") as stdout, stderr_file.open("w") as stderr:
        child = subprocess.Popen(
            [str(casa_python), str(child_script)],
            stdout=stdout,
            stderr=stderr,
            env=env,
        )
    try:
        wait_for(ready, timeout_seconds=60)
        sampler = subprocess.Popen(
            [
                "sample",
                str(child.pid),
                str(duration_seconds),
                str(interval_ms),
                "-mayDie",
                "-fullPaths",
                "-file",
                str(sample_file),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        go.write_text("go\n")
        child_rc = child.wait()
        sampler_out, _ = sampler.communicate(timeout=duration_seconds + 30)
    except Exception:
        child.kill()
        child.wait()
        raise

    if child_rc != 0:
        raise RuntimeError(
            f"{label} CASA child failed with {child_rc}; see {stderr_file}"
        )
    timing_data = json.loads(timing.read_text())
    return {
        "label": label,
        "elapsed_seconds": timing_data["elapsed_seconds"],
        "child_pid": child.pid,
        "sample_returncode": sampler.returncode,
        "sample_report": str(sample_file),
        "sample_stdout": sampler_out.strip(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "outdir",
        nargs="?",
        default="target/wdad-wave4-123-native-profile",
        help="output artifact directory",
    )
    parser.add_argument(
        "--issue122-dir",
        default=os.environ.get(
            "CASA_RS_ISSUE122_ARTIFACTS", "target/wdad-wave4-122-middlefreq"
        ),
        help="issue #122 artifact directory with TDRW0001_10s.ms and casa-priorcal",
    )
    parser.add_argument("--sample-interval-ms", type=int, default=1)
    parser.add_argument("--apply-duration", type=int, default=8)
    parser.add_argument("--transform-duration", type=int, default=5)
    args = parser.parse_args()

    casa_python = default_casa_python()
    if not casa_python.exists():
        raise SystemExit(f"CASA Python not found: {casa_python}")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    issue122 = Path(args.issue122_dir).resolve()
    input_ms = issue122 / "TDRW0001_10s.ms"
    prior = issue122 / "casa-priorcal"
    if not input_ms.is_dir() or not prior.is_dir():
        raise SystemExit(f"missing issue #122 artifacts under {issue122}")

    apply_ms = outdir / "casa-native-apply.ms"
    transform_ms = outdir / "casa-native-transform.ms"
    if apply_ms.exists():
        shutil.rmtree(apply_ms)
    if transform_ms.exists():
        shutil.rmtree(transform_ms)
    shutil.copytree(input_ms, apply_ms)

    common_env = {
        "CASA_RS_MS_PATH": str(apply_ms),
        "CASA_RS_PRIOR": str(prior),
        "CASA_RS_TRANSFORM_OUT": str(transform_ms),
    }
    apply_body = """
    prior = os.environ["CASA_RS_PRIOR"]
    applycal(
        vis=os.environ["CASA_RS_MS_PATH"],
        field="1",
        gaintable=[f"{prior}/cal.ant", f"{prior}/cal.gc", f"{prior}/cal.tau"],
        interp=["", "nearest", "nearest"],
        calwt=False,
        applymode="calonly",
    )
    """
    transform_body = """
    mstransform(
        vis=os.environ["CASA_RS_MS_PATH"],
        outputvis=os.environ["CASA_RS_TRANSFORM_OUT"],
        field="1",
        spw="0:7~58",
        datacolumn="corrected",
        reindex=False,
    )
    """
    results = []
    results.append(
        run_sampled_task(
            casa_python=casa_python,
            outdir=outdir,
            label="applycal",
            task_body=apply_body,
            duration_seconds=args.apply_duration,
            interval_ms=args.sample_interval_ms,
            extra_env=common_env,
        )
    )
    results.append(
        run_sampled_task(
            casa_python=casa_python,
            outdir=outdir,
            label="mstransform",
            task_body=transform_body,
            duration_seconds=args.transform_duration,
            interval_ms=args.sample_interval_ms,
            extra_env=common_env,
        )
    )
    summary = {
        "casa_python": str(casa_python),
        "issue122_dir": str(issue122),
        "sample_interval_ms": args.sample_interval_ms,
        "results": results,
    }
    (outdir / "casa-native-profile-summary.json").write_text(
        json.dumps(summary, indent=2)
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
