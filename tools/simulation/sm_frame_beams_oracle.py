#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record focused CASA 6.7.6.14 primary-beam/frame controls (SIM-10).

Run with CASA's Python. Inputs and the combined fixture are explicit outputs;
independent workers keep their logs and exact commands under --out.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys


def scenarios():
    component = ["tools/simulation/sm_component_signals_oracle.py", "--beam",
                 "--beam-observer", "ARRAY"]
    image = ["tools/simulation/sm_image_layouts_oracle.py"]
    jobs = [
        ("components-array", component + ["--only", "frame_jmean", "frame_app",
                                         "frame_hadec", "frame_azel"]),
        ("components-refresh", component + ["--only", "frame_azel", "--refresh-beam"]),
        ("components-alma", ["tools/simulation/sm_component_signals_oracle.py",
                             "--beam", "--only", "frame_azel"]),
    ]
    jobs += [(f"image-{frame}", image + ["--case", f"frame_{frame}_beam",
                                       "--location", "ARRAY"])
             for frame in ("jmean", "app", "hadec", "azel")]
    jobs += [
        ("image-refresh", image + ["--case", "frame_azel_beam", "--location",
                                   "ARRAY", "--refresh-beam"]),
        ("image-geocentre", image + ["--case", "frame_hadec_beam"]),
        ("image-squint", image + ["--case", "frame_hadec_beam", "--location", "ARRAY", "--squint"]),
        ("component-jones", component + ["--only", "frame_hadec", "--jones-beam"]),
    ]
    return jobs


def run(root, scratch, job):
    name, arguments = job
    fixture = scratch / (name + ".json.gz")
    command = [sys.executable, *arguments, "--out", str(scratch / name),
               "--fixture", str(fixture)]
    with (scratch / (name + ".log")).open("w") as log:
        result = subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
    return dict(name=name, command=command, exit_code=result.returncode,
                fixture=str(fixture))


def collect(root, results, destination):
    cases = []
    destination.mkdir(parents=True, exist_ok=True)
    for result in results:
        with gzip.open(result["fixture"], "rt") as stream:
            reference = json.load(stream)
        assert reference["casa_version"] == "6.7.6.14"
        source = Path(result["fixture"]).parent / result["name"] / "inputs"
        for case in reference["cases"]:
            name = case["name"]
            if "spec" in case:
                case["operation"] = "component"
                original, target = source / (name + ".cl"), destination / (name + ".cl")
            else:
                original, target = source / "model.image", destination / (name + ".image")
            shutil.copytree(original, target, ignore=shutil.ignore_patterns("table.lock"))
            case["input"] = str(target.relative_to(root / "libs/casa/casa-simulation/tests/data"))
            if case.get("circular") or case.get("constant_frequency_hz") is not None:
                base = destination / (name + ".ms")
                shutil.copytree(source / "base.ms", base, ignore=shutil.ignore_patterns("table.lock"))
                case["base"] = str(base.relative_to(root / "libs/casa/casa-simulation/tests/data"))
            if case.get("vp_table"):
                beam = destination / (name + "-beam")
                beam.mkdir()
                shutil.copytree(source / case["vp_table"], beam / "beam.vp",
                                ignore=shutil.ignore_patterns("table.lock"))
                if case.get("jones_beam"):
                    shutil.copytree(source / "jones.image", beam / "jones.image",
                                    ignore=shutil.ignore_patterns("table.lock"))
                case["vp_table"] = str((beam / "beam.vp").relative_to(root / "libs/casa/casa-simulation/tests/data"))
            cases.append(case)
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    scratch = args.out.resolve()
    scratch.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda job: run(root, scratch, job), scenarios()))
    (scratch / "commands.json").write_text(json.dumps(results, indent=2) + "\n")
    for result in results:
        print(result["name"], result["exit_code"], flush=True)
    assert all(result["exit_code"] == 0 for result in results)
    cases = collect(root, results, args.inputs.resolve())
    fixture = args.fixture.resolve()
    fixture.parent.mkdir(parents=True, exist_ok=True)
    with fixture.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as stream:
        stream.write((json.dumps(dict(casa_version="6.7.6.14", cases=cases),
                                 separators=(",", ":")) + "\n").encode())
    print("Saved", len(cases), "cases", fixture, flush=True)


if __name__ == "__main__":
    main()
