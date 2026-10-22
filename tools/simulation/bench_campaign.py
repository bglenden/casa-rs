#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Simulation Phase 1d benchmark campaign (#841): CASA's `sm` against casa-rs.

Run through `bench_campaign.sh`, with any Python 3 (standard library only).
For each scenario of the case file (`bench_scenarios.json`), in file order:

1. write the scenario's model images with `make_bench_model.py` if missing;
2. run the CASA oracle (`casa_sm_observe_oracle.py --only <case>`) once under
   `/usr/bin/time -l`, recording its in-process total and per-call seconds
   from the manifest, then delete its MS and model copies;
3. replay that manifest with casa-rs's `observe_scenarios` example: Metal
   (backend auto), CPU at the default workers, CPU with one worker, CPU and
   Metal with every thread, Metal again, each under `/usr/bin/time -l`,
   deleting each MS once it is sized.

A rest precedes every timed run, and each run records the load averages
before and after it. After every run a line is appended to `runs.jsonl` and
`results.json` and `results.md` are rewritten from it; after every case the
summary (results, logs and CASA manifests, no MSs) is copied to `--nas-dir`
when that is given. The header records the checkout's commit, whether its
tracked files are clean, and the binary's SHA-256.

A benchmark times completed work: CASA's run counts only if every call
returned `True` (the oracle records a `False` or an exception and carries
on, and the casa-rs replay agrees with whatever CASA did), so a case with
any other outcome is not replayed and gets no multiple (#853 review). A
failing run is recorded and the campaign moves on; it exits 1 if any case
failed.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))

THREADS = os.cpu_count() or 1

# The default policy (Balanced: the performance cores) on Metal and the CPU,
# one worker, and every thread (as today's simulator, `bench_old_simulator.py`,
# uses them).
VARIANTS = [
    ("metal-1", ["--backend", "auto"]),
    ("cpu", ["--backend", "cpu"]),
    ("cpu-w1", ["--backend", "cpu", "--workers", "1"]),
    (f"cpu-w{THREADS}", ["--backend", "cpu", "--workers", str(THREADS)]),
    (f"metal-w{THREADS}", ["--backend", "auto", "--workers", str(THREADS)]),
    ("metal-2", ["--backend", "auto"]),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default=os.path.join(HERE, "bench_scenarios.json"))
    parser.add_argument("--out", required=True, help="campaign directory")
    parser.add_argument("--models", help="model directory (default <out>/models)")
    parser.add_argument("--bin", required=True, help="the observe_scenarios example binary")
    parser.add_argument("--casa-python", required=True, help="CASA's python3")
    parser.add_argument("--rest-s", type=float, default=60.0, help="rest before each timed run")
    parser.add_argument("--only", nargs="*", help="run only these cases")
    parser.add_argument("--variants", nargs="*", help="run only these casa-rs variants")
    parser.add_argument("--nas-dir", help="where each case's summary is copied")
    return parser.parse_args()


def now() -> str:
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{now()}] {message}", flush=True)


def loadavg() -> str:
    result = subprocess.run(["sysctl", "-n", "vm.loadavg"], capture_output=True, text=True)
    return result.stdout.strip()


def du_bytes(path: str) -> int | None:
    if not os.path.exists(path):
        return None
    result = subprocess.run(["du", "-sk", path], capture_output=True, text=True)
    try:
        return int(result.stdout.split()[0]) * 1024
    except (IndexError, ValueError):
        return None


def number(text: str):
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return text


def parse_time_l(stderr: str) -> dict:
    """Wall, user and system seconds and peak memory from `/usr/bin/time -l`."""
    found = {}
    match = re.search(r"([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys", stderr)
    if match:
        found["wall_s"], found["user_s"], found["sys_s"] = (float(g) for g in match.groups())
    match = re.search(r"(\d+)\s+maximum resident set size", stderr)
    if match:
        found["max_rss_bytes"] = int(match.group(1))
    match = re.search(r"(\d+)\s+peak memory footprint", stderr)
    if match:
        found["peak_footprint_bytes"] = int(match.group(1))
    return found


def parse_pass(stderr: str) -> dict | None:
    """The fields of casa-rs's "simulation pass" event."""
    for line in stderr.splitlines():
        if "simulation pass" in line:
            fields = dict(re.findall(r"(\w+)=(\S+)", line))
            return {key: number(value) for key, value in fields.items()}
    return None


def timed(command: list[str], stem: str) -> dict:
    """Run `command` under `/usr/bin/time -l`, logging to `<stem>.out`/`.err`."""
    record = {"command": command, "load_before": loadavg(), "started": now()}
    started = time.perf_counter()
    with open(stem + ".out", "w") as out, open(stem + ".err", "w") as err:
        process = subprocess.run(["/usr/bin/time", "-l", *command], stdout=out, stderr=err)
    record["elapsed_s"] = time.perf_counter() - started
    record["finished"] = now()
    record["load_after"] = loadavg()
    record["exit"] = process.returncode
    with open(stem + ".err") as handle:
        stderr = handle.read()
    record.update(parse_time_l(stderr))
    record["stdout"] = os.path.basename(stem + ".out")
    record["stderr"] = os.path.basename(stem + ".err")
    if process.returncode != 0:
        record["stderr_tail"] = stderr.splitlines()[-25:]
    return record


def rest(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def failed_calls(entry: dict) -> list[str]:
    """The calls of a CASA manifest entry that did not return `True`."""
    failures = []
    for call in entry["calls"]:
        if "raised" in call:
            failures.append(f"{call['op']} raised {call['raised']}")
        elif call.get("returned") is not True:
            failures.append(f"{call['op']} returned {call.get('returned')}")
    return failures


def add_multiples(cases: dict) -> None:
    """Each successful casa-rs run's CASA multiple, against a CASA run whose
    every call returned `True`."""
    for case in cases.values():
        casa = case["casa"] or {}
        casa_total = casa.get("casa_total_s")
        if casa.get("exit") != 0 or casa.get("failed_calls") or not casa_total:
            continue
        for run in case["casa_rs"].values():
            if run.get("wall_s") and run["exit"] == 0:
                run["casa_multiple_wall"] = casa_total / run["wall_s"]


def sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def model_names(scenario: dict) -> list[str]:
    names = []
    for call in scenario["calls"]:
        if call["op"] == "predict":
            images = call.get("imagename", [])
            names.extend([images] if isinstance(images, str) else images)
    return [name for name in names if name]


class Campaign:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out = os.path.abspath(args.out)
        self.models = os.path.abspath(args.models or os.path.join(self.out, "models"))
        self.logs = os.path.join(self.out, "logs")
        os.makedirs(self.logs, exist_ok=True)
        os.makedirs(self.models, exist_ok=True)
        with open(args.cases, encoding="utf-8") as handle:
            self.cases = json.load(handle)
        commit = subprocess.run(
            ["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip()
        changed = subprocess.run(
            ["git", "-C", REPO, "status", "--porcelain", "--untracked-files=no"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        binary = os.path.abspath(args.bin)
        self.failures: list[str] = []
        self.header = {
            "schema_version": 1,
            "purpose": "simulation Phase 1d benchmarks (#841): CASA sm against casa-rs",
            "host": socket.gethostname(),
            "commit": commit,
            "checkout_clean": not changed,
            "cases_file": os.path.abspath(args.cases),
            "binary": binary,
            "binary_sha256": sha256(binary),
            "binary_mtime": datetime.datetime.fromtimestamp(os.path.getmtime(binary))
            .astimezone()
            .isoformat(timespec="seconds"),
            "casa_python": args.casa_python,
            "rest_s": args.rest_s,
            "nice": os.nice(0),
            "started": now(),
        }
        self.runs_path = os.path.join(self.out, "runs.jsonl")
        self.runs: list[dict] = []
        if os.path.exists(self.runs_path):
            os.rename(self.runs_path, self.runs_path + f".{int(time.time())}.old")

    def record(self, run: dict) -> None:
        self.runs.append(run)
        with open(self.runs_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(run) + "\n")
        self.write_results()

    def make_models(self, scenario: dict) -> bool:
        missing = [n for n in model_names(scenario) if not os.path.exists(os.path.join(self.models, n))]
        if not missing:
            return True
        stem = os.path.join(self.logs, f"{scenario['name']}.models")
        log(f"{scenario['name']}: writing models {missing}")
        run = timed(
            [
                self.args.casa_python,
                os.path.join(HERE, "make_bench_model.py"),
                "--scenarios",
                os.path.abspath(self.args.cases),
                "--model-root",
                self.models,
                "--only",
                *missing,
            ],
            stem,
        )
        run.update(case=scenario["name"], kind="models", models=missing)
        self.record(run)
        if run["exit"] != 0:
            self.failures.append(f"{scenario['name']}: model generation exit {run['exit']}")
        return run["exit"] == 0

    def run_casa(self, scenario: dict) -> str | None:
        name = scenario["name"]
        casa_out = os.path.join(self.out, "casa", name)
        rest(self.args.rest_s)
        log(f"{name}: CASA (load {loadavg()})")
        run = timed(
            [
                self.args.casa_python,
                os.path.join(HERE, "casa_sm_observe_oracle.py"),
                "--out",
                casa_out,
                "--scenarios",
                os.path.abspath(self.args.cases),
                "--model-root",
                self.models,
                "--only",
                name,
            ],
            os.path.join(self.logs, f"{name}.casa"),
        )
        run.update(case=name, kind="casa", variant="casa")
        manifest_path = os.path.join(casa_out, "manifest.json")
        manifest = None
        if run["exit"] == 0 and os.path.exists(manifest_path):
            with open(manifest_path, encoding="utf-8") as handle:
                manifest = json.load(handle)
            shutil.copy(manifest_path, os.path.join(self.logs, f"{name}.casa-manifest.json"))
            entry = manifest["scenarios"][0]
            run["casa_total_s"] = entry["seconds"]
            run["calls"] = [
                {key: call[key] for key in ("op", "seconds", "returned", "raised") if key in call}
                for call in entry["calls"]
                if "seconds" in call
            ]
            run["ms_bytes"] = du_bytes(entry["ms"])
            run["failed_calls"] = failed_calls(entry)
        for item in os.listdir(casa_out) if os.path.isdir(casa_out) else []:
            if item.endswith(".ms") or item.endswith(".image"):
                shutil.rmtree(os.path.join(casa_out, item), ignore_errors=True)
        self.record(run)
        log(f"{name}: CASA exit {run['exit']}, total {run.get('casa_total_s')} s")
        if manifest is None:
            self.failures.append(f"{name}: CASA exit {run['exit']}, no manifest")
            return None
        if run["failed_calls"]:
            self.failures.append(f"{name}: CASA calls failed: {'; '.join(run['failed_calls'])}")
            return None
        return manifest_path

    def run_casa_rs(self, scenario: dict, manifest_path: str) -> None:
        name = scenario["name"]
        for variant, flags in VARIANTS:
            if self.args.variants and variant not in self.args.variants:
                continue
            rs_out = os.path.join(self.out, "rs", f"{name}-{variant}")
            shutil.rmtree(rs_out, ignore_errors=True)
            rest(self.args.rest_s)
            log(f"{name}: casa-rs {variant} (load {loadavg()})")
            stem = os.path.join(self.logs, f"{name}.rs-{variant}")
            run = timed([os.path.abspath(self.args.bin), manifest_path, rs_out, *flags], stem)
            run.update(case=name, kind="casa-rs", variant=variant, flags=flags)
            with open(stem + ".out") as handle:
                stdout = handle.read()
            match = re.search(r"written in ([\d.]+) s", stdout)
            run["written_in_s"] = float(match.group(1)) if match else None
            match = re.search(r"workers: (\d+) streaming", stdout)
            run["streaming_workers"] = int(match.group(1)) if match else None
            with open(stem + ".err") as handle:
                run["pass"] = parse_pass(handle.read())
            run["ms_bytes"] = du_bytes(os.path.join(rs_out, f"{name}.ms"))
            shutil.rmtree(rs_out, ignore_errors=True)
            self.record(run)
            if run["exit"] != 0:
                self.failures.append(f"{name}: casa-rs {variant} exit {run['exit']}")
            log(
                f"{name}: casa-rs {variant} exit {run['exit']}, wall {run.get('wall_s')} s, "
                f"user {run.get('user_s')} s"
            )

    def copy_to_nas(self) -> None:
        if not self.args.nas_dir:
            return
        try:
            os.makedirs(self.args.nas_dir, exist_ok=True)
            for item in ("results.json", "results.md", "runs.jsonl"):
                path = os.path.join(self.out, item)
                if os.path.exists(path):
                    shutil.copy(path, self.args.nas_dir)
            shutil.copytree(self.logs, os.path.join(self.args.nas_dir, "logs"), dirs_exist_ok=True)
            log(f"summary copied to {self.args.nas_dir}")
        except OSError as error:
            log(f"NAS copy failed: {error}")

    def write_results(self) -> None:
        cases = {}
        for run in self.runs:
            case = cases.setdefault(run["case"], {"casa": None, "casa_rs": {}, "models": None})
            if run["kind"] == "casa":
                case["casa"] = run
            elif run["kind"] == "models":
                case["models"] = run
            else:
                case["casa_rs"][run["variant"]] = run
        add_multiples(cases)
        results = dict(self.header, updated=now(), cases=cases, failures=self.failures)
        with open(os.path.join(self.out, "results.json"), "w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2)
            handle.write("\n")
        with open(os.path.join(self.out, "results.md"), "w", encoding="utf-8") as handle:
            handle.write(markdown(results, self.cases))

    def run(self) -> None:
        log(f"campaign: {json.dumps(self.header)}")
        for scenario in self.cases["scenarios"]:
            name = scenario["name"]
            if self.args.only and name not in self.args.only:
                continue
            if not self.make_models(scenario):
                log(f"{name}: model generation failed; skipping the case")
                continue
            manifest_path = self.run_casa(scenario)
            if manifest_path is None:
                log(f"{name}: CASA failed; skipping casa-rs")
            else:
                self.run_casa_rs(scenario, manifest_path)
            self.copy_to_nas()
        for failure in self.failures:
            log(f"failed: {failure}")
        log(f"campaign done, {len(self.failures)} failures")
        self.write_results()
        self.copy_to_nas()


def fmt(value, digits: int = 1) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def gib(value) -> str:
    return "-" if value is None else f"{value / 2**30:.2f}"


def markdown(results: dict, cases_file: dict) -> str:
    labels = {s["name"]: s.get("case", "") for s in cases_file["scenarios"]}
    lines = [
        "# Simulation Phase 1d benchmarks (#841)",
        "",
        f"- Host `{results['host']}`, commit `{results['commit']}`"
        f" ({'clean' if results.get('checkout_clean') else 'with changes'}), nice {results['nice']}",
        f"- Binary `{results['binary']}` (built {results['binary_mtime']}, "
        f"SHA-256 `{results.get('binary_sha256')}`)",
        f"- Started {results['started']}, updated {results['updated']}; rest {results['rest_s']} s before each run",
        "- CASA total is the oracle's in-process time from `open` through `close` (no Python start-up).",
        "  The multiple is CASA total / casa-rs process wall, for cases whose every CASA call returned True.",
        "",
    ]
    if results.get("failures"):
        lines += ["## Failures", ""] + [f"- {failure}" for failure in results["failures"]] + [""]
    for name, case in results["cases"].items():
        lines += [f"## {name} ({labels.get(name, '')})", ""]
        casa = case["casa"]
        if casa:
            calls = ", ".join(
                f"{c['op']} {fmt(c.get('seconds'), 2)} s" for c in casa.get("calls", [])
            )
            lines += [
                f"- CASA: exit {casa['exit']}, total {fmt(casa.get('casa_total_s'))} s; "
                f"process wall {fmt(casa.get('wall_s'))} s, user {fmt(casa.get('user_s'))} s, "
                f"peak RSS {gib(casa.get('max_rss_bytes'))} GiB; MS {gib(casa.get('ms_bytes'))} GiB",
                f"- CASA per call: {calls}",
                f"- CASA load before/after: {casa['load_before']} / {casa['load_after']}",
            ]
            if casa.get("failed_calls"):
                lines.append(f"- CASA calls that did not return True: {'; '.join(casa['failed_calls'])}")
            lines.append("")
        if case["casa_rs"]:
            lines += [
                "| variant | backend | exit | wall s | user s | sys s | peak RSS GiB | written in s | "
                "workers | geometry s | predict s | cells s | writer write s | finalize s | "
                "MS GiB | CASA multiple | load before |",
                "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
            ]
            for variant, run in case["casa_rs"].items():
                stage = run.get("pass") or {}
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            variant,
                            fmt(stage.get("backend")),
                            str(run["exit"]),
                            fmt(run.get("wall_s"), 2),
                            fmt(run.get("user_s"), 2),
                            fmt(run.get("sys_s"), 2),
                            gib(run.get("max_rss_bytes")),
                            fmt(run.get("written_in_s"), 2),
                            fmt(stage.get("workers")),
                            fmt(stage.get("geometry_s"), 2),
                            fmt(stage.get("predict_s"), 2),
                            fmt(stage.get("cells_s"), 2),
                            fmt(stage.get("writer_write_s"), 2),
                            fmt(stage.get("writer_finalize_s"), 2),
                            gib(run.get("ms_bytes")),
                            fmt(run.get("casa_multiple_wall"), 2),
                            run["load_before"],
                        ]
                    )
                    + " |"
                )
            lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    campaign = Campaign(parse_args())
    campaign.run()
    return 1 if campaign.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
