#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Sustained MAIN-table write rate against the volume's ceiling (#841).

The simulation plan's §5 I/O target: on a dataset at least twice the size of
RAM, casa-rs's sustained MAIN-table rate is at least 70% of the volume's raw
sequential-write ceiling for the write-only case, and at least 50% end to
end with Metal prediction. Run it on the host whose volume is measured
(GLENDENNING, the 1 TB NVMe drive), with any Python 3 (standard library
only), when nothing else is running:

1. **The ceiling.** `dd` writes `--ceiling-gib` of zeros (default: twice RAM
   plus 8 GiB) in 8 MiB blocks to the volume, then `sync`; the ceiling is the
   bytes over the wall time of both. It is measured before the cases and
   again after them, and the file deleted.
2. **The cases.** Each scales a CASA manifest of `bench_campaign.py`'s
   (`--write-manifest`, case 5 write-only; `--predict-manifest`, case 3, a
   one-plane model over the band, with its noise) to `--channels` channels
   over the same band and `--hours` hours of hour angle centred on transit,
   and replays it with the `observe_scenarios` example under
   `/usr/bin/time -l` (`--backend cpu` for the write-only case, `auto` for
   the predicting one), then runs `sync`. The rate is the MS's bytes on disk
   over `close`'s time (the example's "written in", from the pass's setup
   through publication) plus the `sync`; the rate over the whole process
   wall plus the `sync` is reported beside it. The MS is deleted once sized.

A rest precedes every timed step. The page cache cannot be purged without
`sudo purge`, which is the owner's to run: `--purged` records that it was.
Without it, the dataset's size (at least twice RAM, checked after each case)
keeps the cache from holding more than part of it. `results.json` and
`results.md` go to `--out` and, with `--nas-dir`, to the NAS.
"""

from __future__ import annotations

import argparse
import copy
import datetime
import json
import os
import re
import shutil
import socket
import subprocess
import time

from bench_campaign import du_bytes, loadavg, now, parse_pass, parse_time_l, sha256

GIB = 1 << 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="directory on the measured volume (replaced)")
    parser.add_argument("--bin", required=True, help="the release observe_scenarios example")
    parser.add_argument("--write-manifest", required=True, help="CASA manifest of case 5 write-only")
    parser.add_argument("--predict-manifest", required=True, help="CASA manifest of case 3")
    parser.add_argument("--model", required=True, help="case 3's model image on this host")
    parser.add_argument("--channels", type=int, default=4096)
    parser.add_argument("--hours", type=float, default=4.0)
    parser.add_argument("--ceiling-gib", type=float, help="dd size (default twice RAM plus 8 GiB)")
    parser.add_argument("--rest-s", type=float, default=60.0)
    parser.add_argument("--purged", action="store_true", help="`sudo purge` ran before this")
    parser.add_argument("--only", nargs="*", help="run only these steps")
    parser.add_argument("--nas-dir", help="where the results are copied")
    return parser.parse_args()


def log(message: str) -> None:
    print(f"[{now()}] {message}", flush=True)


def ram_bytes() -> int:
    return int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout)


def scaled(manifest_path: str, name: str, channels: int, hours: float, model: str | None) -> dict:
    """The manifest's scenario with `channels` channels over its band and
    `hours` of hour angle centred on transit, renamed `name`."""
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    manifest = copy.deepcopy(manifest)
    entry = manifest["scenarios"][0]
    entry["name"] = name
    for call in entry["calls"]:
        resolved = call["resolved"]
        if call["op"] == "setspwindow":
            band = resolved["deltafreq"] * resolved["nchannels"]
            resolved["nchannels"] = channels
            resolved["deltafreq"] = resolved["freqresolution"] = band / channels
        elif call["op"] == "observe":
            resolved["starttime"] = -hours * 1800.0
            resolved["stoptime"] = hours * 1800.0
        elif call["op"] == "predict":
            resolved["imagename"] = [model]
    manifest["scenarios"] = [entry]
    return manifest


def timed_shell(command: list[str], stem: str) -> dict:
    """Run `command` then `sync`, each timed, logging to `<stem>.out`/`.err`."""
    record = {"command": command, "load_before": loadavg(), "started": now()}
    started = time.perf_counter()
    with open(stem + ".out", "w") as out, open(stem + ".err", "w") as err:
        process = subprocess.run(["/usr/bin/time", "-l", *command], stdout=out, stderr=err)
    record["elapsed_s"] = time.perf_counter() - started
    synced = time.perf_counter()
    subprocess.run(["sync"], check=True)
    record["sync_s"] = time.perf_counter() - synced
    record["finished"] = now()
    record["load_after"] = loadavg()
    record["exit"] = process.returncode
    with open(stem + ".err") as handle:
        stderr = handle.read()
    record.update(parse_time_l(stderr))
    record["pass"] = parse_pass(stderr)
    if process.returncode != 0:
        record["stderr_tail"] = stderr.splitlines()[-25:]
    return record


class Sustained:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out = os.path.abspath(args.out)
        shutil.rmtree(self.out, ignore_errors=True)
        self.logs = os.path.join(self.out, "logs")
        os.makedirs(self.logs)
        self.ram = ram_bytes()
        self.ceiling_bytes = int((args.ceiling_gib or (2 * self.ram + 8 * GIB) / GIB) * GIB)
        binary = os.path.abspath(args.bin)
        repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        commit = subprocess.run(
            ["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip()
        self.results = {
            "schema_version": 1,
            "purpose": "simulation plan section 5 I/O target (#841): sustained MAIN rate against the ceiling",
            "host": socket.gethostname(),
            "commit": commit,
            "binary": binary,
            "binary_sha256": sha256(binary),
            "ram_bytes": self.ram,
            "volume": subprocess.run(["df", "-h", self.out], capture_output=True, text=True).stdout,
            "purged": args.purged,
            "channels": args.channels,
            "hours": args.hours,
            "started": now(),
            "ceilings": [],
            "cases": {},
            "failures": [],
        }

    def write(self) -> None:
        self.results["updated"] = now()
        with open(os.path.join(self.out, "results.json"), "w", encoding="utf-8") as handle:
            json.dump(self.results, handle, indent=2)
            handle.write("\n")
        with open(os.path.join(self.out, "results.md"), "w", encoding="utf-8") as handle:
            handle.write(markdown(self.results))
        if self.args.nas_dir:
            try:
                os.makedirs(self.args.nas_dir, exist_ok=True)
                for item in ("results.json", "results.md"):
                    shutil.copy(os.path.join(self.out, item), self.args.nas_dir)
                shutil.copytree(self.logs, os.path.join(self.args.nas_dir, "logs"), dirs_exist_ok=True)
            except OSError as error:
                log(f"NAS copy failed: {error}")

    def ceiling(self, label: str) -> None:
        time.sleep(self.args.rest_s)
        path = os.path.join(self.out, "ceiling.bin")
        blocks = self.ceiling_bytes // (8 << 20)
        log(f"ceiling {label}: dd {blocks} blocks of 8 MiB (load {loadavg()})")
        run = timed_shell(
            ["dd", "if=/dev/zero", f"of={path}", "bs=8m", f"count={blocks}"],
            os.path.join(self.logs, f"ceiling-{label}"),
        )
        run["bytes"] = du_bytes(path)
        os.remove(path)
        if run["exit"] == 0 and run["bytes"]:
            run["rate_bytes_per_s"] = run["bytes"] / (run["elapsed_s"] + run["sync_s"])
        else:
            self.results["failures"].append(f"ceiling {label}: dd exit {run['exit']}")
        run["label"] = label
        self.results["ceilings"].append(run)
        self.write()
        log(f"ceiling {label}: {fmt_rate(run.get('rate_bytes_per_s'))}")

    def case(self, name: str, manifest: dict, backend: str) -> None:
        time.sleep(self.args.rest_s)
        manifest_path = os.path.join(self.out, f"{name}.manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2)
        shutil.copy(manifest_path, self.logs)
        rs_out = os.path.join(self.out, name)
        log(f"{name}: casa-rs --backend {backend} (load {loadavg()})")
        stem = os.path.join(self.logs, name)
        run = timed_shell([os.path.abspath(self.args.bin), manifest_path, rs_out, "--backend", backend], stem)
        with open(stem + ".out") as handle:
            match = re.search(r"written in ([\d.]+) s", handle.read())
        run["written_in_s"] = float(match.group(1)) if match else None
        run["ms_bytes"] = du_bytes(os.path.join(rs_out, f"{name}.ms"))
        shutil.rmtree(rs_out, ignore_errors=True)
        if run["exit"] != 0 or not run["ms_bytes"] or not run["written_in_s"]:
            self.results["failures"].append(f"{name}: exit {run['exit']}")
        else:
            run["rate_bytes_per_s"] = run["ms_bytes"] / (run["written_in_s"] + run["sync_s"])
            run["process_rate_bytes_per_s"] = run["ms_bytes"] / (run["wall_s"] + run["sync_s"])
            run["at_least_twice_ram"] = run["ms_bytes"] >= 2 * self.ram
            if not run["at_least_twice_ram"]:
                self.results["failures"].append(f"{name}: MS smaller than twice RAM")
        self.results["cases"][name] = run
        self.write()
        log(f"{name}: {fmt_rate(run.get('rate_bytes_per_s'))}, MS {gib(run['ms_bytes'])} GiB")

    def run(self) -> None:
        args = self.args
        steps = [
            ("ceiling-before", lambda: self.ceiling("before")),
            (
                "write-only",
                lambda: self.case(
                    "sustained-write",
                    scaled(args.write_manifest, "sustained-write", args.channels, args.hours, None),
                    "cpu",
                ),
            ),
            (
                "metal-predict",
                lambda: self.case(
                    "sustained-metal",
                    scaled(
                        args.predict_manifest,
                        "sustained-metal",
                        args.channels,
                        args.hours,
                        os.path.abspath(args.model),
                    ),
                    "auto",
                ),
            ),
            ("ceiling-after", lambda: self.ceiling("after")),
        ]
        for name, step in steps:
            if args.only and name not in args.only:
                continue
            step()
        log(f"done, {len(self.results['failures'])} failures")


def fmt_rate(rate) -> str:
    return "-" if rate is None else f"{rate / 1e9:.2f} GB/s"


def gib(value) -> str:
    return "-" if value is None else f"{value / GIB:.1f}"


def markdown(results: dict) -> str:
    rates = [c["rate_bytes_per_s"] for c in results["ceilings"] if c.get("rate_bytes_per_s")]
    ceiling = min(rates) if rates else None
    lines = [
        "# Sustained MAIN-table rate against the volume's ceiling (#841)",
        "",
        f"- Host `{results['host']}`, commit `{results['commit']}`, RAM {gib(results['ram_bytes'])} GiB, "
        f"page cache purged first: {'yes' if results['purged'] else 'no'}",
        f"- Binary `{results['binary']}` (SHA-256 `{results['binary_sha256']}`)",
        f"- {results['channels']} channels over each case's band, {results['hours']} h of hour angle",
        "- Ceiling: dd of zeros in 8 MiB blocks plus sync. Case rate: MS bytes / (close's"
        " written-in + sync); process rate: MS bytes / (process wall + sync). The fraction"
        " uses the lower ceiling.",
        "",
        "| step | GiB | dd or close s | sync s | wall s | rate | process rate | fraction of ceiling | target | load before |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for run in results["ceilings"]:
        lines.append(
            f"| ceiling {run['label']} | {gib(run.get('bytes'))} | {run['elapsed_s']:.1f} | "
            f"{run['sync_s']:.1f} | {run['elapsed_s']:.1f} | {fmt_rate(run.get('rate_bytes_per_s'))} | - | - | - | "
            f"{run['load_before']} |"
        )
    targets = {"sustained-write": 0.70, "sustained-metal": 0.50}
    for name, run in results["cases"].items():
        rate = run.get("rate_bytes_per_s")
        fraction = f"{rate / ceiling:.0%}" if rate and ceiling else "-"
        written = run.get("written_in_s")
        lines.append(
            f"| {name} ({(run.get('pass') or {}).get('backend', '-')}) | {gib(run.get('ms_bytes'))} | "
            f"{'-' if written is None else f'{written:.1f}'} | {run['sync_s']:.1f} | {run.get('wall_s', 0):.1f} | "
            f"{fmt_rate(rate)} | {fmt_rate(run.get('process_rate_bytes_per_s'))} | {fraction} | "
            f"{targets.get(name, 0):.0%} | {run['load_before']} |"
        )
    if results["failures"]:
        lines += ["", "## Failures", ""] + [f"- {failure}" for failure in results["failures"]]
    return "\n".join(lines) + "\n"


def main() -> int:
    sustained = Sustained(parse_args())
    sustained.run()
    return 1 if sustained.results["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
