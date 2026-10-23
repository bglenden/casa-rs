#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Simulation Phase 1d no-regression benchmark (#841): today's simulator.

Times the plan's section 5 cases (`bench_scenarios.json`) on today's
simulator, the `casa-ms` `simobserve` binary at e73ae19af2, for comparison
with the `casa-simulation` session timed by `bench_campaign.py`. Run through
`bench_old_simulator.sh`, with any Python 3 (standard library only); the CASA
steps run `old_simulator_casa.py` with `--casa-python`.

For each scenario, in file order:

1. read the campaign's CASA manifest for it (`<manifests>/<case>.casa-manifest.json`,
   the resolved calls `observe_scenarios` replays) and translate the calls
   into one `simobserve --json-run` request (`bench_old_simulator.json`
   records the translation and every difference from the new session's
   workload);
2. export the case's model image to FITS with CASA's `ia.tofits` if missing,
   or write its component list (the case file's `complists` entry) as the
   old binary's analytic model;
3. run the request `--repeats` times for each `--variants` entry (`auto`, the
   old binary's default worker policy; `w1`, one prediction and corruption
   worker beside the writer, as casa-rs's `cpu-w1`) under `/usr/bin/time -l`,
   numbering the runs after earlier ones when `--append` keeps `runs.jsonl`. Before each
   run the model FITS is read once (warm page cache), the rest is taken, and
   the harness waits until no other benchmark, CASA or build runs on the host
   and the one-minute load is below `--max-load`. After each run the MS is
   sized, summarized with CASA's table tool (rows, DATA shape, TIME range,
   subtables), checked against the expected rows and DATA shape (and against
   the new session's pass rows when `--reference-results` names a campaign's
   `results.json`), and deleted.

After every run a line is appended to `runs.jsonl` and `results.json` and
`results.md` are rewritten from it; after every case the summary (no MSs or
FITS) is copied to `--nas-dir` when that is given. `--tabulate` only rewrites
the summary, for example against a newer campaign's `--reference-results`.
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import re
import shutil
import socket
import subprocess
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
HELPER = os.path.join(HERE, "old_simulator_casa.py")

# Processes that make the host busy: the new simulator's campaign, CASA, builds.
COMPETITORS = re.compile(
    r"observe_scenarios|bench_campaign|casa_sm_|CASA\.app|casatools|cargo|rustc|simobserve"
)
# Shells and tools whose command lines merely mention a competitor.
BYSTANDERS = {"zsh", "bash", "sh", "ssh", "sshd", "sshd-session", "pgrep", "ps", "grep", "tail"}

# Worker variants: the old binary's default (auto: available_parallelism
# prediction and corruption workers), and one worker beside the writer.
VARIANT_PREFIX = {"auto": "old", "w1": "old-w1"}
VARIANT_SUFFIX = {"auto": "", "w1": ".w1"}
# Which old runs each new-session campaign variant is set beside.
REFERENCE_PAIRS = {"metal-1": "old", "cpu": "old", "metal-2": "old", "cpu-w1": "old-w1"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default=os.path.join(HERE, "bench_scenarios.json"))
    parser.add_argument("--mapping", default=os.path.join(HERE, "bench_old_simulator.json"))
    parser.add_argument("--manifests", required=True, help="directory of <case>.casa-manifest.json")
    parser.add_argument("--out", required=True, help="output directory (requests, logs, results)")
    parser.add_argument("--models", help="FITS model directory (default <out>/models)")
    parser.add_argument("--image-root", help="read the CASA model images here, not the manifest's path")
    parser.add_argument("--bin", help="the old simobserve binary")
    parser.add_argument("--casa-python", help="CASA's python3")
    parser.add_argument("--repeats", type=int, default=2, help="runs per case and variant")
    parser.add_argument(
        "--variants",
        nargs="+",
        choices=sorted(VARIANT_PREFIX),
        default=["auto"],
        help="worker variants: auto (the default policy), w1 (one worker beside the writer)",
    )
    parser.add_argument("--append", action="store_true", help="add to runs.jsonl instead of replacing it")
    parser.add_argument("--rest-s", type=float, default=60.0, help="rest before each timed run")
    parser.add_argument("--casa-time-shift-s", type=float, default=37.0, help="as observe_scenarios")
    parser.add_argument("--max-load", type=float, default=3.0, help="one-minute load to start below")
    parser.add_argument("--busy-deadline-s", type=float, default=3600.0, help="give up waiting after")
    parser.add_argument("--only", nargs="*", help="run only these cases")
    parser.add_argument("--keep-ms", action="store_true", help="keep each MS (about 1.4 GiB)")
    parser.add_argument("--reference-results", help="a bench_campaign results.json to set beside")
    parser.add_argument("--nas-dir", help="where the summary is copied after each case")
    parser.add_argument("--tabulate", action="store_true", help="only rewrite results from runs.jsonl")
    args = parser.parse_args()
    if not args.tabulate and not (args.bin and args.casa_python):
        parser.error("--bin and --casa-python are required unless --tabulate")
    return args


def now() -> str:
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{now()}] {message}", flush=True)


def loadavg() -> str:
    result = subprocess.run(["sysctl", "-n", "vm.loadavg"], capture_output=True, text=True)
    return result.stdout.strip()


def current_nice() -> int | None:
    """This process's nice value (read, not set: a sandbox may refuse `os.nice`)."""
    try:
        return os.getpriority(os.PRIO_PROCESS, 0)
    except OSError:
        return None


def load1() -> float:
    try:
        return float(loadavg().strip("{} ").split()[0])
    except (IndexError, ValueError):
        return math.inf


def du_bytes(path: str) -> int | None:
    if not os.path.exists(path):
        return None
    result = subprocess.run(["du", "-sk", path], capture_output=True, text=True)
    try:
        return int(result.stdout.split()[0]) * 1024
    except (IndexError, ValueError):
        return None


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


def last_json_line(text: str) -> dict:
    for line in reversed(text.splitlines()):
        if line.startswith("{"):
            return json.loads(line)
    raise ValueError("no JSON line")


def own_lineage() -> set[int]:
    """This process and its ancestors."""
    table = subprocess.run(["ps", "-Ao", "pid=,ppid="], capture_output=True, text=True).stdout
    parent = {}
    for line in table.splitlines():
        fields = line.split()
        if len(fields) == 2:
            parent[int(fields[0])] = int(fields[1])
    lineage, pid = set(), os.getpid()
    while pid > 1 and pid not in lineage:
        lineage.add(pid)
        pid = parent.get(pid, 1)
    return lineage


def competitors() -> list[str]:
    table = subprocess.run(["ps", "-Ao", "pid=,command="], capture_output=True, text=True).stdout
    lineage = own_lineage()
    busy = []
    for line in table.splitlines():
        pid_text, _, command = line.strip().partition(" ")
        if not pid_text.isdigit() or int(pid_text) in lineage or not COMPETITORS.search(command):
            continue
        if os.path.basename(command.split()[0]).lstrip("-") in BYSTANDERS:
            continue
        busy.append(f"{pid_text} {command[:160]}")
    return busy


class Busy(RuntimeError):
    pass


def wait_until_idle(args: argparse.Namespace) -> dict:
    """Wait for no competitor and a one-minute load below `--max-load`."""
    started = time.monotonic()
    while True:
        busy, load = competitors(), load1()
        waited = time.monotonic() - started
        if not busy and load < args.max_load:
            return {"waited_s": round(waited, 1), "load1_at_start": load}
        if waited > args.busy_deadline_s:
            raise Busy(f"host still busy after {waited:.0f} s: load {load}, {busy}")
        time.sleep(30)


def fits_header(path: str) -> dict:
    """BITPIX, axis lengths and types of a FITS primary header."""
    cards = {}
    with open(path, "rb") as handle:
        while True:
            block = handle.read(2880)
            if len(block) < 2880:
                break
            for offset in range(0, 2880, 80):
                card = block[offset : offset + 80].decode("ascii", "replace")
                key, value = card[:8].strip(), card[10:].strip()
                if value.startswith("'"):
                    value = value[1:].split("'")[0].strip()
                else:
                    value = value.split("/")[0].strip()
                if key == "END":
                    naxis = int(cards.get("NAXIS", 0))
                    return {
                        "bitpix": int(cards.get("BITPIX", 0)),
                        "shape": [int(cards.get(f"NAXIS{i}", 0)) for i in range(1, naxis + 1)],
                        "ctype": [cards.get(f"CTYPE{i}", "") for i in range(1, naxis + 1)],
                        "bunit": cards.get("BUNIT"),
                    }
                if card[8:10] == "= ":
                    cards[key] = value
    return {}


ANGLE_UNITS = {"rad": 1.0, "deg": math.pi / 180.0, "arcsec": math.pi / 180.0 / 3600.0}
FREQUENCY_UNITS = {"Hz": 1.0, "MHz": 1.0e6, "GHz": 1.0e9}


def quantity(text: str, units: dict) -> float:
    """`text`, a number followed by one of `units`' names, in their base unit."""
    for unit, scale in sorted(units.items(), key=lambda item: -len(item[0])):
        if text.endswith(unit):
            return float(text[: -len(unit)]) * scale
    raise ValueError(f"{text!r} is not in {sorted(units)}")


def analytic_model(spec: list[dict], centre_rad: list[float]) -> list[dict]:
    """The old binary's analytic components for a scenario file's component
    list (`cl.addcomponent` arguments): J2000 points and Gaussians of Stokes I
    in Jy, offset from `centre_rad` as l = Δα cos δ₀ and m = Δδ, with each
    one's spectral index at its reference frequency."""
    ra0, dec0 = centre_rad
    components = []
    for index, entry in enumerate(spec):
        frame, ra, dec = entry["dir"]
        flux = entry["flux"]
        spectrum = entry.get("spectrumtype", "constant")
        if (
            frame != "J2000"
            or entry.get("polarization", "Stokes") != "Stokes"
            or entry.get("fluxunit", "Jy") != "Jy"
            or any(flux[1:])
            or entry["shape"] not in ("point", "gaussian")
            or spectrum not in ("constant", "spectral index")
        ):
            raise ValueError(
                f"component {index}: the old binary takes J2000 points and Gaussians of Stokes I "
                "in Jy with a spectral index"
            )
        delta_ra = math.remainder(quantity(ra, ANGLE_UNITS) - ra0, 2.0 * math.pi)
        component = {
            "kind": entry["shape"],
            "name": f"c{index}",
            "l_rad": delta_ra * math.cos(dec0),
            "m_rad": quantity(dec, ANGLE_UNITS) - dec0,
            "spectrum": {
                "flux_jy": flux[0],
                "spectral_index": entry.get("index", 0.0) if spectrum == "spectral index" else 0.0,
                "reference_frequency_hz": quantity(entry["freq"], FREQUENCY_UNITS),
            },
        }
        if entry["shape"] == "gaussian":
            component.update(
                major_fwhm_rad=quantity(entry["majoraxis"], ANGLE_UNITS),
                minor_fwhm_rad=quantity(entry["minoraxis"], ANGLE_UNITS),
                position_angle_rad=quantity(entry["positionangle"], ANGLE_UNITS),
            )
        components.append(component)
    return components


class Harness:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out = os.path.abspath(args.out)
        self.models = os.path.abspath(args.models or os.path.join(self.out, "models"))
        self.logs = os.path.join(self.out, "logs")
        self.requests = os.path.join(self.out, "requests")
        self.ms_dir = os.path.join(self.out, "ms")
        for path in (self.logs, self.requests, self.models, self.ms_dir):
            os.makedirs(path, exist_ok=True)
        with open(args.cases, encoding="utf-8") as handle:
            self.cases = json.load(handle)
        with open(args.mapping, encoding="utf-8") as handle:
            self.mapping = json.load(handle)
        self.runs_path = os.path.join(self.out, "runs.jsonl")
        self.header_path = os.path.join(self.out, "header.json")
        self.runs: list[dict] = []
        self.reference = self.load_reference()
        if args.tabulate or args.append:
            with open(self.header_path, encoding="utf-8") as handle:
                self.header = json.load(handle)
            with open(self.runs_path, encoding="utf-8") as handle:
                self.runs = [json.loads(line) for line in handle if line.strip()]
            if args.append:
                self.header.setdefault("appended", []).append(
                    {"started": now(), "variants": args.variants, "repeats": args.repeats,
                     "only": args.only, "nice": current_nice()}
                )
                self.write_header()
            return
        commit = subprocess.run(
            ["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip() or "not in a git checkout"
        binary = os.path.abspath(args.bin)
        self.header = {
            "schema_version": 1,
            "purpose": self.mapping["purpose"],
            "old_commit": self.mapping["old_commit"],
            "host": socket.gethostname(),
            "harness_commit": commit,
            "cases_file": os.path.abspath(args.cases),
            "manifests": os.path.abspath(args.manifests),
            "binary": binary,
            "binary_mtime": datetime.datetime.fromtimestamp(os.path.getmtime(binary))
            .astimezone()
            .isoformat(timespec="seconds"),
            "casa_python": args.casa_python,
            "rest_s": args.rest_s,
            "repeats": args.repeats,
            "variants": args.variants,
            "max_load": args.max_load,
            "nice": current_nice(),
            "started": now(),
        }
        self.write_header()
        if os.path.exists(self.runs_path):
            os.rename(self.runs_path, self.runs_path + f".{int(time.time())}.old")

    def write_header(self) -> None:
        with open(self.header_path, "w", encoding="utf-8") as handle:
            json.dump(self.header, handle, indent=2)
            handle.write("\n")

    def load_reference(self) -> dict | None:
        if not self.args.reference_results:
            return None
        with open(self.args.reference_results, encoding="utf-8") as handle:
            return json.load(handle)

    def casa(self, *helper_args: str) -> dict:
        """One `old_simulator_casa.py` call; CASA's own log goes to `logs/`."""
        result = subprocess.run(
            [self.args.casa_python, HELPER, *helper_args],
            capture_output=True,
            text=True,
            cwd=self.logs,
        )
        if result.returncode != 0:
            raise RuntimeError(f"CASA helper {helper_args[0]} failed: {result.stderr[-2000:]}")
        return last_json_line(result.stdout)

    def record(self, run: dict) -> None:
        self.runs.append(run)
        with open(self.runs_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(run) + "\n")
        self.write_results()

    def manifest_entry(self, name: str) -> dict:
        path = os.path.join(self.args.manifests, f"{name}.casa-manifest.json")
        with open(path, encoding="utf-8") as handle:
            manifest = json.load(handle)
        for entry in manifest["scenarios"]:
            if entry["name"] == name:
                return entry
        raise KeyError(f"{name} is not in {path}")

    def build_request(self, name: str, entry: dict) -> tuple[dict, dict]:
        """The old binary's request for one manifest, and what it should write."""
        calls = [call for call in entry["calls"] if call.get("returned", True) is not False]
        resolved = {}
        for call in calls:
            resolved.setdefault(call["op"], call.get("resolved", {}))
        config = resolved["setconfig"]
        antennas = [
            {"name": antname, "station": padname, "position_m": [x, y, z], "dish_diameter_m": dish}
            for antname, padname, x, y, z, dish in zip(
                config["antname"], config["padname"], config["x"], config["y"], config["z"],
                config["dishdiameter"],
            )
        ]
        window = resolved["setspwindow"]
        stokes = window["stokes"].split()
        times = resolved["settimes"]
        field = resolved["setfield"]
        observes = [call.get("resolved", {}) for call in calls if call["op"] == "observe"]
        observe = observes[0]
        # Several observe calls: the old binary takes one span and cycles its
        # fields an integration at a time, so they must be contiguous.
        for previous, following in zip(observes, observes[1:]):
            if following["starttime"] != previous["stoptime"]:
                raise ValueError(f"{name}: the old binary observes one span; observe calls leave a gap")
        ra, dec = field["sourcedirection"]
        offset = {"offset_s": 0.0}
        if times.get("usehourangle"):
            offset = self.casa(
                "hour-angle", "--ra", repr(ra), "--dec", repr(dec),
                "--mjd-days", repr(times["referencetime"]),
                "--itrf", *(repr(value) for value in antennas[0]["position_m"]),
            )
        start = (
            times["referencetime"] * 86400.0
            + offset["offset_s"]
            + observe["starttime"]
            + self.args.casa_time_shift_s
        )
        duration = observes[-1]["stoptime"] - observe["starttime"]
        request = {
            "output_ms": os.path.join(self.ms_dir, f"{name}.ms"),
            "overwrite": True,
            "telescope_name": config["telescopename"],
            "field_name": field["sourcename"],
            "antennas": antennas,
            "phase_center_rad": [ra % (2.0 * math.pi), dec],
            "start_time_mjd_seconds": start,
            "duration_seconds": duration,
            "integration_seconds": times["integrationtime"],
            "allow_below_elevation_limit": True,
            "spectral_windows": [
                {
                    "name": window["spwname"],
                    "start_frequency_hz": window["freq"],
                    "channel_width_hz": window["deltafreq"],
                    "channel_count": window["nchannels"],
                }
            ],
            "polarization_setup": {
                "basis": "linear" if any("X" in token for token in stokes) else "circular",
                "correlation_count": len(stokes),
            },
            "predict_model": "predict" in resolved,
        }
        if len(observes) > 1:
            directions = {
                call["resolved"]["sourcename"]: call["resolved"]["sourcedirection"]
                for call in calls
                if call["op"] == "setfield"
            }
            request["fields"] = [
                {
                    "name": scan["sourcename"],
                    "phase_center_rad": [
                        directions[scan["sourcename"]][0] % (2.0 * math.pi),
                        directions[scan["sourcename"]][1],
                    ],
                }
                for scan in observes
            ]
        info = {"model_image": None, "model_fits": None, "model_json": None}
        if "predict" in resolved:
            predict = resolved["predict"]
            images = [image for image in predict.get("imagename", []) if image]
            if images:
                image = images[0]
                if self.args.image_root:
                    image = os.path.join(self.args.image_root, os.path.basename(image))
                fits = os.path.join(self.models, os.path.basename(image).removesuffix(".image") + ".fits")
                request["model"] = {"kind": "fits_image", "path": fits}
                info.update(model_image=image, model_fits=fits)
            else:
                listed = os.path.basename(predict["complist"])
                model = {
                    "schema_version": 1,
                    "name": listed,
                    "components": analytic_model(
                        self.cases["complists"][listed], [ra % (2.0 * math.pi), dec]
                    ),
                }
                path = os.path.join(self.models, f"{name}.analytic.json")
                with open(path, "w", encoding="utf-8") as handle:
                    json.dump(model, handle, indent=1)
                    handle.write("\n")
                request["model"] = {"kind": "analytic_components", "path": path}
                info.update(model_json=path)
        if "corrupt" in resolved and "setnoise" in resolved:
            request["corruption"] = {
                "seed": int(resolved.get("setseed", {}).get("seed", 1)),
                "noise": {"mode": "simplenoise", "simplenoise_jy": resolved["setnoise"]["simplenoise"]},
            }
        samples = math.ceil(duration / times["integrationtime"])
        baselines = len(antennas) * (len(antennas) - 1) // 2
        info.update(
            hour_angle_offset_s=offset["offset_s"],
            start_time_mjd_seconds=start,
            expected_rows=samples * baselines,
            expected_samples=samples,
            expected_data_shape=[len(stokes), window["nchannels"]],
            expected_time_first=start + 0.5 * times["integrationtime"],
            expected_corr_type=[{"XX": 9, "YY": 12, "RR": 5, "LL": 8, "XY": 10, "YX": 11,
                                 "RL": 6, "LR": 7}[token] for token in stokes],
            expected_chan_freq_first_hz=window["freq"],
            expected_antennas=len(antennas),
            expected_telescope=config["telescopename"],
        )
        return {"kind": "run", "request": request}, info

    def export_model(self, name: str, info: dict) -> None:
        fits = info["model_fits"]
        if fits is None:
            return
        if not os.path.exists(fits):
            log(f"{name}: exporting {info['model_image']} to {fits}")
            started = time.perf_counter()
            result = self.casa("tofits", "--image", info["model_image"], "--fits", fits)
            self.record(
                dict(result, case=name, kind="models", seconds=time.perf_counter() - started, finished=now())
            )
        info["fits_header"] = fits_header(fits)

    def reference_rows(self, name: str) -> int | None:
        case = ((self.reference or {}).get("cases") or {}).get(name) or {}
        for run in (case.get("casa_rs") or {}).values():
            rows = (run.get("pass") or {}).get("rows")
            if rows is not None:
                return rows
        return None

    def check(self, name: str, info: dict, report: dict, summary: dict) -> dict:
        checks = {
            "rows": summary.get("rows") == info["expected_rows"] == report.get("main_row_count"),
            "data_shape": summary.get("data_shape") == info["expected_data_shape"],
            "time_samples": summary.get("time_samples") == info["expected_samples"],
            "time_first": abs((summary.get("time_first") or 0.0) - info["expected_time_first"]) < 1e-3,
            "no_flags": summary.get("flagged_rows") == 0 and report.get("flagged_row_count") == 0,
            "no_autocorrelations": summary.get("autocorrelation_rows") == 0,
            "telescope": summary.get("telescope") == [info["expected_telescope"]],
            "antennas": summary.get("antennas") == info["expected_antennas"],
            "corr_type": summary.get("corr_type") == info["expected_corr_type"],
            "chan_freq": abs(summary.get("chan_freq_first_hz", 0.0) - info["expected_chan_freq_first_hz"]) < 1.0,
        }
        reference = self.reference_rows(name)
        if reference is not None:
            checks["rows_equal_reference"] = summary.get("rows") == reference
        return checks

    def next_variant(self, name: str, prefix: str) -> str:
        """`<prefix>-<n>`, numbered after this case's earlier runs (with `--append`)."""
        taken = [
            int(match.group(1))
            for run in self.runs
            if run["case"] == name and run["kind"] == "oldsim"
            for match in [re.fullmatch(re.escape(prefix) + r"-(\d+)", run["variant"])]
            if match
        ]
        return f"{prefix}-{max(taken, default=0) + 1}"

    def run_case(self, scenario: dict) -> None:
        name = scenario["name"]
        request, info = self.build_request(name, self.manifest_entry(name))
        self.export_model(name, info)
        for kind in self.args.variants:
            body = dict(request["request"])
            if kind == "w1":
                # One prediction and corruption worker, beside the writer: casa-rs's cpu-w1.
                body.update(worker_policy="fixed", row_workers=1, channel_workers=1)
            request_path = os.path.join(self.requests, f"{name}{VARIANT_SUFFIX[kind]}.json")
            with open(request_path, "w", encoding="utf-8") as handle:
                json.dump({"kind": "run", "request": body}, handle, indent=1)
                handle.write("\n")
            for _ in range(self.args.repeats):
                variant = self.next_variant(name, VARIANT_PREFIX[kind])
                self.timed_run(name, variant, request_path, body["output_ms"], info)

    def timed_run(self, name: str, variant: str, request_path: str, ms_path: str, info: dict) -> None:
        if info["model_fits"]:
            subprocess.run(["cat", info["model_fits"]], stdout=subprocess.DEVNULL, check=True)
        time.sleep(max(self.args.rest_s, 0.0))
        idle = wait_until_idle(self.args)
        log(f"{name}: {variant} (load {loadavg()}, waited {idle['waited_s']} s)")
        stem = os.path.join(self.logs, f"{name}.{variant}")
        run = timed([os.path.abspath(self.args.bin), "--json-run", request_path], stem)
        run.update(case=name, kind="oldsim", variant=variant, request=os.path.basename(request_path))
        run.update(idle=idle, info=info)
        with open(stem + ".out") as handle:
            stdout = handle.read()
        try:
            result = json.loads(stdout[stdout.index("{"):])
            report = result["result"]["report"]
            run["report"] = {key: value for key, value in report.items() if key != "timing"}
            run["timing"] = report["timing"]
            run["elapsed_millis"] = result["result"].get("elapsed_millis")
        except (ValueError, KeyError) as error:
            run["report_error"] = str(error)
            report = {}
        run["ms_bytes"] = du_bytes(ms_path)
        if run["exit"] == 0 and os.path.exists(ms_path):
            try:
                run["ms"] = self.casa("inspect", "--ms", ms_path)
                run["checks"] = self.check(name, info, report, run["ms"])
            except RuntimeError as error:
                run["inspect_error"] = str(error)
        if not self.args.keep_ms:
            shutil.rmtree(ms_path, ignore_errors=True)
        self.record(run)
        failed = [key for key, ok in (run.get("checks") or {}).items() if not ok]
        log(
            f"{name}: {variant} exit {run['exit']}, wall {run.get('wall_s')} s, "
            f"user {run.get('user_s')} s, rows {(run.get('ms') or {}).get('rows')}, "
            f"failed checks {failed}"
        )

    def copy_to_nas(self) -> None:
        if not self.args.nas_dir:
            return
        try:
            os.makedirs(self.args.nas_dir, exist_ok=True)
            for item in ("results.json", "results.md", "runs.jsonl", "header.json"):
                path = os.path.join(self.out, item)
                if os.path.exists(path):
                    shutil.copy(path, self.args.nas_dir)
            for folder in ("logs", "requests"):
                shutil.copytree(
                    os.path.join(self.out, folder),
                    os.path.join(self.args.nas_dir, folder),
                    dirs_exist_ok=True,
                )
            shutil.copy(self.args.mapping, self.args.nas_dir)
            log(f"summary copied to {self.args.nas_dir}")
        except OSError as error:
            log(f"NAS copy failed: {error}")

    def write_results(self) -> None:
        cases = {}
        for run in self.runs:
            case = cases.setdefault(run["case"], {"models": [], "oldsim": {}})
            if run["kind"] == "models":
                case["models"].append(run)
            else:
                case["oldsim"][run["variant"]] = run
        reference = None
        if self.reference:
            reference = {key: self.reference.get(key) for key in ("commit", "binary", "binary_mtime", "started")}
        results = dict(self.header, updated=now(), reference=reference, cases=cases)
        with open(os.path.join(self.out, "results.json"), "w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2)
            handle.write("\n")
        with open(os.path.join(self.out, "results.md"), "w", encoding="utf-8") as handle:
            handle.write(markdown(results, self.cases, self.mapping, self.reference))

    def run(self) -> None:
        log(f"old simulator: {json.dumps(self.header)}")
        for scenario in self.cases["scenarios"]:
            if self.args.only and scenario["name"] not in self.args.only:
                continue
            try:
                self.run_case(scenario)
            except Busy as error:
                log(f"{scenario['name']}: {error}; stopping")
                break
            finally:
                self.copy_to_nas()
        log("old simulator done")


def fmt(value, digits: int = 1) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def gib(value) -> str:
    return "-" if value is None else f"{value / 2**30:.2f}"


def seconds(millis) -> float | None:
    return None if millis is None else millis / 1000.0


def residual_s(timing: dict) -> float | None:
    """Report total minus its listed stages: chiefly the per-channel model grids and beam."""
    rows = timing.get("main_rows") or {}
    parts = [timing.get(key) for key in ("validate_millis", "setup_millis", "metadata_millis",
                                         "model_prepare_millis", "save_millis")]
    parts += [rows.get(key) for key in ("uvw_and_row_setup_millis", "prediction_millis",
                                        "corruption_millis", "scalar_column_millis",
                                        "data_io_enqueue_millis")]
    if timing.get("total_millis") is None or any(part is None for part in parts):
        return None
    return (timing["total_millis"] - sum(parts)) / 1000.0


def old_means(runs: dict) -> dict:
    """Mean wall and user seconds of each old variant's successful runs."""
    groups: dict[str, list[dict]] = {}
    for variant, run in runs.items():
        if run["exit"] == 0 and run.get("wall_s"):
            groups.setdefault(variant.rsplit("-", 1)[0], []).append(run)
    return {
        prefix: {
            "runs": len(group),
            "wall_s": sum(run["wall_s"] for run in group) / len(group),
            "user_s": sum(run["user_s"] for run in group) / len(group),
        }
        for prefix, group in groups.items()
    }


def reference_lines(name: str, reference: dict | None, runs: dict) -> list[str]:
    """The reference campaign's new-session runs beside the matching old variant."""
    case = ((reference or {}).get("cases") or {}).get(name)
    if not case or not case.get("casa_rs"):
        return []
    means = old_means(runs)
    lines = [
        f"- New session in the reference campaign (commit `{reference.get('commit')}`, binary built "
        f"{reference.get('binary_mtime')}), each set beside the old variant with the same worker "
        "model; ratios are new / old mean:",
        "",
        "| new variant | new wall s | new user s | old variant (runs) | old mean wall s | old mean user s | "
        "wall ratio | user ratio |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for variant, run in case["casa_rs"].items():
        prefix = REFERENCE_PAIRS.get(variant)
        old = means.get(prefix)
        wall, user = run.get("wall_s"), run.get("user_s")
        lines.append(
            "| "
            + " | ".join(
                [
                    variant,
                    fmt(wall, 2),
                    fmt(user, 2),
                    f"{prefix} ({old['runs']})" if old else f"{prefix or '-'} (none)",
                    fmt(old["wall_s"], 2) if old else "-",
                    fmt(old["user_s"], 2) if old else "-",
                    f"{wall / old['wall_s']:.3f}" if old and wall else "-",
                    f"{user / old['user_s']:.3f}" if old and user else "-",
                ]
            )
            + " |"
        )
    return lines + [""]


def markdown(results: dict, cases_file: dict, mapping: dict, reference: dict | None) -> str:
    labels = {s["name"]: s.get("case", "") for s in cases_file["scenarios"]}
    lines = [
        "# Simulation Phase 1d no-regression benchmark (#841): today's simulator",
        "",
        f"- Host `{results['host']}`, nice {results['nice']}; harness commit `{results['harness_commit']}`",
        f"- Today's simulator: `simobserve` at `{results['old_commit']}`, binary `{results['binary']}` "
        f"(built {results['binary_mtime']})",
        f"- Manifests `{results['manifests']}`",
        f"- Started {results['started']}, updated {results['updated']}"
        + "".join(f"; appended {item['started']}" for item in results.get("appended", []))
        + f"; rest {results['rest_s']} s before each run, then a wait for no other benchmark and "
        f"load < {results['max_load']}",
        "- Variants: `old-<n>` is the old binary's default worker policy (auto); `old-w1-<n>` is one "
        "prediction and corruption worker beside the writer (worker_policy fixed, 1 row and 1 channel worker).",
        "- Process wall, user, sys and peak RSS from `/usr/bin/time -l`; stage seconds from the old binary's "
        "report; residual = report total minus its listed stages (chiefly the per-channel model grids and beam).",
        "",
        "## Workload differences from the new session",
        "",
    ]
    lines += [f"- {note}" for note in mapping.get("notes", [])]
    lines.append("")
    for name, case in results["cases"].items():
        lines += [f"## {name} ({labels.get(name, '')})", ""]
        lines += [f"- {text}" for text in mapping.get("cases", {}).get(name, {}).get("differences", [])]
        runs = case["oldsim"]
        if runs:
            first = next(iter(runs.values()))
            info = first.get("info", {})
            commands = {}
            for run in runs.values():
                program, *rest = run["command"]
                commands.setdefault(" ".join([os.path.basename(program), *rest]), None)
            lines += [f"- Command: `{command}`" for command in commands]
            lines += [
                f"- Start {fmt(info.get('start_time_mjd_seconds'), 3)} MJD s (hour-angle offset "
                f"{fmt(info.get('hour_angle_offset_s'), 3)} s); expected {info.get('expected_rows')} rows of "
                f"{info.get('expected_data_shape')}; model FITS {info.get('fits_header') or '-'}",
                "",
                "| run | exit | wall s | user s | sys s | peak RSS GiB | MS GiB | rows | DATA shape | samples | "
                "checks | model read s | prediction s | corruption s | writer write s | save s | residual s | "
                "channel workers | load before |",
                "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
            ]
            for variant, run in runs.items():
                timing = run.get("timing") or {}
                rows = timing.get("main_rows") or {}
                ms = run.get("ms") or {}
                checks = run.get("checks") or {}
                failed = [key for key, ok in checks.items() if not ok]
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            variant,
                            str(run["exit"]),
                            fmt(run.get("wall_s"), 2),
                            fmt(run.get("user_s"), 2),
                            fmt(run.get("sys_s"), 2),
                            gib(run.get("max_rss_bytes")),
                            gib(run.get("ms_bytes")),
                            fmt(ms.get("rows")),
                            fmt(ms.get("data_shape")),
                            fmt(ms.get("time_samples")),
                            ("all pass" if checks and not failed else ("failed: " + ", ".join(failed) if failed else "-")),
                            fmt(seconds(timing.get("model_prepare_millis")), 2),
                            fmt(seconds(rows.get("prediction_millis")), 2),
                            fmt(seconds(rows.get("corruption_millis")), 2),
                            fmt(seconds(rows.get("data_io_write_millis")), 2),
                            fmt(seconds(timing.get("save_millis")), 2),
                            fmt(residual_s(timing), 2),
                            fmt(rows.get("channel_prediction_workers")),
                            run["load_before"],
                        ]
                    )
                    + " |"
                )
            lines.append("")
            lines += reference_lines(name, reference, runs)
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    harness = Harness(args)
    if args.tabulate:
        harness.write_results()
        return 0
    harness.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
