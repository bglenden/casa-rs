#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check casa-atmosphere against CASA's `at` (atmosphere) tool.

casa-atmosphere corrects four of ATM's data errors
(`atm-corrections.patch`, `docs/CASA (C++) bugs.md`), so it no longer
equals the `at` tool. The check links the two through the C++ oracle
built both ways (`build-atm-oracle.sh`). Runs under CASA's Python, which
has `casatools`:

  cargo build --release -p casa-atmosphere --example at_tool_quantities
  tools/atmosphere/build-atm-oracle.sh <ATM> --binary $TMPDIR/atm_oracle
  tools/atmosphere/build-atm-oracle.sh <ATM> --as-shipped --binary $TMPDIR/atm_oracle_as_shipped
  /Applications/CASA.app/Contents/MacOS/python3 tools/atmosphere/at_tool_check.py \\
      --rust $CARGO_TARGET_DIR/release/examples/at_tool_quantities \\
      --cpp $TMPDIR/atm_oracle --cpp-as-shipped $TMPDIR/atm_oracle_as_shipped

For seven atmospheres (all five `at` types at Chajnantor, a humid sea level
and the VLA in winter) and fifteen bands from 22 GHz to 1 THz it reads, per
channel, the opacities, phase delays, path lengths and sky temperatures the
`at` tool gives, at the profile's water column and at two other water
columns and airmasses. It computes the same with the C++ oracle's
`--at-tool` mode, built as CASA ships ATM and with the corrections, and with
casa-atmosphere (each reads this script's configuration on stdin). It
makes three comparisons and prints the largest relative difference of each
quantity in each:

1. ATM as CASA ships it against the `at` tool: the oracle compiles what
   CASA runs.
2. casa-atmosphere against ATM with the corrections: the port is ATM, less
   its data errors.
3. ATM with the corrections against the `at` tool: what the corrections
   change. This one is reported, not gated.

It fails if any side is missing a profile, window, set, channel or
quantity, if any value or difference is not finite, or if, in the first two
comparisons, a sky temperature or opacity differs by more than the plan's
acceptance, 1e-6 relative (`compare`; `test_at_tool_check.py` feeds it
fault cases).
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys

ACCEPTANCE = 1e-6
ACCEPTED = ("dry", "wet", "tebb", "tebb_1mm", "trj", "average_tebb", "average_trj", "set_tebb", "set_wet")

# Every quantity each side must give: one value per channel, one per
# window, and one per channel at each water column and airmass set.
CHANNEL_QUANTITIES = (
    "freq_hz", "dry", "dry_cont", "o2", "o3", "co", "n2o", "wet", "h2o_lines", "h2o_cont", "tebb", "tebb_1mm",
    "trj", "disp_phase", "nondisp_phase", "nondisp_dry_phase", "disp_path", "nondisp_path", "nondisp_dry_path",
    "o2_path", "o3_path", "co_path", "n2o_path",
)
WINDOW_QUANTITIES = ("average_tebb", "average_trj")
SET_QUANTITIES = ("set_tebb", "set_wet", "set_disp_phase", "set_nondisp_phase")

PROFILES = [
    {"name": f"chajnantor type {t}", "altitude_m": 5000.0, "temperature_k": 270.0, "pressure_mbar": 560.0,
     "maxAltitude_km": 48.0, "humidity": 20.0, "dTem_dh_k_per_km": -5.6, "dP_mbar": 10.0, "h0_km": 2.0,
     "atmType": t}
    for t in (1, 2, 3, 4, 5)
] + [
    {"name": "humid sea level", "altitude_m": 0.0, "temperature_k": 293.0, "pressure_mbar": 1013.0,
     "maxAltitude_km": 48.0, "humidity": 60.0, "dTem_dh_k_per_km": -6.5, "dP_mbar": 10.0, "h0_km": 2.0,
     "atmType": 2},
    {"name": "vla winter", "altitude_m": 2124.0, "temperature_k": 265.0, "pressure_mbar": 790.0,
     "maxAltitude_km": 45.0, "humidity": 40.0, "dTem_dh_k_per_km": -6.5, "dP_mbar": 12.5, "h0_km": 2.5,
     "atmType": 3},
]

# (centre, width, resolution) in GHz: line centres and windows, odd and
# even channel counts.
BANDS = [
    (22.235, 2.1, 0.5), (45.0, 2.0, 0.25), (60.3, 1.0, 0.25), (90.0, 2.1, 0.5), (118.75, 0.5, 0.125),
    (183.31, 2.1, 0.5), (230.0, 2.0, 0.25), (345.8, 2.1, 0.5), (380.2, 2.0, 0.25), (460.0, 2.1, 0.5),
    (557.0, 2.0, 0.25), (650.0, 2.1, 0.5), (690.0, 2.0, 0.25), (850.0, 2.1, 0.5), (990.0, 2.0, 0.25),
]

# Water columns and airmasses set on the tool after the default reads.
SETS = [{"water_m": 2.0e-3, "airmass": 1.5}, {"water_m": 0.5e-3, "airmass": 2.5}]

UNITS = {"K": 1.0, "neper": 1.0, "deg": 1.0, "m": 1.0, "mm": 1e-3, "GHz": 1e9, "Hz": 1.0, "": 1.0}


def value(result) -> float:
    """A float, or the value of a casatools Quantity in this script's units."""
    if isinstance(result, dict):
        return float(result["value"][0]) * UNITS[result["unit"]]
    return float(result)


def at_tool(profile: dict, bands: list[dict], sets: list[dict]) -> dict:
    from casatools import atmosphere

    at = atmosphere()
    at.initAtmProfile(
        altitude={"value": profile["altitude_m"], "unit": "m"},
        temperature={"value": profile["temperature_k"], "unit": "K"},
        pressure={"value": profile["pressure_mbar"], "unit": "mbar"},
        maxAltitude={"value": profile["maxAltitude_km"], "unit": "km"},
        humidity=profile["humidity"],
        dTem_dh={"value": profile["dTem_dh_k_per_km"], "unit": "K/km"},
        dP={"value": profile["dP_mbar"], "unit": "mbar"},
        dPm=1.2,
        h0={"value": profile["h0_km"], "unit": "km"},
        atmType=profile["atmType"],
    )
    n = len(bands)
    at.initSpectralWindow(
        nbands=n,
        fCenter={"value": [b["centre_ghz"] for b in bands], "unit": "GHz"},
        fWidth={"value": [b["width_ghz"] for b in bands], "unit": "GHz"},
        fRes={"value": [b["res_ghz"] for b in bands], "unit": "GHz"},
    )
    default = {"value": -1, "unit": "mm"}
    one_mm = {"value": 1.0, "unit": "mm"}
    calls = {
        "freq_hz": lambda c, s: at.getChanFreq(c, s),
        "dry": lambda c, s: at.getDryOpacity(c, s),
        "dry_cont": lambda c, s: at.getDryContOpacity(c, s),
        "o2": lambda c, s: at.getO2LinesOpacity(c, s),
        "o3": lambda c, s: at.getO3LinesOpacity(c, s),
        "co": lambda c, s: at.getCOLinesOpacity(c, s),
        "n2o": lambda c, s: at.getN2OLinesOpacity(c, s),
        "wet": lambda c, s: at.getWetOpacity(c, s),
        "h2o_lines": lambda c, s: at.getH2OLinesOpacity(c, s),
        "h2o_cont": lambda c, s: at.getH2OContOpacity(c, s),
        "tebb": lambda c, s: at.getTebbSky(c, s, default),
        "tebb_1mm": lambda c, s: at.getTebbSky(c, s, one_mm),
        "trj": lambda c, s: at.getTrjSky(c, s, default),
        "disp_phase": lambda c, s: at.getDispersivePhaseDelay(c, s),
        "nondisp_phase": lambda c, s: at.getNonDispersivePhaseDelay(c, s),
        "nondisp_dry_phase": lambda c, s: at.getNonDispersiveDryPhaseDelay(c, s),
        "disp_path": lambda c, s: at.getDispersivePathLength(c, s),
        "nondisp_path": lambda c, s: at.getNonDispersivePathLength(c, s),
        "nondisp_dry_path": lambda c, s: at.getNonDispersiveDryPathLength(c, s),
        "o2_path": lambda c, s: at.getO2LinesPathLength(c, s),
        "o3_path": lambda c, s: at.getO3LinesPathLength(c, s),
        "co_path": lambda c, s: at.getCOLinesPathLength(c, s),
        "n2o_path": lambda c, s: at.getN2OLinesPathLength(c, s),
    }
    set_calls = {
        "set_tebb": lambda c, s: at.getTebbSky(c, s, default),
        "set_wet": lambda c, s: at.getWetOpacity(c, s),
        "set_disp_phase": lambda c, s: at.getDispersivePhaseDelay(c, s),
        "set_nondisp_phase": lambda c, s: at.getNonDispersivePhaseDelay(c, s),
    }
    windows = []
    for s, band in enumerate(bands):
        channels = at.getNumChan(s)
        assert channels == band["channels"], (band, channels)
        window = {key: [value(call(c, s)) for c in range(channels)] for key, call in calls.items()}
        window["average_tebb"] = value(at.getAverageTebbSky(s, default))
        window["average_trj"] = value(at.getAverageTrjSky(s, default))
        window.update({key: [] for key in set_calls})
        windows.append(window)
    for set_ in sets:
        at.setUserWH2O({"value": set_["water_m"] * 1e3, "unit": "mm"})
        at.setAirMass(set_["airmass"])
        for s, window in enumerate(windows):
            for key, call in set_calls.items():
                window[key].append([value(call(c, s)) for c in range(len(window["dry"]))])
    ground = value(at.getGroundWH2O())
    at.done()
    return {"ground_water_m": ground, "windows": windows}


def oracle_input(config: dict) -> str:
    """The configuration as the C++ oracle's `--at-tool` mode reads it."""
    keys = ("altitude_m", "temperature_k", "pressure_mbar", "maxAltitude_km", "humidity", "dTem_dh_k_per_km",
            "dP_mbar", "h0_km")
    lines = [str(len(config["profiles"]))]
    lines += [" ".join(repr(float(p[k])) for k in keys) + f" {int(p['atmType'])}" for p in config["profiles"]]
    lines.append(str(len(config["bands"])))
    lines += [f"{int(b['channels'])} {float(b['centre_ghz'])!r} {float(b['res_ghz'])!r}" for b in config["bands"]]
    lines.append(str(len(config["sets"])))
    lines += [f"{float(s['water_m'])!r} {float(s['airmass'])!r}" for s in config["sets"]]
    return "\n".join(lines) + "\n"


def relative(ours: float, reference: float) -> float:
    if reference == ours:
        return 0.0
    if reference == 0.0:
        return math.inf
    return abs(ours - reference) / abs(reference)


def finite_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def compare(
    config: dict, reference_profiles, ours: dict, names=("casa-rs", "the at tool"), gate: bool = True
) -> tuple[dict, list[str]]:
    """The worst relative difference of each quantity, and every problem.

    `ours` is a result as the binaries write it, `{"profiles": [...]}`;
    `reference_profiles` the reference's profiles; `names` name the two
    sides in that order. A problem is anything that would make a success
    meaningless or a failure: a profile, window, set, channel or quantity
    missing from either side (or extra), a value or difference that is not
    finite, or, with `gate`, an accepted quantity beyond ACCEPTANCE. The
    comparison passes only with none.
    """
    worst: dict[str, tuple[float, str]] = {}
    problems: list[str] = []
    profiles, bands, sets = config["profiles"], config["bands"], config["sets"]
    ours_name, reference_name = names
    ours_profiles = ours.get("profiles") if isinstance(ours, dict) else None
    if not isinstance(ours_profiles, list) or len(ours_profiles) != len(profiles):
        problems.append(f"{ours_name} gave {len(ours_profiles) if isinstance(ours_profiles, list) else 'no'} "
                        f"profiles for {len(profiles)}")
        return worst, problems
    if not isinstance(reference_profiles, list) or len(reference_profiles) != len(profiles):
        count = len(reference_profiles) if isinstance(reference_profiles, list) else "no"
        problems.append(f"{reference_name} gave {count} profiles for {len(profiles)}")
        return worst, problems

    def pair(key: str, a, b, place: str) -> None:
        if not finite_number(a) or not finite_number(b):
            problems.append(f"{place} {key}: not a finite pair ({ours_name} {a!r}, {reference_name} {b!r})")
            return
        d = relative(a, b)
        if not math.isfinite(d):
            problems.append(f"{place} {key}: difference not finite ({ours_name} {a!r}, {reference_name} {b!r})")
            return
        if key not in worst or d > worst[key][0]:
            worst[key] = (d, place)

    def channel_pairs(key: str, a, b, place: str, channels: int) -> None:
        lengths = [len(v) if isinstance(v, list) else None for v in (a, b)]
        if lengths != [channels, channels]:
            problems.append(f"{place} {key}: {ours_name} has {lengths[0]} and {reference_name} {lengths[1]} "
                            f"channels for {channels}")
            return
        for c, (x, y) in enumerate(zip(a, b)):
            pair(key, x, y, f"{place} channel {c}")

    def windows_of(side: str, result, name: str):
        windows = result.get("windows") if isinstance(result, dict) else None
        if isinstance(windows, list) and len(windows) == len(bands) and all(isinstance(w, dict) for w in windows):
            return windows
        count = len(windows) if isinstance(windows, list) else "no"
        problems.append(f"{name}: {side} gave {count} windows for {len(bands)}")
        return None

    for profile, mine, theirs in zip(profiles, ours_profiles, reference_profiles):
        name = profile["name"]
        ours_windows = windows_of(ours_name, mine, name)
        reference_windows = windows_of(reference_name, theirs, name)
        if ours_windows is None or reference_windows is None:
            continue
        pair("ground_water_m", mine.get("ground_water_m"), theirs.get("ground_water_m"), name)
        for band, ow, rw in zip(bands, ours_windows, reference_windows):
            place = f"{name} band {band['centre_ghz']} GHz"
            for key in CHANNEL_QUANTITIES:
                channel_pairs(key, ow.get(key), rw.get(key), place, band["channels"])
            for key in WINDOW_QUANTITIES:
                pair(key, ow.get(key), rw.get(key), place)
            for key in SET_QUANTITIES:
                a, b = ow.get(key), rw.get(key)
                counts = [len(v) if isinstance(v, list) else None for v in (a, b)]
                if counts != [len(sets), len(sets)]:
                    problems.append(f"{place} {key}: {ours_name} has {counts[0]} and {reference_name} {counts[1]} "
                                    f"sets for {len(sets)}")
                    continue
                for k, (x, y) in enumerate(zip(a, b)):
                    channel_pairs(key, x, y, f"{place} set {k}", band["channels"])
    for key in (*CHANNEL_QUANTITIES, *WINDOW_QUANTITIES, *SET_QUANTITIES, "ground_water_m"):
        if key not in worst:
            problems.append(f"{key}: no value compared")
    if gate:
        for key in ACCEPTED:
            if key in worst and worst[key][0] > ACCEPTANCE:
                problems.append(f"{key}: {worst[key][0]:.3e} at {worst[key][1]} exceeds {ACCEPTANCE:g}")
    return worst, problems


def run(command: list[str], stdin: str) -> dict:
    return json.loads(subprocess.run(command, input=stdin, capture_output=True, text=True, check=True).stdout)


def print_table(title: str, worst: dict, gate: bool) -> None:
    print(f"\n{title}")
    print(f"{'quantity':20s} {'max relative difference':>24s}  where")
    for key, (d, place) in worst.items():
        mark = "  FAIL" if gate and key in ACCEPTED and d > ACCEPTANCE else ""
        print(f"{key:20s} {d:24.3e}  {place}{mark}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rust", required=True, help="casa-atmosphere's at_tool_quantities example")
    parser.add_argument("--cpp", required=True, help="the C++ oracle with the corrections (--binary)")
    parser.add_argument("--cpp-as-shipped", required=True, help="the C++ oracle as CASA ships ATM")
    parser.add_argument("--report", help="write every value and difference here as JSON")
    args = parser.parse_args()

    bands = [
        {"centre_ghz": c, "width_ghz": w, "res_ghz": r, "channels": math.ceil(w / r)} for c, w, r in BANDS
    ]
    config = {"profiles": PROFILES, "bands": bands, "sets": SETS}
    rust = run([args.rust], json.dumps(config))
    cpp = run([args.cpp, "--at-tool"], oracle_input(config))
    shipped = run([args.cpp_as_shipped, "--at-tool"], oracle_input(config))
    casa = [at_tool(profile, bands, SETS) for profile in PROFILES]
    checks = {
        "shipped": ("ATM as CASA ships it, against the at tool",
                    compare(config, casa, shipped, ("the C++ as shipped", "the at tool")), True),
        "port": ("casa-atmosphere, against ATM with its corrections",
                 compare(config, cpp["profiles"] if isinstance(cpp, dict) else None, rust,
                         ("casa-rs", "the C++ with corrections")), True),
        "corrections": ("What the corrections change: ATM with them, against the at tool (not gated)",
                        compare(config, casa, cpp, ("the C++ with corrections", "the at tool"), gate=False), False),
    }
    problems = []
    for key, (title, (worst, found), gate) in checks.items():
        print_table(title, worst, gate)
        problems += [f"{key}: {problem}" for problem in found]
    if args.report:
        with open(args.report, "w") as handle:
            json.dump({"checks": {key: {"worst": worst, "problems": found}
                                  for key, (_, (worst, found), _) in checks.items()},
                       "casa": casa, "rust": rust, "cpp": cpp, "cpp_as_shipped": shipped}, handle)
    for problem in problems[:50]:
        print(f"at_tool_check: {problem}", file=sys.stderr)
    if problems:
        print(f"at_tool_check: FAILED with {len(problems)} problems", file=sys.stderr)
        return 1
    print("\nat_tool_check: every quantity present and finite; ATM as shipped is the at tool, and casa-atmosphere "
          "ATM with the corrections, within 1e-6 where gated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
