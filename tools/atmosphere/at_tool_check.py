#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare casa-atmosphere with CASA's `at` (atmosphere) tool.

Runs under CASA's Python, which has `casatools`:

  cargo build --release -p casa-atmosphere --example at_tool_quantities
  /Applications/CASA.app/Contents/MacOS/python3 tools/atmosphere/at_tool_check.py \\
      --rust $CARGO_TARGET_DIR/release/examples/at_tool_quantities

For seven atmospheres (all five `at` types at Chajnantor, a humid sea level
and the VLA in winter) and fifteen bands from 22 GHz to 1 THz it reads, per
channel, the opacities, phase delays, path lengths and sky temperatures the
`at` tool gives, computes the same with casa-atmosphere (the example reads
this script's configuration on stdin), and prints the largest relative
difference of each quantity. It fails if either side is missing a
profile, window, channel or quantity, if any value or difference is not
finite, or if a sky temperature or opacity differs by more than the plan's
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
ACCEPTED = ("dry", "wet", "tebb", "tebb_1mm", "tebb_set", "trj", "average_tebb", "average_trj")

# Every quantity both sides must give: one value per channel, and one per
# window.
CHANNEL_QUANTITIES = (
    "freq_hz", "dry", "dry_cont", "o2", "o3", "co", "n2o", "wet", "h2o_lines", "h2o_cont", "tebb", "tebb_1mm",
    "trj", "disp_phase", "nondisp_phase", "nondisp_dry_phase", "disp_path", "nondisp_path", "nondisp_dry_path",
    "o2_path", "o3_path", "co_path", "n2o_path", "tebb_set",
)
WINDOW_QUANTITIES = ("average_tebb", "average_trj")

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

# Water column and airmass set on the tool after the default reads.
SET = {"water_m": 2.0e-3, "airmass": 1.5}

UNITS = {"K": 1.0, "neper": 1.0, "deg": 1.0, "m": 1.0, "mm": 1e-3, "GHz": 1e9, "Hz": 1.0, "": 1.0}


def value(result) -> float:
    """A float, or the value of a casatools Quantity in this script's units."""
    if isinstance(result, dict):
        return float(result["value"][0]) * UNITS[result["unit"]]
    return float(result)


def at_tool(profile: dict, bands: list[dict]) -> dict:
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
    windows = []
    for s, band in enumerate(bands):
        channels = at.getNumChan(s)
        assert channels == band["channels"], (band, channels)
        window = {key: [value(call(c, s)) for c in range(channels)] for key, call in calls.items()}
        window["average_tebb"] = value(at.getAverageTebbSky(s, default))
        window["average_trj"] = value(at.getAverageTrjSky(s, default))
        windows.append(window)
    at.setUserWH2O({"value": SET["water_m"] * 1e3, "unit": "mm"})
    at.setAirMass(SET["airmass"])
    for s, window in enumerate(windows):
        window["tebb_set"] = [value(at.getTebbSky(c, s, default)) for c in range(len(window["dry"]))]
    ground = value(at.getGroundWH2O())
    at.done()
    return {"ground_water_m": ground, "windows": windows}


def relative(rust: float, casa: float) -> float:
    if casa == rust:
        return 0.0
    if casa == 0.0:
        return math.inf
    return abs(rust - casa) / abs(casa)


def finite_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def compare(config: dict, casa_profiles: list, rust: dict) -> tuple[dict, list[str]]:
    """The worst relative difference of each quantity, and every problem.

    A problem is anything that would make a success meaningless or a
    failure: a profile, window, channel or quantity missing from either
    side (or extra), a value or difference that is not finite, or an
    accepted quantity beyond ACCEPTANCE. The check passes only with none.
    """
    worst: dict[str, tuple[float, str]] = {}
    problems: list[str] = []
    profiles, bands = config["profiles"], config["bands"]
    ours_profiles = rust.get("profiles") if isinstance(rust, dict) else None
    if not isinstance(ours_profiles, list) or len(ours_profiles) != len(profiles):
        problems.append(f"casa-rs gave {len(ours_profiles) if isinstance(ours_profiles, list) else 'no'} "
                        f"profiles for {len(profiles)}")
        return worst, problems
    if len(casa_profiles) != len(profiles):
        problems.append(f"the at tool gave {len(casa_profiles)} profiles for {len(profiles)}")
        return worst, problems

    def pair(key: str, ours, casa, place: str) -> None:
        if not finite_number(ours) or not finite_number(casa):
            problems.append(f"{place} {key}: not a finite pair (casa-rs {ours!r}, at tool {casa!r})")
            return
        d = relative(ours, casa)
        if not math.isfinite(d):
            problems.append(f"{place} {key}: difference not finite (casa-rs {ours!r}, at tool {casa!r})")
            return
        if key not in worst or d > worst[key][0]:
            worst[key] = (d, place)

    def windows_of(side: str, result, name: str):
        windows = result.get("windows") if isinstance(result, dict) else None
        if isinstance(windows, list) and len(windows) == len(bands) and all(isinstance(w, dict) for w in windows):
            return windows
        count = len(windows) if isinstance(windows, list) else "no"
        problems.append(f"{name}: {side} gave {count} windows for {len(bands)}")
        return None

    for profile, ours, casa in zip(profiles, ours_profiles, casa_profiles):
        name = profile["name"]
        ours_windows, casa_windows = windows_of("casa-rs", ours, name), windows_of("the at tool", casa, name)
        if ours_windows is None or casa_windows is None:
            continue
        pair("ground_water_m", ours.get("ground_water_m"), casa.get("ground_water_m"), name)
        for band, rw, cw in zip(bands, ours_windows, casa_windows):
            place = f"{name} band {band['centre_ghz']} GHz"
            for key in CHANNEL_QUANTITIES:
                rv, cv = rw.get(key), cw.get(key)
                lengths = [len(v) if isinstance(v, list) else None for v in (rv, cv)]
                if lengths != [band["channels"], band["channels"]]:
                    problems.append(f"{place} {key}: casa-rs has {lengths[0]} and the at tool {lengths[1]} "
                                    f"channels for {band['channels']}")
                    continue
                for c, (r, a) in enumerate(zip(rv, cv)):
                    pair(key, r, a, f"{place} channel {c}")
            for key in WINDOW_QUANTITIES:
                pair(key, rw.get(key), cw.get(key), place)
    for key in (*CHANNEL_QUANTITIES, *WINDOW_QUANTITIES, "ground_water_m"):
        if key not in worst:
            problems.append(f"{key}: no value compared")
    for key in ACCEPTED:
        if key in worst and worst[key][0] > ACCEPTANCE:
            problems.append(f"{key}: {worst[key][0]:.3e} at {worst[key][1]} exceeds {ACCEPTANCE:g}")
    return worst, problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rust", required=True, help="the at_tool_quantities example binary")
    parser.add_argument("--report", help="write every value and difference here as JSON")
    args = parser.parse_args()

    bands = [
        {"centre_ghz": c, "width_ghz": w, "res_ghz": r, "channels": math.ceil(w / r)} for c, w, r in BANDS
    ]
    config = {"profiles": PROFILES, "bands": bands, "set": SET}
    rust = json.loads(subprocess.run([args.rust], input=json.dumps(config), capture_output=True, text=True,
                                     check=True).stdout)
    casa = [at_tool(profile, bands) for profile in PROFILES]
    worst, problems = compare(config, casa, rust)
    print(f"{'quantity':20s} {'max relative difference':>24s}  where")
    for key, (d, place) in worst.items():
        mark = "  FAIL" if key in ACCEPTED and d > ACCEPTANCE else ""
        print(f"{key:20s} {d:24.3e}  {place}{mark}")
    if args.report:
        with open(args.report, "w") as handle:
            json.dump({"worst": worst, "problems": problems, "casa": casa, "rust": rust}, handle)
    for problem in problems[:50]:
        print(f"at_tool_check: {problem}", file=sys.stderr)
    if problems:
        print(f"at_tool_check: FAILED with {len(problems)} problems", file=sys.stderr)
        return 1
    print("at_tool_check: every quantity present, finite and, where gated, within 1e-6")
    return 0


if __name__ == "__main__":
    sys.exit(main())
