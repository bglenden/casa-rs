#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Generate casa-atmosphere's data tables from CASA's ATM C++ sources.

ATM keeps its line catalogues and standard atmospheres as `static const`
arrays inside the functions that use them. This script copies every such
array into a Rust constant, so the tables are never transcribed by hand:

  python3 tools/atmosphere/extract_atm_tables.py \
      ~/SoftwareProjects/casa/casatools/src/code/atmosphere/ATM

writes `crates/casa-atmosphere/src/refractivity/tables/*.rs` and
`crates/casa-atmosphere/src/profile/tables.rs`, as `static` arrays. Each value is parsed with
correct rounding and printed as the shortest decimal that reads back to the
same `f64`, so the Rust constants hold the doubles the C++ compiler made.

C++ zero-fills an array initialised with fewer values than it declares; the
script does the same and lists every such array, because each one is a
place where ATM's data may be incomplete. An array with more values than it
declares is an error.
"""

from __future__ import annotations

import argparse
import math
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
CRATE = ROOT / "crates" / "casa-atmosphere" / "src"

# ATMRefractiveIndex.cc function -> (Rust module, one-line description).
REFRACTIVITY = {
    "mkSpecificRefractivity_so2": ("so2", "SO2 lines"),
    "mkSpecificRefractivity_hcl": ("hcl", "HCl lines"),
    "mkSpecificRefractivity_hcn": ("hcn", "HCN lines"),
    "mkSpecificRefractivity_no2": ("no2", "NO2 lines"),
    "mkSpecificRefractivity_n2o": ("n2o", "N2O lines"),
    "mkSpecificRefractivity_co": ("co", "CO lines"),
    "mkSpecificRefractivity_hh16o_v2": ("hh16o_v2", "H2(16)O lines from the first bending state"),
    "mkSpecificRefractivity_hh16o": ("hh16o", "H2(16)O lines from the ground state"),
    "mkSpecificRefractivity_hh17o": ("hh17o", "H2(17)O lines"),
    "mkSpecificRefractivity_hdo": ("hdo", "HDO lines"),
    "mkSpecificRefractivity_hh18o": ("hh18o", "H2(18)O lines"),
    "mkSpecificRefractivity_16o16o_vib": ("o2_vib", "(16)O(16)O lines from the first vibrational state"),
    "mkSpecificRefractivity_16o17o": ("o2_16o17o", "(16)O(17)O lines"),
    "mkSpecificRefractivity_16o18o": ("o2_16o18o", "(16)O(18)O lines"),
    "mkSpecificRefractivity_16o16o": ("o2", "(16)O(16)O lines from the ground state"),
    "mkSpecificRefractivity_16o16o16o_v1": ("o3_v1", "(16)O3 lines from the nu1 state"),
    "mkSpecificRefractivity_16o16o16o_v3": ("o3_v3", "(16)O3 lines from the nu3 state"),
    "mkSpecificRefractivity_16o16o16o_v2": ("o3_v2", "(16)O3 lines from the nu2 state"),
    "mkSpecificRefractivity_16o16o16o": ("o3", "(16)O3 lines from the ground state"),
    "mkSpecificRefractivity_16o16o18o": ("o3_16o16o18o", "(16)O(16)O(18)O lines"),
    "mkSpecificRefractivity_16o16o17o": ("o3_16o16o17o", "(16)O(16)O(17)O lines"),
    "mkSpecificRefractivity_16o18o16o": ("o3_16o18o16o", "(16)O(18)O(16)O lines"),
    "mkSpecificRefractivity_16o17o16o": ("o3_16o17o16o", "(16)O(17)O(16)O lines"),
}

# Catalogues ATM never reads: its (16)O(17)O function returns zero before
# its line loop. They are kept for the record.
UNREAD = {"o2_16o17o": "ATM returns zero before summing (16)O(17)O"}

# ATMProfile.cc functions whose tables go to profile/tables.rs.
PROFILE = ("st76", "mkAtmProfile")

ARRAY = re.compile(
    r"static\s+const\s+(unsigned\s+int|double|int)\s+(\w+)\s*((?:\[\s*\w+\s*\])+)\s*=\s*\{",
)
NUMBER = re.compile(r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?")


def strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    return re.sub(r"//[^\n]*", " ", text)


def functions(source: str, names) -> dict[str, str]:
    """The text of each named function: from its definition to the next one.

    A definition is a line naming `Class::function(`; calls inside bodies do
    not carry the class name. The first definition of an overloaded name is
    the one with the tables.
    """
    source = strip_comments(source)
    starts = [
        (match.start(), match.group(1))
        for match in re.finditer(r"(?m)^[^\n;{}]*\b(?:RefractiveIndex|AtmProfile)::~?(\w+)\s*\(", source)
    ]
    bodies = {}
    for (start, name), (end, _) in zip(starts, starts[1:] + [(len(source), "")]):
        if name in names and name not in bodies:
            bodies[name] = source[start:end]
    missing = set(names) - set(bodies)
    if missing:
        raise SystemExit(f"extract_atm_tables: no definition of {sorted(missing)}")
    return bodies


def arrays(body: str, constants: dict[str, int]):
    """Each `static const` array in a body: name, element type, shape, values."""
    for match in ARRAY.finditer(body):
        kind, name, dims = match.groups()
        shape = []
        for dim in re.findall(r"\[\s*(\w+)\s*\]", dims):
            shape.append(int(dim) if dim.isdigit() else constants[dim])
        depth, i = 1, match.end()
        while depth:
            depth += {"{": 1, "}": -1}.get(body[i], 0)
            i += 1
        values = NUMBER.findall(body[match.end() : i - 1])
        yield name, kind.split()[-1], shape, values


def rust_value(token: str, kind: str) -> str:
    if kind == "double":
        value = float(token)
        assert math.isfinite(value), token
        text = repr(value)
        return text if any(c in text for c in ".e") else text + ".0"
    value = int(token)
    assert 0 <= value <= 0xFFFF, token
    return str(value)


def emit_array(name: str, kind: str, shape: list[int], values: list[str], per_line: int) -> list[str]:
    rust_kind = "f64" if kind == "double" else "u16"
    rust_type = rust_kind
    for dim in reversed(shape):
        rust_type = f"[{rust_type}; {dim}]"
    flat = [rust_value(v, kind) for v in values]
    lines = ["#[rustfmt::skip]", f"pub(crate) static {name.upper()}: {rust_type} = ["]
    if len(shape) == 1:
        for i in range(0, len(flat), per_line):
            lines.append("    " + ", ".join(flat[i : i + per_line]) + ",")
    elif shape[-1] <= 4:
        rows = [flat[i : i + shape[-1]] for i in range(0, len(flat), shape[-1])]
        for i in range(0, len(rows), 3):
            lines.append("    " + " ".join("[" + ", ".join(row) + "]," for row in rows[i : i + 3]))
    else:
        inner = shape[-1]
        for row in range(len(flat) // inner):
            chunk = flat[row * inner : (row + 1) * inner]
            lines.append("    [")
            for i in range(0, len(chunk), per_line):
                lines.append("        " + ", ".join(chunk[i : i + per_line]) + ",")
            lines.append("    ],")
    lines.append("];")
    return lines


def module(header: str, bodies: dict[str, str], order, report: list[str]) -> str:
    out = [
        "// SPDX-License-Identifier: LGPL-3.0-or-later",
        "// Generated by tools/atmosphere/extract_atm_tables.py from ATM",
        "// (c) Instituto de Estructura de la Materia, Pardo et al., LGPL-2.1-or-later.",
        "// Do not edit; rerun the script.",
        header,
        "#![allow(",
        "    clippy::approx_constant,",
        '    reason = "measured line data, not mathematical constants"',
        ")]",
    ]
    seen = set()
    for function in order:
        prefix = "" if len(order) == 1 else function.upper() + "_"
        for name, kind, shape, values in arrays(strip_comments(bodies[function]), {"MAXNHX": 20}):
            size = math.prod(shape)
            if len(values) > size:
                raise SystemExit(f"{function}: {name}{shape} has {len(values)} values")
            if len(values) < size:
                report.append(f"{function}: {name}{shape} has {len(values)} of {size} values; zero-filled")
                values = values + ["0"] * (size - len(values))
            rust_name = prefix + name
            assert rust_name.upper() not in seen, rust_name
            seen.add(rust_name.upper())
            out.append("")
            out.append(f"/// `{name}` of ATM's `{function}`.")
            out.extend(emit_array(rust_name, kind, shape, values, 10 if kind == "double" else 20))
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("atm", type=pathlib.Path, help="the ATM source directory")
    args = parser.parse_args()
    report: list[str] = []

    source = (args.atm / "ATMRefractiveIndex.cc").read_text()
    bodies = functions(source, REFRACTIVITY)
    tables = CRATE / "refractivity" / "tables"
    tables.mkdir(parents=True, exist_ok=True)
    mods = []
    for function, (name, what) in REFRACTIVITY.items():
        text = module(f"//! Line catalogue for {what}.", bodies, [function], report)
        (tables / f"{name}.rs").write_text(text)
        mods.append(name)
    declarations = []
    for name in sorted(mods):
        if name in UNREAD:
            declarations.append(f'#[expect(dead_code, reason = "{UNREAD[name]}")]\n')
        declarations.append(f"pub(crate) mod {name};\n")
    (tables / "mod.rs").write_text(
        "// SPDX-License-Identifier: LGPL-3.0-or-later\n"
        "//! ATM's line catalogues, generated by `tools/atmosphere/extract_atm_tables.py`.\n\n"
        + "".join(declarations)
    )

    source = (args.atm / "ATMProfile.cc").read_text()
    bodies = functions(source, PROFILE)
    text = module("//! Standard atmospheres from ATM's `AtmProfile`.", bodies, list(PROFILE), report)
    (CRATE / "profile").mkdir(parents=True, exist_ok=True)
    (CRATE / "profile" / "tables.rs").write_text(text)

    for line in report:
        print(f"extract_atm_tables: {line}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
