#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA oracle for the `image` (ia) and `coordsys` (cs) tools (#1025).

Run with CASA's Python, which imports `casatools` 6.7.6.14:

    python -I tools/casa-compat/image_tool_oracle.py --json FILE

Each script in `SCRIPTS` is a list of calls on named tools. A script starts
with a fresh `image` tool `ia` and `coordsys` tool `cs`, in a fresh working
directory that `{tmp}` names in arguments; a call can bind a tool it returns
(`"as"`) for later calls. The JSON record holds every call with what it
returned, typed so a replay can check types and dtypes, or the type and
first line of what it raised. `casars.tools.image`'s and
`casars.tools.coordsys`'s tests replay the same scripts
(`crates/casars-python/python/tests/data/casa_image_tool.json`).

Encoding, shared with the replay (`test_tools_image.py`):

- a NumPy array is `{"ndarray": nested lists, "dtype": name, "shape": [...]}`
  (complex values as `[re, im]` pairs), in arguments and results;
- a complex number is `{"complex": [re, im]}`;
- a returned tool is `{"tool": "image"}` or `{"tool": "coordsys"}`;
- a tuple is `{"tuple": [...]}`;
- the working directory in a returned string is written `{tmp}`.

A call may carry `"compare"`: `"type"` to check only the result's type (a
value that depends on the clock), `"skip"` to check only that it does not
raise, or a list under `"ignore"` of record keys not compared.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile

import numpy as np

import casatools

def encode(value):
    """`value`, a CASA result or argument, as JSON (module docstring)."""
    if isinstance(value, casatools.image):
        return {"tool": "image"}
    if isinstance(value, casatools.coordsys):
        return {"tool": "coordsys"}
    if isinstance(value, dict):
        return {str(key): encode(item) for key, item in value.items()}
    if isinstance(value, np.ndarray):
        if np.iscomplexobj(value):
            items = np.stack([value.real, value.imag], axis=-1).tolist()
        else:
            items = value.tolist()
        return {"ndarray": items, "dtype": value.dtype.name, "shape": list(value.shape)}
    if isinstance(value, tuple):
        return {"tuple": [encode(item) for item in value]}
    if isinstance(value, list):
        return [encode(item) for item in value]
    if isinstance(value, (complex, np.complexfloating)):
        return {"complex": [float(value.real), float(value.imag)]}
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value)
    return value


# Pixels and masks used by the scripts.
CUBE = np.arange(4 * 5 * 3 * 2, dtype=np.float32).reshape((4, 5, 3, 2), order="F")
PLANE = (np.arange(6 * 7, dtype=np.float64).reshape((6, 7)) - 20.5) / 3.0
SMALL = np.array([[1.5, -2.0], [3.25, 4.0]], dtype=np.float32)
COMPLEX = (np.arange(12).reshape(3, 4) + 1j * np.arange(12).reshape(3, 4)[::-1]).astype(
    np.complex64
)


def array(value: np.ndarray) -> dict:
    """`value` encoded as an argument."""
    return encode(value)


def call(target: str, method: str, compare=None, bind: str | None = None, **args) -> dict:
    """One call of a script."""
    entry: dict = {"target": target, "method": method, "args": args}
    if compare is not None:
        entry["compare"] = compare
    if bind is not None:
        entry["as"] = bind
    return entry


def ia(method: str, compare=None, bind=None, **args) -> dict:
    return call("ia", method, compare, bind, **args)


def cs(method: str, compare=None, bind=None, **args) -> dict:
    return call("cs", method, compare, bind, **args)


SCRIPTS: dict[str, list[dict]] = {
    "metadata": [
        ia("isopen"),
        ia("fromshape", outfile="{tmp}/a.im", shape=[4, 5, 3, 2]),
        ia("isopen"),
        ia("ispersistent"),
        ia("name"),
        ia("name", strippath=True),
        ia("shape"),
        ia("pixeltype"),
        ia("type"),
        ia("brightnessunit"),
        ia("setbrightnessunit", unit="Jy/beam"),
        ia("brightnessunit"),
        ia("miscinfo"),
        ia("setmiscinfo", info={"a": 1, "b": "text", "c": [1.5, 2.5]}),
        ia("miscinfo"),
        ia("history", list=False),
        ia("sethistory", origin="probe", history=["first", "second"]),
        ia("history", list=False),
        ia("restoringbeam"),
        ia("setrestoringbeam", major="3arcsec", minor="2arcsec", pa="30deg"),
        ia("restoringbeam"),
        ia("setrestoringbeam", remove=True),
        ia("restoringbeam"),
        ia("haslock"),
        ia("close"),
        ia("isopen"),
        ia("open", infile="{tmp}/a.im"),
        ia("brightnessunit"),
        ia("miscinfo"),
        ia("shape"),
        ia("done"),
        ia("isopen"),
        ia("newimage", infile="{tmp}/a.im", bind="other"),
        call("other", "shape"),
        call("other", "name", strippath=True),
        call("other", "done"),
    ],
    "pixels": [
        ia("fromshape", outfile="{tmp}/p.im", shape=[4, 5, 3, 2]),
        ia("getchunk"),
        ia("putchunk", pixels=array(CUBE)),
        ia("getchunk"),
        ia("getchunk", blc=[1, 1, 0, 0], trc=[2, 3, 1, 1]),
        ia("getchunk", blc=[0, 0, 0, 0], trc=[3, 4, 2, 1], inc=[2, 2, 1, 1]),
        ia("getchunk", blc=[0, 0, 0, 0], trc=[3, 4, 0, 0], dropdeg=True),
        ia("getchunk", blc=[1, 2], trc=[2, 3]),
        ia("getchunk", getmask=True),
        ia("getchunk", axes=[0, 1]),
        ia("pixelvalue", pixel=[1, 2, 0, 1]),
        ia("pixelvalue"),
        ia("putchunk", pixels=array(SMALL), blc=[1, 1, 0, 0]),
        ia("getchunk", blc=[0, 0, 0, 0], trc=[3, 3, 0, 0]),
        ia("putchunk", pixels=array(SMALL), blc=[1, 1, 2, 1], inc=[2, 2, 1, 1]),
        ia("getchunk", blc=[0, 0, 2, 1], trc=[3, 4, 2, 1]),
        ia("makearray", v=2.5, shape=[2, 3]),
        ia("set", pixels=1.25),
        ia("getchunk", blc=[0, 0, 0, 0], trc=[1, 1, 0, 0]),
        ia("done"),
        ia("fromarray", outfile="", pixels=array(PLANE)),
        ia("shape"),
        ia("pixeltype"),
        ia("ispersistent"),
        ia("name"),
        ia("getchunk"),
        ia("pixelvalue", pixel=[2, 3]),
        ia("done"),
        ia("fromarray", outfile="{tmp}/d.im", pixels=array(PLANE), type="d"),
        ia("pixeltype"),
        ia("getchunk", blc=[0, 0], trc=[1, 1]),
        ia("done"),
        ia("fromarray", outfile="", pixels=array(COMPLEX)),
        ia("pixeltype"),
        ia("getchunk"),
        ia("pixelvalue", pixel=[1, 1]),
        ia("done"),
        ia("fromshape", outfile="", shape=[3, 3], type="c"),
        ia("pixeltype"),
        ia("getchunk"),
        ia("done"),
    ],
    "coordsys": [
        ia("fromshape", outfile="", shape=[4, 5, 3, 2]),
        ia("coordsys", bind="c"),
        call("c", "type"),
        call("c", "naxes"),
        call("c", "naxes", world=False),
        call("c", "ncoordinates"),
        call("c", "coordinatetype"),
        call("c", "coordinatetype", which=1),
        call("c", "axiscoordinatetypes"),
        call("c", "names"),
        call("c", "units"),
        call("c", "referencepixel"),
        call("c", "referencevalue"),
        call("c", "increment"),
        call("c", "projection"),
        call("c", "referencecode"),
        call("c", "referencecode", type="spectral"),
        call("c", "restfrequency"),
        call("c", "stokes"),
        call("c", "telescope"),
        call("c", "observer"),
        call("c", "epoch"),
        call("c", "findcoordinate", type="spectral"),
        call("c", "findcoordinate", type="stokes"),
        call("c", "findcoordinate", type="linear"),
        call("c", "findaxis", axis=2),
        call("c", "findaxisbyname", axisname="freq"),
        call("c", "lineartransform", type="direction"),
        call("c", "toworld", value=[1, 1, 0, 0]),
        call("c", "topixel", value=[0.5, 0.5, 1.0, 1415001000.0]),
        call("c", "torecord"),
        call("c", "axesmap"),
        call("c", "parentname"),
        ia("coordsys", axes=[0, 1], bind="d"),
        call("d", "naxes"),
        call("d", "names"),
        call("d", "torecord"),
    ],
    "newcoordsys": [
        cs("newcoordsys", direction=True, spectral=True, stokes=["I", "V"], linear=2, bind="n"),
        call("n", "naxes"),
        call("n", "names"),
        call("n", "units"),
        call("n", "referencevalue"),
        call("n", "torecord", compare={"ignore": ["obsdate"]}),
        call("n", "epoch", compare="type"),
        call("n", "telescope"),
        cs("naxes"),
        cs("ncoordinates"),
        cs("telescope"),
        cs("observer"),
        cs("torecord", compare={"ignore": ["obsdate"]}),
    ],
}


def decode(value, tmp: str):
    """An encoded argument as Python, with `{tmp}` replaced."""
    if isinstance(value, dict):
        if "ndarray" in value:
            dtype = np.dtype(value["dtype"])
            items = np.array(value["ndarray"], dtype=np.float64 if dtype.kind == "c" else dtype)
            if dtype.kind == "c":
                items = (items[..., 0] + 1j * items[..., 1]).astype(dtype)
            return items.reshape(value["shape"])
        if "complex" in value:
            return complex(*value["complex"])
        return {key: decode(item, tmp) for key, item in value.items()}
    if isinstance(value, list):
        return [decode(item, tmp) for item in value]
    if isinstance(value, str):
        return value.replace("{tmp}", tmp)
    return value


def untmp(value, tmp: str):
    """`value` with the working directory written `{tmp}`."""
    if isinstance(value, dict):
        return {key: untmp(item, tmp) for key, item in value.items()}
    if isinstance(value, list):
        return [untmp(item, tmp) for item in value]
    if isinstance(value, str):
        return value.replace(tmp, "{tmp}")
    return value


def run_script(calls: list[dict]) -> list[dict]:
    """Every call of a script with its outcome."""
    tmp = os.path.realpath(tempfile.mkdtemp(prefix="image_tool_oracle_"))
    tools = {"ia": casatools.image(), "cs": casatools.coordsys()}
    recorded = []
    try:
        for entry in calls:
            target = tools[entry["target"]]
            args = {key: decode(value, tmp) for key, value in entry["args"].items()}
            outcome = dict(entry)
            try:
                result = getattr(target, entry["method"])(**args)
            except Exception as error:  # noqa: BLE001 - recorded for the replay
                message = str(error).strip().splitlines()
                outcome["raises"] = [type(error).__name__, message[0] if message else ""]
            else:
                outcome["returns"] = untmp(encode(result), tmp)
                if "as" in entry:
                    tools[entry["as"]] = result
            recorded.append(outcome)
    finally:
        for tool in tools.values():
            try:
                tool.done()
            except Exception:  # noqa: BLE001
                pass
        shutil.rmtree(tmp, ignore_errors=True)
    return recorded


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", required=True, help="where to write the record")
    parser.add_argument("--only", nargs="*", help="run only these scripts")
    arguments = parser.parse_args()
    names = arguments.only or list(SCRIPTS)
    record = {
        "casa_version": casatools.version_string(),
        "scripts": {name: run_script(SCRIPTS[name]) for name in names},
    }
    with open(arguments.json, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=1, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    main()
