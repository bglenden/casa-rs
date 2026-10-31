#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Small CASA 6.7.6.14 lifecycle oracle, not representative science (#944).

Run with CASA Python -I, --work on external working storage, --imager naming
a release binary, and --revision its clean source commit. Keep the compact
receipt durably; the generated small MS and products are disposable fixtures.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path


def snapshot(root: Path) -> dict[str, str]:
    """Diagnostic persisted-byte comparison; no production hashing."""
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            with path.open("rb") as stream:
                digest = hashlib.sha256()
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
                result[str(path.relative_to(root))] = digest.hexdigest()
    return result


def model_flux(prefix: Path) -> float:
    from casatools import image
    tool = image()
    try:
        tool.open(str(prefix) + ".model")
        return float(tool.getchunk().sum(dtype="float64"))
    finally:
        tool.done()


def native(binary: Path, directory: Path, values: dict) -> dict:
    run = subprocess.run([str(binary), "--json-run", "-"], cwd=directory,
                         input=json.dumps({"surface": "tclean", "values": values}),
                         text=True, capture_output=True, timeout=120, check=False)
    try:
        reply = json.loads(run.stdout)
    except ValueError:
        raise RuntimeError(f"native reply: {run.stdout}\n{run.stderr}") from None
    return {"exit_code": run.returncode, "reply": reply}


def main() -> None:
    import casatasks
    import casatools
    import numpy as np
    from casatools import table

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--imager", required=True, type=Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    assert args.revision == head, (args.revision, head)
    subprocess.run(["git", "-C", str(repo), "diff", "--exit-code", head, "--", "crates"], check=True)
    assert casatasks.version_string() == "6.7.6.14"
    assert casatools.version_string() == "6.7.6-14"
    args.work.mkdir(parents=True, exist_ok=False)
    setup = Path(__file__).with_name("record_casa_returns.py")
    spec = importlib.util.spec_from_file_location("record_returns", setup)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    vis = args.work / "point.ms"
    module.make_ms(str(vis))
    tb = table()
    tb.open(str(vis), nomodify=False)
    tb.putcol("DATA", np.ones_like(tb.getcol("DATA")))
    tb.close()
    common = dict(vis=str(vis), imsize=[64], cell="2arcsec", weighting="natural",
                  niter=10, gain=0.1, threshold="0Jy", pblimit=-0.1)
    prefix = args.work / "casa"
    first = casatasks.tclean(imagename=str(prefix), **common)
    first_flux = model_flux(prefix)
    second = casatasks.tclean(imagename=str(prefix), **common)
    second_flux = model_flux(prefix)
    assert 0 < first_flux < second_flux, (first_flux, second_flux)
    conflict = None
    try:
        casatasks.tclean(imagename=str(prefix), startmodel=str(prefix) + ".model", **common)
    except Exception as error:
        conflict = {"type": type(error).__name__, "message": str(error)}
    assert conflict is not None, "CASA must refuse startmodel with its existing model"
    numbered = []
    for expected in ["casa_2", "casa_3"]:
        before = snapshot(args.work)
        casatasks.tclean(imagename=str(prefix), restart=False, **common)
        assert (args.work / (expected + ".model")).is_dir()
        after = snapshot(args.work)
        # Existing image bytes must survive; table.lock is process metadata.
        science = {k:v for k,v in before.items() if k.startswith("casa.") and not k.endswith("table.lock")}
        assert all(after[k] == value for k,value in science.items())
        numbered.append(expected)
    output = args.work / "native-output"
    output.mkdir()
    values = {**common, "imagename": str(output / "image"), "backend": "cpu", "gridprecision": "f64"}
    initial = native(args.imager.resolve(), output, values)
    assert initial["exit_code"] == 0, initial
    before = snapshot(output)
    repeated = native(args.imager.resolve(), output, {**values, "niter": 40})
    assert repeated["exit_code"] != 0 and "#944" in repeated["reply"]["message"], repeated
    assert before == snapshot(output), "native retry changed existing output bytes"
    receipt = dict(
        kind="small lifecycle/refusal oracle; not representative science",
        casa_tasks=casatasks.version_string(), casa_tools=casatools.version_string(),
        oracle_python=sys.executable, casars_revision=args.revision,
        command=sys.argv, inputs="record_casa_returns.make_ms; noiseless unit DATA",
        parameters=common, native_backend="cpu", native_gridprecision="f64",
        casa=dict(first_iterdone=int(first["iterdone"]), second_iterdone=int(second["iterdone"]),
                  first_model_flux=first_flux, second_model_flux=second_flux,
                  startmodel_conflict=conflict, restart_false_prefixes=numbered),
        native=dict(first_status=initial["reply"].get("status"), repeated=repeated,
                    unchanged_output_files=len(before)),
        result="passed lifecycle assertions; restart equivalence remains unfinished",
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"Lifecycle assertions passed: {args.out}")


if __name__ == "__main__":
    main()
