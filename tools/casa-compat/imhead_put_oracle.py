#!/usr/bin/env python3
"""Record CASA imhead string metadata mutations and their readback."""

import json
import shutil
import tempfile
from pathlib import Path

from casatasks import imhead
from casatools import image, table, version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_put")
    root.mkdir(parents=True, exist_ok=True)
    base = root / "base.im"
    ia = image()
    ia.fromshape(str(base), [4, 4, 1, 2], overwrite=True)
    ia.setrestoringbeam(major="3arcmin", minor="2arcsec", pa="0.3rad")
    ia.setbrightnessunit("Jy/beam")
    ia.done()
    log = table()
    log.open(str(base / "logtable"), nomodify=False)
    log.putcell("LOCATION", 0, "")
    log.close()
    unusual = root / "unusual_imtype.im"
    if unusual.exists():
        shutil.rmtree(unusual)
    shutil.copytree(base, unusual)
    tab = table()
    tab.open(str(unusual), nomodify=False)
    tab.putkeyword("imageinfo", {"imagetype": "mystery"})
    tab.close()
    absent = root / "without_imageinfo.im"
    ia.fromshape(str(absent), [4, 4], overwrite=True)
    ia.done()
    for lock in base.rglob("table.lock"):
        lock.unlink()
    for lock in absent.rglob("table.lock"):
        lock.unlink()
    for lock in unusual.rglob("table.lock"):
        lock.unlink()
    cases = [
        ("object", "  Galaxy  "), ("object", "none"), ("object", "auto"),
        ("object", ""), ("observer", "  Somebody  "), ("telescope", "none"),
        ("imtype", "Beam"), ("imtype", "unknown"), ("bunit", "K"),
        ("bunit", ""), ("bunit", "none"),
        ("bunit", "Jy / beam"), ("bunit", "Jy beam-1"),
        ("bunit", "Jy**2"), ("bunit", "Jy^2"),
        ("bunit", "   K  "), ("bunit", " "), ("bunit", "Jy/beam "),
        ("object", "x" * 301),
    ]
    result = {"casa_version": version_string(), "cases": [],
              "unusual_imtype": imhead(str(unusual), mode="get", hdkey="imtype")}
    with tempfile.TemporaryDirectory(prefix="imhead-put-oracle-") as directory:
        path = Path(directory) / "without_imageinfo.im"
        shutil.copytree(absent, path)
        result["missing_imageinfo"] = {"initial_imtype": imhead(str(path), mode="get", hdkey="imtype")}
        imhead(str(path), mode="put", hdkey="imtype", hdvalue="Beam")
        ia.open(str(path))
        result["missing_imageinfo"]["history"] = ia.history(list=False)[-3]
        ia.done()
        for index, (key, value) in enumerate(cases):
            path = Path(directory) / f"case-{index}.im"
            shutil.copytree(base, path)
            entry = {"key": key, "value": value}
            try:
                entry["return"] = imhead(str(path), mode="put", hdkey=key, hdvalue=value)
            except RuntimeError as error:
                entry["error"] = str(error).split(" at File:", 1)[0]
            entry["readback"] = imhead(str(path), mode="get", hdkey=key)
            entry["beam"] = imhead(str(path), mode="summary")["restoringbeam"]
            if "error" not in entry:
                ia = image()
                ia.open(str(path))
                entry["history"] = [line.replace(str(path), "<IMAGE>") for line in ia.history(list=False)[-3:]]
                ia.done()
            result["cases"].append(entry)
    (root / "expected.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    for lock in unusual.rglob("table.lock"):
        lock.unlink()


if __name__ == "__main__":
    main()
