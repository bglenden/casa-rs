#!/usr/bin/env python3
"""Record imhead observation-date formatting from CASA-created images."""

import json
from pathlib import Path

from casatasks import imhead
from casatools import image, table, version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_dates")
    root.mkdir(parents=True, exist_ok=True)
    cases = {"missing": None, "j2000": 51544.00000000116, "noon": 58849.5, "fraction": 60000.123456789}
    result = {"casa_version": version_string(), "cases": {}}
    for name, mjd in cases.items():
        path = root / f"{name}.im"
        ia = image()
        ia.fromshape(str(path), [4, 4], overwrite=True)
        ia.done()
        tb = table()
        tb.open(str(path), nomodify=False)
        coords = tb.getkeyword("coords")
        if mjd is None:
            del coords["obsdate"]
        else:
            coords["obsdate"]["m0"]["value"] = mjd
        tb.putkeyword("coords", coords)
        tb.close()
        result["cases"][name] = {
            "date-obs": imhead(str(path), mode="get", hdkey="date-obs"),
            "epoch": imhead(str(path), mode="get", hdkey="epoch"),
            "list": imhead(str(path), mode="list")["date-obs"],
        }
        for lock in path.rglob("table.lock"):
            lock.unlink()
    (root / "expected.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
