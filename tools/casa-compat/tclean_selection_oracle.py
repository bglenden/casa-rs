#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Freeze CASA physical MAIN-row selection for tclean work (#54/#980).

Run under CASA 6.7.6.14 Python -I on the extracted, unchanged repository
mssel_test_small.ms fixture. This records MSSelection rows and axes, not
imaging/scientific acceptance. MSRange::ROWS uses Table::rowNumbers(), so
the IDs refer to physical MAIN rows even after a selection.
"""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def plain(value):
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


def main():
    import casatasks
    from casatools import ms, table, quanta

    assert casatasks.version_string() == "6.7.6.14"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vis", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    revision = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "-C", str(repo), "diff", "--exit-code", "--", str(Path(__file__).resolve())], check=True)
    tb = table()
    tb.open(str(args.vis))
    times = sorted(set(tb.getcol("TIME").tolist()))
    total = tb.nrows()
    tb.close()
    tb.open(str(args.vis / "FIELD"))
    names = tb.getcol("NAME").tolist()
    tb.close()
    assert names[:2] == ["J1617-7717", "J1423-7829"], names
    qa = quanta()
    middle = (times[len(times)//2-1] + times[len(times)//2]) / 2
    date = qa.time(qa.quantity(middle, "s"), form="ymd", prec=12)[0]
    cases = {
        "all": {}, "field_id": {"field": "0"},
        "field_name": {"field": names[0]}, "field_glob": {"field": "J1423*"},
        "field_id_and_name": {"field": "0," + names[1]},
        "field_both_names": {"field": ",".join(names[:2])},
        "antenna_id": {"antenna": "0"}, "antenna_name": {"antenna": "DA41"},
        "baseline": {"antenna": "DA41&DA56"},
        "scan1": {"scan": "1"}, "scan3": {"scan": "3"},
        "time_before": {"timerange": "<" + date},
        "time_after": {"timerange": ">" + date},
        "observation0": {"observation": "0"},
        "combined": {"field": names[0], "antenna": "DA41", "scan": "1"},
        "no_such_field": {"field": "NO_SUCH_FIELD"},
    }
    api_names = {"field": "field", "antenna": "baseline", "timerange": "time",
                 "scan": "scan", "observation": "observation"}
    recorded = {}
    for name, selectors in cases.items():
        handle = ms()
        try:
            handle.open(str(args.vis), nomodify=True)
            selected = handle.msselect({api_names[key]: value for key, value in selectors.items()}, onlyparse=False)
            count = handle.nrow(True)
            rows = handle.range(["rows"])["rows"].tolist() if count else []
            assert len(rows) == count
            assert all(0 <= row < total for row in rows)
            recorded[name] = dict(selectors=selectors, selected=bool(selected), rows=rows,
                                  indices=plain(handle.msselectedindices()))
        except Exception as error:
            recorded[name] = dict(selectors=selectors, error={"type": type(error).__name__, "message": str(error)})
        finally:
            handle.done()
    assert recorded["all"]["rows"] == list(range(total))
    assert recorded["field_id"]["rows"] == recorded["field_name"]["rows"]
    assert recorded["field_id_and_name"]["rows"] == recorded["all"]["rows"]
    archive = repo / "crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz"
    output = dict(casa_tasks=casatasks.version_string(), casa_tools="6.7.6-14",
                  oracle_script_revision=revision, fixture=archive.name,
                  fixture_archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                  row_identity="physical zero-based MAIN row numbers, in returned order",
                  scope="CASA MSSelection oracle only; native row and imaging comparisons pending",
                  cases=recorded)
    args.out.write_text(json.dumps(output, indent=2) + "\n")
    print("Recorded", len(recorded), "CASA selection cases;", total, "physical MAIN rows")


if __name__ == "__main__":
    main()
