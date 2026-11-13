#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Actual CASA controls for simulator selection columns, expressions and clauses."""
import argparse
from pathlib import Path
import shutil

from casatasks import version_string
from casatools import simulator

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot


QUERIES = {
    "array_id": "ARRAY_ID == 0",
    "observation_id": "OBSERVATION_ID == 0",
    "state_id": "STATE_ID == 1",
    "processor_id": "PROCESSOR_ID < 0",
    "feed1": "FEED1 == 0",
    "feed2": "FEED2 == 0",
    "interval": "INTERVAL >= 15",
    "exposure": "EXPOSURE >= 10",
    "centroid": "TIME_CENTROID >= MID_TIME",
    "flag_row": "FLAG_ROW",
    "uvw_index": "UVW[1] < -20",
    "uvw_norm": "sumsquare(UVW) > 1",
    "flags_any": "any(FLAG)",
    "flags_count": "ntrue(FLAG) > 0",
    "weight_index": "WEIGHT[1] >= 2",
    "sigma_index": "SIGMA[1] < 1",
    "data_index": "DATA[1,1] == 3+4i",
    "data_real": "real(DATA[1,1]) > 2",
    "corrected_abs": "abs(CORRECTED_DATA[1,1]) > 1",
    "model_index": "MODEL_DATA[1,1] == 7-2i",
    "array_shape": "shape(DATA)[1] == 4",
    "array_elements": "nelements(FLAG) == 16",
    "array_literal": "ANTENNA1 IN [1,3,5]",
    "array_expression": "any([ANTENNA1,ANTENNA2] == 2)",
    "sqrt": "sqrt(ANTENNA1) > 1",
    "square": "square(ANTENNA1) > 3",
    "pow": "pow(ANTENNA1,2) > 3",
    "sine": "sin(ANTENNA1) > 0",
    "atan2": "atan2(ANTENNA1,ANTENNA2) > 0.4",
    "log_exp": "log(exp(ANTENNA1)) > 1",
    "floor": "floor(ANTENNA1/2.0) == 1",
    "round": "round(ANTENNA1/2.0) == 1",
    "sign": "sign(ANTENNA1-2) < 0",
    "minmax": "min(ANTENNA1,ANTENNA2) == 1",
    "int_cast": "int(ANTENNA1/2) == 1",
    "double_cast": "double(ANTENNA1)/2 > 0.5",
    "string_cast": "string(ANTENNA1) == '2'",
    "string_function": "lower('A') == 'a' AND ANTENNA1 == 2",
    "conditional": "iif(ANTENNA1==ANTENNA2,0,1) == 1",
    "isnan": "isnan(ANTENNA1/ANTENNA1)",
    "isfinite": "isfinite(ANTENNA1/ANTENNA1)",
    "rownumber": "rownumber()%2 == 0",
    "rowid": "rowid()%2 == 0",
    "rand_true": "rand() >= 0",
    "rand_false": "rand() < 0",
    "order": "TRUE ORDERBY TIME DESC",
    "order_limit": "TRUE ORDERBY TIME DESC,ANTENNA1 DESC,ANTENNA2 DESC LIMIT 5",
    "limit": "ANTENNA1 != ANTENNA2 LIMIT 5",
    "offset_limit": "TRUE LIMIT 5 OFFSET 3",
    "group": "TRUE GROUPBY FIELD_ID",
    "having": "TRUE GROUPBY FIELD_ID HAVING FIELD_ID == 0",
    "having_empty": "TRUE GROUPBY FIELD_ID HAVING FIELD_ID == 1",
    "group_signed_zero": "TRUE GROUPBY iif(ANTENNA1==0,0.0,-0.0)",
    "order_incompatible": "TRUE ORDERBY iif(ANTENNA1==0,0,'x')",
    "nested_arrays": "any(DATA == DATA+(DATA+(DATA+(DATA+DATA))))",
    "qualified": "$1.ANTENNA1 == 2",
    "invalid_column": "NOT_A_COLUMN == 0",
    "invalid_short_circuit": "TRUE OR (FLAG_ROW+1>0)",
    "data_after_predict": "real(DATA[1,1]) > MID_DATA",
    "corrected_after_predict": "real(CORRECTED_DATA[1,1]) > MID_DATA",
    "model_after_predict": "MODEL_DATA[1,1] == 1 AND ANTENNA1 < 2",
    "data_after_append": "real(DATA[1,1]) > MID_DATA",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, default=Path(__file__).resolve().parents[2] / "crates/casa-simulation/tests/data/sm_remaining_selection", help="captured synthetic CASA inputs")
    parser.add_argument("--case", action="append", choices=QUERIES)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    shutil.copytree(args.inputs, inputs, dirs_exist_ok=True)
    original = snapshot(inputs / "base.ms")
    midpoint = min(row["time"] for row in original["rows"]) + 10.
    before = snapshot(inputs / "base.ms")
    cases = []
    for name, query in QUERIES.items():
        if args.case and name not in args.case:
            continue
        path = root / f"{name}.ms"
        shutil.copytree(inputs / "base.ms", path, ignore=shutil.ignore_patterns("table.lock"))
        sm = simulator()
        assert sm.openfromms(str(path))
        predicate = query.replace("MID_TIME", repr(midpoint))
        pending = "_after_" in name
        if pending:
            assert sm.setdata(fieldid=[0])
            assert sm.predict(complist=str(inputs / "spectrum.cl"))
            if name == "data_after_append":
                assert sm.settimes(integrationtime="10s", usehourangle=False, referencetime="UTC 2024/06/01/00:00:00")
                assert sm.observe(sourcename="target", spwname="band6", starttime="3s", stoptime="23s", project="continued")
            current = snapshot(path)
            levels = sorted({row["data"][0][0] for row in current["rows"] if row["field"] == 0})
            assert len(levels) > 1
            threshold = .5 * (levels[len(levels) // 2 - 1] + levels[len(levels) // 2])
            predicate = predicate.replace("MID_DATA", repr(threshold))
        returned = bool(sm.setdata(fieldid=[0], msselect=predicate))
        assert sm.predict(complist=str(inputs / "spectrum.cl"), incremental=pending)
        sm.close()
        after = snapshot(path)
        selected = sum(a["data"] != b["data"] for a, b in zip(before["rows"], after["rows"]))
        cases.append(dict(name=name, predicate=predicate, returned=returned,
                          operation="components", before=before, after=after,
                          changed_rows=selected, predict_before_selection=pending,
                          append_before_selection=name == "data_after_append", incremental=pending))
        write(args.fixture, dict(casa_version=version_string(), recipe="tools/simulation/sm_remaining_selection_oracle.py", cases=cases))
        print(name, "returned", returned, "changed rows", selected, flush=True)


if __name__ == "__main__":
    main()
