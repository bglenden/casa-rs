#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record meaningful CASA mosaics in time-dependent sky frames (SIM-11)."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
from pathlib import Path

from sm_frame_beams_oracle import collect, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    scratch = args.out.resolve()
    scratch.mkdir(parents=True, exist_ok=True)
    common = ["tools/simulation/sm_image_layouts_oracle.py", "--constant-frequency", "100000000000"]
    jobs = [(frame, common + ["--case", f"frame_{frame}_mosaic", "--location", "ARRAY"])
            for frame in ("jmean", "app", "azel", "hadec")]
    jobs += [
        ("hadec-alma", common + ["--case", "frame_hadec_mosaic", "--location", "ALMA"]),
        ("hadec-all", common + ["--case", "frame_hadec_mosaic", "--location", "ARRAY", "--all-fields"]),
        ("hadec-long", common + ["--case", "frame_hadec_mosaic", "--location", "ARRAY", "--long-track"]),
        ("hadec-antenna", common + ["--case", "frame_hadec_mosaic", "--location", "ARRAY", "--msselect", "ANTENNA1==4"]),
        ("hadec-reordered", common + ["--case", "frame_hadec_mosaic", "--location", "ARRAY", "--reverse-integrations"]),
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda job: run(root, scratch, job), jobs))
    for result in results:
        code = result["exit_code"]
        # The known MosaicFT destructor abort follows saved complete DATA.
        log = (scratch / (result["name"] + ".log")).read_text()
        assert code == 0 or (code == -6 and "pure virtual method called" in log), result
        with gzip.open(result["fixture"], "rt") as stream:
            assert json.load(stream)["cases"][0]["returned"], result
        print(result["name"], code, flush=True)
    (scratch / "commands.json").write_text(json.dumps(results, indent=2) + "\n")
    cases = collect(root, results, args.inputs.resolve())
    with args.fixture.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as stream:
        stream.write((json.dumps(dict(casa_version="6.7.6.14", cases=cases),
                                separators=(",", ":")) + "\n").encode())
    print("Saved", len(cases), "cases", flush=True)


if __name__ == "__main__":
    main()
