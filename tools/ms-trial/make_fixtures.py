# SPDX-License-Identifier: LGPL-3.0-or-later
"""Build the MSv2/MSv4 trial fixtures (docs/measurement-set/trial/README.md).

Run from the repository root, in the locked oracle environment:

    cargo run -p casa-ms --example ms_trial_fixture -- WORK/base.ms
    uv run --project tools/ms-trial python tools/ms-trial/make_fixtures.py WORK/base.ms OUT

The script copies the base MS to OUT/ms_trial.ms and, through casacore (arcae),
adds seeded noise and known antenna gains, then sets deterministic weights and
flags. It converts that MSv2 with XRADIO to OUT/ms_trial.ps.zarr, chunked along
time and frequency so the dataset spans several physical chunks, and writes
OUT/manifest.json with the exact oracle versions, the true gains and the
store's layout. Nothing here runs at production time.
"""

import importlib.metadata
import json
import shutil
import sys
from pathlib import Path

import numpy as np
from arcae.lib.arrow_tables import Table

SEED = 20261024
NOISE_JY = 0.02
GAIN_INTERVAL_S = 60.0
# Receptor pairs of the linear correlations XX, XY, YX, YY.
RECEPTORS = [(0, 0), (0, 1), (1, 0), (1, 1)]
MAIN_CHUNKSIZE = {"time": 5, "frequency": 4}
XRADIO_COMMIT = "3ed765867515095d852eb68c073165c1f6c58761"


def true_gains(rng, intervals, antennas):
    """Complex gains [interval, antenna, receptor]: amplitudes near one and
    phases within +-0.6 rad."""
    amplitude = 1.0 + 0.1 * rng.standard_normal((intervals, antennas, 2))
    phase = rng.uniform(-0.6, 0.6, (intervals, antennas, 2))
    return amplitude * np.exp(1j * phase)


def corrupt(ms):
    main = Table.from_filename(str(ms), readonly=False)
    time = np.asarray(main.getcol("TIME"))
    a1 = np.asarray(main.getcol("ANTENNA1"))
    a2 = np.asarray(main.getcol("ANTENNA2"))
    data = np.asarray(main.getcol("DATA")).astype(np.complex64)  # [row, chan, corr]
    rows, channels, correlations = data.shape
    assert correlations == 4
    rng = np.random.default_rng(SEED)
    interval = np.floor((time - time.min()) / GAIN_INTERVAL_S).astype(int)
    antennas = int(max(a1.max(), a2.max())) + 1
    gains = true_gains(rng, int(interval.max()) + 1, antennas)
    for k, (i, j) in enumerate(RECEPTORS):
        g = gains[interval, a1, i] * np.conj(gains[interval, a2, j])
        data[:, :, k] *= g[:, None].astype(np.complex64)
    noise = rng.standard_normal(data.shape) + 1j * rng.standard_normal(data.shape)
    data += (NOISE_JY * noise).astype(np.complex64)

    # Weights vary by row and correlation; SIGMA follows them.
    row = np.arange(rows)[:, None]
    corr = np.arange(correlations)[None, :]
    weight = (1.0 / NOISE_JY**2) * (0.5 + ((7 * row + 3 * corr) % 11) / 10.0)
    weight = weight.astype(np.float32)
    sigma = (1.0 / np.sqrt(weight)).astype(np.float32)

    # Element flags only: XRADIO does not carry FLAG_ROW, so the fixture keeps
    # FLAG_ROW false and flags whole rows through FLAG.
    flag = np.zeros(data.shape, dtype=bool)
    flag[7, 2, :] = True  # one channel of one row
    flag[15, 5, 1] = True  # one cross hand
    flag[33, :, 3] = True  # every channel of YY
    flag[50, :, :] = True  # a whole row
    flag[60:64, 7, 0] = True  # XX of the last channel over four rows
    flag[95, 0:3, :] = True  # the first three channels of one row
    flag[110, 4, 2] = True

    main.putcol("DATA", data)
    main.putcol("WEIGHT", weight)
    main.putcol("SIGMA", sigma)
    main.putcol("FLAG", flag)
    # The casa-rs synthetic writer leaves UVW's MEASINFO without a reference;
    # CASA writes ITRF, and XRADIO needs one to give UVW the frame its schema
    # requires.
    uvw_keywords = main.getcolkeywords("UVW")
    uvw_keywords["MEASINFO"]["Ref"] = "ITRF"
    main.putcolkeywords("UVW", uvw_keywords)
    main.close()
    return {
        "seed": SEED,
        "noise_jy_per_component": NOISE_JY,
        "gain_interval_s": GAIN_INTERVAL_S,
        "receptor_pairs": RECEPTORS,
        "flagged_elements": int(flag.sum()),
        "elements": int(flag.size),
        "true_gains": {
            "shape": ["interval", "antenna", "receptor"],
            "re": gains.real.tolist(),
            "im": gains.imag.tolist(),
        },
    }


def convert(ms, ps):
    from xradio.measurement_set import convert_msv2_to_processing_set

    convert_msv2_to_processing_set(
        in_file=str(ms),
        out_file=str(ps),
        partition_scheme=[],
        main_chunksize=MAIN_CHUNKSIZE,
        # The synthetic MS has a one-row POINTING table; XRADIO's pointing
        # dataset types its time coordinate as "time" where its own schema
        # requires "time_pointing", and the trial reads no pointing.
        with_pointing=False,
        persistence_mode="w",
    )


def store_layout(ps):
    """Every array of the store: shape, chunks, dtype, codecs and fill value,
    read from its zarr.json, as an independent record of what XRADIO wrote."""
    layout = {}
    for meta in sorted(ps.rglob("zarr.json")):
        node = json.loads(meta.read_text())
        if node.get("node_type") != "array":
            continue
        name = str(meta.parent.relative_to(ps))
        layout[name] = {
            "shape": node["shape"],
            "chunks": node["chunk_grid"]["configuration"]["chunk_shape"],
            "data_type": node["data_type"],
            "codecs": node["codecs"],
            "fill_value": node["fill_value"],
            "dimension_names": node.get("dimension_names"),
        }
    return layout


def versions():
    names = ["xradio", "zarr", "numcodecs", "xarray", "numpy", "arcae", "dask", "toolviper"]
    found = {name: importlib.metadata.version(name) for name in names}
    found["xradio_commit"] = XRADIO_COMMIT
    found["python"] = sys.version.split()[0]
    return found


def main():
    base, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    ms = out / "ms_trial.ms"
    ps = out / "ms_trial.ps.zarr"
    shutil.rmtree(ms, ignore_errors=True)
    shutil.copytree(base, ms)
    corruption = corrupt(ms)
    convert(ms, ps)
    manifest = {
        "recipe": "tools/ms-trial/make_fixtures.py",
        "versions": versions(),
        "main_chunksize": MAIN_CHUNKSIZE,
        "corruption": corruption,
        "store": store_layout(ps),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    print(json.dumps(manifest["versions"], indent=1))


if __name__ == "__main__":
    main()
