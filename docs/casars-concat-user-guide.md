# Concatenating simulated MeasurementSets

`casars.tasks.concat` and the `concat` binary combine simulated interferometric
MeasurementSets through the existing observation creation plan. The task has
CASA 6.7.6's nine parameters, in its order and with its defaults, and returns
`None` in Python.

```python
from casars.tasks import concat
concat(vis=["simulation.12m.ms", "simulation.7m.ms"],
       concatvis="combined.ms", visweightscale=[1.0, 0.34])
```

The simulation subset sorts inputs by their earliest integration, keeps each
weight factor with its input, merges the standard catalogues, offsets scans,
and writes every stored MAIN row, including flagged rows with NaN weights.
It preserves pointing names, targets and
tracking flags, static feed validity, unused antennas' feeds, source-model
records, separate row/element flags, stored sigma and uncertainty spectra.
Weights multiply by their factor; sigma divides by its square root. Stored
spectra may vary by channel. Matching static feeds combine validity as CASA
does. Both sigma representations scale for every input; CASA omits the first
input's SIGMA_SPECTRUM scaling, a documented correction.

An existing `concatvis` is the first input and is replaced atomically after
the complete result is prepared. If it is also in `vis`, it must be the
chronologically first input. Input MeasurementSets are read-only. The output
records the native task and its parameters in HISTORY; it does not copy input
HISTORY or a stale SORTED_TABLE cache.

Matching uses the simulation defaults, 1 Hz and 1 mas. Equivalent quantity
strings or scalar quantity records are accepted. Non-default tolerances,
`respectname=True`, `timesort=True`, nonempty `forcesingleephemfield`, and
zero weight factors are refused with issue #939. The task also refuses
unrepresentable scientific columns/subtables before publication. General
ephemeris, reversed-channel, antenna-order conjugation, FLOAT_DATA and
ancillary calibration/weather concatenation remain outside this subset.

The creation plan admits input metadata, catalogue merging, bounded batches,
original uncertainty/flag columns and output writers before allocation. A
failed preparation or write leaves an existing output intact.

The committed CASA fixtures exercise the public task and compare MAIN and
every standard subtable, including independently changed sigma, raw row flags,
pointing metadata and both input orders. They also check failed append
preservation and unchanged input HISTORY. FEED row order is compared by its
antenna/feed/window keys and SOURCE by source/window; longitudes modulo one turn; unspecified optional
positions and empty provenance vectors as unstated metadata. A dangling CASA
processor reference is corrected to -1, as described in
[`CASA (C++) bugs.md`](CASA%20(C%2B%2B)%20bugs.md).

The larger CASA comparisons use the unmodified m51c 12 m + 7 m pair and
upstream concat tests 1 and 2. The recipe and independent comparison are in
`scripts/concat_parity/`; the small fixture generator is `task_oracle.py`.
The global-allocation law in `creation_plan_memory.rs` covers the production
writer, including long pointing names and large source-model records.

On the full CASA 6.7.6-14 inputs, direct native concatenation and both
CASA/native append directions preserve all 443,772 m51c rows: DATA,
CORRECTED_DATA, MODEL_DATA, weights, sigma, flags, UVW and time auxiliaries
agree exactly. Upstream concat tests 1 and 2 retain 95,256 and 71,442 rows;
DATA and weight spectra have normalized RMS 0, with sigma differences below
1.2e-7 relative. Subtable values agree within direction/time rounding.

The final three-run serial release median for m51c was 2.14 s native and
0.142 s CASA. This is a performance limitation: the existing bounded writer
rewrites all inputs, while CASA copies the first MS and appends. Improving
that creation/append path remains a non-blocking performance idea.
