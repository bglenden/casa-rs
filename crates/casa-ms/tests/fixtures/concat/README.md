# CASA concat task references

`concat_inputs.tar.gz` contains small slices of public CASA test data:
`regression/sim_multi_arrays_and_TP/m51c_reference/m51c.alma_0.5arcsec.ms`,
`m51c.aca.i.ms`, and `measurementset/other/part1.ms` (also exposed by
`unittest/concat`). Source: NRAO's public `CASA/casatestdata` repository.

`scripts/concat_parity/task_oracle.py`, run with CASA 6.7.6-14, makes the
inputs and `concat_casa.json.gz`. It retains 12 pointing rows, replaces the
large virtual-model caches with small opaque records, and adjusts the
observation ranges to the selected integrations. A second weighted input is
shifted by 1000 seconds, has independently changed sigma, and has a true
row flag beside false element flags. These changes distinguish preservation
from reconstruction. The four CASA results cover weighting, chronological
ordering, omitting pointing, and raw flags/uncertainty spectra. HISTORY is
task-specific and checked separately. Empty string vectors are treated as
empty metadata.

The weighted input pair also retains a real flagged row with NaN weights,
varies one spectrum channel independently of its row summary, and has
matching feeds whose overlapping 2000-second validity intervals are 1000
seconds apart. CASA's reference keeps every row and merges feed coverage to
3000 seconds. These cases cover the three findings from the implementation's
single independent review.

The simulated inputs also carry SIGMA_SPECTRUM. CASA scales the first input's
SIGMA but omits its SIGMA_SPECTRUM scaling. The actual CASA references remain
unaltered; a named correction records the first input's row count and weight
factor. Tests apply the independent `sigma / sqrt(factor)` variance law to
those cells. The generator verifies CASA's omission before recording it.

The full, unmodified m51c pair and upstream concat tests 1 and 2 are also
compared with CASA, including both append directions; those larger inputs
and outputs are disposable under `target/939-concat`.
