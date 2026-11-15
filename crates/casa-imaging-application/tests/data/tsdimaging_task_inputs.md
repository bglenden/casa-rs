Actual CASA 6.7.6.14 inputs and reference products generated on 2026-11-15 by
`tools/singledish/tsdimaging_oracle.py`. Its public ACA configuration and analytic
three-channel model produce 242 spectra; its map/gapped/small inputs exercise
weights, flags, gaps and each SD kernel. The existing numerical reference format
is reused unchanged. The archive omits only transient table locks.

`tsdimaging_pointing_inputs.tar.gz` and `tsdimaging_pointing_casa.json.gz`
add controls from `tools/singledish/tsdimaging_pointing_oracle.py`: AZEL,
empty POINTING, times outside coverage, between-pointing interpolation,
TARGET, flagged STATE and FLOAT_DATA without DATA. Empty POINTING makes CASA
crash during beam sampling after writing the grids; the recorded control
finishes its normal weight mask, without inventing a restoring beam.

The public-data acceptance runs use CASA's `measurementset/other/sdimaging.ms`
(the `unittest/tsdimaging` link) and
`regression/sim_multi_arrays_and_TP/m51c_reference/m51c.aca.tp.sd.ms`.
`tools/singledish/tsdimaging_task_parity.py` regenerates their CASA 6.7.6.14
products and has CASA read both native products for pixel, mask, unit and WCS
comparisons. Those larger datasets remain external test data.

`tsdimaging_selection_inputs.tar.gz` and `tsdimaging_selection_casa.json.gz`
come from `tools/singledish/tsdimaging_selection_oracle.py`. They cover an
off-source, one-channel window preceding a selected three-channel target
window; corrected/float brightness-unit precedence; legacy UNIT before
QuantumUnits; fallback past an undeclared unit; and CASA's refusal of a
lowercase explicit brightness unit. The public-task tests compare persisted
pixels, masks, units, every WCS axis and the rest frequency with these CASA
6.7.6.14 records.
