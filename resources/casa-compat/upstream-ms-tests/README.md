# CASA MeasurementSet task tests

The suite files come from `casatasks/tests/tasks/` and `casatools/tests/tools/` in the
[CASA 6.7.6.14 release](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatasks/tests/tasks?at=refs%2Ftags%2F6.7.6.14)
at commit `9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2`. Trailing whitespace
is removed. Upstream assertions and copyright notices are retained. See the
[upstream license](../upstream-image-tests/6.7.6.14/LICENSE).

`tools/casa-compat/upstream_ms_suite.py` runs the selected module under
installed CASA and under the corresponding public `casars.tasks` or
`casars.tools` entry point. Other CASA tools and tasks still build fixtures
and independently read outputs. Each run has its own fixture copy, working
directory and JUnit report. Fixture copies preserve `table.lock`, which in
some upstream datasets contains the current subtable row counts.

The native `agentflagger` currently supports calibration tables. Use
`--only 'test_tsys or test_bpass'` for its two supported upstream groups.
The remaining groups use the MeasurementSet or GUI tool surfaces. The
`ms_statwt` suite exercises the supported `ms.statwt` method. General
`ms` and `msmetadata` coverage follows those tools' public implementations.

The stock `split::test_updateFlagcmd1` and
`mstransform::test_split_update_flagcmd` use lazy ASDM-backed MeasurementSets.
The default replay preserves their `AsdmStMan` visibility columns and reads
the original BDF payloads. Add `--eager-asdm-fixtures` to run a control with
materialized input on both implementations.
