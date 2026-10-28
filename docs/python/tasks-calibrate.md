# Calibrate Tasks

The calibration tasks are generated, CASA-named wrappers in `casars.tasks`:

- `calibrate(mode=..., ...)`, the workflow surface: `apply` (the default),
  `solve_gain`, `solve_bandpass`, `fluxscale`, `gencal`, `summary`, `stats`
  and `export_corrected_data`;
- `applycal`, `gaincal`, `bandpass`, `fluxscale` and `gencal`, the CASA-named
  surfaces.

Each takes the surface's parameters as keyword arguments and returns a
`TaskCompletion`. The call runs through `casars run`, which starts the
`calibrate` binary with the surface as its first word (`calibrate gaincal`)
and sends one `TaskRunRequest` on `--json-run -`: the values the call set,
resolved inside the binary against the same contract as its own command line
(#931). Python projects no flags and builds no Rust request. A refused value
names its parameter; a failed run raises `TaskExecutionError` when
`check=True`.

`gaintables` and `summary_paths` are lists of calibration-table paths: a list,
or one comma-separated string. The per-table controls (`gainfield`, `interp`,
`spwmap`, `calwt`) are `;`-separated strings aligned to `gaintables`, one
value for every table or one per table.

The `calibrate` binary takes the same parameters on its own command line. The
developer subcommands it once had (`summary`, `plan-apply`, `solve-gain` and
the rest) are the surfaces and modes above.
