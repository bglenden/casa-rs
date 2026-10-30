# Calibrate Tasks

The calibration tasks are generated, CASA-named wrappers in `casars.tasks`:

- `calibrate(mode=..., ...)`, the workflow surface: `apply` (the default),
  `solve_gain`, `solve_bandpass`, `fluxscale`, `gencal`, `summary`, `stats`
  and `export_corrected_data`;
- `applycal`, `gaincal`, `bandpass`, `fluxscale` and `gencal`, with CASA
  6.7.6's signatures (#931): CASA's parameters in CASA's order with CASA's
  defaults, then casa-rs's keyword-only `output`, `overwrite` and `format`
  (and `array`, `gaincurve_table` where they apply).

The CASA-named functions return what CASA's tasks return: `gaincal` and
`bandpass` a dictionary of the tables applied (`apply_tables`) and solved
(`solve_tables`), the data selected (`selectvis`) and how many solution
intervals each window and antenna reached at each stage (`solvestats`);
`fluxscale` its flux densities per field and spectral window; `applycal`
and `gencal` `None`. `calibrate(...)` and
`tasks.run(name, ...)` return a `TaskCompletion`. A CASA parameter casa-rs
does not implement yet is taken only at its default, and a CASA value it
does not implement yet is refused; both refusals name #979.

Each call starts the `calibrate` executable directly, with
the surface as its first word (`calibrate gaincal`), and sends one
`TaskRunRequest` on `--json-run -`: the values the call set, resolved inside
the executable against the same contract as its own command line (#931). Its
log lines reach `sys.stderr` as they arrive. Python projects no flags and
builds no Rust request. A refused value names its parameter: the task's
refusal raises `TaskParameterError`, a `ValueError`, and a run that does not
succeed raises `TaskExecutionError`; the `.completion` of either has its
`status`, `message` and `parameter`.

On the CASA-named surfaces the chain is CASA's: `gaintable` is a list of
tables, and `gainfield`, `interp` and `spwmap` are positional lists, so
`gaintable[i]` takes `gainfield[i]`, `interp[i]` and `spwmap[i]`, and a table
past the end of a list takes CASA's default for it (every field, `'linear'`,
no window map). `calwt` holds one flag for every table or one per table.
`docallib=True` applies the calibration library `callib` instead.

On `calibrate`, `gaintables` and `summary_paths` are lists of
calibration-table paths: a list, or one comma-separated string. Its per-table
controls (`gainfield`, `interp`, `spwmap`, `calwt`) are `;`-separated strings
aligned to `gaintables`, one value for every table or one per table.

The `calibrate` binary takes the same parameters on its own command line. The
developer subcommands it once had (`summary`, `plan-apply`, `solve-gain` and
the rest) are the surfaces and modes above.
