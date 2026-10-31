# Python table checkpoint before tclean

Work issues: #1028, #1025. Owner direction: finish `tclean` (#980) first.
Local branch: `codex/python-table-create`; base: merged #1039,
`8a0d7962216284c17d177f1072954d0a13af3194`.

This is an unfinished local checkpoint, not a reviewed or accepted conversion.
Do not expand the table backlog before the tclean equivalence work.
Unrelated untracked `apps/`, `casa`, `data`, and `ms` are excluded.

## Preserved work

- Table creation and multitable creation, with memory-table and write handling.
- Root row numbers, including nested reference mappings and duplicates.
- Descriptions for plain/memory tables, MeasurementSets and identity reference
  tables. Projected references and concatenations have explicit limitations.
- Process-local tool cache reporting (`isopened`, `showcache`, `listlocks`).
  This is not yet evidence of equivalence for every process-wide table opener.
- Native bounded IncrementalStMan index checking (`testincrstman`), including
  duplicate/decreasing rows, both byte orders, and 32/64-bit indexes.
- Numeric statistics and CASA scalar, MeasurementSet and mixed-shape recordings.
  Scalar/1D float-array behavior has focused coverage. Unsupported forms and
  nonfinite inputs still require assessment; this is not complete method parity.

The oracle scenarios are additions to CASA 6.7.6.14 recordings; original
scenarios were retained. Statistics float results use relative tolerance 1e-7
while field names, types and counts remain exact. This accumulation-rounding
choice remains subject to review; no new owner-approved deviation is claimed.

## Verification on 2026-10-31

The Python extension was rebuilt after restoring every temporary Rust mutation.
The final mean is `sum / count`, not the deliberately broken zero.

Using the persistent Python 3.13 venv, with
`PYTHONPATH=crates/casars-python/python`, run:

```sh
python -m pytest crates/casars-python/python/tests/test_tools_table.py \
  crates/casars-python/python/tests/test_tools_table_create.py \
  crates/casars-python/python/tests/test_tools_table_cache.py \
  crates/casars-python/python/tests/test_tools_table_copies.py \
  crates/casars-python/python/tests/test_tools_casa_inventory.py \
  -k 'not create_default_manager and not selected_rows' -q
```

Result: **184 passed, 5 skipped, 2 deselected** (18.06 s). Skips and exclusions
are not passing evidence. A separate run of the two excluded scenarios fails:

- `create_default_manager`: `getdminfo` differs from CASA's default
  StandardStMan; local creation currently uses StManAipsIO. A prior attempted
  manager substitution was reverted; #1037 and structural writes need resolving.
- `selected_rows`: `table.selectrows` still raises `NotImplementedError`
  (#1028). Live writable views, duplicate rows, parent-close lifetime, memory
  tables and locking require proper ownership, not a materialized snapshot.

The statistics mutation replacing the mean with zero fails `statistics_ms`
at `ANTENNA1` (expected 5.217391304347826); restored statistics are included in
the passing run above. Earlier focused row-map/cache/layout mutations and Rust
tests are preserved in the source and #1028 work record; they were not rerun
as a substitute for broader acceptance here.

`CARGO_INCREMENTAL=0 cargo clippy -p casars-python -p casa-tables --all-targets
-- -D warnings` passes. Formatting and whitespace are checked before the
checkpoint commit. Full package/workspace, interoperability, CI and independent
review remain open. Nothing is pushed and no PR is marked ready.

## Resume

Return to this branch and rebuild the extension before using it: the extension
and shared target are build artifacts, not branch-specific state. Fix the two
known failures and finish method-specific acceptance before presenting the
table slice as complete. `tclean` is the next substantive work, regardless of
this remaining table work.
