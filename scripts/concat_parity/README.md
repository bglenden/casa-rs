# Concat parity evidence

The evidence that a casa-rs creation plan (`CreationPlan`, #937) agrees with
CASA 6.7.6's `concat`. CASA runs here as an oracle only: these scripts are
not part of any casa-rs task.

- `casa_concat_refs.py INPUTS OUTPUT`: CASA's `concat` of the
  `unittest/concat` cases (`test1`, `test2`, `test3`, `test8`, `test16`,
  `test17`), each concatenated from fresh copies of its inputs. CASA writes
  a `HISTORY` row into each input it appends.
- `m51c_casa.py refs|append WORK`: CASA's side of the 12 m + 7 m pair
  that `simalma` makes (m51c). `refs` makes CASA's concat of the pair and a
  one-input concat. `append` makes CASA append the 7 m to casa-rs's 12 m.
- `compare_concat.py OURS THEIRS`: compares one output with the other.
  - MAIN is keyed by what each identity is (session, data description,
    field, scan, time, antennas), not by ID, with row multiplicity.
  - `DATA`, `CORRECTED_DATA` and `MODEL_DATA` are compared by normalized
    RMS; `WEIGHT`, `SIGMA`, `UVW` and flags are compared too.
  - The subtables' row counts are reported.
- `run_unittest_concat.sh` and `run_m51c.sh` drive casa-rs's side (the
  `ms_create_from` example) and the comparisons.

## Recipe

Build the example from the commit under review, with the SIMD FFTW
(`docs/agent-reference.md`):

```bash
cargo build --release -p casa-ms --example ms_create_from
```

Then set:

```bash
export MS_CREATE_FROM=$CARGO_TARGET_DIR/release/examples/ms_create_from
export CASA_PYTHON=<CASA.app>/Contents/Frameworks/Python.framework/Versions/3.12/bin/python3.12
```

For `unittest/concat`:
1. Copy casatestdata's `unittest/concat` inputs to `INPUTS`.
2. Run `casa_concat_refs.py INPUTS CASA_REFS` with `$CASA_PYTHON`, once per
   CASA version.
3. Run `run_unittest_concat.sh INPUTS CASA_REFS OUTPUT`.

For m51c:
1. Copy `simalma`'s `m51c.alma_0.5arcsec.ms` and `m51c.aca.i.ms` into a
   `WORK` directory whose path has no spaces.
2. Run `run_m51c.sh WORK`.

Each comparison is a `<name>.compare.json`. A creation agrees with CASA when:
- no MAIN key is only one side's and no multiplicity differs;
- the data NRMS are 0;
- `WEIGHT`, `SIGMA` and `UVW` agree;
- the subtables' row counts agree, except `HISTORY`, which CASA writes and
  casa-rs does not.

The record of each run is the PR's comment: the commit, the command and the
numbers. The outputs are regenerated, not archived.
