# CASA calibration tests

The test modules come from CASA 6.7.6.14 commit
[`9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatasks/tests/tasks?at=9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2).
Trailing whitespace is removed. Upstream assertions and copyright notices are retained.
See the [upstream license](../upstream-image-tests/6.7.6.14/LICENSE).

Run the suite with an installed CASA 6.7.6.14 environment, a casa-rs release
provider and a casatestdata checkout. The runner makes a fresh fixture copy
for each implementation and writes separate logs and JUnit reports.

```sh
python tools/casa-compat/upstream_calibration_suite.py applycal \
  --casatestdata /path/to/casatestdata --out target/upstream-calibration/applycal
```

The `applycal` suite uses `unittest/applycal` from casatestdata commit
[`5e48637233019176f328ab4552e96395ab967581`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casatestdata/browse/unittest/applycal?at=5e48637233019176f328ab4552e96395ab967581).
Hydrate its Git LFS objects and preserve its symbolic links before running.
The Multi-MS shares its POINTING subtable through links between partitions.

## Source inventory

[The source manifest](upstream-sources.json) records the upstream path and SHA-256
for 37 task modules and the `calanalysis` and `calibrater` tool modules. The
copies normalize trailing whitespace and add a final newline where needed.
They preserve all original assertions. The
shared `agentflagger`, `ms_statwt`, flagging, split and mstransform suites are in
the [MeasurementSet suite](../upstream-ms-tests/README.md). The tagged CASA
repository has no standalone Python test module for `ssoflux`, `uvsub`,
`atmosphere`, `singledishms`, `wvr` or the callibrary helpers.

Choose a module name in place of `applycal` in the command above. Use `--only`
to select a pytest case expression and `--fixture` to select source fixture
paths for that run. The `predictcomp` and `sdatmcor` suites also resolve stock
data outside the casatestdata `unittest` directory. The runner adds that pinned
source directory to CASA's search path for those two suites.

For `getephemtable`, run `test_secondaryname_in_parentheses` in a fresh process
with `--only` so the C++ measures reader opens the newly published table. The
original `test_webservice_errors` patches a private CASA Python function.
`tools/casa-compat/getephemtable_oracle.py` exercises real HTTP errors through
both public implementations. The paired native comparison for the original
topocentric fixtures is recorded in [PR #2164](https://github.com/bglenden/casa-rs/pull/2164).
