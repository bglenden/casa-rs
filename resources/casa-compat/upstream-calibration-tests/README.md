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
Hydrate its Git LFS objects and preserve the fixture aliases before running.
