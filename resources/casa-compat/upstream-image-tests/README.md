# CASA image task tests

The suite files come from `casatasks/tests/tasks/` in the
[CASA 6.7.6.14 release](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatasks/tests/tasks?at=refs%2Ftags%2F6.7.6.14)
at commit `9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2`. Trailing whitespace
is removed. The imported assertions and copyright notices are retained.
See the [upstream license](6.7.6.14/LICENSE).

`tools/casa-compat/upstream_image_suite.py` runs each imported module under
installed CASA 6.7.6.14 and the casa-rs Python task interface. It stages the
matching `casatestdata/unittest` fixture directory separately for each run,
retains pytest logs and JUnit results, and uses CASA's tools as independent
fixture builders and output readers. Before a native subprocess reads fixtures
built by an open CASA image tool, the harness flushes those tables to disk.
