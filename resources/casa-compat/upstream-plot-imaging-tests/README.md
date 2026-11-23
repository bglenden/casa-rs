# CASA tclean and PlotMS test sources

These files retain the original tests and copyright notices. Trailing
whitespace was removed. See the shared
[upstream license](../upstream-image-tests/6.7.6.14/LICENSE).

- [CASA 6.7.6.14](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse?at=refs%2Ftags%2F6.7.6.14)
  at `9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2` supplies the public
  `casatasks/tests/tasks/test_task_tclean.py` and the tclean-only regression
  and performance modules under `casatests/`. The source of the public
  task tests matches the hash in [the extracted case inventory](../tclean-casa-test-cases-summary.json).
  Use [the accepted case scope](../tclean-case-scope.json) and #980 for
  the casa-rs imaging checks. The upstream modules require CASA fixtures
  and include MPI and performance configurations.
- [casaplotms 2.9.1](https://open-bitbucket.nrao.edu/projects/CASA/repos/casaplotms/browse/tests/plotms/test_task_plotms.py?at=e05a21b9b343f67f82b24ec1949ffd9d0367f362)
  at `e05a21b9b343f67f82b24ec1949ffd9d0367f362` supplies
  `tests/plotms/test_task_plotms.py`. It collects 106 tests with the pinned
  package and contains both definitions of the upstream duplicated PNG test
  name. The CASA runner uses `casatestdata/unittest/plotms`, a desktop or
  virtual display, and the pinned casaplotms package. #1032 owns the static
  casa-rs task comparisons and performance qualification.
