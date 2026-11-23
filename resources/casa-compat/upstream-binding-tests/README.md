# CASA table and simobserve tests

These are the public `table` tool and `simobserve` task modules from
[CASA 6.7.6.14](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse?at=refs%2Ftags%2F6.7.6.14)
at commit `9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2`. Trailing whitespace
was removed. Original assertions and copyright notices are retained. See
the shared [upstream license](../upstream-image-tests/6.7.6.14/LICENSE).

Run `tools/casa-compat/upstream_binding_suite.py table` or
`tools/casa-compat/upstream_binding_suite.py simobserve` under CASA Python
with the `--casatestdata` and `--out` paths. The runner stages fixtures
and keeps separate products and reports for both implementations. For a
targeted run, pass `--only` and select the required fixtures with
`--fixture`.
