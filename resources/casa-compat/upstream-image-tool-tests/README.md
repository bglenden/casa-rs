# CASA image and coordinate tool tests

These are the 11 Python modules under `casatools/tests/tools/image/` and
the module under `casatools/tests/tools/coordsys/` from CASA 6.7.6.14 at
`9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2`. Copyright notices and test
assertions are retained. Trailing whitespace is removed. See the shared
[upstream license](../upstream-image-tests/6.7.6.14/LICENSE).

Run a module with `tools/casa-compat/upstream_image_tool_suite.py` under
the installed CASA Python. For `--implementation casa-rs`, the runner
uses the public `casars.tools.image` and `casars.tools.coordsys` entry
points. CASA still supplies the other tools used to build fixtures and
inspect results. Both runs get separate local fixture copies and working
directories. Each implementation writes its own pytest log and JUnit report.

The `image_statistics` case `test_CAS_11460` creates a 32.2 GiB Float
cube. Run it on a volume with room for the cube and its scratch data. To
run the other statistics cases on a smaller volume, use
`--only 'not test_CAS_11460'`.
