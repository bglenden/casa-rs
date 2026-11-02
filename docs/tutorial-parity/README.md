# CASA Tutorial Parity

Truth class: current descriptive
Last reality check: 2026-11-02
Verification: just docs-check

The CASA Guide "First Look" tutorials are checked section by section by the
declarative runner in `scripts/tutorial_parity`. Its twelve version-1 section
manifests (`scripts/tutorial_parity/sections/`) cover First Look at Image
Analysis (`imhead`, `imstat`, `immoments`, `exportfits`) and First Look at
Imaging (from `listobs` and the uv-coverage plots through cleaning and
primary-beam correction). Each section runs on three surfaces, CASA, the
command line and Python, and compares their products.

## Runtime contract

- `CASA_RS_TUTORIAL_DATA_ROOT` is the only tutorial-data root; it holds the
  pack paths the manifests declare.
- `CASA_RS_CASA_PYTHON` selects the optional CASA oracle interpreter.
- Native Python is found by `scripts/resolve-python.sh` and counts only when
  it can import the installed `casars._core` extension.
- Task and parameter names are allowlisted by the schema. Workers accept only
  JSON requests produced from validated manifests, never Python or shell
  fragments.
- A missing CASA, native Python, network or dataset is recorded as typed
  `unavailable` evidence, never as a success or a fallback to another surface.
- Plot sections compare text point manifests; casa-rs does not render PNG
  plots.

## Commands

Validate the manifests and list the sections, without external resources:

```sh
python3 scripts/tutorial_parity/runner.py validate --all
python3 scripts/tutorial_parity/runner.py run --dry-run --all
```

Run one section (omit `--surface` for all three):

```sh
python3 scripts/tutorial_parity/runner.py run \
  --section alma-first-look-image-analysis:01-imhead-continuum-header \
  --surface casa --surface cli --surface python
```

Evidence, review records and the section's Markdown and HTML documentation are
written atomically under the pack paths the manifest declares. The runner's
tests run with `scripts/test-perf-tools.sh`.

## Source pages

- CASA Guides: <https://casaguides.nrao.edu/index.php/Main_Page>
- ALMA tutorials: <https://casaguides.nrao.edu/index.php/ALMA_Tutorials>
- VLA tutorials:
  <https://casaguides.nrao.edu/index.php?title=Karl_G._Jansky_VLA_Tutorials>
- Simulation tutorials:
  <https://casaguides.nrao.edu/index.php/Simulating_Observations_in_CASA>
