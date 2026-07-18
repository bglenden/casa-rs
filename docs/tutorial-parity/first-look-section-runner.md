# Declarative First-Look Section Runner

The First Look image-analysis and imaging evidence is owned by
`scripts/tutorial_parity`. Twelve strict version-1 section manifests replace
the former copied, section-specific Python programs.

The 13 deleted runners and plotting helper contained 9,979 lines. Their single
replacement is 2,486 lines of Python plus 204 lines of declarative manifests,
a 73.0% reduction while covering both packs and all five surfaces.

## Runtime contract

- `CASA_RS_TUTORIAL_DATA_ROOT` is the only implicit tutorial-data root. It
  contains the relative pack paths declared by the manifests.
- `CASA_RS_CASA_PYTHON` selects the optional CASA oracle interpreter.
- `CASA_RS_GHOSTTY_CAPTURE` selects the optional GhosttyKit capture binary.
- Native Python is resolved by `scripts/resolve-python.sh` and is available only
  when the interpreter can import the installed `casars._core` extension.
- GUI execution delegates to the manifest-driven `tutorial-journey-gui`
  acceptance journey. The section runner does not duplicate XCUITest planning,
  launch configuration, signing preflight, or artifact discovery.

Every manifest declares exactly five surfaces: CASA, command line, Python, TUI,
and GUI. Task and parameter names are allowlisted by the schema. Workers accept
only JSON request files produced from validated manifests; arbitrary Python or
shell fragments are not part of the contract.

Unavailable CASA, native-Python, GhosttyKit, GUI-account, network, or dataset
prerequisites are retained as typed evidence. They are not converted into a
fabricated success or an implicit fallback.

## Section inventory

| Pack | Section | Comparator |
| --- | --- | --- |
| First Look at Image Analysis | `01-imhead-continuum-header` | normalized header, beam, mask, and coordinate fields |
| First Look at Image Analysis | `02-imstat-continuum-statistics` | scalar statistics and pixel positions |
| First Look at Image Analysis | `03-immoments-n2hp-moment-map` | CASA image pixels, masks, and declared metadata |
| First Look at Image Analysis | `04-exportfits-products` | FITS primary arrays and declared headers |
| First Look at Imaging | `01-listobs-calibrated-ms` | native CLI/Python summary contract plus CASA completion |
| First Look at Imaging | `02-uv-coverage` | native point manifest against CASA `plotms` text plus rendered PNG |
| First Look at Imaging | `03-amplitude-uvdist-by-field` | panel-by-panel point manifests against CASA `plotms` text plus rendered PNG |
| First Look at Imaging | `04-phase-cal-dirty` | image, model, PB, PSF, residual, and sum-weight products |
| First Look at Imaging | `05-phase-cal-clean` | image, model, PB, PSF, residual, and sum-weight products |
| First Look at Imaging | `06-science-target-split` | MeasurementSet rows, fields, data descriptions, and scans |
| First Look at Imaging | `07-science-target-auto-clean` | image, model, PB, PSF, residual, and sum-weight products |
| First Look at Imaging | `08-primary-beam-correction` | PB-corrected image pixels and mask |

## Commands

Validate the checked manifests without resolving external resources:

```sh
python3 scripts/tutorial_parity/runner.py validate --all
python3 scripts/tutorial_parity/runner.py run --dry-run --all
```

Run one pack section on selected surfaces:

```sh
python3 scripts/tutorial_parity/runner.py run \
  --section alma-first-look-image-analysis:01-imhead-continuum-header \
  --surface casa --surface cli --surface python
```

Omit `--surface` to request all five surfaces. Evidence, review records,
screenshot specifications, and Markdown/HTML section documentation are written
atomically under the pack paths declared by the manifest.

Run the focused contract and characterization suite with:

```sh
python3 -m unittest discover -s scripts -p 'test_*.py'
```

## Wave 7 verification

All twelve manifests validate and produce dry-run plans through the shared
runner. The characterization suite covers the full section inventory and the
specific imhead, imstat, plot-point, beam/axis/mask, scalar/position, image,
MeasurementSet, and FITS comparison routes.

A real image-analysis smoke run completed CASA and native CLI `imhead` against
the same pack at
`/private/tmp/wave7-tutorial-image-analysis/.casa-rs/evidence/tutorial-parity/01-imhead-continuum-header.result.json`.
The exact header/beam/axis comparison passed. The result remains typed
`unavailable` overall because this checkout has no installed `casars._core`
Python extension; it does not substitute the CLI result for Python evidence.

The TUI capture is likewise typed unavailable until a GhosttyKit capture binary
is supplied. The GUI journey remains the canonical opt-in live check and the
deterministic native GUI acceptance suite is the default gate; no duplicate GUI
launcher or fallback was added to this runner.
