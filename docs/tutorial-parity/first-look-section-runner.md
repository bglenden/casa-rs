# Declarative First-Look Section Runner

The First Look image-analysis and imaging evidence is owned by
`scripts/tutorial_parity`. Twelve strict version-1 section manifests replace
the former copied, section-specific Python programs.

When it was introduced, the runner replaced 13 runners and a plotting helper
(9,979 lines) with 2,486 lines of Python plus 204 lines of declarative
manifests.

## Runtime contract

- `CASA_RS_TUTORIAL_DATA_ROOT` is the only implicit tutorial-data root. It
  contains the relative pack paths declared by the manifests.
- `CASA_RS_CASA_PYTHON` selects the optional CASA oracle interpreter.
- Native Python is resolved by `scripts/resolve-python.sh` and is available only
  when the interpreter can import the installed `casars._core` extension.

Every manifest declares exactly three surfaces: CASA, command line, and Python.
Task and parameter names are allowlisted by the schema. Workers accept only
JSON request files produced from validated manifests; arbitrary Python or shell
fragments are not part of the contract.

Unavailable CASA, native-Python, network, or dataset prerequisites are retained
as typed evidence. They are not converted into a fabricated success or an
implicit fallback.

## Section inventory

| Pack | Section | Comparator |
| --- | --- | --- |
| First Look at Image Analysis | `01-imhead-continuum-header` | normalized header, beam, mask, and coordinate fields |
| First Look at Image Analysis | `02-imstat-continuum-statistics` | scalar statistics and pixel positions |
| First Look at Image Analysis | `03-immoments-n2hp-moment-map` | CASA image pixels, masks, and declared metadata |
| First Look at Image Analysis | `04-exportfits-products` | FITS primary arrays and declared headers |
| First Look at Imaging | `01-listobs-calibrated-ms` | native CLI/Python summary contract plus CASA completion |
| First Look at Imaging | `02-uv-coverage` | native text point manifest against CASA `plotms` text |
| First Look at Imaging | `03-amplitude-uvdist-by-field` | panel-by-panel text point manifests against CASA `plotms` text |
| First Look at Imaging | `04-phase-cal-dirty` | image, model, PB, PSF, residual, and sum-weight products |
| First Look at Imaging | `05-phase-cal-clean` | image, model, PB, PSF, residual, and sum-weight products |
| First Look at Imaging | `06-science-target-split` | MeasurementSet rows, fields, data descriptions, and scans |
| First Look at Imaging | `07-science-target-auto-clean` | image, model, PB, PSF, residual, and sum-weight products |
| First Look at Imaging | `08-primary-beam-correction` | PB-corrected image pixels and mask |

Plot sections compare text point manifests only; casa-rs does not render PNG
plots.

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

Omit `--surface` to request all three surfaces. Evidence, review records, and
Markdown/HTML section documentation are written atomically under the pack paths
declared by the manifest.

Run the focused contract and characterization suite with:

```sh
python3 -m unittest discover -s scripts/tutorial_parity -p 'test_*.py'
```

## Wave 7 verification

At Wave 7 all twelve manifests validated and produced dry-run plans through the
shared runner. The contract and characterization suite covered the full section
inventory; resource and prerequisite resolution; timeout cancellation; atomic
typed evidence, review, and documentation updates; and the specific imhead,
imstat, plot-point, beam/axis/mask, scalar/position, image, MeasurementSet, and
FITS comparison routes.

A real image-analysis smoke run completed CASA and native CLI `imhead` against
the same pack at
`/private/tmp/wave7-tutorial-image-analysis/.casa-rs/evidence/tutorial-parity/01-imhead-continuum-header.result.json`.
The exact header/beam/axis comparison passed. The result remained typed
`unavailable` overall because that checkout had no installed `casars._core`
Python extension; the runner does not substitute the CLI result for Python
evidence.
