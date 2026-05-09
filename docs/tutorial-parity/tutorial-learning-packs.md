# Tutorial Learning Packs

Truth class: current descriptive
Last reality check: 2026-05-09
Verification: just docs-check

Tutorial learning packs are the delivery format for walking through CASA Guide
tutorial material in casa-rs across the GUI, TUI, Python, and command-line
interfaces. A pack has two customers:

- Learners need data, concise instructions, visible parameters, screenshots,
  and expected products so they can run the tutorial themselves.
- Maintainers and agents need reproducible correctness, performance,
  completeness, provenance, and regression evidence.

The pack manifest is the source of truth for both views. Learner documentation,
screenshots, evidence summaries, and regression records are projections from
that manifest and its generated evidence files.

## Artifact Shape

Version 0 is a directory pack:

```text
<tutorial-pack>/
  pack.json
  inputs/
  workspace/
    native/
    oracle/
    scratch/
  docs/
    index.md
    sections/
  evidence/
    data-manifest.json
    native-runs.jsonl
    oracle-runs.jsonl
    comparisons.json
    timings.jsonl
    provider-provenance.json
    review/
  screenshots/
    source/
    annotated/
    specs/
```

`pack.json` follows [`resources/tutorial-pack.schema.json`](../../resources/tutorial-pack.schema.json).
Section review records follow
[`resources/tutorial-pack-review.schema.json`](../../resources/tutorial-pack-review.schema.json).
The no-data pilot template is
[`resources/tutorial-packs/alma-first-look-image-analysis.template.json`](../../resources/tutorial-packs/alma-first-look-image-analysis.template.json).

`inputs/` is immutable once a pack is generated. `workspace/` is the writable
area for native casa-rs products, CASA oracle products, and scratch state.
Generated packs live under the existing tutorial-data policy:

```text
${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/...
```

or the default user location:

```text
~/SoftwareProjects/casa-tutorial-data/tutorial-parity/...
```

The repository stores the schema, templates, generators, tests, and docs. It
does not commit official CASA tutorial datasets. Generated pack manifests must
record registry keys, source URLs, source artifact URLs, size or checksum
policy, local staging policy, and directory-manifest identity where applicable.

Generate the current pilot pack with:

```bash
scripts/generate-tutorial-pack.py --pack alma-first-look-image-analysis
```

By default this writes:

```text
${CASA_RS_TUTORIAL_DATA_ROOT:-~/SoftwareProjects/casa-tutorial-data}/tutorial-parity/alma/first-look/twhya/image-analysis/alma-first-look-image-analysis.pack
```

The generator copies local tutorial inputs when they are already staged under
the tutorial-data root. Use `--no-materialize-inputs` to create only the pack
skeleton and leave each input marked `missing` in `evidence/data-manifest.json`.
The macOS GUI can open either the pack directory or its `pack.json` manifest.

## Learner View

The learner view should be small and runnable. It contains:

- tutorial section index and short setup notes
- exact GUI, TUI, Python, and CLI parameters for each section
- expected products and what to inspect
- annotated parameter-setting screenshots when available
- a compact note about known differences or unsupported options

Learner docs should not include raw oracle logs, full comparison payloads,
internal timing records, or every screenshot/capture intermediate. Those stay
in the regression overlay and can be summarized where helpful.

## Regression Overlay

The regression overlay contains:

- native run records for each tutorial step
- CASA oracle run records when CASA is available
- comparison reports and tolerances
- timing records for performance regression checks
- provider provenance for every executable step
- data manifests, checksums, and generated product paths
- human evaluation records for each observable tutorial chunk
- source screenshot captures, annotation specs, and rendered annotated images

Native tutorial steps must record `provider_kind: "native-rust"`. A native step
must fail validation if it routes through `casars-casa-task`, CASA Python, or
another adapter path. CASA is allowed only for steps explicitly marked as
oracle evidence.

## Human Checkpoints

Each observable tutorial chunk ends with a human checkpoint before the next
chunk advances. The checkpoint records:

1. CASA source material and the expected observable result.
2. casa-rs GUI, TUI, Python, and CLI equivalents with exact parameters.
3. CASA and casa-rs products, plus timings where relevant.
4. Draft learner documentation for the chunk.
5. Regression evidence: inputs, commands or API calls, outputs, tolerances,
   checksums, provider provenance, and logs.
6. Human outcome: accepted, needs revision, or product gap.

The review record is intentionally machine-readable. It includes CASA source
calls, casa-rs equivalents for all four user surfaces, observable product
references, regression evidence references, and the human evaluation outcome.

Early ALMA First Look chunks should be reviewed one at a time. Once the pack
format and review style stabilize, later chunks can be batched while retaining
explicit checkpoint records.

## First Pilot

The first pilot is `alma/first-look/twhya/image-analysis` because it uses
small image inputs and a narrow image-analysis task surface:

- `twhya_cont.image`
- `twhya_n2hp.image`
- `imhead`
- `imstat`
- `immoments`
- `exportfits`

The pilot currently defines the pack contract, local pack generation, GUI pack
loading, and the first-step review workflow. It does not claim full ALMA First
Look parity or make screenshots canonical.

Open the pilot pack in the macOS GUI from `apps/casars-mac` with:

```bash
./script/build_and_run.sh --tutorial-pack ~/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/image-analysis/alma-first-look-image-analysis.pack
```

For non-interactive regression checks, dump the same loaded state with:

```bash
swift run casars-mac --dump-debug-state --open-tutorial-pack ~/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/image-analysis/alma-first-look-image-analysis.pack
```

The Tutorial tab shows input staging status, the observable section list, the
learner docs path, the regression evidence paths, and an `Open Task` action
that applies the section's GUI parameters to the native task panel.
