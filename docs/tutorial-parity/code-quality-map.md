# Expansion-Readiness Code-Quality Map

Truth class: current descriptive
Last reality check: 2026-04-26
Verification: just docs-check

Wave issue: #137
Child issue: #131

This audit is intentionally not a generic cleanup request. It records concrete
code-quality risks that could make tutorial expansion brittle, then assigns
them to shaped issues.

## Measurement Snapshot

Local scan command shape:

```bash
find crates -path '*/target' -prune -o -name '*.rs' -print0 | xargs -0 wc -l
rg -n 'unwrap\\(|expect\\(|panic!\\(|unimplemented!\\(|todo!\\(' crates --glob '*.rs'
rg -n '\\.clone\\(|collect::<Vec|collect::<HashMap|collect::<BTreeMap' crates --glob '*.rs'
rg -n 'pub fn|pub struct|pub enum|pub trait|pub mod' crates/casa-{tables,ms,images,imaging,calibration,vla,lattices,coordinates,types}/src --glob '*.rs'
```

Snapshot:

- Total Rust LOC under `crates/`: 345,635
- Product-path Rust LOC: 271,379
- Test-path Rust LOC: 74,256
- Product-path panic/unwrap/expect/todo/unimplemented candidates: 6,635
- Product-path clone/collect candidates: 2,150
- Public declarations across expansion-critical library crates: 2,480+

These counts are triage signals only. They are not acceptance metrics. The
important question is whether a candidate is reachable from external CASA data,
tutorial-scale data volume, or public API expansion.

## Expansion-Critical Crate Map

| Crate / area | Expansion role | Current signal | Classification | Owner |
|---|---|---:|---|---|
| `casa-tables` | Persistent table/MS/image storage substrate | 1,416 panic candidates, 434 clone/collect candidates, several storage modules over 2k LOC | Needs focused audit and memory-bounded design before larger MS workflows | #94, #132, #135 |
| `casa-ms` | MS summaries, selectors, plotting, tutorial `listobs` / `plotms` parity | `msexplore.rs` 9,440 LOC, `listobs.rs` 3,810 LOC, `spectral_selection.rs` 3,065 LOC | Needs selector and plot seams before tutorial expansion | #73, #75, #132, #135 |
| `casa-calibration` | `gaincal`, `bandpass`, `fluxscale`, `applycal` workflow owner | `execute.rs` 3,457 LOC, CLI 3,942 LOC, performance-sensitive apply path | Needs table-access/performance audit and panic-path hardening | #94, #132, #135 |
| `casa-imaging` | Core imaging algorithms for tutorial `tclean` paths | `lib.rs` 8,850 LOC | Already has focused split and cube coverage issues | #76, #74 |
| `casars-imager` | App/task orchestration for imaging | `lib.rs` 12,847 LOC | Large but app-facing; split only when tutorial work touches it | #133 if needed later |
| `casa-images` | Image analysis, FITS export, image objects | 1,652 panic candidates, 297 clone/collect candidates, `image_expr.rs` 4,753 LOC, `image_view.rs` 4,363 LOC | Needs public API and error-path audit before image-analysis expansion | #132, #134, #135 |
| `casa-vla` | VLA import/archive support and possible VLA tutorial inputs | `importer.rs` 4,171 LOC | Healthy enough for existing importvla surface; harden only paths used by VLA verticals | #132 if reached by #121 |
| `casa-lattices` / `casa-coordinates` | Image coordinate/statistics substrate | Moderate public and panic candidate counts | Audit public surface and user-data error paths before image-analysis breadth | #132, #134 |
| `casa-test-support` | CASA/casacore oracles, fixture staging, parity helpers | `lib.rs` 5,487 LOC, heavily reused by parity tests | Needs consolidation before tutorial regression growth | #136 |

## Hotspot Classification

| File | LOC | Classification | Reason | Owner |
|---|---:|---|---|---|
| `crates/casars/src/app.rs` | 17,421 | Defer | TUI shell hotspot, but Wave 0 tutorial expansion starts in libraries/oracles | none now |
| `crates/casars-imager/src/lib.rs` | 12,847 | Watch | App/task orchestration hotspot; split only when a tutorial issue touches it | #133 if needed |
| `crates/casa-ms/src/msexplore.rs` | 9,440 | Needs seam extraction | Plotting surface is tutorial-critical and currently coupled to generic scatter logic | #73, #135 |
| `crates/casa-imaging/src/lib.rs` | 8,850 | Already shaped | Imaging cube/trace ownership must be clearer before cube tutorial expansion | #76, #74 |
| `crates/casa-tables/src/storage/tiled_stman.rs` | 6,643 | Needs audit | Storage behavior underpins large MS/image workflows; avoid incidental churn | #94, #135 |
| `crates/casa-test-support/src/lib.rs` | 5,487 | Needs consolidation | Tutorial parity tests will otherwise duplicate staging/oracle helpers | #136 |
| `crates/casa-images/src/image_expr.rs` | 4,753 | Defer unless touched | Expression expansion is not in the first vertical spine | #134/#135 if image-analysis requires it |
| `crates/casa-images/src/image_view.rs` | 4,363 | Needs public/error audit | Image-analysis tutorial paths will use image views/statistics/regions | #132, #134 |
| `crates/casa-vla/src/importer.rs` | 4,171 | Watch | Important for VLA data handling, but first VLA spine starts from tutorial MS archives | #132 if user-data failures appear |
| `crates/casa-calibration/src/execute.rs` | 3,457 | Needs performance/error audit | Calibration apply/solve is central to ALMA/VLA verticals | #94, #132, #135 |

## First Cleanup Map

The audit produced these implementation-ready stabilization waves:

- #132 harden reachable product-path panics/errors.
- #133 split expansion-critical hotspots not already covered by #73/#75/#76.
- #134 audit public library surface and boundary hygiene.
- #135 audit allocation/copy patterns on tutorial-scale paths.
- #136 consolidate shared test-support seams.

Existing localized issues remain valid:

- #73 decouple curated visibility plots from generic `msexplore` scatter.
- #74 add cube trace seam regression coverage.
- #75 extract `listobs` selector parsing and TAQL building.
- #76 split `casa-imaging` trace/cube orchestration.
- #78 split and regression-test provider-contract core.
- #94 study fast, memory-bounded table access.
- #97 stabilize shared test-data discovery.

## Stop Conditions For Later Waves

Stop before implementation if a cleanup requires:

- a public API or persisted-format change;
- a provider-contract schema bundle change;
- a new top-level crate or app family;
- a substantial dependency;
- runtime/concurrency model changes;
- weakening or deleting tests without replacement.

