# Proposal: enable numeric scan and observation selection

Status: proposed, not applied. Work issues: #980 and #54. Provider32 remains active.

Apply the prepared casa-rs provider32→33 candidate to enable nonnegative
numeric scan and observation IDs, comma lists and strictly ascending inclusive
`a~b` ranges. Selection applies with either value of `selectdata`. Preserve exact
selector text through profiles, Python mappings, JSON and CLI; exact empty means
unset, while whitespace-only and literal `none`/`None` reach the native refusal.
Padded valid IDs remain valid. Richer CASA-valid grammar, including scan `>1`,
remains explicitly unfinished. This is a bounded activation, not whole-parameter
or whole-task completion, release, merge or scientific acceptance.

The CASA signature and literal defaults do not change: all92 CASA parameters
and all114 total binding names/order/defaults are unchanged. No CASA installation
is modified. This corrects casa-rs's internal normalization of these two values.
Two revision2 concepts for the existing IDs use Identity normalization; all
revision1 concepts and other surface contracts remain unchanged. Rust's existing
row-selection owner remains the parser. Reviewed coexistence metadata explains
the normalization distinction; unsupported-form metadata retains the wider
grammar as unfinished. Both private raw-restoration paths and fixture bypasses
are removed, so execution uses the ordinary resolved values.

For supported versioned native profiles28–32 only, migration replaces exact
typed `"none"` with `""` for the two selectors and emits a visible warning.
There is no trimming or case folding before this exact match. Old whitespace,
`"None"` and `" none "` are preserved and refused. Current33 profiles, direct
kwargs, CLI and unversioned JSON do not receive legacy conversion. A warning
explains the stored representation was translated, without reconstructing lost
original input or permitting the old spelling in current calls.

The shared migration engine now reports successful explicit ReplaceValue
conversions using the existing parameter-specific Migrated diagnostic. Python
shows those Rust-owned messages on file/Last/LastSuccessful load or reload,
before Mapping expansion; ordinary refresh stays silent. This presentation also
applies to existing conversions on other old profiles; their values are unchanged.
CLI exact empty is admitted only for string-domain bindings whose literal default
is empty; missing arguments, whitespace-only values and other typed domains retain
their refusals. No persisted format or diagnostic schema changes.

## Concrete candidate and evidence

A verified durable source/patch/gate copy is preserved on the NAS home share at
`casa-rs/tclean/20261031/scan-observation-contract-proposal-20261101.tar`.
The cloud mount or download path is environment-specific.

- [Unapplied full patch](tclean-provider33-candidate-20261101.patch.gz),
  gzip-compressed to preserve exact patch whitespace; uncompressed SHA256
  `fa7f7af6aee69c6b5928f3fd044b847b523d57806787dbee351ffa31dddaf084`; proposed tclean33 fingerprint `20b8efdf4cbfc9f19fd3c0191a6a6dcba7111e91461603b67b2c826b0baeb9f9`.
- The supporting file inventory, preparation and limits are in the NAS
  proposal archive. History/generator/signature checks and `git apply --check`
  passed against the mixed committed/work-in-progress state preserved in
  `50d565e372`; the cleaned current tree needs a context rebase. The patch is
  a review artifact, not an applied contract or runtime declaration.
- The cloned candidate executes the actual native profile→projected JSON→
  re-resolution→plain imager-reader→sparse-save corpus, including old-profile
  exact-match migration. It does not depend on selector `given()` restoration.
- Current component repairs pass100 native runtime tests,47 real Python
  parameter/runner/signature tests and touched all-target clippy/structure checks.
  Six actual file/managed load+reload migration notices pass; ordinary refresh and
  a saved current profile emit none. The native extension hash is recorded in the
  probe receipt. These checks do not establish public provider33 Python execution.
- [Engine receipt](../../resources/casa-compat/evidence/tclean-scan-observation-engine-20261101.json):
  the committed291e engine passes all39 scoped scientific-product/count/sample
  pairs against CASA6.7.6.14/6.7.6-14. Two f32 CLEAN cyclethreshold checks fail the
  original limit by a small margin and remain raw failures. No waiver is requested
  by this contract decision. The final candidate still needs its own paired runs.
  A subsequent residual-unit correction at `2851c6dcdc` passes the original
  checks in three scoped backend probes. The original failures remain recorded,
  and final-source/public-provider verification is still required.
- [Independent review](https://chatgpt.com/c/6ae7afa9-d6bc-83e8-829d-a65bfdeb9a01)
  recommends the scoped semantic fix after inspecting pinned public baseline code
  and supplied local changes. Separate review of the prepared patch and repaired
  warning/fixture findings has no outstanding finding. Review advice is not owner
  approval.

The review's portable findings are: keep each existing selector concept ID
and add a revision with identity normalization for tclean; carry distinctions
in the resolved value rather than hidden input provenance; migrate only the
exact legacy `"none"` value and surface the warning before Mapping expansion;
remove fixture overrides that can conceal a normalization failure; and record
richer grammar as unsupported forms. Existing consumers retain revision 1.
The linked review conversation may not be accessible from a cloud session; its
recommendations and their limits are summarized here. A fresh independent review
is still required at the acceptance boundary.

After approval, apply this exact candidate, rebuild the extension and selected
task binary coherently, verify real kwargs/profile/draft/Mapping/save-reload paths
and RuntimeError refusals, rerun final-source CASA science and applicable normal
gates, and consolidate acceptance review. Keep both scalar failures open until
repaired or brought back as precise decisions. Preserve original raw receipts and
all previously agreed decisions; this approval grants no numerical waiver.

[AGENTS.md](../../AGENTS.md#ask-first) requires asking before “Changing persisted
formats, provider-contract bundles, or other external contracts.” That is the
reason approval is required to apply this prepared bundle. It does not add an
approval requirement for local algorithm experiments or routine repairs.
