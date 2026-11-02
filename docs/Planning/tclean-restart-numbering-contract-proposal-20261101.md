# Enable the prepared tclean restart=False behavior

Status: owner approved activation on 2026-11-01 (this chat: “Approve activation”).
Provider32 is applied; focused provider/public-resolution/migration/conformance
checks pass. Public CLI/Python products and package gates pass in the bounded
scope; one exact Metal scalar result is separately owner-accepted and preserving prior
image histories is an agreed deviation. See the activation receipt.
Work issues: #944 and #980. CASA oracle: casatasks 6.7.6.14 / casatools 6.7.6-14.

## Approved decision

Enable casa-rs's existing `restart=False` parameter for a single main image by
updating its provider bundle from 31 to 32. All 92 CASA names, signatures and
defaults stay unchanged, including `restart=True`. CASA itself is unchanged.

The native implementation preserves previous products and chooses fresh
`image_2`, `image_3`, … outputs using CASA's highest recognized suffix plus 1.
It keeps the full absolute directory, as already approved. The normal native
compile/run path creates the selected products and summary; it does not seed
from the previous model. Only the output namespace policy changes.

Coordinated outlier numbering and occupied chosen orphan namespaces remain
explicit unfinished refusals. Other restart work, including calcres/calcpsf
reuse, stays open; this decision does not close the equivalence programme.

## Exact activation

The approved activation patch changes the tclean surface version 31->32,
removes only restart's `unimplemented` marker, replaces its stale surface
note, and adds a 31->32 migration with no changed defaults or value rewrites.
It replaces the universal False-refusal test with public-resolution and
outlier-refusal checks. Contract history and Python binding snapshots are
regenerated through the existing scripts after approval.

Patch SHA256: `7d9e7c3a3dbd0a2395e8fd15baf9f0ff370a783328aec7c1083610bdc7508fe3`.
The exact patch and commands are in the verified archive below; the working
copy is `/private/tmp/tclean-numbering-proposal-20261101/activation-proposal.patch`.
Applied at `99b1cfad559bf92de9ac070a387b4d2c9165ec97`. All114 binding names,
order and defaults and every other task surface are unchanged. Snapshot inspection
found no contract snapshot change; scalar shapes/types are shared across surfaces.

## Historical preview evidence

- Six helper tests match pinned CASA filename cases, including gaps, Taylor
  suffixes, arbitrary-length numeric IDs, Unicode digits and byte parsing.
- Six restart T1 tests pass, including the public-run native preview:
  originals stay unchanged, `_2` then `_3` start fresh, and products/summaries
  use the selected prefix. That preview bypasses only the catalog refusal.
- The unchanged production False-refusal test passes. Production activation
  has not been counted as a pass.
- Touched all-target clippy, dependency, formatting and diff checks pass.
  Independent review has no outstanding findings.

APFS cannot create invalid-byte filenames. Those cases have explicit helper
surrogateescape and native byte-parser checks; physical Linux filesystem
acceptance is separate. This limit is recorded without counting a skipped
physical fixture as a pass.

Current completed science source `fd7b8e7b2d` passes 18 CASA-paired scalar/mask
cases and the full normal gate: 4,698 workspace tests/45 skips, doctests,
workspace clippy, architecture/SPDX/fmt, 21 CLI hosts and 875 performance-tool
tests/307 subtests. These gates retain their own revision identity; the
numbering preview has the focused checks above.

## Activation evidence

The [activation receipt](../../resources/casa-compat/evidence/tclean-numbering-activation-20261101.json)
records all20 native product/preservation rows passing,19/20 selected scalar
rows passing the original limits, and Python package gates of632 editable,632
wheel and99 fresh-sdist tests passing. Raw failures and fixture repairs remain
recorded. The full native gate is running. Both separate decisions are recorded in that receipt.
This is bounded activation evidence, not whole-task equivalence.

## Authority and durable evidence

[AGENTS.md](../../AGENTS.md#ask-first) explicitly requires asking before
“Changing persisted formats, provider-contract bundles, or other external
contracts.” This request covers the provider bundle activation.

Preview receipt:
[`tclean-numbering-preview-20261101.json`](../../resources/casa-compat/evidence/tclean-numbering-preview-20261101.json).
Verified NAS archive: `/Volumes/home/casa-rs/tclean/20261031/restart-numbering-preview-20261101.tar`;
696,320 bytes, SHA256 `bc98a438ff90629866659ce278126039bd4ee5c776e9acf9567776670ddc4cf2`.
All 56 archived members and the reread NAS archive were verified.
