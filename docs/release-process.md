# Release process

Truth class: normative
Last reality check: 2026-11-27
Verification: just docs-check

## Functionality and tickets

1. Create a GitHub milestone named for the release version.
2. Create an issue for each new piece of functionality required for the
   release and assign it to the milestone. Give each issue a concrete done
   condition.
3. Review every open issue in the repository.
   - Close issues that are no longer relevant. Record the reason in each issue
     and link to any fix or replacement.
   - Assign issues required for the release to its milestone.
   - Leave other relevant issues open.

## Documentation cleanup

Remove material that is no longer relevant and could confuse humans or aliens.
Review each document in the repository and decide what to do with it.

1. Delete documents that mislead readers by describing outdated behavior or
   abandoned work. Preserve useful historical rationale under step 2.
2. Archive historical documents when they explain why the code works as it
   does and that explanation is likely to be needed again. Remove them from
   active documentation and save a commit-pinned link in the relevant issue or
   pull request. If the explanation can be recovered by reading the code,
   delete the document.
3. Keep documents that constrain how new code or processes should be written,
   including accepted architecture decision records (ADRs).
4. Keep documents that describe or support ongoing work, such as the
   [CASA (C++) bugs record](CASA%20(C%2B%2B)%20bugs.md).
5. Read all retained documents. Update them to reflect current behavior and
   accepted intentions, and repair affected links and navigation.

Ask the repository owner about any document that fits none of these categories.
Include its path and what is unclear before changing its disposition.

Run `just docs-check` after the cleanup.

## Release scope and branch

Before feature freeze, record the release's public Python and CLI interfaces,
supported platforms and Python versions, MSv2 and MSv4 behavior, and saved
parameter profile compatibility. Decide which channels will receive the
release, including whether it includes Rust crates on crates.io. State the
compatibility commitment for each published public API.

At feature freeze, cut `release/<major>.<minor>` from validated `main`. Use it
for all candidates, the final release, and patches in that series. For 1.0,
use `release/1.0` for `v1.0.0-rc1`, any subsequent RCs, `v1.0.0`, and patches
starting with `v1.0.1`. Continue development for the next release on `main`.
During the RC phase, limit changes on the release branch to release blockers
and release preparation.

## Rehearse and qualify the candidate

1. Prepare the first RC commit and local tag with the
   [release command](agent-reference.md#release-and-installation).
   Push the candidate commit to the release branch and manually run the Python
   Release workflow there before pushing the tag. Verify the wheel and source
   distribution smoke checks, suite and binary bundles on both supported
   platforms, and packaging versions. Verify that the rehearsal skips
   publication.
2. Push the RC tag after the rehearsal passes. Verify the tag's release
   checks and GitHub prerelease assets. Use the
   [release gates](https://github.com/bglenden/casa-rs/blob/main/TESTING.md#releases-and-waivers)
   for required evidence, including C++ interoperability and CASA parity.
3. Run the RC acceptance checks below using the published artifacts.
4. Resolve any remaining release blockers before promoting the candidate.
   If executable behavior or packaging changes, cut the next RC and rerun
   its release gates and the affected acceptance workloads.

### RC acceptance checks

- **Install and run.** Use clean Linux x86_64 and macOS arm64 environments
  with the oldest OS versions the release claims to support. Install and
  import each shipped Python wheel. Run a CLI task and a Python task from
  the downloaded suite, and smoke-test the standalone executable bundle.
  Run these checks outside the source checkout.
- **Upgrade and roll back.** Verify that a
  [side-by-side RC install](install.md#install-a-release-candidate-side-by-side)
  preserves the active stable install. Activate the RC, switch back to
  stable, and verify which CLI and Python package the user gets.
- **Check scientific results.** Name a small set of existing cases for
  MSv2/MSv4 conversion and round-trip, flagging and calibration, and imaging
  on CPU and Metal. Run them through the published suite. Check required
  outputs against the existing CASA or casacore reference and its
  established tolerances, including persisted data where applicable.
- **Check macOS first launch.** On a fresh Mac with default Gatekeeper
  settings, try the documented terminal installer and the browser download
  and extraction path. Run a CLI command and import the native Python
  extension. For the first RC, record any warnings or blocks and decide how
  Developer ID signing and notarization will be handled for the final release.

Record the candidate tag, workflow run, platforms and Python versions tested,
and results in the milestone issues or the release PR. Resolve failures in
required checks before promotion.

## Publish and verify the final release

Prepare the final tag on the same branch from the accepted RC, with the final
version metadata and release notes. Include supported workflows, known
limitations, upgrade and rollback instructions, and links to documentation
at the release tag.

Verify the final tag's gates, published assets, and installation from the
intended channels. Confirm that the suite manifest and installed Python
package agree with the final version. Close the milestone once its required
issues and release verification are complete. Treat published tags and
assets as immutable. Publish corrections under a new version.

## Maintain the release series

Land fixes on `main` and backport the relevant commits through pull requests
to the release branch. Keep patches compatible with the series and record
user-visible changes in the release notes. Run the release gates for each
patch, and use a patch RC when a change carries material scientific,
data-integrity, or packaging risk.
