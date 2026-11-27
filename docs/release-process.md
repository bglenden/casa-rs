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

Run `just docs-check` after the cleanup. The
[agent reference](agent-reference.md#release-and-installation) covers the
release command.
