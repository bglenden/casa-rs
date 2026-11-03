# evidence-pins

This branch keeps CASA-compatibility evidence commits reachable (#1089).

`resources/casa-compat/semantics/*.json` pins evidence to exact commits
(`source_revision`), and CI's clone holds only commits reachable from origin's
refs. A squash merge leaves a pull request's own commits reachable from its
branch alone, so a pin to one of them is lost when that branch is deleted.
Each commit of this branch merges the pinned commits it keeps; its tree holds
only this file. `scripts/casa_compat_semantics.py` accepts a pin that main or
this branch contains.

To keep a new pin, merge it here and push (never force-push this branch):

    git fetch origin evidence-pins
    git commit-tree origin/evidence-pins^{tree} -p origin/evidence-pins -p <sha> \
        -m "Keep <sha> reachable: <evidence>"
    git push origin <new commit>:refs/heads/evidence-pins

Do not delete this branch: main's semantic evidence check fails without it.
