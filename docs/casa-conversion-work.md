# Completing CASA conversions in chunks

Truth class: normative
Last reality check: 2026-11-12
Verification: just docs-check

## Choose a functional outcome

Divide the authorized scope into ordered chunks of observable CASA behavior.
A chunk follows an operation or a connected upstream execution path, such as
applying calibration tables, predicting models, or generating pointings.
Parameters and test cases describe its coverage. A single parameter value,
random seed, interpolation boundary or test failure is usually part of a chunk,
rather than another delivery unit.

Use the accepted brief, the pinned CASA source, existing behavior inventories
and CASA's tests to define the list. Include known missing capabilities and
known missing evidence. Distinguish them: implemented behavior awaiting a
comparison is different from a missing implementation. Do not infer completed
behavior from a signature or a method's overall status.

The work issue is the single place for the ordered list and current status.
Keep numerical evidence and detailed API coverage in their existing catalogs
and receipts; link them instead of maintaining another competing inventory.
For resumed work, map existing changes and valid evidence onto the chunks.
Do not restart comparisons merely to populate a new tracking format.

## Record each chunk before implementing it

Each chunk has these fields, compactly in a table or checklist on the issue:

| Field | Required content |
|---|---|
| Identity | Stable ID, observable outcome and order. |
| CASA anchor | Release and upstream functions or CASA test groups defining the behavior. |
| Coverage | Methods, parameters, effective defaults, modes and relevant interactions included. Explicitly name boundaries to other chunks or deferred work. |
| Acceptance | Named independent cases or a bounded case matrix, compared products, tolerances and expected refusals. Add applicable interoperability, backend, performance and memory requirements. |
| Starting state | What is implemented, which valid evidence already exists, and the actual gaps. |
| Dependencies | A named scenario and the exact missing capability that blocks it. A whole supporting tool is not a dependency. |
| State | Queued, implementing, checking, delivered or blocked; current branch/PR and evidence links. A blocker names the action that removes it. |

Choose cases that distinguish the required behavior and cover its meaningful
interactions. Do not construct the Cartesian product of every parameter unless
the contract requires it. Unknown coverage is recorded as unknown, not silently
passed or automatically expanded into a new capability.

## Follow the list

Keep one chunk under implementation. Finish its named cases and related fixes
together, normally in one PR for that functional outcome. Do not create a PR
for each argument form or boundary case and leave the same outcome unfinished.

When a finding is outside the active chunk, record it against an existing
queued chunk, or add a proposed chunk with the failing scenario and reason it
belongs to the authorized scope. Continue the active work. Fix a dependency
when it prevents the chunk's acceptance; record why it is necessary. A general
cleanup, full supporting-tool conversion or new task does not become authorized
because the agent encountered it.

Change a chunk's coverage explicitly on the issue, stating the trigger and
effect on completion. The owner's scope and priority prevail. Do not silently
drop requirements to finish, or widen acceptance after each passing run.

Check a representative case cheaply before a large run. Use focused regressions
while editing; run the applicable full gates on the stable chunk. Reuse current
green evidence as TESTING.md allows. One required review means one review of
the stable diff, not a new review for each parameter variant. None of this
waives a gate or changes its tolerance.

While a chunk waits for CI or review, keep responsibility for it and implement
the next independent listed chunk on a separate branch when needed. Do not
invent work to avoid waiting. Follow AGENTS.md's continuation and monitoring
rules when all remaining chunks are blocked or waiting.

## Complete and report by behavior

A chunk is delivered when its specified behavior and acceptance cases pass,
required gates and review are resolved, its evidence and documentation match,
and its change has merged. An exception needs the decision required by
AGENTS.md. An open limitation stays visible; a useful partial implementation
does not become a completed chunk.

Progress reports name the active chunk, completed requirements, remaining
requirements, blocker if any, and next listed chunk. Report progress by accepted
behavior, rather than counting PRs, commits or successful test invocations.
The authorized task ends at its stated outcome, after all required chunks;
finishing a chunk or setting auto-merge does not end it.
