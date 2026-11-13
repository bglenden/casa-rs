# ADR-0020: Build for one investigator

Status: accepted
Date: 2026-11-12
Truth class: normative

casa-rs is in development with no downstream users. Optimize the time it
takes one investigator to get a scientifically sound result and to understand
or change the code that produced it.

- **Compute efficiently.** Keep one native Rust path organized around the
  mathematics. Measure the affected computation, memory, and I/O before
  adding complexity for speed. Use focused regression tests and CASA
  comparisons to defend the result.
- **Keep scientific data interoperable.** Preserve the ability to exchange
  CASA/casacore data in both directions, and preserve MSv4 conformance and
  exchange where supported. These are external data requirements. CASA is a
  test oracle; casa-rs performs its own computation. Record justified
  numerical divergences from CASA.
- **Keep local interfaces current.** Change casa-rs-owned APIs, settings,
  and developer artifacts directly when a simpler design serves current
  work. A saved parameter file can carry one current version marker and
  give a clear error when stale. Do not maintain migrations, legacy
  aliases, append-only version histories, or support layers for hypothetical
  future users. Revisit only when a real user or workflow needs them.
- **Keep the work record small.** An issue states the outcome and remaining
  work; its PR and executable tests carry the evidence. Reuse a single
  handoff for long work. Use ADRs for durable architectural decisions,
  consolidated when they overlap. Scale review to the scientific, data,
  and implementation risk. Keep decisions the owner already made in the
  record without opening a second approval or tracking process.

This replaces ADR-0006's requirement to preserve old casa-rs profile
contracts and their history. It does not change CASA/MSv4 formats or
scientific acceptance.
