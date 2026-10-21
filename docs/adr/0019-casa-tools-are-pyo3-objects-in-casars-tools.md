# ADR-0019: CASA tools are Rust-declared pyo3 objects in `casars.tools`

Status: accepted
Date: 2026-10-20
Truth class: normative
Supersedes:
Superseded by:

## Context

ADR-0017 makes a Python package that mirrors CASA's Python interfaces the
product surface. ADR-0018 puts all computation in Rust, but leaves open how
each surface binds to Rust. Tasks already have a mechanism: the parameter
contract and `casars run`. CASA tools do not yet. The simulation programme
brings the first one, the `simulator` (`sm`), and `ia`, `tb`, `ms`, `cl` and
others will follow. Every later tool reuses whatever pattern the first one
sets.

CASA scripts construct tools as objects, for example
`from casatools import simulator; sm = simulator()`, and then call methods
with CASA's parameter names. The defaults those calls see are the installed
Python binding's, not the text in the tool's XML. In particular, parameters of
"any" type default to `[]`, which the C++ reads as 0 before applying its own
fallbacks.

## Decision

1. **Namespace.**
   - CASA tool classes live in `casars.tools`, under casatools' class names:
     `simulator`, and later `image`, `table`, `measurementset`,
     `componentlist` and the rest.
   - Tasks stay in `casars.tasks`.
   - A CASA script changes only its imports.
   - casa-rs ships no `casatools` or `casatasks` alias packages, because they
     would shadow a real CASA installation.
2. **Each tool is a pyo3 class** in the `casars._core` extension, exposed
   directly; the Python package adds only the module path.
   - Methods and parameters have CASA's names, and return what CASA's return.
   - Signatures and defaults are declared in Rust (pyo3 `signature`). They
     reproduce the installed CASA binding's effective defaults, not the XML
     text, where the two differ.
   - The object-surface schema bundle describes each method from the same
     Rust types.
3. **Controls CASA lacks are keyword-only constructor arguments** with
   Rust-owned defaults: the memory budget, the backend (`auto`, `cpu`, `metal`)
   and the worker count. No tool reads environment variables (ADR-0016).
4. **Tools release the GIL.** Tool state is `Send`, and classes are not
   `unsendable`. Every method that touches the disk, copies array data or
   computes releases the GIL. The existing `casars.data` `Image` and `Table`
   bindings move to this rule under #839.
5. **Tools are thin over idiomatic Rust APIs** (ADR-0002, ADR-0018 item 5).
   - The pyo3 layer maps CASA's argument conventions (quantity strings,
     measures records, scalar-or-list arguments) onto Rust types.
   - Those conversions are shared by every tool, not written once per tool.

## Consequences

Positive:
- Every tool follows one pattern, so a CASA script works the same way across
  tools.
- Signatures and defaults have one Rust-owned source, which tests can compare
  against CASA's installed binding.
- Long calls do not freeze notebooks or other Python threads.

Negative:
- Copying CASA's binding defaults exactly, including their quirks, takes care,
  and each one must be checked against an installed CASA.
- Shared state behind a `Send` boundary makes the tool code more complex than
  thread-pinned objects.

Neutral / tradeoffs:
- `casars.data.Image` and `casars.data.Table` stay until CASA-shaped `image`
  and `table` tools arrive. Whether `casars.data` remains after that is
  decided then.
- Tools bypass the task parameter contract and Last state, as CASA's tools do.

## Alternatives considered

1. **Python wrapper classes over Rust primitives.** Rejected: defaults and
   argument handling would live in Python, against ADR-0018.
2. **UniFFI objects in `casars-frontend-services`.** Rejected: there is no
   precedent, and the pyo3 `Image` and `Table` classes already work in-process
   with NumPy.
3. **`casatools` and `casatasks` alias packages.** Rejected: they would shadow
   a real CASA installation in the same environment.

## Enforcement

This decision is enforced by:
- tests: each tool's Python tests construct it from `casars.tools`. With the
  first tool (simulation Phase 1c), a test compares every method's signature
  and defaults against the installed CASA binding when CASA is available.
- lint/import/dependency rules: none automatic
- CI checks: the existing `python_package` job
- review trigger:
  - a tool written as a Python class;
  - a default declared in Python;
  - a tool reading environment variables;
  - an `unsendable` tool class;
  - a `casatools` or `casatasks` package.
- none / guidance only:

## Drift detection

Suspect drift if:
- a tool's method signature or default differs from CASA's installed binding
  without a recorded divergence
- per-tool code re-implements quantity, measures or list conversion
- a long tool call holds the GIL
