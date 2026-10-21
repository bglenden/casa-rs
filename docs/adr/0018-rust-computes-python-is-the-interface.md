# ADR-0018: Rust computes; Python is the interface

Status: accepted
Date: 2026-10-20
Truth class: normative
Supersedes:
Superseded by:

## Context

ADR-0017 makes the product surface a Python package that mirrors CASA's Python
interfaces: casatasks-style tasks and `ia`/`tb`-style tools. CASA puts a large
share of its user-level logic in Python. Whole tasks such as `simobserve`,
`simanalyze` and `simalma` are Python scripts over C++ tools, and helper
modules such as `simutil` compute pointings, sensitivities and sky-model
rescaling with NumPy. Copying that structure would put interpreted,
GIL-bound code on the paths users time, and would split each task's behaviour
between two languages with two sets of defaults and validation.

Performance is a top priority for casa-rs, and its Rust crates already own
parallelism, memory budgets and casacore-compatible I/O.

## Decision

1. **Computation lives in Rust.** Numerical work, data access, file formats,
   and the orchestration that CASA writes in Python (task scripts and helpers
   such as `simutil`) are implemented in Rust crates. When CASA implements a
   task or tool in Python, casa-rs reads that Python as the specification and
   ports its semantics to Rust, as it does for C++.
2. **Python is an interface.** The `casars` package:
   - presents CASA's module, function and method names, signatures and
     return shapes;
   - converts arguments and results: Python scalars, lists and dicts, and
     NumPy arrays as an exchange format;
   - maps errors to Python exceptions;
   - starts the work.

   It contains no loops over rows, pixels or channels, no numerical
   algorithms and no file-format code.
3. **One source of defaults and validation.** Defaults, value normalization
   and validation come from Rust-owned definitions, for tools as well as
   tasks. For tasks that is the parameter contract of ADR-0006. The Python
   layer keeps no second copy.
4. **Plot rendering stays in Python**, under ADR-0017 item 3. Python may turn
   Rust-built plot documents into Matplotlib figures, including deriving the
   coordinate metadata that Matplotlib and Astropy need. This is the only
   computation the Python layer performs.
5. **Rust APIs stay idiomatic** (ADR-0002). CASA's call shapes exist only in
   the Python layer, which maps them onto Rust APIs. Rust crates do not grow
   CASA-shaped wrappers for the binding's convenience.
6. **Rust owns parallelism, and long calls release the GIL.**
   - Python never divides work into chunks, processes or threads for speed.
   - A Rust call that can run for more than a moment releases the GIL, so
     other Python threads, such as a notebook kernel's event loop, keep
     running.
   - The existing `casars._core` `Image` and `Table` bindings do not release
     it yet; #839 tracks that fix. New bindings release it from the start.

This decision does not fix the binding mechanism for each surface. Today:
- tasks run through the headless task host (`casars run`);
- stateful tools are expected to be in-process objects in the `casars._core`
  extension, like `casars.data.Image` and `casars.data.Table`;
- ADR-0017 item 4 limits the UniFFI layer to what the Python package calls.

## Consequences

Positive:
- Each behaviour has one implementation, in the language that sets its speed.
- CASA scripts run at casa-rs speed without Python-level hot loops.
- Tasks and the tools they are built on share Rust code, so they cannot
  drift apart.
- Rust callers get the full functionality without Python.

Negative:
- Porting CASA's Python-level logic, such as task scripts and `simutil`,
  takes more work than wrapping or copying it.
- Each new tool needs Rust bindings for its methods; it cannot start as a
  quick Python prototype that ships.
- Quirks in CASA's Python logic must be reproduced on purpose in Rust, or
  recorded as divergences in `docs/CASA (C++) bugs.md`.

Neutral / tradeoffs:
- NumPy remains a dependency of the Python package, for array exchange rather
  than computation.
- Exploratory Python may be used during development to understand CASA
  behaviour, but it does not ship.

## Alternatives considered

1. Mirror CASA's split, porting its Python task scripts and helpers as Python
   over Rust primitives. Rejected: hot paths would run in the interpreter, and
   behaviour would be split across two languages.
2. Allow NumPy computation in Python where it is "fast enough". Rejected: the
   boundary would erode case by case, and NumPy code is single-threaded and
   allocates on every operation.

## Enforcement

This decision is enforced by:
- tests: Python tests exercise behaviour through the Rust implementation;
  parity tests compare it with CASA
- lint/import/dependency rules: none automatic
- CI checks: none specific; the existing `python_package` job runs the
  Python tests
- review trigger: a change under `crates/casars-python/python/casars` that
  does any of the following:
  - computes on data;
  - parses a file format;
  - loops over rows, pixels or channels;
  - keeps its own default or validation;
  - splits work for parallelism.
- none / guidance only:

## Drift detection

Suspect drift if:
- a `casars` module other than plot rendering does arithmetic on NumPy arrays
  or iterates over data
- a Python default or validation rule duplicates, or disagrees with, the Rust
  definition
- a CASA task is implemented by porting its Python script into the `casars`
  package
- a Rust crate gains an API whose only purpose is to mimic a CASA call shape
