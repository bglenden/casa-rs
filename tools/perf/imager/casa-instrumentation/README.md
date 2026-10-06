# CASA instrumentation

## Serial Python major-cycle envelopes

Set `CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES=1` when running the checked-in
`casa_tclean.py` recipe protocol to wrap the installed CASA helper in place.
The diagnostic changes no task parameters and restores the original methods
after the call, including on failure. With the variable absent it does not
import the helper or read a clock.

Each `casa_major_cycle_envelope` JSON line reports the outer `runMajorCycle`
duration, its nested `runMajorCycleCore` duration, the difference, core call
count, invocation ordinal, clean-cycle role and completion status. The outer
MFS envelope includes model and residual normalization and iteration
bookkeeping. The core includes C++ preparation, source traversal, model
prediction/subtraction, gridding and finalization. **Neither is a convolution
kernel timer.** Missing records or a zero model do not establish a measured
nonzero-model residual refresh. These serial process-local diagnostics need
observer-off/on controls and matched scientific evidence before use in a
performance claim. They do not bypass runtime, mask or CF-cache identity
validation; a newer CASA installation is not the frozen older runtime.

## Native cache-only diagnostic guard (development)

`perf_harness/casa_cf_guard.py` is a diagnostic-only LLDB guard, separate from
the frozen recipe protocol. An outer supervisor must enforce the approved
combined deadline and process-group RSS ceiling. Before launch, the guard
installs classic and TM2 AW generation/fill breakpoints with prologue skipping
disabled. One initial interpreter `exec` stop may continue with the pending
guards enabled and unhit; repeated exec or other unexpected stops fail closed.
The bootstrap imports CASA and stops before running the separately
manifested workload. Continuation requires every breakpoint to be enabled,
unhit, runtime-resolved and located at its symbol's first instruction in the
exact manifested library path and UUID. Any later stop or generation hit fails
and terminates the child; it is never resumed past a forbidden function entry.

The policy unit tests use mocks and import neither CASA nor LLDB. They do not
prove installed-runtime interception, complete native callgraph coverage, cache
compatibility, model equivalence, or numerical acceptance. Those remain explicit
preflight obligations before using this guard for a timing. No automatic
retry is supplied. In particular, a read-only
cache directory or a Python `fillCFCache` wrapper is not a substitute for the
native guard, and guard success alone is not matched-performance evidence.

## T51 one-pair fixed-model diagnostic (development)

`perf_harness/t51_pair_driver.py` connects that native guard to one explicitly
approved pipeline. `--authorized-once` is not standing permission: this dated
attempt requires the user's one-pair authorization. Both `--output` (a new
absolute artifact directory) and `--casa-python` (the explicitly selected,
debugger-compatible Python 3.12 interpreter) are required; there is no interpreter
fallback or default retry directory. The diagnostic loads the installed CASA
site-packages and records the separate interpreter, resolved executable and
Python framework hashes. It rechecks these before native launch, inside CASA
before source access, and at final immutability validation. This isolated
interpreter is not asserted to be the frozen CASA benchmark runtime.
The exclusive artifact directory cannot be overwritten. One 900-second outer deadline covers
setup, compilation, source/input hashes, the Rust production CLEAN1 run and
read-only exact-F64 model export, a separate copy of all raw CF pairs, CASA
Float-image conversion, the CASA residual refresh, full-array comparisons and
final immutability checks. No stage restarts the allowance or retries failure.

RSS is sampled across the initial process group and observed descendants
(including debugger children in another group); reaching 32 GiB terminates the
scope. The receipt explicitly reports a sampled peak, not an instantaneous
kernel-enforced aggregate limit. No complete visibility input is materialized.

The Rust production lifecycle owns the sole model. Its exported generation must
match the final normal state and both timed Rust envelopes. CASA starting images
use its Float representation, preserve support and use coordinate templates only
after exact checks against the exported coordinate law. Template pixels are not
model authority. Direction-coordinate equality is checked in CASA's canonical
record representation: the exported law goes through the same native
`coordsys.fromrecord`/`torecord` conversion as the image. This accounts for
casacore's internal degree conversion and solved celestial pole; comparisons
remain exact, without an added epsilon or regridding. The retained-image native
metadata regression can run with `CASA_RS_T51_COORDINATE_PROBE_ROOT` pointing to
a Rust subset output containing `authoritative-model/manifest.json` and both
`probe.model.tt*` images. It reads metadata only and rejects a one-ULP change
away from the canonical reference direction.
Spectral numeric fields use the existing shared coordinate-equivalence rule
(`coordinate_records_equivalent`, relative and absolute tolerances `1e-12`),
as explicitly approved for this diagnostic. Spectral frames and axis topology
remain exact; this neither changes the frozen recipe nor adds a Float/model
or product-error allowance.
CASA's own normalizer prepares the apparent starting model.
Immediately after native divide/scatter and before prediction, the diagnostic
streams the actual physical model images and checks coefficients, support and
coordinates. Failure prevents the core call. The measured CASA envelope uses
two disjoint intervals around this check; validation is separately reported and
still charged to the outer allowance. Rust reports the terminal replay envelope
plus disjoint product preparation and residual/model normalization intervals.
Both boundaries retain their explicitly labelled shared/planning overhead.

The separate direct CASA task invocation changes only the diagnostic baseline
selection, starting images, `niter=0` and `restoration=False` after constructing
the full-field/full-band recipe. It does not change the frozen recipe protocol
or its allowlist. Residual Taylor planes, PB, sum weights and validity use the
existing full-array numerical ceilings. This subset component pair is neither
full CLEAN acceptance nor evidence that inclusive CF-reader time is removable.

## Issue 540 C++ component instrumentation

The issue-540 CASA build is intentionally isolated from the user's dirty CASA
checkout. Its source baseline is the exact CASA 6.7.6.14 tag, commit
`9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2`. The earlier local 6.7.5.9-dirty
checkout was rejected as a timing baseline before any measurement was used.
The tag pins casacore at `aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6`
and gRPC at `cca868ac8f3641df4003a82b7fc54c921e14f765`. The untouched and
instrumented `SynthesisImagerVi2.cc` SHA-256 values are respectively
`11af77f2d1de0049248ea238d4e967362bf6b1ebb16a2bd259ad61149fc44a77`
and `03d33464377ff7b221915f0b61086b630ae1ee13cdd2ce0461590a50b6ee41f9`.

The isolated source and build root are under
`/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/issue540-casa-instrumented`.
No file in `/Users/brianglendenning/SoftwareProjects/CASA` is changed by this
experiment.

The exact source needed three isolated build-environment adaptations on the
current arm64 Homebrew toolchain: pkg-config selection for gRPC/protobuf,
explicit LAPACK/BLAS discovery and synthesis linkage, and discovery of the
current GCC runtime directory through `gfortran -print-file-name`. The intact
`xml-casa-assembly-1.88.jar` already present under the local CASA task source
was reused after the casatools-side copy was found truncated. These changes do
not alter imaging behavior. The resulting synthesis dylib has SHA-256
`4cf8685206d31c23a687e4f6e184f381eee14dfcdbcba1430f944a8f5e35f9aa`.

`SynthesisImagerVi2::runMajorCycle` has one environment-gated aggregate trace,
`CASA_RS_TRACE_MAJOR_TIMING`. It emits one line per major-cycle invocation with
the cycle ordinal, PSF role, buffer and row counts, and mutually exclusive
timings for mapper initialization, validation, model-buffer zeroing, degridding,
gridding, mapper finalization, and the remaining source/control envelope. It
does not read a clock inside a sample or convolution-tap loop.

The instrumented file can be audited against the untouched checkout with:

```sh
diff -u \
  /Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/issue540-casa-instrumented/source-6.7.6.14/casatools/src/code/synthesis/ImagerObjects/SynthesisImagerVi2.cc.orig \
  /Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/issue540-casa-instrumented/source-6.7.6.14/casatools/src/code/synthesis/ImagerObjects/SynthesisImagerVi2.cc
```

The full timing run must use the frozen
`wave3-standard-mfs-single-term-heavy-wave2-serial` workload without parameter
overrides. Instrumentation-off/on/off turnaround runs use the same 64-channel,
1024-pixel geometry with `niter=1` and `nmajor=1`; they are observer-cost checks,
not replacement performance anchors.

The OFF/ON/OFF controls were 133.343391, 132.328566, and 131.461036 seconds.
The traced run was 0.0556 percent faster than the bracketing OFF mean, so trace
overhead is below run noise. The full traced run completed 500 iterations in
ten 50-iteration minor cycles and 586.779085 seconds. Its ten later major
cycles averaged 50.484554 seconds: 17.601407 seconds degridding, 31.091320
seconds gridding, and 1.791826 seconds elsewhere. Because this locally built
runtime is 14.84 percent faster than the frozen official CASA.app run, the
688.996833-second official run remains the absolute pass/fail anchor; the local
run supplies stage attribution. Exact observations are recorded in
`../evidence/artifacts/20260828-issue540-casa-instrumented-major-timing.json`.

Before timing, CASA exposed a stale, unheld MAIN `table.lock`: its persisted
data-manager-change vector had length one while the table has eight managers.
The old lock was moved to a named backup and casacore regenerated transient
lock metadata; no table or science content changed. After CASA timing, the
original casa-rs-compatible lock was restored and CASA's regenerated lock was
retained in the isolated build root. Locking was not disabled and the casacore
assertion was not patched out.
