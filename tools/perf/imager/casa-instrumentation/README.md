# CASA issue 540 instrumentation

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
