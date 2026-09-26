# CASA, Obit and LibRA FFT implementation notes

Truth class: non-normative primary-source research
Last reality check: 2026-09-26
Verification: local source inspection; no new benchmark or implementation

## Finding

All three inspected CPU imaging implementations delegate FFT arithmetic to
FFTW. Their adapters, storage choices and memory traffic differ. LibRA's HPG
backend additionally delegates GPU FFTs to NVIDIA cuFFT, not Metal. This is
source evidence, not proof of the backend or thread count of every distributed
binary or a prediction of casa-rs speedup.

| Implementation path | Library and calls | Layout and surrounding work |
|---|---|---|
| CASA standard GridFT2 | `FFT2D::c2cFFT` -> `doFFT` -> rank-2 `fftw[f]_plan_dft` and `fftw[f]_execute_dft` | Full complex plane, in-place; forward/backward plans retained by the adapter. Explicit image centering and inverse normalization remain. |
| CASA Clark convolution | `LatConvEquation` -> `LatticeConvolver<Float>` -> `LatticeFFT` -> `FFTServer` -> FFTW | Real/half-complex convolution; lattice axis traversal and FFTServer work-buffer copies remain. Not uniformly a direct rank-2 call. |
| Obit base CPU imaging | `ObitUVGridFFT2Im[Par]` -> `ObitFFTC2R` -> `fftwf_execute_dft_c2r` | Single-precision half-complex grid to real image; complete 2D FFTW plans, retained image/beam plan objects, followed by centering and normalization/correction. |
| LibRA standard GridFT2 | CASA-derived `FFT2D` -> FFTW | Same whole-plane in-place strategy, with surrounding shift/normalization work. |
| LibRA HPG CPU / CUDA | `fftw[f]_plan_many_dft` / `cufftPlanMany`, rank 2 | Explicit strides and batches; in-place/out-of-place kernels. CUDA uses `cufftExecC2C` or `cufftExecZ2Z`. The inspected kernels create/destroy plans per kernel call; do not claim universal persistent plan caching. |

## Primary-source pins and inspection locations

### CASA

Local repository: `/Users/brianglendenning/SoftwareProjects/casa`, HEAD
`61020062cee290f5466cffed5ec5032e0c7a3434`.
Upstream: [NRAO CASA6](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse).
Paths below are relative to `casatools/src/code/`:

- `synthesis/TransformMachines2/GridFT.cc`: `ft_p.c2cFFT` at the
  model-to-grid and grid-to-image boundaries. This file has local diagnostic
  modifications; the same FFT call sites were verified in `git show HEAD:...`.
- `synthesis/Utilities/FFT2D.h:46`: FFTW is the constructor default.
- `synthesis/Utilities/FFT2D.cc:54-64`: FFTW float/double thread initialization;
  thread count follows host/OpenMP configuration, not an unconditional one.
- `FFT2D.cc:349-431`: centered transform, cached forward/backward rank-2
  plans, `FFTW_ESTIMATE`, direct in-place array pointers and new-array execution.
- `FFT2D.cc:500-550`: explicit quadrant swaps using a quarter-plane temporary;
  inverse normalization is combined with that operation. FFTW does not remove
  these application-level passes.
- `synthesis/MeasurementEquations/LatConvEquation.h`: owns a
  `LatticeConvolver<Float>`; Clark's residual calculation calls its equation.

CASA's bundled casacore source is at
`/Users/brianglendenning/SoftwareProjects/casa/casatools/casacore`, HEAD
`25b653f6963a78a1dcfc8e16954081e091a50fbe`:

- `lattices/LatticeMath/LatticeConvolver.tcc:178-245`: reduced first Fourier
  axis, real-to-complex transform, spectrum multiplication and inverse.
- `lattices/LatticeMath/LatticeFFT.tcc:49-77`: complex-plane transform when
  memory permits, otherwise line-by-line traversal. Its real-transform path
  also iterates axes/lines through FFTServer.
- `scimath/Mathematics/FFTServer.hcc:260-297`: copies complex input into
  `itsWorkC2C`, invokes FFTW, normalizes inverse output and copies back.
  Lines 36-38 document removal of its FFTPACK alternative in 2020. Do not infer
  current generic FFTServer uses FFTPACK from older documentation.

The inspected FFT2D and casacore files are unmodified locally. CASA has multiple
FFT users; the direct GridFT path must not be generalized to every utility or
minor-cycle convolution.

### Obit

Pinned source: [ObitFFT.c](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitFFT.c)
and [ObitUVGrid.c](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVGrid.c).
Inspected local build source:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/obit-build/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/core/src/`.

- `ObitFFT.c:350-367, 490-494`: direct single-precision FFTW execution on
  the array buffers for C2R and C2C.
- `ObitFFT.c:748-820`: reverses the dimension list to reconcile column-major
  Obit arrays with FFTW's row-major interpretation, without transposing pixels;
  creates full 2D C2C/R2C/C2R plans with `FFTW_ESTIMATE`.
- `ObitUVGrid.c:722-725, 787-790`: grid-to-real FFT followed by image centering.
- `ObitUVGrid.c:876-925`: verifies the grid's first dimension is `Nx/2+1`,
  configures FFT threads, creates image/beam plans only when absent and executes
  them. The half-spectrum avoids storing the redundant half of a real image's
  Fourier representation; it is not a general substitution for complex images.
- The file retains compile-time FFTW2/GSL alternatives. The existing local
  reference build's `REFERENCE.md:73` records FFTW3f 3.3.11. No additional Obit
  installation or timing was performed.

### LibRA and HPG

Local repository: `/Users/brianglendenning/SoftwareProjects/libRA`, HEAD
`0ab99e261878334d6588eafa360cef3b673e897f`,
[local-fork revision](https://github.com/bglenden/libRA/tree/0ab99e261878334d6588eafa360cef3b673e897f),
[upstream](https://github.com/ARDG-NRAO/LibRA).
`src/synthesis/Utilities/FFT2D.cc` is unmodified and contains the same rank-2
FFTW strategy described above; `TransformMachines2/GridFT.cc` calls it.
Older `TransformMachines/GridFT.cc` retains `LatticeFFT` calls.

HPG dependency: `/Users/brianglendenning/SoftwareProjects/libRA/dependencies/HPG`,
HEAD `79667f64746425eb3cf34c3b83081e1af19df3db`,
[pinned FFT source](https://gitlab.nrao.edu/mpokorny/hpg/-/blob/79667f64746425eb3cf34c3b83081e1af19df3db/include/hpg/fft.hpp).
The inspected `include/hpg/fft.hpp` is unmodified:

- Lines 70-212: single/double FFTW execution and batched planning adapters.
- Lines 218-288: rank-2 batched CPU planning with explicit strides/layout,
  `FFTW_ESTIMATE | FFTW_PRESERVE_INPUT`, execution and plan destruction.
- Lines 342-434: float/double cuFFT execution, rank-2 `cufftPlanMany`, CUDA
  stream association and in-place execution. This is an available CUDA backend,
  not evidence that the local macOS build executes GPU FFTs.

## Implication for casa-rs

A thin FFT adapter is normal and useful. The avoidable mechanism is our own
repeated lane gathering/scattering around a 1D library, not the existence of a
wrapper itself. Direct library-owned multidimensional transforms are the
relevant CPU experiment. Obit's reduced Hermitian representation is another
memory/work lesson for genuinely real transforms; adopting it would require
checking the particular operator's semantics rather than assuming all grids
are Hermitian. Do not reproduce CASA's older work-buffer copies merely for
implementation similarity. No backend replacement, speedup or scientific
acceptance is claimed by this research.

## Licensing clarification

Checked 2026-09-26. The owner, formerly responsible for the principal authors
of CASA, Obit and LibRA, confirms none relies on a private FFTW licensing
arrangement. There is no need to pursue that hypothesis.

FFTW is [GPL-2.0-or-later](https://www.fftw.org/fftw3_doc/License-and-Copyright.html).
The relevant source headers provide a public-license route:

- CASA `synthesis/Utilities/FFT2D.cc:1-17` is GPL-2.0-or-later. CASA also
  contains LGPL components: its Python package LICENSE.txt contains Library
  GPL v2, and casacore's FFTW adapter is Library-GPL-2.0-or-later. A package
  label is not a complete license inventory of the linked imaging stack.
- Obit `core/LICENSE` supplies GPLv2, and `core/src/ObitFFT.c:1-20` explicitly
  grants GPLv2 or any later version.
- LibRA's `src/synthesis/Utilities/FFT2D.cc:1-17` and
  `TransformMachines2/GridFT.cc:1-17` retain GPL-2.0-or-later grants.
  HPG's own `LICENSE.spdx` and FFT header identify Apache-2.0. That grant
  does not replace FFTW's license. Apache-2.0 and GPLv3 are compatible;
  Apache-2.0 is not compatible with GPLv2-only
  ([Apache's explanation](https://www.apache.org/licenses/GPL-compatibility)).
  The GPL components' later-version permission provides the relevant GPLv3
  combination route.

Under the [FSF's stated interpretation](https://www.gnu.org/licenses/gpl-faq.en.html#IfLibraryIsGPL),
a distributed program linked with a GPL library must satisfy GPL terms for
the combined work, including applicable source, notices and license duties.
Individual compatible components can retain their original source licenses;
this is not automatic relicensing of every independent file or package.
[Dynamic linking](https://www.gnu.org/licenses/gpl-faq.en.html#GPLStaticVsDynamic)
or an LGPL/permissively licensed wrapper is not an exemption from the GPL
dependency's terms.

casa-rs currently declares `LGPL-3.0-or-later` in workspace Cargo.toml.
A GPLv3-compliant FFTW-linked distribution is a viable route, subject to
checking the actual release's complete dependency set and packaging. Independent
casa-rs components can retain LGPL source licensing, but a downstream consumer
cannot treat the FFTW-linked combination as LGPL-only with unrestricted
proprietary-linking permission. No paid FFTW license is needed merely to develop,
benchmark, or distribute a GPL-compliant build. Preserving LGPL-only downstream
linking flexibility for that same FFTW-linked product would require another
licensing solution or a different backend.

This is an engineering reading of published terms, not a legal opinion or a
compliance audit of any project's release artifacts. No casa-rs license,
dependency, or distribution policy was changed. NVIDIA cuFFT has separate terms;
this clarification concerns the FFTW CPU paths, not a CUDA distribution audit.
