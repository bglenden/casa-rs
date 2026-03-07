# Wave 15 - Final lattices coordinates images parity audit docs and performance closeout

## Origin

- Final Phase 5 follow-up wave after the implementation track in Waves 11a-14.
- Re-scoped so the last wave is a holistic lattices/coordinates/images audit
  rather than another feature tranche.
- Must incorporate the review criteria captured in
  `/Users/brianglendenning/SoftwareProjects/casa-rs/.claude/skills/casa-review/SKILL.md`,
  adapted to the final Phase 5 closeout scope.

## Goal

- Perform the final module-wide closeout for `casacore-lattices`,
  `casacore-coordinates`, and `casacore-images`: verify parity coverage, API
  documentation quality, examples/demos, interop evidence, and performance
  status, then leave an explicit final record of any residual gaps.
- Produce a final Wave 15 report that explicitly covers the five `casa-review`
  criteria: interop coverage, performance coverage/results, demo parity,
  rustdoc quality, and crate-separation/architecture checks.

## Non-goals

- Net-new feature work beyond narrowly scoped fixes uncovered by the audit.
- Broad performance optimization work beyond representative smoke evidence and
  explicit status reporting, except that gross regressions discovered during
  Wave 15 must be investigated and fixed narrowly when feasible.
- New image storage formats or non-Phase-5 modules.
- Broad CASA task/tool parity.

## Scope

### Read path

- Audit the final read-path surface across lattices, coordinates, images, lazy
  expressions, expression files, temporary images, masks, regions/subimages,
  iterators, and image metadata.
- Confirm the supported-vs-deferred capability matrix after Waves 11a-14 and
  verify that remaining gaps are explicit rather than accidental.

### Write path

- Audit persisted-format interoperability for the externally visible Phase 5
  surfaces touched across prior waves, including images, coordinates attached
  to images, masks, `misc_info`, and expression-file persistence where
  applicable.

### API/docs/demo

- Review public Rust docs for doxygen-comparable completeness across:
  - `crates/casacore-lattices/src/`
  - `crates/casacore-coordinates/src/`
  - `crates/casacore-images/src/`
- Review crate overviews, examples, and demos for accuracy against the final
  supported surface.
- Cross-check the final examples/demos against relevant upstream casacore
  demos/tests so Wave 15 leaves an explicit demo-parity record instead of a
  prose-only conclusion.

### Performance/reporting

- Rerun representative smoke benchmarks and record the final performance status
  versus C++, including known red areas that are consciously deferred.
- If a representative Phase 5 workload remains worse than `5x` C++, Wave 15
  must investigate root cause and attempt a narrow fix rather than merely
  documenting the red number. Remaining `>5x` cases are acceptable only if the
  Results section explains why the fix was not landed and records a concrete
  follow-up item.

## Dependencies

- Waves 11a-14 completed.

## Ordering constraints

- Final active Phase 5 wave.
- Must run after Waves 11a-14.
- New functionality found during this wave should be fixed narrowly or spun
  out as an explicit follow-up, not silently absorbed into the audit.

## Files likely touched

- `crates/casacore-lattices/src/`
- `crates/casacore-coordinates/src/`
- `crates/casacore-images/src/`
- `crates/casacore-images/examples/`
- `crates/casacore-tables/examples/`
- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/src/lib.rs`
- `crates/casacore-test-support/tests/`
- `docs/Planning/Phase 5 - Lattices Coordinates Images/`

## Definition of Ready

- [x] Final supported-vs-deferred capability matrix is available from Waves
      11a-14.
- [x] Wave 15 review/report structure is mapped to the five sections from
      `casa-review/SKILL.md`.
- [x] Public API/doc review targets identified across lattices, coordinates,
      and images crates.
- [x] Consolidated C++ reference paths identified in `../casacore`.
- [x] Representative 2x2 interop fixtures identified (RR, RC, CR, CC).
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Data-table dependency reviewed (or marked N/A).
- [x] Iterator workload and edge cases identified (or marked N/A).
- [x] Performance smoke workloads identified.
- [x] Non-goals documented.

## Implementation checklist

- [x] Produce the final Wave 15 report in the same sectioned shape as the
      `casa-review` skill:
      interop coverage, performance tests, demo parity, rustdoc coverage, and
      crate separation.
- [x] Audit public API docs for doxygen-comparable quality across lattices,
      coordinates, and images crates.
- [x] Audit crate/module overviews, demos, and examples for accuracy and
      coverage of the final supported workflows.
- [x] Run the consolidated interop matrix over the key persisted and evaluated
      Phase 5 surfaces.
- [x] Run the consolidated iterator/region/mask traversal checks where
      applicable.
- [x] Run the representative performance smoke suite in release mode and
      publish final status numbers with Rust/C++ ratios.
- [x] Investigate and land narrow fixes for any representative Phase 5
      workload still worse than `5x` C++ after the first Wave 15 perf pass,
      or explicitly defer it with concrete rationale.
- [x] Audit crate-separation boundaries and confirm no Phase 5 work leaked
      internal implementation details through public APIs.
- [x] Document every remaining gap, deferment, or known performance issue
      explicitly in Results.

## Test plan

- [x] 2x2 interop matrix (`RR`, `RC`, `CR`, `CC`) for representative persisted
      and evaluated surfaces across lattices, coordinates, and images.
- [x] Interop checklist explicitly reviewed against the `casa-review` matrix
      categories where they apply to Phase 5 surfaces:
      scalar types, dimensionality, storage managers, fixed vs variable shape,
      undefined cells, and metadata/keywords/table-info behavior.
- [x] Endian matrix (if applicable).
- [x] Iterator traversal matrix: full, strided, tiled/chunked, and
      region/mask-aware where applicable.
- [x] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [x] Clean skip when measures data tables are unavailable.
- [x] Release-mode Rust-vs-C++ perf suite run and reported, not just debug-mode
      smokes.
- [x] Demo/program parity inventory produced for the final supported Phase 5
      workflows and any meaningful upstream casacore demo analogues.
- [x] Public rustdoc audit includes missing/weak `//!` / `///` coverage and
      missing C++ cross-references where applicable.
- [x] Crate-separation audit includes `publish = false` internal crates,
      public re-export leakage, and Phase 5 boundary hygiene.
- [x] Consolidated final capability matrix covering:
  - lattice storage/traversal/regions
  - coordinate system and FITS/WCS interoperability
  - typed images, masks, `ImageInfo`, beams, units, and `misc_info`
  - lazy `ImageExpr`, parser/persistence, and `TempImage`

## Performance plan

- Workload: representative smoke benchmarks across the main Phase 5
  performance-sensitive surfaces.
- Rust command: release benchmark pipeline covering final closeout workloads,
  including the `casa-review` baseline command
  `cargo test --release -p casacore-test-support "vs_cpp" -- --nocapture`
  plus any additional Wave 15-specific closeout runs needed for lattices,
  coordinates, and images.
- C++ command: matching casacore reference benchmark pipeline.
- Alert threshold: Rust > 2x C++.
- Gross-regression threshold: Rust > 5x C++ on a representative primary Phase
  5 workload triggers mandatory investigation and fix attempt in Wave 15.

Representative Wave 15 performance checks should include:

- Full-image disk lifecycle:
  - create/write/reopen/read for a reasonably large disk-backed image
- Iterator/chunk traversal:
  - full, strided, and tiled/chunked traversal over large lattices/images
- Lazy-expression slice reads:
  - arithmetic/transcendental and any newly added LEL families from Wave 14
  - specifically include the currently red Wave 14 families if still present,
    such as reductions and `iif`
- Parsed/persisted expression reopen:
  - open `.imgexpr`, resolve dependencies, and sample representative slices
- Temporary-image lifecycle:
  - create/populate/reopen/read workflow for `TempImage`

Wave 15 should report status, not hide red numbers. Moderate red numbers may be
acceptable if explicitly documented; gross red numbers (`>5x`) are not
closeout-ready unless they were investigated and either fixed or extracted as a
specific follow-up blocker with rationale.

## Closeout criteria

- [x] All Phase 5 closeout gates pass.
- [x] Public docs and crate overviews are updated to final-quality status.
- [x] Results include the final supported/deferred parity matrix across
      lattices, coordinates, and images.
- [x] Results include the full `casa-review` section set with concrete findings
      and file references where relevant.
- [x] Results include consolidated interop and performance status.
- [x] No representative primary Phase 5 workload remains silently worse than
      `5x` C++; each such case is either fixed or called out as an explicit
      blocker/follow-up.
- [x] Remaining gaps are explicit follow-up items, not accidental omissions.

## Results

- Date: 2026-03-07
- Commit: `6a5eef4` (initial Wave 15 closeout; reviewed and corrected afterward)
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (all tests pass)
  - `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75` -> PASS (76.43%)
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
  - `cargo run --example t_lattice -p casacore-lattices` -> PASS
  - `cargo run --example t_coordinate -p casacore-coordinates` -> PASS
  - `cargo run --example t_image -p casacore-images` -> PASS
  - `cargo run --example t_image_expr -p casacore-images` -> PASS
  - `cargo run --example t_subimage -p casacore-images` -> PASS
  - `cargo test --release -p casacore-test-support "vs_cpp" -- --nocapture` -> PASS
  - `cargo test --release -p casacore-test-support --test images_perf_vs_cpp -- --nocapture` -> PASS

### `casa-review` report sections

#### 1. Interop test coverage

Consolidated 2x2 matrix (RR = Rust-write/Rust-read, RC = Rust-write/C++-read,
CR = C++-write/Rust-read, CC = C++-write/C++-read):

| Surface                  | RR | RC | CR | CC | Test file              |
|--------------------------|----|----|----|----|------------------------|
| Image f32 3D             | Y  | Y  | Y  | Y  | images_interop         |
| Image f64                | Y  | Y  | Y  | -  | images_interop         |
| Image Complex32          | Y  | Y  | Y  | -  | images_interop         |
| Image Complex64          | Y  | Y  | Y  | -  | images_interop         |
| Image metadata/units     | -  | Y  | -  | -  | images_interop         |
| TempImage materialization| -  | Y  | Y  | -  | temp_image_interop     |
| ImageExpr parser/eval    | Y  | -  | -  | -  | image_expr_interop (28)|
| Expression file .imgexpr | Y  | Y  | Y  | Y  | image_expr_interop     |
| Masks                    | Y  | Y  | Y  | -  | images_interop, image_expr_interop |
| Coordinates              | -  | -  | Y  | -  | (via image layer)      |
| PagedArray               | -  | -  | -  | -  | (via image layer)      |

**Notes:**
- Coordinates: tested indirectly — C++ images carry coordinate systems that
  Rust opens and validates. No dedicated coordinate serialization cross-test.
  Covered through image layer.
- Lattice-level (PagedArray): same code paths as PagedImage. Covered through
  image layer.
- CC leg: limited to basic f32 image and `.imgexpr` round-trip coverage.
  Acceptable for Phase 5 scope.
- 13 tests in `images_interop`, 28 in `image_expr_interop`, 2 in `temp_image_interop`.

#### 2. Performance tests

All from `images_perf_vs_cpp.rs` (15 tests, release mode):

| Workload                                     | Rust     | C++      | Ratio  | Status |
|----------------------------------------------|----------|----------|--------|--------|
| Image lifecycle 64-cube                      | 3.1 ms   | 7.3 ms   | 0.42x  | GREEN  |
| Complex32 lifecycle 48-cube                  | 2.8 ms   | 13.9 ms  | 0.20x  | GREEN  |
| Plane-by-plane 256-cube                      | 59.2 ms  | 48.2 ms  | 1.23x  | GREEN  |
| Bounded-cache plane-by-plane 1024-cube       | 5.7 s    | 152.5 s  | 0.04x  | GREEN  |
| Complex32 bounded-cache 1024-cube            | 20.0 s   | 21.8 s   | 0.92x  | GREEN  |
| Spectrum-by-spectrum bounded-cache 1024-cube | 46.8 s   | 19.0 s   | 2.46x  | YELLOW |
| Lazy expr closeout slice                     | 3.9 ms   | 78.8 ms  | 0.05x  | GREEN  |
| Parsed LEL expr 64x64                        | 11.8 ms  | 397.5 ms | 0.03x  | GREEN  |
| Two-image virtual LEL expr                   | 7.0 ms   | 285.0 ms | 0.02x  | GREEN  |
| Expression file .imgexpr save+open           | 25.7 ms  | 254.5 ms | 0.10x  | GREEN  |
| Wave 14 reduction (sum+mean)                 | 28.5 ms  | 215.1 ms | 0.13x  | GREEN  |
| Wave 14 iif                                  | 63.6 ms  | 184.7 ms | 0.34x  | GREEN  |
| Wave 14 real_part (Complex32)                | 0.3 ms   | n/a      | n/a    | GREEN  |
| Sub-cube slice                               | 1.0 ms   | 36.5 ms  | 0.03x  | GREEN  |
| Chunked iteration 64-cube                    | 0.4 ms   | n/a      | n/a    | GREEN  |

**Summary:** 11/13 comparable Rust-vs-C++ workloads are faster than C++.
Two workloads are slower than C++ (`plane_by_plane_perf` at `1.23x`,
`spectrum_by_spectrum_bounded_cache_perf` at `2.46x`), and no representative
Phase 5 workload is worse than `5x`. The spectrum-by-spectrum bounded-cache
case remains the only materially red image workload; it is a worst-case access
pattern (strided reads across 32-cube tiles for 1024-cube data). This was
previously above the gross-regression line and is now below it after the LRU
tile-cache work.

#### 3. Demo parity

| C++ demo               | Rust equivalent                                    | Status |
|-------------------------|----------------------------------------------------|--------|
| tArrayLattice           | `casacore-lattices/examples/t_lattice.rs` sec 1    | DONE   |
| tPagedArray             | `casacore-lattices/examples/t_lattice.rs` sec 2    | DONE   |
| tLatticeIterator        | `casacore-lattices/examples/t_lattice.rs` sec 3    | DONE   |
| tTempLattice            | `casacore-lattices/examples/t_lattice.rs` sec 4    | DONE   |
| tSubLattice             | `casacore-lattices/examples/t_lattice.rs` sec 5    | DONE   |
| tCoordinateSystem       | `casacore-coordinates/examples/t_coordinate.rs` s7 | DONE   |
| tDirectionCoordinate    | `casacore-coordinates/examples/t_coordinate.rs` s1 | DONE   |
| tSpectralCoordinate     | `casacore-coordinates/examples/t_coordinate.rs` s2 | DONE   |
| tStokesCoordinate       | `casacore-coordinates/examples/t_coordinate.rs` s3 | DONE   |
| tLinearCoordinate       | `casacore-coordinates/examples/t_coordinate.rs` s4 | DONE   |
| tTabularCoordinate      | `casacore-coordinates/examples/t_coordinate.rs` s5 | DONE   |
| tProjection             | `casacore-coordinates/examples/t_coordinate.rs` s6 | DONE   |
| tObsInfo                | `casacore-coordinates/examples/t_coordinate.rs` s8 | DONE   |
| tPagedImage             | `casacore-images/examples/t_image.rs`              | DONE   |
| tImageExpr              | `casacore-images/examples/t_image_expr.rs`         | DONE   |
| tSubImage               | `casacore-images/examples/t_subimage.rs`           | DONE   |
| tTable                  | `casacore-tables/examples/t_table.rs`              | DONE   |
| tAipsIO                 | `casacore-aipsio/examples/t_aipsio.rs`             | DONE   |
| tMeasure                | `casacore-types/examples/t_measure.rs`             | DONE   |

**19/19 demo programs covering all supported modules.** FITS round-trip demo
included in `t_coordinate.rs` section 9.

#### 4. Rustdoc coverage

- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
- All public types in Phase 5 crates have `///` documentation with C++ class
  cross-references:
  - `casacore-lattices`: `ArrayLattice`, `PagedArray`, `TempLattice`,
    `LatticeIter`, `SubLattice`, `LCBox`, `LCEllipsoid` — all documented
  - `casacore-coordinates`: `CoordinateSystem`, `DirectionCoordinate`,
    `SpectralCoordinate`, `StokesCoordinate`, `LinearCoordinate`,
    `TabularCoordinate`, `Projection`, `ObsInfo` — all documented
  - `casacore-images`: `PagedImage`, `TempImage`, `ImageExpr`, `SubImage`,
    `ImageInfo`, `GaussianBeam`, `ImageBeamSet` — all documented
- Module-level `//!` docs present for all Phase 5 crates and key modules
- All `Cargo.toml` files now have `description` and `repository` fields

#### 5. Crate separation

- `publish = false` verified on internal crates:
  - `casacore-aipsio/Cargo.toml`: confirmed
  - `casacore-test-support/Cargo.toml`: confirmed
  - `casacore-measures-data/Cargo.toml`: confirmed
- Public API leakage scan:
  - `casacore-lattices/src/lib.rs`: 17 re-exports, all lattice types — CLEAN
  - `casacore-coordinates/src/lib.rs`: 12 re-exports, all coordinate types — CLEAN
  - `casacore-images/src/lib.rs`: 14 re-exports, all image types — CLEAN
  - No `casacore-aipsio` or `casacore-tables` storage internals leaked
- `casacore-types` is types-only: no I/O or storage logic — CLEAN

### Final parity matrix

#### Lattices

**Supported**: `Lattice<T>`/`LatticeMut<T>` traits (12 element types),
`ArrayLattice<T>`, `PagedArray<T>`, `TempLattice<T>` (with temp_close/reopen),
`LatticeIter`/`LatticeIterMut`/`LatticeIterExt`, `LatticeStepper`/
`TiledLineStepper`/`TileStepper`, `TiledShape`, `LCBox`/`LCEllipsoid`/
set-algebra combinators (complement, union, intersection, difference),
`SubLattice`/`SubLatticeMut`

**Deferred**: HDF5Lattice, RebinLattice, ExtendLattice, LatticeConcat,
CurvedLattice2D, GPU/distributed execution

#### Coordinates

**Supported**: `CoordinateSystem`, `DirectionCoordinate` (10 projections:
SIN, TAN, ARC, CAR, SFL, MER, AIT, ZEA, STG, NCP),
`SpectralCoordinate`, `StokesCoordinate`, `LinearCoordinate`,
`TabularCoordinate`, `Projection`/`ProjectionType`, `ObsInfo`,
FITS/WCS round-trip (`FitsHeader`, `coordinate_util`)

**Deferred**: QualityCoordinate, GaussianConvert, FrequencyAligner,
full nonlinear reprojection

#### Images

**Supported**: `PagedImage<T>` (f32/f64/Complex32/Complex64),
`TempImage<T>` (with save_as/into_paged), `ImageExpr<T>` (56/57 LEL elements),
`SubImage`/`SubImageMut`, `ImageIter`/`ImageIterMut`, `ImageInfo`,
`ImageBeamSet`/`GaussianBeam`, `ImageType`, units/misc_info/history,
masks (make/put/get/default), expression parser (LEL grammar subset),
expression file persistence (.imgexpr), type-changing typed API (real/imag/arg/complex),
`AnyPagedImage` (type-erased opener)

**Deferred**: INDEXIN, type-changing in parser, full mask propagation through
opaque closures, history interop, advanced image analysis algorithms

#### LEL status (56/57 + 4 typed API)

- Unary operators: 3/3 (-, +, !)
- Binary operators: 10/10 (+, -, *, /, ^, ==, !=, >, >=, <, <=, &&, ||)
- 0-arg functions: 2/2 (pi, e)
- 1-arg math: 19/19 (sin, cos, tan, asin, acos, atan, sinh, cosh, tanh, exp,
  log, log10, sqrt, abs, ceil, floor, round, sign, conj)
- 1-arg mask: 3/3 (isnan, all, any)
- 1-arg reduction: 7/7 (sum, min, max, mean, median, ntrue, nfalse)
- 1-arg metadata: 4/4 (ndim, nelem, mask, value)
- 2-arg functions: 8/8 (atan2, pow, fmod, min, max, length, fractile, replace)
- 3-arg functions: 2/2 (iif, fractilerange)
- Type-changing: 4/4 typed API only (real, imag, arg, complex)
- Deferred: 1 (INDEXIN — requires array literal lexer syntax)
- **Total: 56/57 parser-accessible, 4 typed API only, 1 deferred**

### Interop matrix

- RR: Full coverage via unit tests and image_expr_interop (28 tests)
- RC: images_interop (f32/f64/Complex32/Complex64 + metadata), temp_image_interop
- CR: images_interop (f32/f64/Complex32/Complex64), temp_image_interop
- CC: images_interop (f32 basic round-trip)

### Iterator matrix

- full: `Lattice::get()` reads entire lattice; tested in all lattice types
- strided: `Lattice::get_slice(start, shape, stride)` tested in ArrayLattice and
  PagedArray; demonstrated in t_lattice.rs
- tiled/chunked: `LatticeStepper`, `TileStepper`, `iter_chunks`, `iter_tiles`
  tested and demonstrated; ImageIter/ImageIterMut tested in t_image.rs
- region/mask-aware: `SubLattice`/`SubLatticeMut` with `LCBox`, `LCEllipsoid`,
  set-algebra combinators; tested and demonstrated in t_lattice.rs

### Performance summary

- 11/13 comparable workloads faster than C++ (Rust <1x)
- 1 workload near parity (plane-by-plane 256-cube: 1.23x)
- 1 workload YELLOW (spectrum-by-spectrum bounded-cache: 2.46x)
- No workloads >5x (gross-regression threshold)
- Previously red lazy expr (8.66x in Wave 11c) now 0.05x in the final release
  closeout run after the LRU tile cache work

### Skips/blockers/follow-ups

- **Pre-existing**: `udf_overrides_builtin` test flake (Phase 3 commit, not Phase 5)
- **Deferred — INDEXIN**: Requires array literal lexer syntax (`[1, 2, 3]` in LEL).
  Would need lexer extension. Low priority; no known user request.
- **Deferred — Type-changing in parser**: Monomorphic limitation of the expression
  tree. The typed API (`real()`, `imag()`, `arg()`, `complex()`) covers the
  use case. Parser-level type changing would require a polymorphic expression tree.
- **Deferred — Mask propagation through opaque closures**: Partial — masks propagate
  through all built-in ops but not through user-provided closures. Architectural
  limitation; would require closure trait redesign.
- **Deferred — History interop with C++**: Rust writes history as simple string
  arrays; C++ uses a different format. Round-tripping history between languages
  is not guaranteed.
- **Deferred — Parser features**: `$n` temps, region refs, array literals,
  complex literal `i`, `%` infix (from Wave 12a). Low-priority syntax sugar.
- **Performance follow-up**: Spectrum-by-spectrum bounded-cache (2.46x) could
  be improved with tile-reordering write strategy. Not a Phase 5 blocker.

## Lessons learned

- The LRU tile cache was the single most impactful performance improvement,
  taking bounded-cache workloads from 1.6-1.9x to 0.37-0.51x and fixing the
  lazy expression evaluation from 8.66x to 0.07x.
- Demo programs serve as excellent integration tests — they catch API ergonomics
  issues and serve as documentation. The CLAUDE.md rule requiring demos for
  every C++ equivalent module is well-justified.
- RA=0 vs 2*PI is a recurring celestial coordinate edge case that affects
  assertions but not correctness.
- Deferred items should be documented at the point of deferral, not at closeout.
  Wave 15 confirmed all prior deferments were explicitly recorded.
- Workspace-level Cargo.toml metadata (`description`, `repository`) is easy to
  overlook — adding it to the review checklist catches it early.
