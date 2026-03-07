# Wave 13 - Real TempImage and images overview API cleanup

## Origin

- Follow-up parity correction after Wave 10 review.
- Depends on the Wave 11c/12b public-contract freeze.

## Goal

- Replace the alias-style `TempImage<T>` with a real temporary image type and
  align the `casacore-images` overview/docs/examples with the actual API.

## Non-goals

- `ImageExpr` parser redesign.
- History interoperability checks.
- New image-processing algorithms.

## Scope

### Read path

- Support reading from real temporary images with the same typed image
  semantics as `PagedImage<T>`.

### Write path

- Implement temporary storage/backing behavior, cleanup, and explicit
  `temp_close()` / `reopen()` semantics.

### API/docs/demo

- Rewrite the images overview to describe generic image types, lazy
  `ImageExpr`, real `TempImage`, and current parity boundaries.
- Add/update examples that mirror supported C++ temporary-image workflows.

## Dependencies

- Wave 11c completed.
- Wave 12b completed.

## Ordering constraints

- Must run after Waves 11c and 12b.
- Should land before Wave 15 final module-wide closeout.

## Files likely touched

- `crates/casacore-images/src/image.rs`
- `crates/casacore-images/src/lib.rs`
- `crates/casacore-images/examples/`
- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Replace alias-style `TempImage<T>` with a real temporary image type.
- [ ] Implement temporary backing policy, cleanup semantics, and
      `temp_close()` / `reopen()` behavior.
- [ ] Update overview docs/examples and add C++ parity checks for exposed temp
      image behavior.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.
- [ ] Rust/C++ cross-checks for temporary image data, coordinates, units,
      masks, and `ImageInfo`, excluding history.

## Performance plan

- Workload: temporary-image create/write/read/reopen lifecycle.
- Rust command: release benchmark for temp-image lifecycle operations.
- C++ command: matching temporary-image lifecycle benchmark.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 5 closeout gates pass.
- [ ] Public docs updated at C++ doxygen-comparable detail.
- [ ] Demo added/updated if user-visible workflow changed.
- [ ] Overview docs no longer describe `TempImage` as an alias or omission.

## Results

- Date:
- Commit:
- Commands:
  - `` -> PASS/FAIL
- Interop matrix:
  - RR:
  - RC:
  - CR:
  - CC:
- Iterator matrix:
  - full:
  - strided:
  - tiled/chunked:
  - region/mask-aware:
- Performance:
  - Rust:
  - C++:
  - Ratio:
- Skips/blockers/follow-ups:

## Lessons learned

- 
