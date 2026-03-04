# Wave 24 - Data manager introspection and showtableinfo

## Origin

- Backlog items: 10.6, 21.1, 21.2, 22.2.

## Goal

- Add data-manager info API, structure/keyword dump methods, and
  `showtableinfo` CLI.

## Non-goals

- Other command-line tools (`showtablelock`, ascii/multifile tools).

## Scope

### Read path

- Add `data_manager_info` exposure and rich structure/keyword formatters.

### Write path

- N/A.

### API/docs/demo

- Add `showtableinfo` executable integration.

## Dependencies

- Table and storage metadata accessors.

## Ordering constraints

- Should run after Wave 18 (better metadata/keyword fidelity).
- Should run after Wave 21 (include set/index/iterator-related visibility).

## Files likely touched

- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/storage/mod.rs`
- `crates/casacore-tables/src/bin/` or `examples/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add `data_manager_info` API.
- [ ] Add `show_structure`/`show_keywords` APIs.
- [ ] Add `showtableinfo` CLI and output tests.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC).
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary).
- [ ] Clean skip when `pkg-config casacore` is unavailable.

## Performance plan

- Workload: N/A.
- Rust command: N/A.
- C++ command: N/A.
- Alert threshold: N/A.

## Closeout criteria

- [ ] All Phase 2 closeout gates pass.
- [ ] Public docs updated at C++ doxygen-comparable detail.
- [ ] Demo added/updated if user-visible workflow changed.

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
- Performance:
  - Rust:
  - C++:
  - Ratio:
- Skips/blockers/follow-ups:

## Lessons learned

-
