# Wave 14 - taql command-line executor

## Origin

- Backlog item: 22.1.

## Goal

- Provide a `taql` CLI entry point for interactive and single-command TaQL
  execution.

## Non-goals

- `showtableinfo`/other CLI tools.
- Additional TaQL language features.

## Scope

### Read path

- Read commands from stdin or command argument.
- Execute queries through existing TaQL APIs.

### Write path

- Optional output table writes only via TaQL command semantics.

### API/docs/demo

- New executable and usage docs.

## Dependencies

- Stable `execute_taql` behavior and printable result formatting.

## Files likely touched

- `crates/casacore-tables/examples/taql.rs` or `src/bin/taql.rs`
- `crates/casacore-tables/src/table.rs` (if glue needed)
- CLI tests/snapshots

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add CLI binary with stdin/arg modes.
- [ ] Add result pretty-printer.
- [ ] Add basic integration tests.

## Test plan

- [ ] Run command mode on fixture table.
- [ ] Run stdin mode on fixture table.
- [ ] Validate output and error paths.
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
