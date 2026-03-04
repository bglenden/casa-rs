# Wave 3: Table Locking

**Why:** Multi-process access to the same table is fundamental for radio
astronomy pipelines (e.g. one process writes visibilities, another reads).

**C++ reference:** `TableLock`, `TableLockData`, `LockFile`.

## Scope

1. **Lock file protocol** — implement the casacore lock file format
   (`table.lock`). Support read locks (shared) and write locks (exclusive).

2. **Lock modes** — `PermanentLocking` (hold lock for lifetime),
   `AutoLocking` (acquire/release around each operation),
   `UserLocking` (explicit `lock()`/`unlock()` calls).

3. **Lock/unlock API** — `Table::lock(mode)`, `Table::unlock()`,
   `Table::has_lock()`.

4. **Synchronization** — on unlock/flush, write pending changes; on lock
   acquisition, re-read if another process modified the table.

5. **Timeout and retry** — configurable attempt count for lock acquisition.

## Tests

- Single-process lock/unlock round-trip.
- Two-process test: writer holds lock, reader waits, then reads after unlock.
- Lock file format matches C++ (interop both directions).
- Auto-locking mode smoke test.

## Lessons learned

1. **fcntl locks are per-process, not per-fd.** Two `Table` instances in the
   same process cannot contend for fcntl locks. Single-process tests exercise
   the sync data mechanism (reload on `modify_counter` change) but not actual
   lock contention. Real contention tests require `std::process::Command` to
   spawn child processes.

2. **`create: true` truncates the lock file.** `LockFile::create_or_open`
   with `create: true` uses `O_CREAT | O_TRUNC`, wiping existing sync data.
   `open_with_lock` must pass `create: false` so that existing lock files
   (written by another Table instance or process) are preserved. The
   `!path.exists()` guard in `create_or_open` still creates when needed.
