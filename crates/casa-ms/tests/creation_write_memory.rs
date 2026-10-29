// SPDX-License-Identifier: LGPL-3.0-or-later
//! A MeasurementSet creation session holds what its plan says it does, and
//! no more as the MeasurementSet grows (ADR-0008).
//!
//! The producer fills column-shaped visibility batches
//! ([`MeasurementSetWriteSession::visibility_columns`]) and pushes the MAIN
//! scalars a batch at a time, as a simulator does. While it streams, the
//! session's heap beyond what was live before it stays within the plan's
//! writer-owned bytes ([`MeasurementSetWritePlan::maximum_resident_bytes`])
//! and the producer's own batch, and so do finalizing the scalar columns
//! and saving MAIN. For four times the rows the plan is the same, the
//! session stays within it, and what streaming and finishing allocate apart
//! from the visibility batches does not grow. The batches are counted
//! apart because how many are live is a scheduling matter: two or three
//! while the producer streams, as the writer falls behind it, and one or
//! two still being written when the producer's last is sent, which the
//! writer frees while MAIN saves. A plan of scalar columns alone, at the
//! smallest budget it admits, holds its plan through the save that encodes
//! them; a smaller budget is refused.
//!
//! This binary's global allocator counts the live heap, and apart from it
//! the blocks allocated in a phase; the counters are the process's, so
//! each law runs alone in a process of its own ([`alone`]).

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_ms::{
    MeasurementSet, MeasurementSetBuilder, MeasurementSetCreateTarget,
    MeasurementSetScalarColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, OptionalMainColumn, standard_main_scalar_column_plans,
};
use casa_tables::{
    ColumnOverrides, StreamedScalarType, StreamedScalarValues,
    streamed_scalar_columns_resident_bytes,
};
use casa_types::ScalarValue;
use num_complex::Complex32;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their largest value, and
/// apart from them the bytes of blocks allocated in an open phase
/// ([`open_phase`]). Each block is preceded by a header that holds the
/// phase it was allocated in, so a phase's frees of older blocks are not
/// netted against what the phase allocates.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

/// The open phase, which new blocks are tagged with; 0 while none is open.
static PHASE: AtomicU64 = AtomicU64::new(0);
/// Live bytes of blocks allocated in the open phase, and their largest
/// value.
static PHASE_LIVE: AtomicU64 = AtomicU64::new(0);
static PHASE_PEAK: AtomicU64 = AtomicU64::new(0);

/// Count, until [`close_phase`], the blocks allocated from now on.
fn open_phase() {
    static OPENED: AtomicU64 = AtomicU64::new(0);
    PHASE_LIVE.store(0, Ordering::Relaxed);
    PHASE_PEAK.store(0, Ordering::Relaxed);
    // Released after the counts are zeroed, so a block tagged with the new
    // phase is counted after them.
    PHASE.store(OPENED.fetch_add(1, Ordering::Relaxed) + 1, Ordering::Release);
}

/// The most bytes of the blocks allocated since [`open_phase`] that were
/// live at once.
fn close_phase() -> u64 {
    PHASE.store(0, Ordering::Release);
    PHASE_PEAK.load(Ordering::Relaxed)
}

std::thread_local! {
    /// Whether this thread's blocks are left out of the open phase
    /// ([`uncounted`]).
    static UNCOUNTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Run `work` with the blocks this thread allocates in it left out of the
/// open phase.
fn uncounted<T>(work: impl FnOnce() -> T) -> T {
    UNCOUNTED.set(true);
    let result = work();
    UNCOUNTED.set(false);
    result
}

/// The phase a block allocated now is tagged with: the open one, or 0
/// while none is or this thread's blocks are [`uncounted`].
fn counted_phase() -> u64 {
    if UNCOUNTED.try_with(std::cell::Cell::get).unwrap_or(false) {
        return 0;
    }
    PHASE.load(Ordering::Acquire)
}

/// Count a new block of `bytes` tagged `phase`.
fn grow(bytes: u64, phase: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
    if phase != 0 {
        let live = PHASE_LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
        PHASE_PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

/// Count the free of a block of `bytes` tagged `phase`.
fn shrink(bytes: u64, phase: u64) {
    LIVE.fetch_sub(bytes, Ordering::Relaxed);
    if phase != 0 && phase == PHASE.load(Ordering::Acquire) {
        PHASE_LIVE.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// The header's bytes before a block of `layout`: the phase tag, padded to
/// keep the caller's bytes aligned.
fn header(layout: Layout) -> usize {
    layout.align().max(16)
}

/// What the system allocates for `size` bytes of `layout` behind a header.
fn widened(layout: Layout, size: usize) -> Option<Layout> {
    let header = header(layout);
    Layout::from_size_align(size.checked_add(header)?, header).ok()
}

/// Write `phase` into the header of the block at `base`, and return the
/// caller's bytes.
///
/// # Safety
///
/// `base` starts a block of at least `header` bytes aligned to `header`,
/// which is at least 16.
unsafe fn tag(base: *mut u8, header: usize, phase: u64) -> *mut u8 {
    // SAFETY: the caller's bytes start `header` bytes in, and the tag is
    // the aligned word before them.
    unsafe {
        let pointer = base.add(header);
        pointer.cast::<u64>().sub(1).write(phase);
        pointer
    }
}

/// The start of the block whose caller's bytes are at `pointer`, and the
/// phase it was tagged with.
///
/// # Safety
///
/// `pointer` came from this allocator for `layout`.
unsafe fn untag(pointer: *mut u8, layout: Layout) -> (*mut u8, u64) {
    // SAFETY: `tag` wrote the word before the caller's bytes, `header`
    // bytes into the block.
    unsafe {
        (
            pointer.sub(header(layout)),
            pointer.cast::<u64>().sub(1).read(),
        )
    }
}

impl CountingHeap {
    /// Allocate `layout` behind a header through `allocate`, and tag and
    /// count it.
    ///
    /// # Safety
    ///
    /// `allocate` returns null or a block of the layout it is given.
    unsafe fn tagged(layout: Layout, allocate: impl FnOnce(Layout) -> *mut u8) -> *mut u8 {
        let Some(wide) = widened(layout, layout.size()) else {
            return std::ptr::null_mut();
        };
        let base = allocate(wide);
        if base.is_null() {
            return base;
        }
        let phase = counted_phase();
        grow(layout.size() as u64, phase);
        // SAFETY: `base` is a block of `wide`, which holds the header before
        // the caller's bytes, at its alignment.
        unsafe { tag(base, header(layout), phase) }
    }
}

// SAFETY: forwards to `System` with the caller's layout widened by a
// header before the caller's bytes, aligned as the caller asked, and adds
// atomic bookkeeping only.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`, on `wide`, which is
        // larger; `System` returns null or a block of it.
        unsafe { Self::tagged(layout, |wide| System.alloc(wide)) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`, on `wide`, which
        // is larger; `System` returns null or a block of it.
        unsafe { Self::tagged(layout, |wide| System.alloc_zeroed(wide)) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`: `pointer` came from
        // `alloc` for `layout`, which widened it alike.
        unsafe {
            let (base, phase) = untag(pointer, layout);
            let wide = widened(layout, layout.size()).unwrap_unchecked();
            System.dealloc(base, wide);
            shrink(layout.size() as u64, phase);
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (old, new) = (layout.size() as u64, new_size as u64);
        if widened(layout, new_size).is_none() {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller's contract for `realloc`, on the block behind
        // `pointer` widened alike; the header moves with the block.
        unsafe {
            let (base, old_phase) = untag(pointer, layout);
            let wide = widened(layout, layout.size()).unwrap_unchecked();
            let moved = System.realloc(base, wide, new_size + header(layout));
            if moved.is_null() {
                return moved;
            }
            // A block reallocated in a phase is counted as the phase's,
            // whole.
            let phase = counted_phase();
            if moved != base {
                // Old and new were held at once while the block moved.
                PEAK.fetch_max(LIVE.load(Ordering::Relaxed) + new, Ordering::Relaxed);
                if phase != 0 {
                    PHASE_PEAK.fetch_max(
                        PHASE_LIVE.load(Ordering::Relaxed) + new,
                        Ordering::Relaxed,
                    );
                }
            }
            shrink(old, old_phase);
            grow(new, phase);
            tag(moved, header(layout), phase)
        }
    }
}

/// The environment variable that names the law a child process of
/// [`alone`] runs.
const LAW_VAR: &str = "CASA_RS_COUNTING_LAW";

/// The test a law's process runs before the law ([`alone`]).
const SETTLE: &str = "a_counting_law_process_settles";

/// Whether [`SETTLE`] ran in this process.
static SETTLED: AtomicBool = AtomicBool::new(false);

/// Runs first in a law's process ([`alone`]): libtest's main thread sets up
/// its wait for results while the first test it starts runs, so the law,
/// which runs second, measures none of it. The pause makes sure the main
/// thread is waiting before this test reports.
#[test]
#[ignore = "alone runs it before a law, in the law's process"]
fn a_counting_law_process_settles() {
    std::thread::sleep(std::time::Duration::from_millis(10));
    SETTLED.store(true, Ordering::Relaxed);
}

/// Whether this process runs the law `name` alone; if it does not, run the
/// law alone and assert that it passed (see `visibility_write_memory.rs`,
/// whose helper this is).
fn alone(name: &str) -> bool {
    // libtest names each test's thread after the test.
    assert_eq!(
        std::thread::current().name(),
        Some(name),
        "a law runs alone under its own name"
    );
    let ran = format!("{name} ran alone");
    if std::env::var_os(LAW_VAR).is_some_and(|law| law == name) {
        assert!(
            SETTLED.load(Ordering::Relaxed),
            "{SETTLE} runs before {name}"
        );
        println!("{ran}");
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            SETTLE,
            name,
            "--exact",
            "--include-ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(LAW_VAR, name)
        .output()
        .expect("the law's process runs");
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{name} alone: {}\n{stdout}",
        output.status
    );
    assert!(stdout.contains(&ran), "{name} did not run alone:\n{stdout}");
    false
}

const CORRELATIONS: usize = 2;
const CHANNELS: usize = 64;
const BASELINES: usize = 351;

/// Small allocations outside any batch: channel and thread bookkeeping,
/// file handles, and the save's table metadata.
const SLACK_BYTES: u64 = 256 * 1024;

/// What one creation of `rows` rows held, beyond the heap live before it.
struct Peaks {
    /// The plan's writer-owned bytes.
    planned: u64,
    /// The producer's batch at the plan's batch size.
    producer_batch: u64,
    /// The peak while the producer streamed every row.
    streaming: u64,
    /// The most bytes that streaming allocated and held at once, apart from
    /// the producer's visibility batches.
    streaming_own: u64,
    /// The peak while the session finalized the scalars and saved MAIN.
    finishing: u64,
    /// The most bytes that finalizing the scalars and saving MAIN
    /// allocated and held at once, apart from what streaming left live.
    finishing_own: u64,
}

/// Create a MeasurementSet of `rows` rows under `root`, streaming DATA,
/// FLAG and UVW in column batches and the 16 MAIN scalars a batch at a time.
fn create(root: &Path, rows: usize) -> Peaks {
    let output = root.join(format!("create-{rows}.ms"));
    let target = MeasurementSetCreateTarget::prepare(&output, false).expect("output target");
    let staging = target.staging_path().to_path_buf();
    let mut measurement_set = MeasurementSet::create(
        &staging,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create MeasurementSet");
    std::fs::create_dir_all(&staging).expect("staging directory");
    let plan = MeasurementSetWritePlan::visibility_creation(
        rows,
        CORRELATIONS,
        CHANNELS,
        "ALMA",
        MeasurementSetWriteResources {
            available_bytes: 32 * 1024 * 1024,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 256 * 1024,
        },
    )
    .expect("write plan");
    let batch_rows = plan.batch_rows();
    assert!(batch_rows * 3 < rows, "the write spans several batches");
    let planned = plan.maximum_resident_bytes() as u64;
    let producer_batch = (batch_rows
        * (CORRELATIONS * CHANNELS * size_of::<Complex32>() + 1 + 3 * size_of::<f64>()))
        as u64;
    // The producer's scalar runs, allocated once, before the measurement.
    let mut time = Vec::with_capacity(batch_rows);
    let mut antenna1 = Vec::with_capacity(batch_rows);
    let mut antenna2 = Vec::with_capacity(batch_rows);
    let mut flag_row = Vec::with_capacity(batch_rows);

    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    open_phase();
    let mut session = MeasurementSetWriteSession::start(&staging, plan).expect("session");
    let mut first = 0;
    while first < rows {
        let batch_range = first..rows.min(first + batch_rows);
        // Two or three batches are live at once, as scheduling leaves the
        // writer behind the producer; the plan bounds them, and the rest of
        // what streaming holds is counted apart from them.
        let mut batch = uncounted(|| session.visibility_columns());
        time.clear();
        antenna1.clear();
        antenna2.clear();
        flag_row.clear();
        for row in batch_range.clone() {
            let value = Complex32::new(row as f32, -(row as f32));
            batch
                .data
                .extend(std::iter::repeat_n(value, CORRELATIONS * CHANNELS));
            batch.flag_row.push(row.is_multiple_of(97));
            batch.uvw.push([row as f64, 0.5, -0.5]);
            time.push(4.0e9 + (row / BASELINES) as f64 * 10.0);
            antenna1.push((row % BASELINES / 27) as i32);
            antenna2.push((row % BASELINES % 27) as i32);
            flag_row.push(row.is_multiple_of(97));
        }
        let mut push = |column: &str, values: StreamedScalarValues<'_>| {
            let id = session.scalar_column(column).expect("scalar column");
            session.push_scalar_column(id, values).expect("push run");
        };
        push("ANTENNA1", StreamedScalarValues::Int32(&antenna1));
        push("ANTENNA2", StreamedScalarValues::Int32(&antenna2));
        push("FLAG_ROW", StreamedScalarValues::Bool(&flag_row));
        push("TIME", StreamedScalarValues::Float64(&time));
        push("TIME_CENTROID", StreamedScalarValues::Float64(&time));
        for (column, value) in [
            ("ARRAY_ID", ScalarValue::Int32(0)),
            ("DATA_DESC_ID", ScalarValue::Int32(0)),
            ("EXPOSURE", ScalarValue::Float64(10.0)),
            ("FEED1", ScalarValue::Int32(0)),
            ("FEED2", ScalarValue::Int32(0)),
            ("FIELD_ID", ScalarValue::Int32(0)),
            ("INTERVAL", ScalarValue::Float64(10.0)),
            ("OBSERVATION_ID", ScalarValue::Int32(0)),
            ("PROCESSOR_ID", ScalarValue::Int32(0)),
            ("SCAN_NUMBER", ScalarValue::Int32(1)),
            ("STATE_ID", ScalarValue::Int32(0)),
        ] {
            let id = session.scalar_column(column).expect("scalar column");
            session
                .push_repeated_scalar(id, Some(value), batch_range.len())
                .expect("push constant");
        }
        session
            .send_visibility_columns(batch)
            .expect("send visibility batch");
        first = batch_range.end;
    }
    let streaming_own = close_phase();
    let streaming = PEAK.load(Ordering::Relaxed) - baseline;

    // The writer still holds the batches it has not written yet, one or
    // two as scheduling leaves them, and the batch it recycled after the
    // producer's last; it frees them while MAIN saves, before or after the
    // save's buffers are allocated. What finishing allocates is also
    // counted apart from them.
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    open_phase();
    let mut overrides = ColumnOverrides::for_row_count(rows);
    for column in ["DATA", "FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"] {
        overrides.insert_deferred(column);
    }
    session
        .save_and_finish(&mut measurement_set, &overrides)
        .expect("finish MeasurementSet");
    let finishing_own = close_phase();
    let finishing = PEAK.load(Ordering::Relaxed) - baseline;
    drop(measurement_set);
    target.commit().expect("publish MeasurementSet");
    let reopened = MeasurementSet::open(&output).expect("reopen");
    assert_eq!(reopened.row_count(), rows);
    // FLAG_CATEGORY is left undefined, as CASA's simulator leaves it (#847).
    for row in [0, rows - 1] {
        assert!(
            !reopened
                .main_table()
                .is_cell_defined(row, "FLAG_CATEGORY")
                .expect("FLAG_CATEGORY exists"),
            "FLAG_CATEGORY row {row} is undefined"
        );
    }
    Peaks {
        planned,
        producer_batch,
        streaming,
        streaming_own,
        finishing,
        finishing_own,
    }
}

/// Assert that a creation of `rows` rows streamed and finished within its
/// plan and the producer's batch.
fn assert_within_plan(rows: usize, peaks: &Peaks) {
    for (phase, peak) in [
        ("streaming", peaks.streaming),
        ("finishing", peaks.finishing),
    ] {
        assert!(
            peak <= peaks.planned + peaks.producer_batch + SLACK_BYTES,
            "{rows} rows: {phase} held {peak} bytes; plan {} and producer batch {}",
            peaks.planned,
            peaks.producer_batch
        );
    }
}

#[test]
fn a_creation_session_streams_within_its_plan_and_one_producer_batch() {
    if !alone("a_creation_session_streams_within_its_plan_and_one_producer_batch") {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let peaks = create(root.path(), 40_000);
    println!(
        "planned {} producer batch {} streaming peak {} (its own {}) finishing peak {} (its own \
         {})",
        peaks.planned,
        peaks.producer_batch,
        peaks.streaming,
        peaks.streaming_own,
        peaks.finishing,
        peaks.finishing_own
    );
    assert_within_plan(40_000, &peaks);
}

#[test]
fn a_creation_session_holds_no_more_for_more_rows() {
    if !alone("a_creation_session_holds_no_more_for_more_rows") {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let small = create(root.path(), 40_000);
    let large = create(root.path(), 160_000);
    println!(
        "40,000 rows: streaming {} (its own {}) finishing {} (its own {}); 160,000 rows: \
         streaming {} (its own {}) finishing {} (its own {})",
        small.streaming,
        small.streaming_own,
        small.finishing,
        small.finishing_own,
        large.streaming,
        large.streaming_own,
        large.finishing,
        large.finishing_own
    );
    // The batches the plan admits are the same for more rows, and the
    // session holds no more than them.
    assert_eq!(
        (large.planned, large.producer_batch),
        (small.planned, small.producer_batch),
        "the plan and its batch do not grow with the rows"
    );
    assert_within_plan(160_000, &large);
    // Both peaks are a scheduling matter: streaming's holds two batches or
    // three as the writer falls behind the producer, and finishing's
    // starts from one batch more or less left from streaming, with the
    // save's buffers on top or after the writer frees a batch. What each
    // allocates apart from the batches is not, so that is compared.
    for (phase, small, large) in [
        ("streaming", small.streaming_own, large.streaming_own),
        ("finishing", small.finishing_own, large.finishing_own),
    ] {
        assert!(
            large <= small + SLACK_BYTES,
            "{phase}'s own allocations grew from {small} to {large} bytes"
        );
    }
}

/// Rows per batch of the scalar-only creation.
const SCALAR_BATCH_ROWS: usize = 1024;

/// Resources of one live batch.
fn one_batch(available_bytes: usize) -> MeasurementSetWriteResources {
    MeasurementSetWriteResources {
        available_bytes,
        maximum_live_batches: 1,
        tiled_column_buffer_bytes: 0,
    }
}

fn scalar_row_bytes(columns: &[MeasurementSetScalarColumnPlan]) -> usize {
    columns
        .iter()
        .map(|column| match column.value_type {
            StreamedScalarType::Bool => 2,
            StreamedScalarType::Int32 | StreamedScalarType::Float32 => 5,
            StreamedScalarType::Float64 => 9,
        })
        .sum()
}

/// Create a MeasurementSet of `rows` rows whose session streams MAIN's 16
/// scalar columns and nothing else, at the smallest budget that admits
/// [`SCALAR_BATCH_ROWS`]-row batches.
fn create_scalars(root: &Path, rows: usize) -> Peaks {
    let output = root.join(format!("scalars-{rows}.ms"));
    let target = MeasurementSetCreateTarget::prepare(&output, false).expect("output target");
    let staging = target.staging_path().to_path_buf();
    let mut measurement_set =
        MeasurementSet::create(&staging, MeasurementSetBuilder::new()).expect("create MS");
    std::fs::create_dir_all(&staging).expect("staging directory");
    let columns = standard_main_scalar_column_plans();
    let reserved = streamed_scalar_columns_resident_bytes(columns.len()).expect("reservation");
    let available = reserved + SCALAR_BATCH_ROWS * scalar_row_bytes(&columns);
    let plan = MeasurementSetWritePlan::variable_array_creation(
        rows,
        Vec::new(),
        columns.clone(),
        one_batch(available),
    )
    .expect("scalar plan");
    assert_eq!(plan.batch_rows(), SCALAR_BATCH_ROWS);
    let planned = plan.maximum_resident_bytes() as u64;
    // The producer's runs, allocated once, before the measurement.
    let mut ints = Vec::with_capacity(SCALAR_BATCH_ROWS);
    let mut doubles = Vec::with_capacity(SCALAR_BATCH_ROWS);
    let mut flags = Vec::with_capacity(SCALAR_BATCH_ROWS);
    let mut overrides = ColumnOverrides::for_row_count(rows);
    for column in ["FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"] {
        overrides.insert_deferred(column);
    }

    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    open_phase();
    let mut session = MeasurementSetWriteSession::start(&staging, plan).expect("session");
    let mut first = 0;
    while first < rows {
        let batch = first..rows.min(first + SCALAR_BATCH_ROWS);
        ints.clear();
        doubles.clear();
        flags.clear();
        for row in batch.clone() {
            ints.push((row % 351) as i32);
            doubles.push(4.0e9 + (row / 351) as f64 * 10.0);
            flags.push(row.is_multiple_of(97));
        }
        for column in &columns {
            let id = session.scalar_column(&column.name).expect("scalar column");
            let values = match column.value_type {
                StreamedScalarType::Bool => StreamedScalarValues::Bool(&flags),
                StreamedScalarType::Int32 => StreamedScalarValues::Int32(&ints),
                StreamedScalarType::Float64 => StreamedScalarValues::Float64(&doubles),
                other => panic!("no {other:?} MAIN column"),
            };
            session.push_scalar_column(id, values).expect("push run");
        }
        first = batch.end;
    }
    let streaming_own = close_phase();
    let streaming = PEAK.load(Ordering::Relaxed) - baseline;

    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    open_phase();
    session
        .save_and_finish(&mut measurement_set, &overrides)
        .expect("finish MeasurementSet");
    let finishing_own = close_phase();
    let finishing = PEAK.load(Ordering::Relaxed) - baseline;
    Peaks {
        planned,
        producer_batch: 0,
        streaming,
        streaming_own,
        finishing,
        finishing_own,
    }
}

#[test]
fn a_scalar_creation_session_saves_within_its_plan() {
    if !alone("a_scalar_creation_session_saves_within_its_plan") {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let peaks = create_scalars(root.path(), 3 * 4096 + 123);
    println!(
        "planned {} streaming peak {} (its own {}) finishing peak {} (its own {})",
        peaks.planned, peaks.streaming, peaks.streaming_own, peaks.finishing, peaks.finishing_own
    );
    for (phase, peak) in [
        ("streaming", peaks.streaming),
        ("finishing", peaks.finishing),
    ] {
        assert!(
            peak <= peaks.planned + SLACK_BYTES,
            "{phase} held {peak} bytes; plan {}",
            peaks.planned
        );
    }
}

#[test]
fn a_scalar_plan_reserves_the_save_that_encodes_it() {
    let time = vec![MeasurementSetScalarColumnPlan {
        name: "TIME".to_string(),
        value_type: StreamedScalarType::Float64,
    }];
    // The review's case: the column's append buffer and one batch fit, but
    // not the save's read buffer, records and encoded block.
    let error = MeasurementSetWritePlan::variable_array_creation(
        4096,
        Vec::new(),
        time.clone(),
        one_batch(65_545),
    )
    .expect_err("the save's scratch does not fit");
    assert!(
        error.to_string().contains("scalar column buffers require"),
        "{error}"
    );
    let reserved = streamed_scalar_columns_resident_bytes(1).expect("reservation");
    let plan = MeasurementSetWritePlan::variable_array_creation(
        4096,
        Vec::new(),
        time,
        one_batch(reserved + 9),
    )
    .expect("the reservation and one row fit");
    assert_eq!(plan.batch_rows(), 1);
    assert!(plan.maximum_resident_bytes() >= reserved);
    assert!(plan.maximum_resident_bytes() <= reserved + 9);
}
