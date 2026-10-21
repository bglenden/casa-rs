// SPDX-License-Identifier: LGPL-3.0-or-later
//! A MeasurementSet creation session holds what its plan says it does, and
//! no more as the MeasurementSet grows (ADR-0008).
//!
//! The producer fills column-shaped visibility batches
//! ([`MeasurementSetWriteSession::visibility_columns`]) and pushes the MAIN
//! scalars a batch at a time, as a simulator does. While it streams, the
//! session's heap beyond what was live before it stays within the plan's
//! writer-owned bytes ([`MeasurementSetWritePlan::maximum_resident_bytes`])
//! and the producer's own batch. Finalizing the scalar columns and saving
//! MAIN do not grow with the row count either. A plan of scalar columns
//! alone, at the smallest budget it admits, holds its plan through the
//! save that encodes them; a smaller budget is refused.
//!
//! This binary's global allocator counts the live heap; the counters are
//! the process's, so each law runs alone in a process of its own
//! ([`alone`]).

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

/// The system allocator, counting live bytes and their largest value.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: forwards to `System` with the caller's arguments and adds atomic
// bookkeeping only.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`.
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (old, new) = (layout.size() as u64, new_size as u64);
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if moved != pointer {
                // Old and new were held at once while the block moved.
                PEAK.fetch_max(LIVE.load(Ordering::Relaxed) + new, Ordering::Relaxed);
            }
            if new >= old {
                grow(new - old);
            } else {
                LIVE.fetch_sub(old - new, Ordering::Relaxed);
            }
        }
        moved
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
    /// The peak while the session finalized the scalars and saved MAIN.
    finishing: u64,
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
    let mut session = MeasurementSetWriteSession::start(&staging, plan).expect("session");
    let mut first = 0;
    while first < rows {
        let batch_range = first..rows.min(first + batch_rows);
        let mut batch = session.visibility_columns();
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
    let streaming = PEAK.load(Ordering::Relaxed) - baseline;

    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let mut overrides = ColumnOverrides::for_row_count(rows);
    for column in ["DATA", "FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"] {
        overrides.insert_deferred(column);
    }
    session
        .save_and_finish(&mut measurement_set, &overrides)
        .expect("finish MeasurementSet");
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
        finishing,
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
        "planned {} producer batch {} streaming peak {} finishing peak {}",
        peaks.planned, peaks.producer_batch, peaks.streaming, peaks.finishing
    );
    for (phase, peak) in [
        ("streaming", peaks.streaming),
        ("finishing", peaks.finishing),
    ] {
        assert!(
            peak <= peaks.planned + peaks.producer_batch + SLACK_BYTES,
            "{phase} held {peak} bytes; plan {} and producer batch {}",
            peaks.planned,
            peaks.producer_batch
        );
    }
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
        "40,000 rows: streaming {} finishing {}; 160,000 rows: streaming {} finishing {}",
        small.streaming, small.finishing, large.streaming, large.finishing
    );
    assert!(
        large.streaming <= small.streaming + SLACK_BYTES,
        "streaming grew from {} to {} bytes",
        small.streaming,
        large.streaming
    );
    assert!(
        large.finishing <= small.finishing + SLACK_BYTES,
        "finishing grew from {} to {} bytes",
        small.finishing,
        large.finishing
    );
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
    let streaming = PEAK.load(Ordering::Relaxed) - baseline;

    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    session
        .save_and_finish(&mut measurement_set, &overrides)
        .expect("finish MeasurementSet");
    let finishing = PEAK.load(Ordering::Relaxed) - baseline;
    Peaks {
        planned,
        producer_batch: 0,
        streaming,
        finishing,
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
        "planned {} streaming peak {} finishing peak {}",
        peaks.planned, peaks.streaming, peaks.finishing
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
