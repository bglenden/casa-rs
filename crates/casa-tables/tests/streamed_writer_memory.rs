// SPDX-License-Identifier: LGPL-3.0-or-later
//! The streamed column writers hold what they charge (ADR-0008).
//!
//! A variable-shape `TiledShapeStMan` writer holds at most the bytes it
//! reports ([`StreamedTiledShapeColumn::max_resident_bytes`]), however many
//! shapes its rows visit. A table save that encodes streamed scalar columns
//! holds at most the bytes planners reserve for them
//! ([`streamed_scalar_columns_resident_bytes`]), in StandardStMan and
//! IncrementalStMan, at a block boundary and across several blocks. The
//! StandardStMan column writer, which writes groups holding strings, holds
//! the same heap whatever the row count, and a string longer than a string
//! bucket costs it a bucket or two, not the buckets it spans.
//!
//! This binary's global allocator counts the live heap; each law compares a
//! write's peak beyond the heap live before it with the writer's charge. The
//! counters are the process's, so each law runs alone in a process of its
//! own ([`alone`]).

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_tables::{
    ColumnBinding, ColumnOverrides, ColumnSchema, DataManagerKind, EndianFormat,
    GeneratedScalarColumn, StreamedScalarType, StreamedTiledShapeColumn,
    StreamedTiledShapeCubeLayout, StreamedTiledShapeValueType, StreamingScalarColumnWriter,
    StreamingTiledShapeWriter, Table, TableOptions, TableSchema,
    streamed_scalar_columns_resident_bytes,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};
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
/// law alone and assert that it passed (the helper of
/// `casa-ms/tests/visibility_write_memory.rs`).
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

/// Small allocations a shape writer does not charge: its cube files'
/// paths, the shape-to-cube map, each cube's tile plans and the row maps'
/// spare capacity.
const SHAPE_SLACK_BYTES: u64 = 4 * 1024;

/// One variable-shape write: the shapes the column declares (cell shape,
/// rows, tile shape), the shape of each row in order, and the budget.
struct ShapeCase {
    name: &'static str,
    value_type: StreamedTiledShapeValueType,
    layouts: Vec<(Vec<usize>, usize, Vec<usize>)>,
    order: Vec<usize>,
    budget: usize,
}

/// The heap a shape writer held beyond what was live before it, from its
/// creation to its finished column, and the column it wrote.
fn write_shapes(dir: &Path, case: &ShapeCase) -> (u64, StreamedTiledShapeColumn) {
    let path = dir.join(case.name);
    let layouts = case
        .layouts
        .iter()
        .map(|(cell, rows, tile)| {
            StreamedTiledShapeCubeLayout::new(cell.clone(), *rows, tile.clone())
        })
        .collect::<Vec<_>>();
    // The producer's cells, made before the measurement.
    let complex = case
        .layouts
        .iter()
        .map(|(cell, _, _)| vec![Complex32::new(1.5, -0.5); cell.iter().product()])
        .collect::<Vec<_>>();
    let flags = case
        .layouts
        .iter()
        .map(|(cell, _, _)| {
            (0..cell.iter().product::<usize>())
                .map(|sample| sample % 3 == 0)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let mut writer =
        StreamingTiledShapeWriter::create(path, case.value_type, layouts, case.budget, false)
            .expect("create writer");
    for &shape in &case.order {
        let cell = &case.layouts[shape].0;
        match case.value_type {
            StreamedTiledShapeValueType::Bool => writer.push_bool_row(cell, &flags[shape]),
            StreamedTiledShapeValueType::Complex32 => {
                writer.push_complex32_row(cell, &complex[shape])
            }
            other => panic!("no {other:?} case"),
        }
        .expect("push row");
    }
    let column = writer.finish().expect("finish");
    (PEAK.load(Ordering::Relaxed) - baseline, column)
}

#[test]
fn a_shape_writer_holds_what_it_reports_across_shapes() {
    if !alone("a_shape_writer_holds_what_it_reports_across_shapes") {
        return;
    }
    let cases = [
        // The review's case: three shapes of one tile row each, two tiles
        // across every cell, and a budget below their bands.
        ShapeCase {
            name: "complex-three-shapes",
            value_type: StreamedTiledShapeValueType::Complex32,
            layouts: vec![
                (vec![1, 1025], 1, vec![1, 1024, 1]),
                (vec![1, 1026], 1, vec![1, 1024, 1]),
                (vec![1, 1027], 1, vec![1, 1024, 1]),
            ],
            order: vec![0, 1, 2],
            budget: 17_000,
        },
        // Shapes whose largest row and largest tile row differ, rows
        // interleaved, tile rows written in place.
        ShapeCase {
            name: "complex-interleaved-in-place",
            value_type: StreamedTiledShapeValueType::Complex32,
            layouts: vec![
                (vec![2, 900], 6, vec![1, 128, 2]),
                (vec![1, 300], 5, vec![1, 300, 3]),
                (vec![4, 64], 4, vec![4, 64, 4]),
            ],
            order: vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 0],
            budget: 24_000,
        },
        ShapeCase {
            name: "bool-interleaved-in-place",
            value_type: StreamedTiledShapeValueType::Bool,
            layouts: vec![
                (vec![2, 700], 5, vec![2, 512, 2]),
                (vec![2, 300], 4, vec![1, 300, 4]),
            ],
            order: vec![0, 1, 0, 1, 0, 1, 0, 1, 0],
            budget: 3_000,
        },
        // A budget that assembles every band.
        ShapeCase {
            name: "complex-interleaved-banded",
            value_type: StreamedTiledShapeValueType::Complex32,
            layouts: vec![
                (vec![2, 900], 6, vec![1, 128, 2]),
                (vec![1, 300], 5, vec![1, 300, 3]),
                (vec![4, 64], 4, vec![4, 64, 4]),
            ],
            order: vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 0],
            budget: 1 << 20,
        },
    ];
    let root = tempfile::tempdir().expect("tempdir");
    for case in &cases {
        let (peak, column) = write_shapes(root.path(), case);
        let reported = column.max_resident_bytes() as u64;
        println!(
            "{}: peak {peak} reported {reported} budget {}",
            case.name, case.budget
        );
        assert!(
            reported <= case.budget as u64,
            "{}: reported {reported} bytes over the budget {}",
            case.name,
            case.budget
        );
        assert!(
            peak <= reported + SHAPE_SLACK_BYTES,
            "{}: held {peak} bytes; reported {reported}",
            case.name
        );
    }
}

/// Table-save allocations a streamed scalar column's charge does not cover:
/// the table and column descriptions, the data-manager blobs and the
/// storage-manager indexes.
const SAVE_SLACK_BYTES: u64 = 32 * 1024;

/// Rows of the review's case: one full encoding block.
const BLOCK_ROWS: usize = 4096;

/// Rows past three encoding blocks.
const SEVERAL_BLOCK_ROWS: usize = 3 * 4096 + 123;

/// One streamed scalar column: its name, type and storage manager.
type ScalarColumn = (&'static str, StreamedScalarType, DataManagerKind);

fn primitive(scalar_type: StreamedScalarType) -> PrimitiveType {
    match scalar_type {
        StreamedScalarType::Bool => PrimitiveType::Bool,
        StreamedScalarType::Int32 => PrimitiveType::Int32,
        StreamedScalarType::Float32 => PrimitiveType::Float32,
        StreamedScalarType::Float64 => PrimitiveType::Float64,
    }
}

fn value(scalar_type: StreamedScalarType, row: usize) -> Option<ScalarValue> {
    (row % 1000 != 999).then_some(match scalar_type {
        StreamedScalarType::Bool => ScalarValue::Bool(row.is_multiple_of(3)),
        StreamedScalarType::Int32 => ScalarValue::Int32((row % 27) as i32),
        StreamedScalarType::Float32 => ScalarValue::Float32(row as f32 * 0.5),
        StreamedScalarType::Float64 => ScalarValue::Float64(4.0e9 + row as f64),
    })
}

/// The heap a table save of `columns`, each streamed from a construction
/// source of `rows` rows, held beyond what was live before it.
fn save_streamed(path: &Path, columns: &[ScalarColumn], rows: usize) -> u64 {
    let schema = TableSchema::new(
        columns
            .iter()
            .map(|(name, scalar_type, _)| ColumnSchema::scalar(*name, primitive(*scalar_type)))
            .collect(),
    )
    .expect("schema");
    let mut table = Table::with_schema(schema);
    std::fs::create_dir_all(path).expect("table directory");
    let mut overrides = ColumnOverrides::for_row_count(rows);
    let mut bindings = HashMap::new();
    for &(name, scalar_type, manager) in columns {
        let mut writer = StreamingScalarColumnWriter::create(
            path.join(format!(".{name}.scalar.tmp")),
            rows,
            scalar_type,
        )
        .expect("construction source");
        for row in 0..rows {
            writer.push(value(scalar_type, row)).expect("push");
        }
        overrides.insert_streamed_scalar(name, writer.finish().expect("finish"));
        bindings.insert(
            name.to_string(),
            ColumnBinding {
                data_manager: manager,
                tile_shape: None,
            },
        );
    }
    let options = TableOptions::new(path).with_endian_format(EndianFormat::LittleEndian);

    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    table
        .prepare_write()
        .save_with_column_overrides(options, &bindings, &overrides)
        .expect("save");
    PEAK.load(Ordering::Relaxed) - baseline
}

#[test]
fn a_streamed_scalar_save_holds_what_planners_reserve() {
    if !alone("a_streamed_scalar_save_holds_what_planners_reserve") {
        return;
    }
    use DataManagerKind::{IncrementalStMan as Ism, StandardStMan as Ssm};
    use StreamedScalarType::{Bool, Float64, Int32};
    let main_like: &[ScalarColumn] = &[
        ("ANTENNA1", Int32, Ssm),
        ("ANTENNA2", Int32, Ssm),
        ("ARRAY_ID", Int32, Ism),
        ("DATA_DESC_ID", Int32, Ssm),
        ("EXPOSURE", Float64, Ism),
        ("FEED1", Int32, Ism),
        ("FEED2", Int32, Ism),
        ("FIELD_ID", Int32, Ism),
        ("FLAG_ROW", Bool, Ssm),
        ("INTERVAL", Float64, Ism),
        ("OBSERVATION_ID", Int32, Ism),
        ("PROCESSOR_ID", Int32, Ism),
        ("SCAN_NUMBER", Int32, Ism),
        ("STATE_ID", Int32, Ism),
        ("TIME", Float64, Ism),
        ("TIME_CENTROID", Float64, Ism),
    ];
    let cases: [(&str, &[ScalarColumn], usize); 6] = [
        // The review's case: one Float64 column of one block.
        ("float64-ism-block", &[("TIME", Float64, Ism)], BLOCK_ROWS),
        ("float64-ssm-block", &[("TIME", Float64, Ssm)], BLOCK_ROWS),
        (
            "float64-ism-blocks",
            &[("TIME", Float64, Ism)],
            SEVERAL_BLOCK_ROWS,
        ),
        (
            "float64-ssm-blocks",
            &[("TIME", Float64, Ssm)],
            SEVERAL_BLOCK_ROWS,
        ),
        // A Bool bucket holds 262,144 rows, many blocks.
        ("bool-ssm-bucket", &[("FLAG_ROW", Bool, Ssm)], 300_000),
        ("main-like", main_like, SEVERAL_BLOCK_ROWS),
    ];
    let root = tempfile::tempdir().expect("tempdir");
    for (name, columns, rows) in cases {
        let peak = save_streamed(&root.path().join(name), columns, rows);
        let reserved =
            streamed_scalar_columns_resident_bytes(columns.len()).expect("reservation") as u64;
        println!("{name}: {rows} rows, save peak {peak}, reserved {reserved}");
        assert!(
            peak <= reserved + SAVE_SLACK_BYTES,
            "{name}: the save held {peak} bytes; {} columns reserve {reserved}",
            columns.len()
        );
    }
}

/// Rows of the smaller column-writer save: about 50 data buckets of 2,048
/// rows and 50 string buckets.
const COLUMN_WRITER_ROWS: usize = 100_000;

/// How many times the smaller save's rows the larger save writes: about
/// 2,400 data buckets and 2,600 string buckets, enough that keeping 24
/// bytes for each data bucket's index entry, or for each string bucket,
/// lifts the peak, which the column phase's buckets otherwise set, by more
/// than [`COLUMN_WRITER_GROWTH_BYTES`].
const COLUMN_WRITER_SCALE: usize = 50;

/// What the larger column-writer save may hold beyond the smaller.
const COLUMN_WRITER_GROWTH_BYTES: u64 = 8 * 1024;

/// The heap a StandardStMan save of `rows` rows of a generated Int32 column
/// and a generated column of strings too long to inline held beyond what was
/// live before it. The strings go to string buckets, so the group is
/// written by the column writer.
fn save_scalar_and_long_strings(path: &Path, rows: usize) -> u64 {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ID", PrimitiveType::Int32),
        ColumnSchema::scalar("NAME", PrimitiveType::String),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    let mut overrides = ColumnOverrides::for_row_count(rows);
    overrides.insert_generated_scalar(
        "ID",
        GeneratedScalarColumn::new(rows, |row| Some(ScalarValue::Int32(row as i32))),
    );
    overrides.insert_generated_scalar(
        "NAME",
        GeneratedScalarColumn::new(rows, |row| {
            Some(ScalarValue::String(format!("long name {row:07}")))
        }),
    );
    let options = TableOptions::new(path)
        .with_data_manager(DataManagerKind::StandardStMan)
        .with_endian_format(EndianFormat::LittleEndian);

    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    table
        .prepare_write()
        .save_with_column_overrides(options, &HashMap::new(), &overrides)
        .expect("save");
    PEAK.load(Ordering::Relaxed) - baseline
}

#[test]
fn the_ssm_column_writer_holds_the_same_heap_whatever_the_rows() {
    if !alone("the_ssm_column_writer_holds_the_same_heap_whatever_the_rows") {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    // A first save settles what the process allocates once.
    save_scalar_and_long_strings(&root.path().join("settle"), 1000);
    let rows = COLUMN_WRITER_ROWS;
    let small = save_scalar_and_long_strings(&root.path().join("small"), rows);
    let large_rows = rows * COLUMN_WRITER_SCALE;
    let large = save_scalar_and_long_strings(&root.path().join("large"), large_rows);
    println!("column writer: {rows} rows held {small} bytes, {large_rows} rows held {large} bytes");
    assert!(
        large <= small + COLUMN_WRITER_GROWTH_BYTES,
        "{large_rows} rows held {large} bytes, {rows} rows {small}: \
         the writer holds more as rows grow"
    );
}

/// Bytes of a long cell: a string, or a string array's elements together,
/// spanning 129 string buckets of 32 KiB.
const LONG_CELL_BYTES: usize = 4 << 20;

/// Elements of the string array cell.
const ARRAY_ELEMENTS: usize = 64;

/// Bytes of a short cell's strings, beyond inlining but within a bucket.
const SHORT_BYTES: usize = 100;

/// The string buckets the column writer holds while it writes a cell, the
/// open one and the one it is finishing, with room to spare (32 KiB
/// each, StandardStMan's bucket for a table of a few rows).
const CELL_BUCKETS_BYTES: u64 = 4 * 32 * 1024;

/// Other allocations a long cell may cost a save: its array's element
/// references.
const LONG_CELL_SLACK_BYTES: u64 = 8 * 1024;

/// The heap a StandardStMan save of one row of the string `name` and the
/// string array `list` held beyond what was live before it, which holds the
/// row. Both cells are too long to inline, so the column writer stores them
/// in string buckets.
fn save_strings(path: &Path, name: usize, list: usize) -> u64 {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("NAME", PrimitiveType::String),
        ColumnSchema::array_variable("LIST", PrimitiveType::String, Some(1)),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    let element = "e".repeat(list / ARRAY_ELEMENTS);
    let elements = ArrayD::from_shape_vec(IxDyn(&[ARRAY_ELEMENTS]), vec![element; ARRAY_ELEMENTS])
        .expect("shape");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("NAME", Value::Scalar(ScalarValue::String("n".repeat(name)))),
            RecordField::new("LIST", Value::Array(ArrayValue::String(elements))),
        ]))
        .expect("row");
    let options = TableOptions::new(path)
        .with_data_manager(DataManagerKind::StandardStMan)
        .with_endian_format(EndianFormat::LittleEndian);

    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    table.save(options).expect("save");
    PEAK.load(Ordering::Relaxed) - baseline
}

#[test]
fn the_ssm_column_writer_holds_a_string_bucket_or_two_whatever_the_cell() {
    if !alone("the_ssm_column_writer_holds_a_string_bucket_or_two_whatever_the_cell") {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    // A first save settles what the process allocates once.
    save_strings(&root.path().join("settle"), SHORT_BYTES, SHORT_BYTES);
    let short = save_strings(&root.path().join("short"), SHORT_BYTES, SHORT_BYTES);
    let long_name = save_strings(&root.path().join("name"), LONG_CELL_BYTES, SHORT_BYTES);
    let long_list = save_strings(&root.path().join("list"), SHORT_BYTES, LONG_CELL_BYTES);
    println!(
        "string writer: short cells held {short} bytes, a {LONG_CELL_BYTES}-byte string \
         {long_name}, a {LONG_CELL_BYTES}-byte string array {long_list}"
    );
    assert!(
        long_name <= short + CELL_BUCKETS_BYTES + LONG_CELL_SLACK_BYTES,
        "a {LONG_CELL_BYTES}-byte string held {long_name} bytes, short cells {short}: \
         the writer holds the buckets a string spans"
    );
    // A string array is encoded whole, its elements' lengths and bytes,
    // before it is stored.
    let encoding = (LONG_CELL_BYTES + 4 * ARRAY_ELEMENTS) as u64;
    assert!(
        long_list <= short + encoding + CELL_BUCKETS_BYTES + LONG_CELL_SLACK_BYTES,
        "a {LONG_CELL_BYTES}-byte string array held {long_list} bytes, short cells {short}: \
         the writer holds more than the array's encoding and a bucket or two"
    );
}
