// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstrate `ColumnsIndex` lookup on a multi-column table (mirrors C++ `tColumnsIndex.cc`).
//!
//! Creates a table with "antenna_id" (Int32) and "scan_number" (Int32) columns,
//! builds a `ColumnsIndex` on "antenna_id", and performs exact lookups, unique
//! lookups, and range queries.
//!
//! # Usage
//!
//! ```bash
//! cargo run -p casacore-tables --example t_columns_index
//! ```

use casacore_tables::{ColumnSchema, ColumnsIndex, Table, TableSchema};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ColumnsIndex Demo (cf. casacore tColumnsIndex.cc) ===\n");

    // ── 1. Define schema ──────────────────────────────────────────────
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("antenna_id", PrimitiveType::Int32),
        ColumnSchema::scalar("scan_number", PrimitiveType::Int32),
    ])?;

    // ── 2. Populate 10 rows ──────────────────────────────────────────
    //
    // antenna_id values: 0,1,2,0,1,2,0,1,2,3
    // scan_number values: 1,1,1,2,2,2,3,3,3,1
    let data: Vec<(i32, i32)> = vec![
        (0, 1),
        (1, 1),
        (2, 1),
        (0, 2),
        (1, 2),
        (2, 2),
        (0, 3),
        (1, 3),
        (2, 3),
        (3, 1),
    ];

    let mut table = Table::with_schema(schema);
    for &(ant, scan) in &data {
        table.add_row(RecordValue::new(vec![
            RecordField::new("antenna_id", Value::Scalar(ScalarValue::Int32(ant))),
            RecordField::new("scan_number", Value::Scalar(ScalarValue::Int32(scan))),
        ]))?;
    }
    println!("Created table with {} rows\n", table.row_count());

    // ── 3. Build index on antenna_id ─────────────────────────────────
    let idx = ColumnsIndex::new(&table, &["antenna_id"])?;
    println!(
        "Index on {:?}, unique={}",
        idx.column_names(),
        idx.is_unique()
    );

    // ── 4. Exact lookup: antenna_id == 0 ─────────────────────────────
    let rows = idx.lookup(&[("antenna_id", &ScalarValue::Int32(0))]);
    println!("\nLookup antenna_id=0 => rows {:?}", rows);
    assert_eq!(rows.len(), 3, "antenna 0 appears in 3 rows");

    // ── 5. Exact lookup: antenna_id == 3 (single occurrence) ─────────
    let rows = idx.lookup(&[("antenna_id", &ScalarValue::Int32(3))]);
    println!("Lookup antenna_id=3 => rows {:?}", rows);
    assert_eq!(rows.len(), 1);

    // ── 6. Unique lookup ─────────────────────────────────────────────
    let row = idx.lookup_unique(&[("antenna_id", &ScalarValue::Int32(3))])?;
    println!("Unique lookup antenna_id=3 => row {:?}", row);
    assert_eq!(row, Some(9));

    // ── 7. Missing key ───────────────────────────────────────────────
    let rows = idx.lookup(&[("antenna_id", &ScalarValue::Int32(99))]);
    println!("Lookup antenna_id=99 => rows {:?} (empty)", rows);
    assert!(rows.is_empty());

    // ── 8. Range query: antenna_id in [1, 2] inclusive ───────────────
    let rows = idx.lookup_range(
        &[("antenna_id", &ScalarValue::Int32(1))],
        &[("antenna_id", &ScalarValue::Int32(2))],
        true,
        true,
    );
    println!("\nRange [1,2] inclusive => {} rows: {:?}", rows.len(), rows);
    assert_eq!(rows.len(), 6, "antennas 1 and 2 each appear 3 times");

    // ── 9. Range query: antenna_id > 1 (open upper bound) ────────────
    let rows = idx.lookup_range(
        &[("antenna_id", &ScalarValue::Int32(1))],
        &[],
        false, // exclusive lower
        true,
    );
    println!("Range (1, +inf) => {} rows: {:?}", rows.len(), rows);
    // antenna_id 2 (3 rows) + antenna_id 3 (1 row) = 4 rows
    assert_eq!(rows.len(), 4);

    // ── 10. Multi-column index: (antenna_id, scan_number) ────────────
    println!("\n--- Multi-column index ---\n");
    let idx2 = ColumnsIndex::new(&table, &["antenna_id", "scan_number"])?;
    println!(
        "Index on {:?}, unique={}",
        idx2.column_names(),
        idx2.is_unique()
    );

    let rows = idx2.lookup(&[
        ("antenna_id", &ScalarValue::Int32(1)),
        ("scan_number", &ScalarValue::Int32(2)),
    ]);
    println!("Lookup (antenna_id=1, scan_number=2) => rows {:?}", rows);
    assert_eq!(rows.len(), 1);

    // ── 11. Batch lookup ─────────────────────────────────────────────
    let results = idx.lookup_many(&[
        vec![("antenna_id", &ScalarValue::Int32(0))],
        vec![("antenna_id", &ScalarValue::Int32(1))],
        vec![("antenna_id", &ScalarValue::Int32(2))],
    ]);
    println!("\nBatch lookup:");
    for (ant, rows) in results.iter().enumerate() {
        println!("  antenna_id={ant} => {} rows", rows.len());
    }

    println!("\nAll assertions passed.");
    Ok(())
}
