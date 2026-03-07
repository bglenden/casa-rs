// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstrate row iteration grouped by a column value (mirrors C++ `tTableIter.cc`).
//!
//! Creates a table with a "category" String column and a "value" Float64
//! column, populates 9 rows spread across 3 categories, then uses
//! `Table::iter_groups` to walk the groups and print their contents.
//!
//! # Usage
//!
//! ```bash
//! cargo run -p casacore-tables --example t_table_iter
//! ```

use casacore_tables::{ColumnSchema, SortOrder, Table, TableSchema};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Table Iterator Demo (cf. casacore tTableIter.cc) ===\n");

    // ── 1. Define schema ──────────────────────────────────────────────
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("category", PrimitiveType::String),
        ColumnSchema::scalar("value", PrimitiveType::Float64),
    ])?;

    // ── 2. Populate 9 rows across 3 categories ───────────────────────
    let data: Vec<(&str, f64)> = vec![
        ("star", 1.0),
        ("galaxy", 2.0),
        ("star", 3.0),
        ("nebula", 4.0),
        ("galaxy", 5.0),
        ("nebula", 6.0),
        ("star", 7.0),
        ("galaxy", 8.0),
        ("nebula", 9.0),
    ];

    let mut table = Table::with_schema(schema);
    for &(cat, val) in &data {
        table.add_row(RecordValue::new(vec![
            RecordField::new("category", Value::Scalar(ScalarValue::String(cat.into()))),
            RecordField::new("value", Value::Scalar(ScalarValue::Float64(val))),
        ]))?;
    }
    println!("Created table with {} rows\n", table.row_count());

    // ── 3. Group by "category" (ascending sort) ──────────────────────
    println!("--- Groups (sorted ascending by category) ---\n");
    let mut group_count = 0;
    for group in table.iter_groups(&[("category", SortOrder::Ascending)])? {
        group_count += 1;
        let key_val = group.keys.get("category").expect("category key present");
        println!(
            "Group: category={:?}  ({} rows, indices={:?})",
            key_val,
            group.row_indices.len(),
            group.row_indices,
        );

        // Print each row in this group.
        for &row_idx in &group.row_indices {
            let cat = table.cell(row_idx, "category").unwrap();
            let val = table.cell(row_idx, "value").unwrap();
            println!("  row {row_idx}: category={cat:?}, value={val:?}");
        }
        println!();
    }

    assert_eq!(group_count, 3, "expected 3 groups");
    println!("Total groups: {group_count}");

    // ── 4. Group without sorting (natural insertion order) ───────────
    println!("\n--- Groups (no-sort, insertion order) ---\n");
    let mut nosort_count = 0;
    for group in table.iter_groups_nosort(&["category"])? {
        nosort_count += 1;
        let key_val = group.keys.get("category").expect("category key present");
        println!(
            "Group: category={:?}  ({} rows, indices={:?})",
            key_val,
            group.row_indices.len(),
            group.row_indices,
        );
    }
    println!("\nTotal no-sort groups: {nosort_count}");
    // Without sorting, non-adjacent duplicates form separate groups.
    assert!(nosort_count >= 3, "expected at least 3 no-sort groups");

    println!("\nAll assertions passed.");
    Ok(())
}
