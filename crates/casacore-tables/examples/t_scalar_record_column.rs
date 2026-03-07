// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstrate reading/writing record-typed scalar columns (mirrors C++
//! `tScalarRecordColumn.cc`).
//!
//! Creates a table with an "id" Int32 column and a "meta" Record column.
//! Writes record values into the "meta" cells, reads them back, and verifies
//! the round-trip.
//!
//! # Usage
//!
//! ```bash
//! cargo run -p casacore-tables --example t_scalar_record_column
//! ```

use casacore_tables::{ColumnSchema, Table, TableSchema};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Scalar Record Column Demo (cf. casacore tScalarRecordColumn.cc) ===\n");

    // ── 1. Define schema with a record column ─────────────────────────
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::record("meta"),
    ])?;

    // ── 2. Create table with 4 rows ──────────────────────────────────
    let mut table = Table::with_schema(schema);
    let n_rows = 4;
    for i in 0..n_rows {
        table.add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
            // Leave meta empty for now — we will set it in step 3.
        ]))?;
    }
    println!("Created table with {} rows", table.row_count());

    // ── 3. Write record values into the "meta" column ────────────────
    let records = [
        RecordValue::new(vec![
            RecordField::new(
                "source_name",
                Value::Scalar(ScalarValue::String("3C273".into())),
            ),
            RecordField::new("dec_deg", Value::Scalar(ScalarValue::Float64(2.052))),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "source_name",
                Value::Scalar(ScalarValue::String("CygA".into())),
            ),
            RecordField::new("dec_deg", Value::Scalar(ScalarValue::Float64(40.734))),
        ]),
        RecordValue::new(vec![
            RecordField::new(
                "source_name",
                Value::Scalar(ScalarValue::String("CasA".into())),
            ),
            RecordField::new("dec_deg", Value::Scalar(ScalarValue::Float64(58.815))),
            RecordField::new("obs_band", Value::Scalar(ScalarValue::String("L".into()))),
        ]),
        // Row 3: an empty record (valid for a record column).
        RecordValue::default(),
    ];

    for (i, rec) in records.iter().enumerate() {
        table.set_record_cell(i, "meta", rec.clone())?;
    }
    println!("Wrote record values to 'meta' column\n");

    // ── 4. Read back and verify ──────────────────────────────────────
    for (i, expected) in records.iter().enumerate() {
        let actual = table.record_cell(i, "meta")?;
        println!("Row {i}: meta = {actual:?}");
        assert_eq!(&actual, expected, "record mismatch at row {i}");
    }

    // ── 5. Demonstrate reading individual fields from a record cell ──
    println!("\n--- Reading individual fields ---\n");
    let meta0 = table.record_cell(0, "meta")?;
    let source = meta0.get("source_name").expect("source_name exists");
    let dec = meta0.get("dec_deg").expect("dec_deg exists");
    println!("Row 0: source_name={source:?}, dec_deg={dec:?}");
    assert_eq!(source, &Value::Scalar(ScalarValue::String("3C273".into())),);

    // Row 2 has an extra field "obs_band".
    let meta2 = table.record_cell(2, "meta")?;
    let band = meta2.get("obs_band").expect("obs_band exists in row 2");
    println!("Row 2: obs_band={band:?}");
    assert_eq!(band, &Value::Scalar(ScalarValue::String("L".into())),);

    // Row 3 is an empty record.
    let meta3 = table.record_cell(3, "meta")?;
    println!("Row 3: meta = {meta3:?} (empty)");
    assert!(meta3.fields().is_empty(), "expected empty record in row 3");

    // ── 6. Iterate over the record column ────────────────────────────
    println!("\n--- Iterating record column ---\n");
    for cell in table.get_record_column("meta")? {
        println!(
            "  row {}: {} field(s)",
            cell.row_index,
            cell.value.fields().len(),
        );
    }

    println!("\nAll assertions passed.");
    Ok(())
}
