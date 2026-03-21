// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstrate `Table::deep_copy()` with schema + data (mirrors C++ `tTableCopy.cc`).
//!
//! Creates a table with three columns (Int32, Float64, String), saves it to a
//! temporary directory, deep-copies it to a second path, then reopens the copy
//! and verifies that all rows and cell values match the original.
//!
//! # Usage
//!
//! ```bash
//! cargo run -p casacore-tables --example t_table_copy
//! ```

use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Table Deep Copy Demo (cf. casacore tTableCopy.cc) ===\n");

    // ── 1. Define schema ──────────────────────────────────────────────
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])?;

    // ── 2. Build table with 3 rows ────────────────────────────────────
    let mut table = Table::with_schema(schema);
    let rows: Vec<(i32, f64, &str)> =
        vec![(1, 3.15, "alpha"), (2, 2.72, "beta"), (3, 1.41, "gamma")];
    for &(id, flux, name) in &rows {
        table.add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
            RecordField::new("flux", Value::Scalar(ScalarValue::Float64(flux))),
            RecordField::new("name", Value::Scalar(ScalarValue::String(name.into()))),
        ]))?;
    }
    println!("Created table with {} rows", table.row_count());

    // ── 3. Save to temp dir ───────────────────────────────────────────
    let tmp = tempfile::tempdir()?;
    let original_path = tmp.path().join("original");
    table
        .save(TableOptions::new(&original_path).with_data_manager(DataManagerKind::StManAipsIO))?;
    println!("Saved original to {}", original_path.display());

    // ── 4. Deep copy to a second path ─────────────────────────────────
    let copy_path = tmp.path().join("copy");
    table
        .deep_copy(TableOptions::new(&copy_path).with_data_manager(DataManagerKind::StManAipsIO))?;
    println!("Deep-copied to {}", copy_path.display());

    // ── 5. Open the copy and verify contents ──────────────────────────
    let copy = Table::open(TableOptions::new(&copy_path))?;
    assert_eq!(
        copy.row_count(),
        table.row_count(),
        "row count mismatch after deep copy"
    );
    println!("\nVerifying {} rows in the copy...", copy.row_count());

    for (i, &(id, flux, name)) in rows.iter().enumerate() {
        let cell_id = copy.cell(i, "id")?.expect("id cell exists");
        let cell_flux = copy.cell(i, "flux")?.expect("flux cell exists");
        let cell_name = copy.cell(i, "name")?.expect("name cell exists");

        assert_eq!(
            cell_id,
            &Value::Scalar(ScalarValue::Int32(id)),
            "id mismatch at row {i}"
        );
        assert_eq!(
            cell_flux,
            &Value::Scalar(ScalarValue::Float64(flux)),
            "flux mismatch at row {i}"
        );
        assert_eq!(
            cell_name,
            &Value::Scalar(ScalarValue::String(name.into())),
            "name mismatch at row {i}"
        );

        println!("  row {i}: id={id}, flux={flux:.2}, name={name:?} -- OK");
    }

    // ── 6. Deep copy with a different storage manager ─────────────────
    let copy2_path = tmp.path().join("copy_ssm");
    table.deep_copy(
        TableOptions::new(&copy2_path).with_data_manager(DataManagerKind::StandardStMan),
    )?;
    let copy2 = Table::open(TableOptions::new(&copy2_path))?;
    assert_eq!(copy2.row_count(), 3);
    println!(
        "\nDeep copy with StandardStMan: {} rows -- OK",
        copy2.row_count()
    );

    println!("\nAll assertions passed.");
    Ok(())
}
