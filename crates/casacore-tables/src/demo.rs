// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demo helpers and runnable outputs for `casacore-tables`.
//!
//! This module mirrors the C++ casacore `tTable.cc` test program. It
//! exercises the core table workflow: define a schema, build rows, persist
//! to disk with different storage managers, reopen, and verify the data.
//!
//! Each Rust section is annotated with the equivalent C++ code so that
//! users migrating from C++ casacore can see the correspondence.

use std::fmt::Write as _;
use std::path::PathBuf;

use casacore_types::{
    ArrayValue, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{Array, IxDyn};

use crate::{
    ColumnSchema, DataManagerKind, EndianFormat, RowRange, Table, TableError, TableOptions,
    TableSchema,
};

/// Run the full table demo (Rust equivalent of C++ `tTable`).
///
/// Returns deterministic text output suitable for snapshot testing.
pub fn run_ttable_like_demo() -> Result<String, TableError> {
    let mut out = String::new();
    appendln(&mut out, "=== Table Demo (cf. casacore tTable.cc) ===");
    appendln(&mut out, "");

    round_trip(&mut out, "StManAipsIO", DataManagerKind::StManAipsIO, None)?;
    appendln(&mut out, "");
    round_trip(
        &mut out,
        "StandardStMan",
        DataManagerKind::StandardStMan,
        None,
    )?;
    appendln(&mut out, "");
    // Explicit little-endian round-trip (StandardStMan respects the setting;
    // StManAipsIO always writes canonical big-endian AipsIO).
    round_trip(
        &mut out,
        "StandardStMan-LE",
        DataManagerKind::StandardStMan,
        Some(EndianFormat::LittleEndian),
    )?;
    appendln(&mut out, "");
    demo_column_iteration(&mut out)?;
    demo_schema_mutation(&mut out)?;
    demo_ref_tables(&mut out)?;
    #[cfg(unix)]
    demo_locking(&mut out)?;

    appendln(&mut out, "end");
    Ok(out)
}

// ── Schema + row construction ────────────────────────────────────────

/// Build the demo schema and 10 rows of data.
///
/// Follows the same column names and value formulas as C++ `tTable.cc`
/// function `a()` so that output is directly comparable.
fn build_demo_table() -> Result<Table, TableError> {
    // C++ (tTable.cc):
    //   TableDesc td("", "1", TableDesc::Scratch);
    //   td.comment() = "A test of class Table";
    //   td.addColumn(ScalarColumnDesc<Int>("ab", "Comment for column ab"));
    //   td.addColumn(ScalarColumnDesc<uInt>("ad", "comment for ad"));
    //   td.addColumn(ScalarColumnDesc<DComplex>("ag"));
    //   td.addColumn(ScalarColumnDesc<float>("ae"));
    //   td.addColumn(ScalarColumnDesc<String>("af"));
    //   td.addColumn(ArrayColumnDesc<float>("arr1",
    //                    IPosition(3,2,3,4), ColumnDesc::Direct));
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ab", PrimitiveType::Int32),
        ColumnSchema::scalar("ad", PrimitiveType::UInt32),
        ColumnSchema::scalar("ag", PrimitiveType::Complex64),
        ColumnSchema::scalar("ae", PrimitiveType::Float32),
        ColumnSchema::scalar("af", PrimitiveType::String),
        ColumnSchema::array_fixed("arr1", PrimitiveType::Float32, vec![2, 3, 4]),
    ])?;

    // C++ (tTable.cc):
    //   Table tab(newtabcp, 10, False, Table::LocalEndian);
    //   Cube<float> arrf(IPosition(3,2,3,4));
    //   indgen(arrf);                     // fill 0.0, 1.0, 2.0, ...
    //   for (i=0; i<10; i++) {
    //       ab1.put(i, i);
    //       ad.put(i, i+2);
    //       arr1.put(i, arrf);
    //       arrf += (float)(arrf.nelements());
    //   }
    let nelem: usize = 2 * 3 * 4; // 24 elements per array cell
    let nrow = 10;

    let mut rows = Vec::with_capacity(nrow);
    for i in 0..nrow {
        let base = (i * nelem) as f32;
        let arr_data: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
        let arr = Array::from_shape_vec(IxDyn(&[2, 3, 4]), arr_data)
            .expect("shape matches element count");

        rows.push(RecordValue::new(vec![
            RecordField::new("ab", Value::Scalar(ScalarValue::Int32(i as i32))),
            RecordField::new("ad", Value::Scalar(ScalarValue::UInt32(i as u32 + 2))),
            RecordField::new(
                "ag",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(
                    i as f64 + 3.0,
                    -(i as f64 + 1.0),
                ))),
            ),
            RecordField::new("ae", Value::Scalar(ScalarValue::Float32(i as f32 + 3.0))),
            RecordField::new("af", Value::Scalar(ScalarValue::String(format!("V{i}")))),
            RecordField::new("arr1", Value::Array(ArrayValue::Float32(arr))),
        ]));
    }

    let mut table = Table::from_rows_with_schema(rows, schema)?;

    // C++ (tTable.cc, function b()):
    //   tab.tableInfo().setType("testtype");
    //   tab.tableInfo().readmeAddLine("first readme line");
    table.keywords_mut().push(RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("test-harness".into())),
    ));
    table.keywords_mut().push(RecordField::new(
        "project",
        Value::Scalar(ScalarValue::String("casa-rs demo".into())),
    ));

    // Column keywords (no direct equivalent in tTable.cc, but standard
    // casacore practice for measurement columns).
    let mut ab_kw = RecordValue::default();
    ab_kw.push(RecordField::new(
        "unit",
        Value::Scalar(ScalarValue::String("count".into())),
    ));
    table.set_column_keywords("ab", ab_kw);

    Ok(table)
}

// ── Round-trip: save → reopen → verify ───────────────────────────────

fn round_trip(
    out: &mut String,
    label: &str,
    dm_kind: DataManagerKind,
    endian: Option<EndianFormat>,
) -> Result<(), TableError> {
    appendln(out, &format!("--- {label} round-trip (10 rows) ---"));

    let table = build_demo_table()?;

    // Save to a temp directory.
    let dir = temp_dir(&format!("tTable_demo_{label}"));
    let mut opts = TableOptions::new(&dir).with_data_manager(dm_kind);
    if let Some(ef) = endian {
        opts = opts.with_endian_format(ef);
    }
    table.save(opts)?;

    // C++ (tTable.cc, function b()):
    //   Table tab("tTable_tmp.data", TableLock(...));
    let reopened = Table::open(TableOptions::new(&dir))?;

    // Verify schema.
    let schema = reopened.schema().expect("reopened table has a schema");
    appendln(out, &format!("schema: {} columns", schema.columns().len()));

    // Verify row count.
    appendln(out, &format!("row_count: {}", reopened.row_count()));

    // C++ (tTable.cc):
    //   for (i=0; i<10; i++) {
    //       ab2.get(i, abval); ad.get(i, adval); ag.get(i, agval);
    //       if (abval != Int(i) || ...) cout << "error in row " << i;
    //       arr1.get(i, arrval);
    //       if (!allEQ(arrval, arrf)) cout << "error in arr1 in row " << i;
    //   }
    let nelem: usize = 2 * 3 * 4;
    let nrow = reopened.row_count();
    let mut cells_verified = 0u32;

    for i in 0..nrow {
        // Scalar checks.
        let ab = reopened.get_scalar_cell(i, "ab")?;
        if *ab != ScalarValue::Int32(i as i32) {
            appendln(out, &format!("MISMATCH ab row {i}: {ab:?}"));
        }
        cells_verified += 1;

        let ad = reopened.get_scalar_cell(i, "ad")?;
        if *ad != ScalarValue::UInt32(i as u32 + 2) {
            appendln(out, &format!("MISMATCH ad row {i}: {ad:?}"));
        }
        cells_verified += 1;

        let ag = reopened.get_scalar_cell(i, "ag")?;
        let expected_ag = ScalarValue::Complex64(Complex64::new(i as f64 + 3.0, -(i as f64 + 1.0)));
        if *ag != expected_ag {
            appendln(out, &format!("MISMATCH ag row {i}: {ag:?}"));
        }
        cells_verified += 1;

        let ae = reopened.get_scalar_cell(i, "ae")?;
        if *ae != ScalarValue::Float32(i as f32 + 3.0) {
            appendln(out, &format!("MISMATCH ae row {i}: {ae:?}"));
        }
        cells_verified += 1;

        let af = reopened.get_scalar_cell(i, "af")?;
        if *af != ScalarValue::String(format!("V{i}")) {
            appendln(out, &format!("MISMATCH af row {i}: {af:?}"));
        }
        cells_verified += 1;

        // Array check.
        let arr = reopened.get_array_cell(i, "arr1")?;
        let base = (i * nelem) as f32;
        let expected: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
        let expected_arr = Array::from_shape_vec(IxDyn(&[2, 3, 4]), expected)
            .expect("shape matches element count");
        if let ArrayValue::Float32(actual) = arr {
            if *actual != expected_arr {
                appendln(out, &format!("MISMATCH arr1 row {i}"));
            }
        } else {
            appendln(out, &format!("WRONG TYPE arr1 row {i}: {arr:?}"));
        }
        cells_verified += 1;
    }
    appendln(out, &format!("cells verified: {cells_verified}"));

    // Verify table keywords.
    let kw = reopened.keywords();
    let mut kw_verified = 0u32;
    match kw.get("observer") {
        Some(Value::Scalar(ScalarValue::String(s))) if s == "test-harness" => kw_verified += 1,
        other => appendln(out, &format!("MISMATCH keyword observer: {other:?}")),
    }
    match kw.get("project") {
        Some(Value::Scalar(ScalarValue::String(s))) if s == "casa-rs demo" => kw_verified += 1,
        other => appendln(out, &format!("MISMATCH keyword project: {other:?}")),
    }
    appendln(out, &format!("table keywords verified: {kw_verified}"));

    // Verify column keywords.
    let mut col_kw_verified = 0u32;
    match reopened.column_keywords("ab") {
        Some(ckw) => match ckw.get("unit") {
            Some(Value::Scalar(ScalarValue::String(s))) if s == "count" => col_kw_verified += 1,
            other => appendln(out, &format!("MISMATCH column keyword ab.unit: {other:?}")),
        },
        None => appendln(out, "MISSING column keywords for ab"),
    }
    appendln(out, &format!("column keywords verified: {col_kw_verified}"));

    // Cleanup.
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

// ── Column iteration patterns ────────────────────────────────────────

fn demo_column_iteration(out: &mut String) -> Result<(), TableError> {
    appendln(out, "--- Column iteration patterns ---");
    let table = build_demo_table()?;

    // get_column: iterate all cells of a scalar column.
    let count = table.get_column("ab")?.count();
    appendln(out, &format!("get_column(\"ab\"): {count} cells"));

    // get_column_range: iterate a sub-range.
    let count = table.get_column_range("ab", RowRange::new(2, 5))?.count();
    appendln(
        out,
        &format!("get_column_range(\"ab\", 2..5): {count} cells"),
    );

    // iter_column_chunks: chunked iteration over an array column.
    let chunks: Vec<_> = table
        .iter_column_chunks("arr1", RowRange::new(0, 10), 3)?
        .collect();
    appendln(
        out,
        &format!(
            "iter_column_chunks(\"arr1\", 0..10, chunk=3): {} chunks",
            chunks.len()
        ),
    );

    // column_cells: materialized vector.
    let cells = table.column_cells("af");
    let defined = cells.iter().filter(|c| c.is_some()).count();
    appendln(out, &format!("column_cells(\"af\"): {defined} values"));

    appendln(out, "");
    Ok(())
}

// ── Schema mutation ──────────────────────────────────────────────────

fn demo_schema_mutation(out: &mut String) -> Result<(), TableError> {
    appendln(out, "--- Schema mutation ---");

    // C++ (Table.h):
    //   tab.addColumn(ScalarColumnDesc<float>("weight"));
    //   tab.removeColumn("ae");
    //   tab.renameColumn("newab", "ab");
    //   tab.removeRow(0);

    let mut table = build_demo_table()?;
    appendln(
        out,
        &format!(
            "before: {} columns, {} rows",
            table.schema().unwrap().columns().len(),
            table.row_count()
        ),
    );

    // Add a column with a default value.
    table.add_column(
        ColumnSchema::scalar("weight", PrimitiveType::Float32),
        Some(Value::Scalar(ScalarValue::Float32(1.0))),
    )?;
    appendln(
        out,
        &format!(
            "after add_column: {} columns",
            table.schema().unwrap().columns().len()
        ),
    );

    // Remove a column.
    table.remove_column("ae")?;
    appendln(
        out,
        &format!(
            "after remove_column(\"ae\"): {} columns",
            table.schema().unwrap().columns().len()
        ),
    );

    // Rename a column.
    table.rename_column("ab", "index")?;
    let has_index = table.schema().unwrap().contains_column("index");
    let has_ab = table.schema().unwrap().contains_column("ab");
    appendln(
        out,
        &format!("after rename_column: has \"index\"={has_index}, has \"ab\"={has_ab}"),
    );

    // Remove rows.
    table.remove_rows(&[0, 5])?;
    appendln(
        out,
        &format!("after remove_rows([0,5]): {} rows", table.row_count()),
    );

    // Insert a row.
    table.insert_row(
        0,
        RecordValue::new(vec![
            RecordField::new("index", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("ad", Value::Scalar(ScalarValue::UInt32(0))),
            RecordField::new(
                "ag",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(0.0, 0.0))),
            ),
            RecordField::new("af", Value::Scalar(ScalarValue::String("inserted".into()))),
            RecordField::new(
                "arr1",
                Value::Array(ArrayValue::Float32(
                    Array::from_shape_vec(IxDyn(&[2, 3, 4]), vec![0.0f32; 24]).unwrap(),
                )),
            ),
            RecordField::new("weight", Value::Scalar(ScalarValue::Float32(0.5))),
        ]),
    )?;
    appendln(
        out,
        &format!("after insert_row(0): {} rows", table.row_count()),
    );

    // Save and reopen to verify persistence.
    let dir = temp_dir("tTable_demo_mutation");
    table.save(TableOptions::new(&dir))?;
    let reopened = Table::open(TableOptions::new(&dir))?;
    appendln(
        out,
        &format!(
            "reopened: {} columns, {} rows",
            reopened.schema().unwrap().columns().len(),
            reopened.row_count()
        ),
    );
    let _ = std::fs::remove_dir_all(&dir);

    appendln(out, "");
    Ok(())
}

// ── Reference tables (views) ──────────────────────────────────────────

fn demo_ref_tables(out: &mut String) -> Result<(), TableError> {
    appendln(out, "--- Reference tables ---");

    let mut table = build_demo_table()?;

    // select_rows: pick specific rows by index.
    let view = table.select_rows(&[0, 2, 4])?;
    appendln(
        out,
        &format!("select_rows([0,2,4]): {} rows", view.row_count()),
    );

    // Read through the view.
    let ab = view.cell(0, "ab")?;
    appendln(out, &format!("  view row 0, ab = {ab:?}"));
    let ab = view.cell(1, "ab")?;
    appendln(out, &format!("  view row 1, ab = {ab:?}"));
    drop(view);

    // select_columns: pick specific columns.
    let view = table.select_columns(&["ab", "af"])?;
    appendln(
        out,
        &format!(
            "select_columns([\"ab\",\"af\"]): {} cols, {} rows",
            view.column_names().len(),
            view.row_count()
        ),
    );
    drop(view);

    // select: filter rows with a predicate.
    let view = table.select(|row| {
        row.get("ab")
            .map(|v| matches!(v, Value::Scalar(ScalarValue::Int32(i)) if *i >= 5))
            .unwrap_or(false)
    });
    appendln(out, &format!("select(ab >= 5): {} rows", view.row_count()));
    drop(view);

    // Write-through: modify parent via view.
    {
        let mut view = table.select_rows(&[0])?;
        view.set_cell(
            0,
            "af",
            Value::Scalar(ScalarValue::String("modified".into())),
        )?;
    }
    let af = table.cell(0, "af").unwrap();
    appendln(out, &format!("write-through: row 0 af = {af:?}"));

    // Save and reopen round-trip.
    let dir = temp_dir("tTable_demo_reftable");
    let parent_path = dir.join("parent.tbl");
    let ref_path = dir.join("ref.tbl");

    table.save(TableOptions::new(&parent_path))?;
    table.set_path(&parent_path);

    let view = table.select_rows(&[1, 3, 5])?;
    view.save(TableOptions::new(&ref_path))?;
    drop(view);

    let reopened = Table::open(TableOptions::new(&ref_path))?;
    appendln(
        out,
        &format!("save+reopen ref table: {} rows", reopened.row_count()),
    );

    let _ = std::fs::remove_dir_all(&dir);
    appendln(out, "");
    Ok(())
}

// ── Table locking ──────────────────────────────────────────────────────

#[cfg(unix)]
fn demo_locking(out: &mut String) -> Result<(), TableError> {
    use crate::{LockMode, LockOptions, LockType};

    // C++ (Table.h):
    //   Table tab("path", TableLock(TableLock::PermanentLocking));
    //   ...
    //   tab.lock(FileLocker::Write);
    //   tab.unlock();

    appendln(out, "--- Table locking ---");

    // Save a table to disk first so we can open it with locking.
    let dir = temp_dir("tTable_demo_locking");
    let table = build_demo_table()?;
    table.save(TableOptions::new(&dir))?;

    // PermanentLocking: lock on open, hold until close.
    {
        let perm = Table::open_with_lock(
            TableOptions::new(&dir),
            LockOptions::new(LockMode::PermanentLocking),
        )?;
        appendln(
            out,
            &format!(
                "permanent lock: has_write={}, rows={}",
                perm.has_lock(LockType::Write),
                perm.row_count()
            ),
        );
        // Lock released on drop.
    }

    // UserLocking: explicit lock/unlock.
    {
        let mut user = Table::open_with_lock(
            TableOptions::new(&dir),
            LockOptions::new(LockMode::UserLocking),
        )?;
        appendln(
            out,
            &format!(
                "user lock (before): has_write={}",
                user.has_lock(LockType::Write)
            ),
        );

        user.lock(LockType::Write, 1)?;
        appendln(
            out,
            &format!(
                "user lock (after lock): has_write={}",
                user.has_lock(LockType::Write)
            ),
        );

        // Modify while locked.
        user.add_row(RecordValue::new(vec![
            RecordField::new("ab", Value::Scalar(ScalarValue::Int32(999))),
            RecordField::new("ad", Value::Scalar(ScalarValue::UInt32(0))),
            RecordField::new(
                "ag",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(0.0, 0.0))),
            ),
            RecordField::new("ae", Value::Scalar(ScalarValue::Float32(0.0))),
            RecordField::new("af", Value::Scalar(ScalarValue::String("locked".into()))),
            RecordField::new(
                "arr1",
                Value::Array(ArrayValue::Float32(
                    Array::from_shape_vec(IxDyn(&[2, 3, 4]), vec![0.0f32; 24]).unwrap(),
                )),
            ),
        ]))?;

        // Unlock flushes to disk.
        user.unlock()?;
        appendln(
            out,
            &format!(
                "user lock (after unlock): has_write={}, rows={}",
                user.has_lock(LockType::Write),
                user.row_count()
            ),
        );
    }

    // Reopen and verify the row written under lock was persisted.
    let reopened = Table::open(TableOptions::new(&dir))?;
    appendln(
        out,
        &format!("reopened after locking demo: {} rows", reopened.row_count()),
    );

    let _ = std::fs::remove_dir_all(&dir);
    appendln(out, "");
    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────

fn appendln(out: &mut String, line: &str) {
    writeln!(out, "{line}").expect("String write never fails");
}

fn temp_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    path.push(format!("{name}.{nanos}.{}", std::process::id()));
    path
}

#[cfg(test)]
mod tests {
    use super::run_ttable_like_demo;

    #[test]
    fn demo_contains_expected_section_headers() {
        let output = run_ttable_like_demo().expect("demo should run");
        assert!(output.contains("=== Table Demo"));
        assert!(output.contains("--- StManAipsIO round-trip"));
        assert!(output.contains("--- StandardStMan round-trip"));
        assert!(output.contains("--- StandardStMan-LE round-trip"));
        assert!(output.contains("--- Column iteration patterns"));
        assert!(output.contains("--- Schema mutation"));
        assert!(output.contains("--- Reference tables"));
        #[cfg(unix)]
        assert!(output.contains("--- Table locking"));
        assert!(output.ends_with("end\n"));
    }
}
