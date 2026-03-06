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
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{Array, IxDyn, ShapeBuilder};

use crate::{
    ColumnSchema, ColumnsIndex, DataManagerKind, EndianFormat, RowRange, Table, TableError,
    TableOptions, TableSchema,
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
    round_trip(
        &mut out,
        "IncrementalStMan",
        DataManagerKind::IncrementalStMan,
        None,
    )?;
    appendln(&mut out, "");
    round_trip(
        &mut out,
        "IncrementalStMan-LE",
        DataManagerKind::IncrementalStMan,
        Some(EndianFormat::LittleEndian),
    )?;
    appendln(&mut out, "");
    demo_column_iteration(&mut out)?;
    demo_schema_mutation(&mut out)?;
    demo_ref_tables(&mut out)?;
    demo_sorting_and_iteration(&mut out)?;
    demo_concat_and_copy(&mut out)?;
    demo_indexing(&mut out)?;
    #[cfg(unix)]
    demo_locking(&mut out)?;
    demo_memory_tables(&mut out)?;
    demo_taql(&mut out)?;
    demo_tiled_storage(&mut out)?;
    demo_virtual_columns(&mut out)?;

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

// ── Sorting and grouped iteration ──────────────────────────────────────

fn demo_sorting_and_iteration(out: &mut String) -> Result<(), TableError> {
    use crate::SortOrder;

    // C++ (Table.h):
    //   Table sorted = tab.sort("ae", Sort::Descending);
    //   TableIterator iter(tab, "ab");
    //   while (!iter.pastEnd()) { Table t = iter.table(); iter.next(); }

    appendln(out, "--- Sorting and table iteration ---");

    let mut table = build_demo_table()?;

    // Sort by "ab" (Int32) ascending.
    {
        let sorted = table.sort(&[("ab", SortOrder::Ascending)])?;
        appendln(out, &format!("sort(ab ASC): {} rows", sorted.row_count()));
        let first = sorted.cell(0, "ab")?;
        appendln(out, &format!("  first row ab = {first:?}"));
        let last = sorted.cell(sorted.row_count() - 1, "ab")?;
        appendln(out, &format!("  last  row ab = {last:?}"));
    }

    // Sort by "ae" (Float64) descending.
    {
        let sorted = table.sort(&[("ae", SortOrder::Descending)])?;
        appendln(out, &format!("sort(ae DESC): {} rows", sorted.row_count()));
        let first = sorted.cell(0, "ae")?;
        appendln(out, &format!("  first row ae = {first:?}"));
    }

    // Grouped iteration by "ab".
    {
        let groups: Vec<crate::TableGroup> = table
            .iter_groups(&[("ab", SortOrder::Ascending)])?
            .collect();
        appendln(
            out,
            &format!("iter_groups(ab ASC): {} groups", groups.len()),
        );
        if let Some(g) = groups.first() {
            appendln(out, &format!("  first group: {} rows", g.row_indices.len()));
        }
    }

    appendln(out, "");
    Ok(())
}

// ── Table concatenation and copy ──────────────────────────────────────

fn demo_concat_and_copy(out: &mut String) -> Result<(), TableError> {
    use crate::ConcatTable;

    // C++ (Table.h):
    //   Table concat(Block<Table>({t1, t2}), Block<String>(), "");
    //   Table::deepCopy("copy", Table::New, True);

    appendln(out, "--- Table concatenation and copy ---");

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])?;

    // Build two small tables.
    let mut t1 = Table::with_schema(schema.clone());
    for i in 0..3 {
        t1.add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
            RecordField::new("name", Value::Scalar(ScalarValue::String(format!("a{i}")))),
        ]))?;
    }

    let mut t2 = Table::with_schema(schema);
    for i in 3..6 {
        t2.add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
            RecordField::new("name", Value::Scalar(ScalarValue::String(format!("b{i}")))),
        ]))?;
    }

    // Concatenate.
    let concat: ConcatTable = Table::concat(vec![t1, t2])?;
    appendln(
        out,
        &format!(
            "concat: {} rows from {} tables",
            concat.row_count(),
            concat.table_count()
        ),
    );

    // Verify row access spans the boundary.
    let r2 = concat.row(2).expect("row 2 exists");
    let r3 = concat.row(3).expect("row 3 exists");
    appendln(
        out,
        &format!(
            "  row 2 id={:?}, row 3 id={:?}",
            r2.get("id").unwrap(),
            r3.get("id").unwrap()
        ),
    );

    // Save concat table and reopen (materializes).
    let dir = temp_dir("tTable_demo_concat");
    let part0 = dir.join("part0.tbl");
    let part1 = dir.join("part1.tbl");
    let concat_path = dir.join("concat.tbl");

    // Save constituents first (required for ConcatTable::save).
    concat.tables()[0].save(TableOptions::new(&part0))?;
    concat.tables()[1].save(TableOptions::new(&part1))?;

    // Create a fresh concat from saved tables.
    let t1 = Table::open(TableOptions::new(&part0))?;
    let t2 = Table::open(TableOptions::new(&part1))?;
    let concat = Table::concat(vec![t1, t2])?;
    concat.save(TableOptions::new(&concat_path))?;

    let reopened = Table::open(TableOptions::new(&concat_path))?;
    appendln(
        out,
        &format!("concat save+reopen: {} rows", reopened.row_count()),
    );

    // Deep copy with DM conversion (StManAipsIO → StandardStMan).
    let copy_path = dir.join("deep_copy.tbl");
    reopened.deep_copy(
        TableOptions::new(&copy_path).with_data_manager(DataManagerKind::StandardStMan),
    )?;
    let deep = Table::open(TableOptions::new(&copy_path))?;
    appendln(out, &format!("deep copy: {} rows", deep.row_count()));

    // Shallow copy (zero rows, schema preserved).
    let shallow_path = dir.join("shallow.tbl");
    reopened.shallow_copy(TableOptions::new(&shallow_path))?;
    let shallow = Table::open(TableOptions::new(&shallow_path))?;
    appendln(
        out,
        &format!(
            "shallow copy: {} rows, {} cols",
            shallow.row_count(),
            shallow.schema().unwrap().columns().len()
        ),
    );

    let _ = std::fs::remove_dir_all(&dir);
    appendln(out, "");
    Ok(())
}

// ── Column indexing ───────────────────────────────────────────────────

fn demo_indexing(out: &mut String) -> Result<(), TableError> {
    // C++ (ColumnsIndex.h):
    //   ColumnsIndex idx(tab, "antenna_id");
    //   RecordFieldPtr<Int> key(idx.accessKey(), "antenna_id");
    //   *key = 3;
    //   Vector<uInt> rows = idx.getRowNumbers();

    appendln(out, "--- Column indexing ---");

    // Build a 50-row table with an antenna_id column (values 0..=9 cycling).
    let schema = TableSchema::new(vec![ColumnSchema::scalar(
        "antenna_id",
        PrimitiveType::Int32,
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for i in 0..50i32 {
        table.add_row(RecordValue::new(vec![RecordField::new(
            "antenna_id",
            Value::Scalar(ScalarValue::Int32(i % 10)),
        )]))?;
    }
    appendln(out, &format!("table rows: {}", table.row_count()));

    // Build index on antenna_id.
    let idx = ColumnsIndex::new(&table, &["antenna_id"])?;
    appendln(out, &format!("index columns: {:?}", idx.column_names()));
    appendln(out, &format!("index is_unique: {}", idx.is_unique()));

    // Exact lookup: antenna_id == 3 → 5 matching rows.
    let rows = idx.lookup(&[("antenna_id", &ScalarValue::Int32(3))]);
    appendln(out, &format!("lookup(antenna_id=3): {} rows", rows.len()));

    // Range query: antenna_id in [2, 4] inclusive.
    let range_rows = idx.lookup_range(
        &[("antenna_id", &ScalarValue::Int32(2))],
        &[("antenna_id", &ScalarValue::Int32(4))],
        true,
        true,
    );
    appendln(
        out,
        &format!("lookup_range([2,4] incl): {} rows", range_rows.len()),
    );

    // unique lookup on a non-unique index returns IndexNotUnique error.
    let unique_result = idx.lookup_unique(&[("antenna_id", &ScalarValue::Int32(5))]);
    appendln(
        out,
        &format!(
            "lookup_unique(antenna_id=5) is_err: {}",
            unique_result.is_err()
        ),
    );

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

// ── Memory tables ──────────────────────────────────────────────────────

fn demo_memory_tables(out: &mut String) -> Result<(), TableError> {
    use crate::SortOrder;

    // C++ (tMemoryTable.cc):
    //   SetupNewTable aNewTab("tmtest", td, Table::New);
    //   aTable = Table(aNewTab, Table::Memory, 10);

    appendln(out, "--- Memory tables ---");

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])?;

    let mut mem = Table::with_schema_memory(schema);
    for i in 0..5 {
        mem.add_row(RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
            RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String(format!("row_{i}"))),
            ),
        ]))?;
    }
    appendln(
        out,
        &format!(
            "memory table: {} rows, kind={:?}",
            mem.row_count(),
            mem.table_kind()
        ),
    );
    appendln(out, &format!("  is_memory={}", mem.is_memory()));

    // Add keywords.
    mem.keywords_mut().push(RecordField::new(
        "origin",
        Value::Scalar(ScalarValue::String("in-memory".into())),
    ));

    // Locking is a no-op.
    #[cfg(unix)]
    {
        use crate::LockType;
        appendln(
            out,
            &format!("  has_lock(Write)={}", mem.has_lock(LockType::Write)),
        );
        assert!(mem.lock(LockType::Write, 1)?);
        mem.unlock()?;
        appendln(out, "  lock/unlock succeeded (no-op)");
    }

    // Sort works on memory tables.
    {
        let sorted = mem.sort(&[("id", SortOrder::Descending)])?;
        let first = sorted.cell(0, "id")?;
        appendln(out, &format!("  sort(id DESC) first={first:?}"));
    }

    // Materialize to disk.
    let dir = temp_dir("tTable_demo_memory");
    mem.save(TableOptions::new(&dir))?;
    let reopened = Table::open(TableOptions::new(&dir))?;
    appendln(
        out,
        &format!(
            "  save+reopen: {} rows, is_memory={}",
            reopened.row_count(),
            reopened.is_memory()
        ),
    );
    let kw = reopened.keywords().get("origin");
    appendln(out, &format!("  keyword origin={kw:?}"));

    // Copy plain table to memory.
    let mem2 = reopened.to_memory();
    appendln(
        out,
        &format!(
            "  to_memory: {} rows, is_memory={}",
            mem2.row_count(),
            mem2.is_memory()
        ),
    );

    let _ = std::fs::remove_dir_all(&dir);
    appendln(out, "");
    Ok(())
}

// ── Tiled storage managers ──────────────────────────────────────────

fn demo_tiled_storage(out: &mut String) -> Result<(), TableError> {
    use ndarray::ShapeBuilder;

    // C++ (tTiledColumnStMan.cc):
    //   TiledColumnStMan sm1("TiledData", IPosition(3, 2, 3, 2));
    //   td.addColumn(ArrayColumnDesc<Float>("data", IPosition(2,2,3),
    //                ColumnDesc::FixedShape));

    appendln(out, "--- Tiled storage managers ---");

    // ── TiledColumnStMan ──
    // Fixed-shape Float32 [2,3] array column, 4 rows, tile shape [2,3,2].
    {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Float32,
            vec![2, 3],
        )])?;

        let nelem = 6usize; // 2*3
        let nrow = 4usize;
        let mut rows = Vec::with_capacity(nrow);
        for i in 0..nrow {
            let base = (i * nelem) as f32;
            let data: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let arr = ndarray::Array::from_shape_vec(IxDyn(&[2, 3]).f(), data).unwrap();
            rows.push(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(arr)),
            )]));
        }
        let table = Table::from_rows_with_schema(rows, schema)?;

        let dir = temp_dir("tTable_demo_tiled_col");
        table.save(
            TableOptions::new(&dir)
                .with_data_manager(DataManagerKind::TiledColumnStMan)
                .with_tile_shape(vec![2, 3, 2]),
        )?;

        let reopened = Table::open(TableOptions::new(&dir))?;
        appendln(
            out,
            &format!("TiledColumnStMan: {} rows", reopened.row_count()),
        );

        let mut ok = true;
        for i in 0..nrow {
            let arr = reopened.get_array_cell(i, "data")?;
            let base = (i * nelem) as f32;
            let expected: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let expected_arr =
                ndarray::Array::from_shape_vec(IxDyn(&[2, 3]).f(), expected).unwrap();
            if let ArrayValue::Float32(actual) = arr {
                if *actual != expected_arr {
                    appendln(out, &format!("  MISMATCH row {i}"));
                    ok = false;
                }
            }
        }
        appendln(out, &format!("  all cells match: {ok}"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── TiledShapeStMan ──
    // Variable-shape Float32, 4 rows: rows 0,3 are [2,3], rows 1,2 are [3,2].
    {
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "data",
            PrimitiveType::Float32,
            Some(2),
        )])?;

        let shapes: &[&[usize]] = &[&[2, 3], &[3, 2], &[3, 2], &[2, 3]];
        let mut rows = Vec::with_capacity(4);
        for (i, shape) in shapes.iter().enumerate() {
            let nelem: usize = shape.iter().product();
            let base = (i * 10) as f32;
            let data: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let arr = ndarray::Array::from_shape_vec(IxDyn(shape).f(), data).unwrap();
            rows.push(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(arr)),
            )]));
        }
        let table = Table::from_rows_with_schema(rows, schema)?;

        let dir = temp_dir("tTable_demo_tiled_shape");
        table.save(
            TableOptions::new(&dir)
                .with_data_manager(DataManagerKind::TiledShapeStMan)
                .with_tile_shape(vec![2, 3, 2]),
        )?;

        let reopened = Table::open(TableOptions::new(&dir))?;
        appendln(
            out,
            &format!("TiledShapeStMan: {} rows", reopened.row_count()),
        );

        let mut ok = true;
        for (i, shape) in shapes.iter().enumerate() {
            let arr = reopened.get_array_cell(i, "data")?;
            let nelem: usize = shape.iter().product();
            let base = (i * 10) as f32;
            let expected: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let expected_arr = ndarray::Array::from_shape_vec(IxDyn(shape).f(), expected).unwrap();
            if let ArrayValue::Float32(actual) = arr {
                if *actual != expected_arr {
                    appendln(out, &format!("  MISMATCH row {i}"));
                    ok = false;
                }
            }
        }
        appendln(out, &format!("  all cells match: {ok}"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── TiledCellStMan ──
    // Variable-shape Float32, 3 rows with unique shapes per row.
    {
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "data",
            PrimitiveType::Float32,
            Some(2),
        )])?;

        let shapes: &[&[usize]] = &[&[2, 3], &[4, 2], &[3, 3]];
        let mut rows = Vec::with_capacity(3);
        for (i, shape) in shapes.iter().enumerate() {
            let nelem: usize = shape.iter().product();
            let base = (i * 10) as f32;
            let data: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let arr = ndarray::Array::from_shape_vec(IxDyn(shape).f(), data).unwrap();
            rows.push(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(arr)),
            )]));
        }
        let table = Table::from_rows_with_schema(rows, schema)?;

        let dir = temp_dir("tTable_demo_tiled_cell");
        table.save(
            TableOptions::new(&dir)
                .with_data_manager(DataManagerKind::TiledCellStMan)
                .with_tile_shape(vec![4, 4]),
        )?;

        let reopened = Table::open(TableOptions::new(&dir))?;
        appendln(
            out,
            &format!("TiledCellStMan: {} rows", reopened.row_count()),
        );

        let mut ok = true;
        for (i, shape) in shapes.iter().enumerate() {
            let arr = reopened.get_array_cell(i, "data")?;
            let nelem: usize = shape.iter().product();
            let base = (i * 10) as f32;
            let expected: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let expected_arr = ndarray::Array::from_shape_vec(IxDyn(shape).f(), expected).unwrap();
            if let ArrayValue::Float32(actual) = arr {
                if *actual != expected_arr {
                    appendln(out, &format!("  MISMATCH row {i}"));
                    ok = false;
                }
            }
        }
        appendln(out, &format!("  all cells match: {ok}"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    appendln(out, "");
    Ok(())
}

// ── Virtual column engines ──────────────────────────────────────────

/// Demonstrate virtual column engines.
///
/// Covers the three virtual column types supported by casacore:
/// - **ForwardColumnEngine** (cf. C++ `tForwardCol.cc`): reads values
///   from a column in another table.
/// - **ScaledArrayEngine** (cf. C++ `tScaledArrayEngine.cc`): computes
///   `virtual = stored * scale + offset` on integer/float arrays.
/// - **ScaledComplexData** (cf. C++ `tScaledComplexData.cc`): converts
///   integer arrays with a leading re/im axis into Complex values using
///   component-wise scaling.
fn demo_virtual_columns(out: &mut String) -> Result<(), TableError> {
    appendln(out, "--- Virtual column engines ---");

    // ── ForwardColumnEngine ──
    // C++ (tForwardCol.cc):
    //   ForwardColumnEngine fce(refTable);
    //   newtab.bindAll(fce);
    //   Table forwTab(newtab, nrow);
    {
        let dir = temp_dir("tTable_demo_fwd_col");
        let base_path = dir.join("base.tbl");
        let fwd_path = dir.join("fwd.tbl");

        // Create and save the base table with scalar + array columns.
        let base_schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
            ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![3]),
        ])?;
        let mut base = Table::with_schema(base_schema);
        let base_data: [(i32, &str, [f32; 3]); 3] = [
            (1, "Alpha", [1.0, 2.0, 3.0]),
            (2, "Beta", [4.0, 5.0, 6.0]),
            (3, "Gamma", [7.0, 8.0, 9.0]),
        ];
        for (id, name, arr) in &base_data {
            base.add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(*id))),
                RecordField::new("name", Value::Scalar(ScalarValue::String(name.to_string()))),
                RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Float32(
                        Array::from_shape_vec(IxDyn(&[3]), arr.to_vec()).unwrap(),
                    )),
                ),
            ]))?;
        }
        base.save(TableOptions::new(&base_path))?;

        // Create forwarding table: same schema, placeholder rows,
        // columns bound to the base table.
        let fwd_schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
            ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![3]),
        ])?;
        let mut fwd = Table::with_schema(fwd_schema);
        for _ in 0..3 {
            fwd.add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("name", Value::Scalar(ScalarValue::String(String::new()))),
                RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Float32(
                        Array::from_shape_vec(IxDyn(&[3]), vec![0.0; 3]).unwrap(),
                    )),
                ),
            ]))?;
        }
        fwd.bind_forward_column("id", &base_path)?;
        fwd.bind_forward_column("name", &base_path)?;
        fwd.bind_forward_column("data", &base_path)?;
        fwd.save(TableOptions::new(&fwd_path))?;

        // Reopen and verify values come from the base table.
        let reopened = Table::open(TableOptions::new(&fwd_path))?;
        appendln(
            out,
            &format!("ForwardColumnEngine: {} rows", reopened.row_count()),
        );

        let mut ok = true;
        for (i, (id, name, arr)) in base_data.iter().enumerate() {
            let got_id = reopened.get_scalar_cell(i, "id")?;
            if *got_id != ScalarValue::Int32(*id) {
                ok = false;
            }
            let got_name = reopened.get_scalar_cell(i, "name")?;
            if *got_name != ScalarValue::String(name.to_string()) {
                ok = false;
            }
            let got_data = reopened.get_array_cell(i, "data")?;
            let expected = Array::from_shape_vec(IxDyn(&[3]), arr.to_vec()).unwrap();
            if let ArrayValue::Float32(v) = got_data {
                if *v != expected {
                    ok = false;
                }
            }
            if !reopened.is_virtual_column("id") {
                ok = false;
            }
        }
        appendln(out, &format!("  all forwarded cells match: {ok}"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── ScaledArrayEngine ──
    // C++ (tScaledArrayEngine.cc):
    //   ScaledArrayEngine<Double,Int> engine("source", "target", 2.5, 10.0);
    //   newtab.bindColumn("source", engine);
    {
        let dir = temp_dir("tTable_demo_scaled_arr");

        // stored_col: Int32 [4], virtual_col: Float64 [4]
        // virtual = stored * 2.5 + 10.0
        let schema = TableSchema::new(vec![
            ColumnSchema::array_fixed("stored_col", PrimitiveType::Int32, vec![4]),
            ColumnSchema::array_fixed("virtual_col", PrimitiveType::Float64, vec![4]),
        ])?;

        let scale = 2.5;
        let offset = 10.0;
        let stored_rows: [[i32; 4]; 3] = [[1, 2, 3, 4], [10, 20, 30, 40], [-5, 0, 5, 100]];

        let mut table = Table::with_schema(schema);
        for arr in &stored_rows {
            table.add_row(RecordValue::new(vec![
                RecordField::new(
                    "stored_col",
                    Value::Array(ArrayValue::Int32(
                        Array::from_shape_vec(IxDyn(&[4]), arr.to_vec()).unwrap(),
                    )),
                ),
                RecordField::new(
                    "virtual_col",
                    Value::Array(ArrayValue::Float64(
                        Array::from_shape_vec(IxDyn(&[4]), vec![0.0; 4]).unwrap(),
                    )),
                ),
            ]))?;
        }
        table.bind_scaled_array_column("virtual_col", "stored_col", scale, offset)?;
        table.save(TableOptions::new(&dir))?;

        let reopened = Table::open(TableOptions::new(&dir))?;
        appendln(
            out,
            &format!("ScaledArrayEngine: {} rows", reopened.row_count()),
        );

        let mut ok = true;
        for (i, stored) in stored_rows.iter().enumerate() {
            let arr = reopened.get_array_cell(i, "virtual_col")?;
            if let ArrayValue::Float64(arr) = arr {
                for (j, &s) in stored.iter().enumerate() {
                    let expected = s as f64 * scale + offset;
                    let got = arr.as_slice().unwrap()[j];
                    if (got - expected).abs() > 1e-10 {
                        appendln(out, &format!("  MISMATCH row {i} elem {j}"));
                        ok = false;
                    }
                }
            }
        }
        appendln(out, &format!("  all scaled cells match: {ok}"));

        // Verify virtual column introspection.
        assert!(reopened.is_virtual_column("virtual_col"));
        assert!(!reopened.is_virtual_column("stored_col"));
        appendln(out, "  is_virtual_column: correct");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── ScaledComplexData ──
    // C++ (tScaledComplexData.cc):
    //   ScaledComplexData<Complex,Short> engine("source", "target",
    //       Complex(0.5, 0.25), Complex(1.0, 2.0));
    //   newtab.bindColumn("source", engine);
    //
    // Component-wise formula (NOT complex multiplication):
    //   re_virtual = re_stored * scale.re + offset.re
    //   im_virtual = im_stored * scale.im + offset.im
    {
        let dir = temp_dir("tTable_demo_scaled_complex");

        // stored_col: Int16 [2, 3] — axis 0 is [re, im], axis 1 is 3 elements
        // virtual_col: Complex32 [3]
        let schema = TableSchema::new(vec![
            ColumnSchema::array_fixed("stored_col", PrimitiveType::Int16, vec![2, 3]),
            ColumnSchema::array_fixed("virtual_col", PrimitiveType::Complex32, vec![3]),
        ])?;

        let scale = Complex64::new(0.5, 0.25);
        let offset = Complex64::new(1.0, 2.0);

        // Each stored row is [2, 3] in Fortran order:
        // physical memory = [re0, im0, re1, im1, re2, im2]
        let stored_rows: [Vec<i16>; 3] = [
            vec![10, 20, 30, 40, 50, 60],
            vec![2, 4, 6, 8, 10, 12],
            vec![-10, -20, 0, 0, 10, 20],
        ];

        let mut table = Table::with_schema(schema);
        for data in &stored_rows {
            let arr = Array::from_shape_vec(IxDyn(&[2, 3]).f(), data.clone()).unwrap();
            table.add_row(RecordValue::new(vec![
                RecordField::new("stored_col", Value::Array(ArrayValue::Int16(arr))),
                RecordField::new(
                    "virtual_col",
                    Value::Array(ArrayValue::Complex32(
                        Array::from_shape_vec(IxDyn(&[3]), vec![Complex32::new(0.0, 0.0); 3])
                            .unwrap(),
                    )),
                ),
            ]))?;
        }
        table.bind_scaled_complex_column("virtual_col", "stored_col", scale, offset)?;
        table.save(TableOptions::new(&dir))?;

        let reopened = Table::open(TableOptions::new(&dir))?;
        appendln(
            out,
            &format!("ScaledComplexData: {} rows", reopened.row_count()),
        );

        let mut ok = true;
        for (i, data) in stored_rows.iter().enumerate() {
            // Fortran-order [2,3]: physical = [re0,im0,re1,im1,re2,im2]
            let arr = reopened.get_array_cell(i, "virtual_col")?;
            if let ArrayValue::Complex32(arr) = arr {
                for j in 0..3 {
                    let re_stored = data[j * 2] as f64;
                    let im_stored = data[j * 2 + 1] as f64;
                    let expected_re = (re_stored * scale.re + offset.re) as f32;
                    let expected_im = (im_stored * scale.im + offset.im) as f32;
                    let got = arr.as_slice().unwrap()[j];
                    if (got.re - expected_re).abs() > 1e-5 || (got.im - expected_im).abs() > 1e-5 {
                        appendln(
                            out,
                            &format!(
                                "  MISMATCH row {i} elem {j}: expected ({expected_re},{expected_im}), got ({},{})",
                                got.re, got.im
                            ),
                        );
                        ok = false;
                    }
                }
            }
        }
        appendln(out, &format!("  all complex cells match: {ok}"));

        let _ = std::fs::remove_dir_all(&dir);
    }

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

// ── TaQL demo ──────────────────────────────────────────────────────

/// Demonstrate TaQL query capabilities.
///
/// Shows SELECT with WHERE, ORDER BY, LIMIT, built-in functions,
/// UPDATE, INSERT, DELETE, and GROUP BY.
fn demo_taql(out: &mut String) -> Result<(), TableError> {
    appendln(out, "--- TaQL (Table Query Language) ---");

    // Build a small table for the demo.
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::scalar("category", PrimitiveType::String),
    ])
    .unwrap();

    let mut table = Table::with_schema(schema);
    let data = [
        (1, "CygA", 100.0, "galaxy"),
        (2, "CasA", 80.0, "snr"),
        (3, "TauA", 60.0, "snr"),
        (4, "VirA", 40.0, "galaxy"),
        (5, "3C84", 20.0, "galaxy"),
        (6, "SunQ", 10.0, "star"),
        (7, "Moon", 5.0, "star"),
        (8, "Jup", 2.0, "planet"),
    ];
    for (id, name, flux, cat) in &data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(*id))),
                RecordField::new("name", Value::Scalar(ScalarValue::String(name.to_string()))),
                RecordField::new("flux", Value::Scalar(ScalarValue::Float64(*flux))),
                RecordField::new(
                    "category",
                    Value::Scalar(ScalarValue::String(cat.to_string())),
                ),
            ]))
            .unwrap();
    }

    // 1. SELECT with WHERE
    {
        let view = table.query("SELECT * WHERE flux > 50.0")?;
        appendln(
            out,
            &format!("SELECT WHERE flux > 50.0: {} rows", view.row_count()),
        );
    }

    // 2. SELECT with ORDER BY
    {
        let view = table.query("SELECT * ORDER BY flux DESC LIMIT 3")?;
        appendln(out, &format!("Top 3 by flux: {} rows", view.row_count()));
        for i in 0..view.row_count() {
            let name = view.cell(i, "name")?;
            let flux = view.cell(i, "flux")?;
            appendln(out, &format!("  {name:?} flux={flux:?}"));
        }
    }

    // 3. SELECT with built-in function
    {
        let view = table.query("SELECT * WHERE sqrt(flux) > 8.0")?;
        appendln(out, &format!("sqrt(flux) > 8.0: {} rows", view.row_count()));
    }

    // 4. UPDATE
    {
        let result = table.execute_taql("UPDATE SET flux = flux * 2.0 WHERE id = 8")?;
        appendln(out, &format!("UPDATE Jup flux: {result:?}"));
    }

    // 5. INSERT
    {
        let result = table.execute_taql(
            "INSERT INTO (id, name, flux, category) VALUES (9, 'NewSrc', 15.0, 'other')",
        )?;
        appendln(out, &format!("INSERT: {result:?}"));
        appendln(
            out,
            &format!("row count after INSERT: {}", table.row_count()),
        );
    }

    // 6. DELETE
    {
        let result = table.execute_taql("DELETE FROM WHERE flux < 5.0")?;
        appendln(out, &format!("DELETE flux < 5.0: {result:?}"));
        appendln(
            out,
            &format!("row count after DELETE: {}", table.row_count()),
        );
    }

    // 7. Parse round-trip
    {
        let query = "SELECT * WHERE flux > 1.0 ORDER BY id ASC";
        let ast = crate::taql::parse(query).map_err(|e| TableError::Taql(e.to_string()))?;
        let displayed = ast.to_string();
        appendln(out, &format!("parse round-trip: {displayed}"));
    }

    appendln(out, "");
    Ok(())
}

// ─── TableQuantum demo ─────────────────────────────────────────────────────

/// Run the table quantum demo (Rust equivalent of key parts of C++ `tTableQuantum`).
///
/// Demonstrates fixed-unit and variable-unit quantum columns for both
/// scalar and array column types.
pub fn run_table_quantum_demo() -> Result<String, TableError> {
    use crate::table_quantum::{
        ArrayQuantColumn, ArrayQuantColumnMut, ScalarQuantColumn, ScalarQuantColumnMut,
        TableQuantumDesc,
    };
    use casacore_types::quanta::Quantity;

    let mut out = String::new();
    appendln(
        &mut out,
        "=== TableQuantum Demo (cf. casacore tTableQuantum.cc) ===",
    );
    appendln(&mut out, "");

    // ── 1. Fixed-unit scalar column ─────────────────────────────────────

    appendln(&mut out, "--- Fixed-unit scalar quantum column");

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ra", PrimitiveType::Float64),
        ColumnSchema::scalar("dec", PrimitiveType::Float64),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    // Attach quantum descriptors.
    TableQuantumDesc::with_unit("ra", "deg").write(&mut table)?;
    TableQuantumDesc::with_unit("dec", "deg").write(&mut table)?;
    appendln(&mut out, "  Attached 'deg' units to ra, dec columns");

    // Add rows.
    for (ra, dec) in [(83.633, 22.015), (201.365, -43.019), (10.684, 41.269)] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("ra", Value::Scalar(ScalarValue::Float64(ra))),
                RecordField::new("dec", Value::Scalar(ScalarValue::Float64(dec))),
            ]))
            .unwrap();
    }

    // Read back with on-read conversion to radians.
    let col_ra = ScalarQuantColumn::with_unit(&table, "ra", "rad")?;
    for i in 0..table.row_count() {
        let q = col_ra.get(i)?;
        appendln(
            &mut out,
            &format!("  Row {i}: ra = {:.6} {}", q.value(), q.unit().name()),
        );
    }

    // ── 2. Variable-unit scalar column ──────────────────────────────────

    appendln(&mut out, "");
    appendln(&mut out, "--- Variable-unit scalar quantum column");

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::scalar("flux_unit", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    TableQuantumDesc::with_variable_units("flux", "flux_unit").write(&mut table)?;
    appendln(&mut out, "  Attached variable units via 'flux_unit' column");

    // Add rows with different units.
    for (v, u) in [(1.5, "Jy"), (250.0, "mJy"), (0.003, "Jy")] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("flux", Value::Scalar(ScalarValue::Float64(v))),
                RecordField::new(
                    "flux_unit",
                    Value::Scalar(ScalarValue::String(u.to_owned())),
                ),
            ]))
            .unwrap();
    }

    let col = ScalarQuantColumn::new(&table, "flux")?;
    for i in 0..table.row_count() {
        let q = col.get(i)?;
        appendln(
            &mut out,
            &format!("  Row {i}: flux = {} {}", q.value(), q.unit().name()),
        );
    }

    // Write a new value using the mutable accessor.
    {
        let mut mcol = ScalarQuantColumnMut::new(&mut table, "flux")?;
        mcol.put(1, &Quantity::new(0.25, "Jy").unwrap())?;
        appendln(&mut out, "  Updated row 1 to 0.25 Jy");
    }

    let q = ScalarQuantColumn::new(&table, "flux")?.get(1)?;
    appendln(
        &mut out,
        &format!("  Row 1 readback: {} {}", q.value(), q.unit().name()),
    );

    // ── 3. Fixed-unit array column ──────────────────────────────────────

    appendln(&mut out, "");
    appendln(&mut out, "--- Fixed-unit array quantum column");

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "freq",
        PrimitiveType::Float64,
        vec![4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    TableQuantumDesc::with_unit("freq", "MHz").write(&mut table)?;
    appendln(&mut out, "  Attached 'MHz' units to freq column");

    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "freq",
            Value::Array(ArrayValue::from_f64_vec(vec![100.0, 200.0, 300.0, 400.0])),
        )]))
        .unwrap();

    let col = ArrayQuantColumn::new(&table, "freq")?;
    let quanta = col.get(0)?;
    let vals: Vec<String> = quanta
        .iter()
        .map(|q| format!("{} {}", q.value(), q.unit().name()))
        .collect();
    appendln(&mut out, &format!("  Row 0: [{}]", vals.join(", ")));

    // Read with conversion to GHz.
    let col_ghz = ArrayQuantColumn::with_unit(&table, "freq", "GHz")?;
    let quanta_ghz = col_ghz.get(0)?;
    let vals_ghz: Vec<String> = quanta_ghz
        .iter()
        .map(|q| format!("{:.4} {}", q.value(), q.unit().name()))
        .collect();
    appendln(
        &mut out,
        &format!("  Row 0 in GHz: [{}]", vals_ghz.join(", ")),
    );

    // Write with unit conversion.
    {
        let mut mcol = ArrayQuantColumnMut::new(&mut table, "freq")?;
        let quanta: Vec<Quantity> = vec![1.0, 2.0, 3.0, 4.0]
            .into_iter()
            .map(|v| Quantity::new(v, "GHz").unwrap())
            .collect();
        mcol.put(0, &quanta)?;
        appendln(&mut out, "  Wrote [1, 2, 3, 4] GHz → stored as MHz");
    }

    let readback = ArrayQuantColumn::new(&table, "freq")?.get(0)?;
    let vals_back: Vec<String> = readback
        .iter()
        .map(|q| format!("{} {}", q.value(), q.unit().name()))
        .collect();
    appendln(&mut out, &format!("  Readback: [{}]", vals_back.join(", ")));

    appendln(&mut out, "");
    appendln(&mut out, "=== TableQuantum Demo complete ===");
    Ok(out)
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
        assert!(output.contains("--- IncrementalStMan round-trip"));
        assert!(output.contains("--- IncrementalStMan-LE round-trip"));
        assert!(output.contains("--- Column iteration patterns"));
        assert!(output.contains("--- Schema mutation"));
        assert!(output.contains("--- Reference tables"));
        assert!(output.contains("--- Sorting and table iteration"));
        assert!(output.contains("--- Table concatenation and copy"));
        assert!(output.contains("--- Column indexing"));
        #[cfg(unix)]
        assert!(output.contains("--- Table locking"));
        assert!(output.contains("--- Memory tables"));
        assert!(output.contains("--- TaQL (Table Query Language)"));
        assert!(output.contains("--- Tiled storage managers"));
        assert!(output.contains("--- Virtual column engines"));
        assert!(output.ends_with("end\n"));
    }

    #[test]
    fn table_quantum_demo_runs() {
        let output = super::run_table_quantum_demo().expect("demo should succeed");
        assert!(output.contains("Fixed-unit scalar quantum column"));
        assert!(output.contains("Variable-unit scalar quantum column"));
        assert!(output.contains("Fixed-unit array quantum column"));
        assert!(output.contains("TableQuantum Demo complete"));
    }
}
