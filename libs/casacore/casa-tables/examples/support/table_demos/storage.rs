// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn demo_tiled_storage(out: &mut String) -> Result<(), TableError> {
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
            let arr = reopened.cell_accessor(i, "data")?.array()?;
            let base = (i * nelem) as f32;
            let expected: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let expected_arr =
                ndarray::Array::from_shape_vec(IxDyn(&[2, 3]).f(), expected).unwrap();
            if let ArrayValue::Float32(actual) = arr
                && *actual != expected_arr
            {
                appendln(out, &format!("  MISMATCH row {i}"));
                ok = false;
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
            let arr = reopened.cell_accessor(i, "data")?.array()?;
            let nelem: usize = shape.iter().product();
            let base = (i * 10) as f32;
            let expected: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let expected_arr = ndarray::Array::from_shape_vec(IxDyn(shape).f(), expected).unwrap();
            if let ArrayValue::Float32(actual) = arr
                && *actual != expected_arr
            {
                appendln(out, &format!("  MISMATCH row {i}"));
                ok = false;
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
            let arr = reopened.cell_accessor(i, "data")?.array()?;
            let nelem: usize = shape.iter().product();
            let base = (i * 10) as f32;
            let expected: Vec<f32> = (0..nelem).map(|k| base + k as f32).collect();
            let expected_arr = ndarray::Array::from_shape_vec(IxDyn(shape).f(), expected).unwrap();
            if let ArrayValue::Float32(actual) = arr
                && *actual != expected_arr
            {
                appendln(out, &format!("  MISMATCH row {i}"));
                ok = false;
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
pub(super) fn demo_virtual_columns(out: &mut String) -> Result<(), TableError> {
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
            let got_id = reopened.cell_accessor(i, "id")?.scalar()?;
            if *got_id != ScalarValue::Int32(*id) {
                ok = false;
            }
            let got_name = reopened.cell_accessor(i, "name")?.scalar()?;
            if *got_name != ScalarValue::String(name.to_string()) {
                ok = false;
            }
            let got_data = reopened.cell_accessor(i, "data")?.array()?;
            let expected = Array::from_shape_vec(IxDyn(&[3]), arr.to_vec()).unwrap();
            if let ArrayValue::Float32(v) = got_data
                && *v != expected
            {
                ok = false;
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
            let arr = reopened.cell_accessor(i, "virtual_col")?.array()?;
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
            let arr = reopened.cell_accessor(i, "virtual_col")?.array()?;
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
