// SPDX-License-Identifier: LGPL-3.0-or-later
//! TaQL result pretty-printer.
//!
//! Formats [`TaqlResult`] and table row data into human-readable text tables.
//!
//! # C++ equivalent
//!
//! The C++ `taql` command uses `TableProxy::toAscii()` for formatted output.

use casacore_types::{ArrayValue, RecordValue, ScalarValue, Value};

use super::TaqlResult;

/// Format a single [`Value`] as a display string.
///
/// Scalars are printed in their natural representation. Arrays use `Debug`
/// formatting. Records use `Debug` formatting.
pub fn format_value(val: &Value) -> String {
    match val {
        Value::Scalar(s) => match s {
            ScalarValue::Bool(b) => b.to_string(),
            ScalarValue::UInt8(v) => v.to_string(),
            ScalarValue::Int16(v) => v.to_string(),
            ScalarValue::UInt16(v) => v.to_string(),
            ScalarValue::Int32(v) => v.to_string(),
            ScalarValue::UInt32(v) => v.to_string(),
            ScalarValue::Int64(v) => v.to_string(),
            ScalarValue::Float32(v) => format!("{v:.6}"),
            ScalarValue::Float64(v) => format!("{v:.6}"),
            ScalarValue::Complex32(c) => format!("({:.6},{:.6})", c.re, c.im),
            ScalarValue::Complex64(c) => format!("({:.6},{:.6})", c.re, c.im),
            ScalarValue::String(s) => s.clone(),
        },
        Value::Array(arr) => format_array_value(arr),
        Value::Record(rec) => format!("{rec:?}"),
        Value::TableRef(path) => format!("table({path})"),
    }
}

/// Format an [`ArrayValue`] as `[val1, val2, ...]`, matching C++ casacore output.
///
/// Iterates in storage (column-major/Fortran) order to match C++ `getStorage()`.
fn format_array_value(arr: &ArrayValue) -> String {
    use std::fmt::Write;
    let mut out = String::from("[");

    /// Get a slice in storage (memory) order — column-major for Fortran-layout arrays.
    fn storage_order<T>(a: &ndarray::ArrayD<T>) -> &[T] {
        a.as_slice_memory_order()
            .expect("casacore arrays are always contiguous")
    }

    macro_rules! fmt_array {
        ($data:expr) => {
            for (i, v) in storage_order($data).iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let _ = write!(out, "{v}");
            }
        };
        ($data:expr, float) => {
            for (i, v) in storage_order($data).iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let _ = write!(out, "{v:.6}");
            }
        };
    }
    match arr {
        ArrayValue::Bool(a) => fmt_array!(a),
        ArrayValue::UInt8(a) => fmt_array!(a),
        ArrayValue::UInt16(a) => fmt_array!(a),
        ArrayValue::UInt32(a) => fmt_array!(a),
        ArrayValue::Int16(a) => fmt_array!(a),
        ArrayValue::Int32(a) => fmt_array!(a),
        ArrayValue::Int64(a) => fmt_array!(a),
        ArrayValue::Float32(a) => fmt_array!(a, float),
        ArrayValue::Float64(a) => fmt_array!(a, float),
        ArrayValue::Complex32(a) => {
            for (i, c) in storage_order(a).iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let _ = write!(out, "({:.6},{:.6})", c.re, c.im);
            }
        }
        ArrayValue::Complex64(a) => {
            for (i, c) in storage_order(a).iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let _ = write!(out, "({:.6},{:.6})", c.re, c.im);
            }
        }
        ArrayValue::String(a) => fmt_array!(a),
    }
    out.push(']');
    out
}

/// Format rows as a text table with the given column names.
///
/// Each row is a [`RecordValue`]; columns are extracted by name and formatted
/// into aligned columns with a header and separator line.
///
/// Returns a multi-line string. An empty row slice produces just the header.
pub fn format_rows(columns: &[String], rows: &[RecordValue]) -> String {
    if columns.is_empty() {
        return "(no columns)\n".to_string();
    }

    let mut col_widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    let mut cell_strings: Vec<Vec<String>> = Vec::with_capacity(rows.len());

    for row in rows {
        let mut row_cells = Vec::with_capacity(columns.len());
        for (ci, col) in columns.iter().enumerate() {
            let s = match row.get(col) {
                Some(val) => format_value(val),
                None => "".to_string(),
            };
            if s.len() > col_widths[ci] {
                col_widths[ci] = s.len();
            }
            row_cells.push(s);
        }
        cell_strings.push(row_cells);
    }

    // Cap column widths.
    for w in &mut col_widths {
        if *w > 40 {
            *w = 40;
        }
    }

    let mut out = String::new();

    // Header.
    let header: Vec<String> = columns
        .iter()
        .zip(&col_widths)
        .map(|(name, w)| format!("{:<width$}", name, width = *w))
        .collect();
    out.push_str(&header.join("  "));
    out.push('\n');

    // Separator.
    let sep: Vec<String> = col_widths.iter().map(|w| "-".repeat(*w)).collect();
    out.push_str(&sep.join("  "));
    out.push('\n');

    // Rows.
    for row_cells in &cell_strings {
        let line: Vec<String> = row_cells
            .iter()
            .zip(&col_widths)
            .map(|(cell, w)| {
                if cell.len() > *w {
                    format!("{}…", &cell[..*w - 1])
                } else {
                    format!("{:<width$}", cell, width = *w)
                }
            })
            .collect();
        out.push_str(&line.join("  "));
        out.push('\n');
    }

    let nrow = rows.len();
    out.push_str(&format!(
        "\n({nrow} row{})\n",
        if nrow == 1 { "" } else { "s" }
    ));

    out
}

/// Format a [`TaqlResult`] summary as a single line.
pub fn format_result(result: &TaqlResult) -> String {
    match result {
        TaqlResult::Select {
            row_indices,
            columns,
        } => {
            format!(
                "Selected {} row(s), {} column(s)",
                row_indices.len(),
                if columns.is_empty() {
                    "all".to_string()
                } else {
                    columns.len().to_string()
                },
            )
        }
        TaqlResult::Materialized { table } => {
            format!("Materialized result: {} row(s)", table.row_count())
        }
        TaqlResult::Update { rows_affected } => {
            format!("Updated {rows_affected} row(s)")
        }
        TaqlResult::Insert { rows_inserted } => {
            format!("Inserted {rows_inserted} row(s)")
        }
        TaqlResult::Delete { rows_deleted } => {
            format!("Deleted {rows_deleted} row(s)")
        }
        TaqlResult::Count { count } => {
            format!("Count: {count}")
        }
        TaqlResult::CreateTable { table_name } => {
            format!("Created table: {table_name}")
        }
        TaqlResult::DropTable { table_name } => {
            format!("Dropped table: {table_name}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casacore_types::RecordField;

    #[test]
    fn format_value_scalars() {
        assert_eq!(
            format_value(&Value::Scalar(ScalarValue::Bool(true))),
            "true"
        );
        assert_eq!(format_value(&Value::Scalar(ScalarValue::Int32(42))), "42");
        assert_eq!(
            format_value(&Value::Scalar(ScalarValue::Float64(1.5))),
            "1.500000"
        );
        assert_eq!(
            format_value(&Value::Scalar(ScalarValue::String("hello".into()))),
            "hello"
        );
    }

    #[test]
    fn format_rows_basic() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("Alice".into()))),
            ]),
            RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("name", Value::Scalar(ScalarValue::String("Bob".into()))),
            ]),
        ];

        let out = format_rows(&columns, &rows);
        assert!(out.contains("id"));
        assert!(out.contains("name"));
        assert!(out.contains("Alice"));
        assert!(out.contains("Bob"));
        assert!(out.contains("(2 rows)"));
    }

    #[test]
    fn format_rows_empty() {
        let columns = vec!["x".to_string()];
        let rows: Vec<RecordValue> = vec![];
        let out = format_rows(&columns, &rows);
        assert!(out.contains("(0 rows)"));
    }

    #[test]
    fn format_rows_no_columns() {
        let out = format_rows(&[], &[]);
        assert_eq!(out, "(no columns)\n");
    }

    #[test]
    fn format_result_select() {
        let r = TaqlResult::Select {
            row_indices: vec![0, 1, 2],
            columns: vec!["a".into()],
        };
        assert_eq!(format_result(&r), "Selected 3 row(s), 1 column(s)");
    }

    #[test]
    fn format_result_update() {
        let r = TaqlResult::Update { rows_affected: 5 };
        assert_eq!(format_result(&r), "Updated 5 row(s)");
    }

    #[test]
    fn format_result_count() {
        let r = TaqlResult::Count { count: 42 };
        assert_eq!(format_result(&r), "Count: 42");
    }
}
