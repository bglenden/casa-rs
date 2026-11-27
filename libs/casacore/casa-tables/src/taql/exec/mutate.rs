// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Execute an UPDATE statement, returning both summary count and modified row indices.
pub(super) fn execute_update_with_rows(
    upd: &UpdateStatement,
    table: &mut crate::Table,
) -> Result<TaqlExecution, TaqlError> {
    let row_count = table.row_count();
    let style = upd.style;

    // Find matching rows
    let mut matching_rows: Vec<usize> = if let Some(ref where_clause) = upd.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            let row = table
                .row_accessor()
                .row(i)
                .map_err(|err| TaqlError::Table(err.to_string()))?;
            let ctx = EvalContext {
                row,
                row_index: i,
                style,
                measures: table.measures_provider(),
            };
            let val = eval_expr(where_clause, &ctx)?;
            if val.to_bool()? {
                indices.push(i);
            }
        }
        indices
    } else {
        (0..row_count).collect()
    };

    // Apply LIMIT
    if let Some(ref limit_expr) = upd.limit {
        let limit = eval_const_int(limit_expr)? as usize;
        matching_rows.truncate(limit);
    }

    // Evaluate all RHS expressions before applying (correct UPDATE semantics)
    let mut updates: Vec<Vec<(String, Value)>> = Vec::with_capacity(matching_rows.len());
    for &row_idx in &matching_rows {
        let row = table
            .row_accessor()
            .row(row_idx)
            .map_err(|err| TaqlError::Table(err.to_string()))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
            style,
            measures: table.measures_provider(),
        };
        let mut row_updates = Vec::with_capacity(upd.assignments.len());
        for assignment in &upd.assignments {
            let val = eval_expr(&assignment.value, &ctx)?;
            let value = expr_value_to_table_value(&val, table, &assignment.column)?;
            row_updates.push((assignment.column.clone(), value));
        }
        updates.push(row_updates);
    }

    // Apply updates
    let count = matching_rows.len();
    for (&row_idx, row_updates) in matching_rows.iter().zip(updates) {
        for (column, value) in row_updates {
            table
                .cell_accessor_mut(row_idx, &column)
                .and_then(|mut cell| cell.set(value))
                .map_err(|e| TaqlError::Table(e.to_string()))?;
        }
    }

    Ok(TaqlExecution {
        result: TaqlResult::Update {
            rows_affected: count,
        },
        affected_rows: matching_rows,
    })
}

/// Execute an INSERT statement, returning inserted row indices.
pub(super) fn execute_insert_with_rows(
    ins: &InsertStatement,
    table: &mut crate::Table,
) -> Result<TaqlExecution, TaqlError> {
    let schema_columns: Vec<String> = if !ins.columns.is_empty() {
        ins.columns.clone()
    } else {
        // Use all columns from schema
        table
            .schema()
            .map(|s| s.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default()
    };

    let start_row = table.row_count();
    let mut count = 0;
    for value_row in &ins.values {
        if !ins.columns.is_empty() && value_row.len() != ins.columns.len() {
            return Err(TaqlError::InsertColumnMismatch {
                columns: ins.columns.len(),
                values: value_row.len(),
            });
        }

        // Build a RecordValue for the row
        let empty_row = RecordValue::new(vec![]);
        let ctx = EvalContext {
            row: &empty_row,
            row_index: 0,
            style: ins.style,
            measures: table.measures_provider(),
        };

        let mut fields = Vec::new();
        for (i, expr) in value_row.iter().enumerate() {
            let val = eval_expr(expr, &ctx)?;
            let col_name = schema_columns
                .get(i)
                .ok_or(TaqlError::InsertColumnMismatch {
                    columns: schema_columns.len(),
                    values: value_row.len(),
                })?;
            let value = expr_value_to_table_value(&val, table, col_name)?;
            fields.push(RecordField::new(col_name.as_str(), value));
        }

        append_omitted_schema_defaults(table, &mut fields);

        table
            .add_row(RecordValue::new(fields))
            .map_err(|e| TaqlError::Table(e.to_string()))?;
        count += 1;
    }

    let affected_rows = (start_row..table.row_count()).collect();
    Ok(TaqlExecution {
        result: TaqlResult::Insert {
            rows_inserted: count,
        },
        affected_rows,
    })
}

/// Execute a DELETE statement.
pub(super) fn execute_delete(
    del: &DeleteStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let row_count = table.row_count();

    // Find matching rows
    let mut matching_rows: Vec<usize> = if let Some(ref where_clause) = del.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            let row = table
                .row_accessor()
                .row(i)
                .map_err(|err| TaqlError::Table(err.to_string()))?;
            let ctx = EvalContext {
                row,
                row_index: i,
                style: del.style,
                measures: table.measures_provider(),
            };
            let val = eval_expr(where_clause, &ctx)?;
            if val.to_bool()? {
                indices.push(i);
            }
        }
        indices
    } else {
        (0..row_count).collect()
    };

    // Apply LIMIT
    if let Some(ref limit_expr) = del.limit {
        let limit = eval_const_int(limit_expr)? as usize;
        matching_rows.truncate(limit);
    }

    // Sort and deduplicate (remove_rows requires sorted, unique indices)
    matching_rows.sort_unstable();
    matching_rows.dedup();

    let count = matching_rows.len();
    table
        .remove_rows(&matching_rows)
        .map_err(|e| TaqlError::Table(e.to_string()))?;

    Ok(TaqlResult::Delete {
        rows_deleted: count,
    })
}

/// Execute a `COUNT (col, ...) FROM ...` statement, returning a materialized table
/// with each grouping expression and a `_COUNT_` column.
pub(super) fn execute_count(
    cnt: &CountStatement,
    table: &crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let mut select_cols: Vec<SelectColumn> = cnt
        .columns
        .iter()
        .cloned()
        .map(|expr| SelectColumn { expr, alias: None })
        .collect();
    select_cols.push(SelectColumn {
        expr: Expr::Aggregate {
            func: AggregateFunc::Count,
            arg: Box::new(Expr::Star),
        },
        alias: Some("_COUNT_".to_string()),
    });
    let synthetic = SelectStatement {
        columns: select_cols,
        from: cnt.from.clone(),
        joins: Vec::new(),
        where_clause: cnt.where_clause.clone(),
        group_by: cnt.columns.clone(),
        having: None,
        order_by: Vec::new(),
        order_distinct: false,
        limit: None,
        offset: None,
        distinct: false,
        style: cnt.style,
        giving: None,
        giving_expr: None,
    };
    execute_group_by(&synthetic, table)
}

/// Execute a CALC statement.
///
/// Evaluates the expression in the context of the first row (if available)
/// and returns a single-row, single-column result.
pub(super) fn execute_calc(
    calc: &CalcStatement,
    table: &crate::Table,
) -> Result<TaqlResult, TaqlError> {
    // Use row 0 if the table has rows, otherwise an empty context
    let empty_row = casa_core::RecordValue::default();
    let row = if table.row_count() > 0 {
        table.row_accessor().row(0).unwrap_or(&empty_row)
    } else {
        &empty_row
    };
    let ctx = EvalContext {
        row,
        row_index: 0,
        style: calc.style,
        measures: table.measures_provider(),
    };
    let _val = eval_expr(&calc.expr, &ctx)?;

    // CALC returns the source row as context; report row 0 as the result row.
    Ok(TaqlResult::Select {
        columns: vec!["result".to_string()],
        row_indices: if table.row_count() > 0 {
            vec![0]
        } else {
            vec![]
        },
    })
}

/// Execute an ALTER TABLE statement.
pub(super) fn execute_alter_table(
    alt: &AlterTableStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    use casa_core::{PrimitiveType, ScalarValue, Value};

    match &alt.operation {
        AlterOperation::AddColumn { name, data_type } => {
            let ptype = match data_type.to_lowercase().as_str() {
                "bool" | "boolean" => PrimitiveType::Bool,
                "int16" | "short" => PrimitiveType::Int16,
                "int32" | "int" | "integer" => PrimitiveType::Int32,
                "int64" | "long" => PrimitiveType::Int64,
                "float32" | "float" => PrimitiveType::Float32,
                "float64" | "double" => PrimitiveType::Float64,
                "complex" | "complex32" => PrimitiveType::Complex32,
                "dcomplex" | "complex64" => PrimitiveType::Complex64,
                "string" | "text" => PrimitiveType::String,
                other => {
                    return Err(TaqlError::TypeError {
                        message: format!("unknown data type '{other}'"),
                    });
                }
            };
            let col = crate::ColumnSchema::scalar(name, ptype);
            let default_val = default_value_for_type(ptype);
            table
                .add_column(col, Some(default_val))
                .map_err(|e| TaqlError::Table(e.to_string()))?;
            Ok(TaqlResult::Update { rows_affected: 0 })
        }
        AlterOperation::DropColumn { name } => {
            table
                .remove_column(name)
                .map_err(|e| TaqlError::Table(e.to_string()))?;
            Ok(TaqlResult::Update { rows_affected: 0 })
        }
        AlterOperation::RenameColumn { old_name, new_name } => {
            table
                .rename_column(old_name, new_name)
                .map_err(|e| TaqlError::Table(e.to_string()))?;
            Ok(TaqlResult::Update { rows_affected: 0 })
        }
        AlterOperation::AddRow { count } => {
            let n = match count {
                Some(expr) => eval_const_int(expr)? as usize,
                None => 1,
            };
            for _ in 0..n {
                // Add a row with default values for each column
                let fields: Vec<casa_core::RecordField> = table
                    .schema()
                    .map(|s| s.columns())
                    .unwrap_or(&[])
                    .iter()
                    .filter_map(|col: &crate::ColumnSchema| {
                        col.data_type().map(|pt| {
                            let default_val = default_value_for_type(pt);
                            casa_core::RecordField::new(col.name(), default_val)
                        })
                    })
                    .collect();
                table
                    .add_row(casa_core::RecordValue::new(fields))
                    .map_err(|e| TaqlError::Table(e.to_string()))?;
            }
            Ok(TaqlResult::Insert { rows_inserted: n })
        }
        AlterOperation::SetKeyword { name, value } => {
            let empty_row = casa_core::RecordValue::default();
            let ctx = EvalContext {
                row: &empty_row,
                row_index: 0,
                style: ast::IndexStyle::default(),
                measures: table.measures_provider(),
            };
            let val = eval_expr(value, &ctx)?;
            let kw_val = match val {
                ExprValue::Bool(b) => Value::Scalar(ScalarValue::Bool(b)),
                ExprValue::Int(n) => Value::Scalar(ScalarValue::Int64(n)),
                ExprValue::Float(v) => Value::Scalar(ScalarValue::Float64(v)),
                ExprValue::Quantity(q) => Value::Scalar(ScalarValue::Float64(q.value())),
                ExprValue::String(s) => Value::Scalar(ScalarValue::String(s)),
                _ => {
                    return Err(TaqlError::TypeError {
                        message: "keyword value must be a scalar".to_string(),
                    });
                }
            };
            table.keywords_mut().upsert(name, kw_val);
            Ok(TaqlResult::Update { rows_affected: 0 })
        }
    }
}

/// Execute a CREATE TABLE statement.
///
/// This creates a new in-memory table with the specified schema. The resulting
/// table is not persisted — callers should use `Table::create_new` for disk
/// tables. The return value confirms the schema was valid.
pub(super) fn execute_create_table(ct: &CreateTableStatement) -> Result<TaqlResult, TaqlError> {
    // Validate all column types
    for col_def in &ct.columns {
        parse_data_type(&col_def.data_type)?;
    }

    Ok(TaqlResult::CreateTable {
        table_name: ct.table_name.clone(),
    })
}

/// Execute a DROP TABLE statement.
///
/// This validates the statement but does not perform filesystem operations.
/// Actual table deletion requires filesystem access via `Table::delete`.
pub(super) fn execute_drop_table(dt: &DropTableStatement) -> Result<TaqlResult, TaqlError> {
    Ok(TaqlResult::DropTable {
        table_name: dt.table_name.clone(),
    })
}

/// Parse a data type string into a PrimitiveType.
pub(super) fn parse_data_type(type_str: &str) -> Result<casa_core::PrimitiveType, TaqlError> {
    use casa_core::PrimitiveType;
    match type_str.to_lowercase().as_str() {
        "bool" | "boolean" => Ok(PrimitiveType::Bool),
        "int16" | "short" => Ok(PrimitiveType::Int16),
        "int32" | "int" | "integer" => Ok(PrimitiveType::Int32),
        "int64" | "long" => Ok(PrimitiveType::Int64),
        "float32" | "float" => Ok(PrimitiveType::Float32),
        "float64" | "double" => Ok(PrimitiveType::Float64),
        "complex" | "complex32" => Ok(PrimitiveType::Complex32),
        "dcomplex" | "complex64" => Ok(PrimitiveType::Complex64),
        "string" | "text" => Ok(PrimitiveType::String),
        other => Err(TaqlError::TypeError {
            message: format!("unknown data type '{other}'"),
        }),
    }
}

/// Return a default value for a given primitive type.
pub(super) fn default_value_for_type(ptype: casa_core::PrimitiveType) -> casa_core::Value {
    use casa_core::{PrimitiveType as PT, ScalarValue as SV, Value};
    match ptype {
        PT::Bool => Value::Scalar(SV::Bool(false)),
        PT::UInt8 => Value::Scalar(SV::UInt8(0)),
        PT::UInt16 => Value::Scalar(SV::UInt16(0)),
        PT::UInt32 => Value::Scalar(SV::UInt32(0)),
        PT::Int16 => Value::Scalar(SV::Int16(0)),
        PT::Int32 => Value::Scalar(SV::Int32(0)),
        PT::Int64 => Value::Scalar(SV::Int64(0)),
        PT::Float32 => Value::Scalar(SV::Float32(0.0)),
        PT::Float64 => Value::Scalar(SV::Float64(0.0)),
        PT::Complex32 => Value::Scalar(SV::Complex32(num_complex::Complex32::new(0.0, 0.0))),
        PT::Complex64 => Value::Scalar(SV::Complex64(num_complex::Complex64::new(0.0, 0.0))),
        PT::String => Value::Scalar(SV::String(String::new())),
    }
}

/// Return a zero-initialized Fortran-layout array value for a fixed-shape column.
pub(super) fn default_array_for_type(ptype: casa_core::PrimitiveType, shape: &[usize]) -> Value {
    use casa_core::{ArrayValue as AV, PrimitiveType as PT};
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    let fshape = IxDyn(shape).f();
    let av = match ptype {
        PT::Bool => AV::Bool(ArrayD::from_elem(fshape, false)),
        PT::UInt8 => AV::UInt8(ArrayD::zeros(fshape)),
        PT::UInt16 => AV::UInt16(ArrayD::zeros(fshape)),
        PT::UInt32 => AV::UInt32(ArrayD::zeros(fshape)),
        PT::Int16 => AV::Int16(ArrayD::zeros(fshape)),
        PT::Int32 => AV::Int32(ArrayD::zeros(fshape)),
        PT::Int64 => AV::Int64(ArrayD::zeros(fshape)),
        PT::Float32 => AV::Float32(ArrayD::zeros(fshape)),
        PT::Float64 => AV::Float64(ArrayD::zeros(fshape)),
        PT::Complex32 => AV::Complex32(ArrayD::zeros(fshape)),
        PT::Complex64 => AV::Complex64(ArrayD::zeros(fshape)),
        PT::String => AV::String(ArrayD::from_elem(fshape, String::new())),
    };
    Value::Array(av)
}

/// Populate omitted schema columns for `INSERT INTO` rows.
///
/// Scalar, fixed-shape array, and record columns receive schema-valid defaults;
/// variable-shape array columns remain omitted so inserted cells stay undefined,
/// matching CASA C++ `INSERT` semantics.
pub(super) fn append_omitted_schema_defaults(table: &crate::Table, fields: &mut Vec<RecordField>) {
    use crate::schema::{ArrayShapeContract, ColumnType};

    let Some(schema) = table.schema() else {
        return;
    };
    for col in schema.columns() {
        if fields.iter().any(|f| f.name == col.name()) {
            continue;
        }
        match col.column_type() {
            ColumnType::Scalar => {
                if let Some(ptype) = col.data_type() {
                    fields.push(RecordField::new(col.name(), default_value_for_type(ptype)));
                }
            }
            ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
                if let Some(ptype) = col.data_type() {
                    fields.push(RecordField::new(
                        col.name(),
                        default_array_for_type(ptype, shape),
                    ));
                }
            }
            ColumnType::Record => {
                fields.push(RecordField::new(
                    col.name(),
                    Value::Record(RecordValue::default()),
                ));
            }
            ColumnType::Array(ArrayShapeContract::Variable { .. }) => {}
        }
    }
}
