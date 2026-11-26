// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bind the metadata lookup functions from casacore's UDFMSCal to stored table columns.

use casa_core::quanta::{Quantity, Unit};
use casa_core::{PrimitiveType, Value};

use crate::{ColumnSchema, ColumnType, Table, TableError, TableOptions, add_directory};

use super::ast::{ColumnRef, Expr, IndexStyle};
use super::error::TaqlError;
use super::eval::{ExprValue, eval_constant_string};

pub(super) fn bind(expr: &mut Expr, table: &Table, style: IndexStyle) -> Result<(), TaqlError> {
    expr.try_for_each_child_mut(&mut |child| bind(child, table, style))?;
    if let Expr::FunctionCall { name, args } = expr
        && let Some(lookup) = bind_lookup(name, args, table, style)?
    {
        *expr = lookup;
    }
    Ok(())
}

fn bind_lookup(
    name: &str,
    args: &mut [Expr],
    table: &Table,
    style: IndexStyle,
) -> Result<Option<Expr>, TaqlError> {
    let lower = name.to_ascii_lowercase();
    let Some(function) = lower
        .strip_prefix("mscal.")
        .or_else(|| lower.strip_prefix("derivedmscal."))
    else {
        return Ok(None);
    };
    let original = format!(
        "{name}({})",
        args.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );

    if function == "stokes" {
        for argument in args.iter_mut().skip(1) {
            super::mscal_help::fold_filter(argument, table, style)?;
        }
        return super::mscal_stokes::bind(name, original, args, table, style)
            .map(|function| Some(Expr::BoundRowFunction(function)));
    }

    if matches!(function, "uvwwvl" | "uvwwvls") {
        check_arity(name, args, 0)?;
        return super::mscal_wavelengths::bind(original, table, function == "uvwwvls")
            .map(|function| Some(Expr::BoundRowFunction(function)));
    }

    let (subtable, column, index, allow_missing) = if function == "subcol" {
        check_arity(name, args, 3)?;
        (
            constant_string(&args[0], name, style)?,
            constant_string(&args[1], name, style)?,
            args[2].clone(),
            false,
        )
    } else {
        let (subtable, id, fixed_column) = match function {
            "ant1name" => ("ANTENNA", "ANTENNA1", Some("NAME")),
            "ant2name" => ("ANTENNA", "ANTENNA2", Some("NAME")),
            "ant1col" => ("ANTENNA", "ANTENNA1", None),
            "ant2col" => ("ANTENNA", "ANTENNA2", None),
            "fieldcol" => ("FIELD", "FIELD_ID", None),
            "spwcol" => ("SPECTRAL_WINDOW", "SPECTRAL_WINDOW_ID", None),
            "polcol" => ("POLARIZATION", "POLARIZATION_ID", None),
            "statecol" => ("STATE", "STATE_ID", None),
            "proccol" => ("PROCESSOR", "PROCESSOR_ID", None),
            // The pinned CASA UDF looks up this literal keyword, including the space.
            "obscol" => (" OBSERVATION", "OBSERVATION_ID", None),
            _ => return Ok(None),
        };
        check_arity(name, args, usize::from(fixed_column.is_none()))?;
        let column = match fixed_column {
            Some(column) => column.to_owned(),
            None => constant_string(&args[0], name, style)?,
        };
        let index = match function {
            "spwcol" | "polcol" => {
                let dd = open_subtable(table, "DATA_DESCRIPTION")?;
                bind_column(&original, column_ref("DATA_DESC_ID"), &dd, id, false)?
            }
            _ => column_ref(id),
        };
        (
            subtable.to_owned(),
            column,
            index,
            matches!(function, "statecol" | "proccol"),
        )
    };

    let subtable = open_subtable(table, &subtable)?;
    let lookup = bind_column(&original, index, &subtable, &column, allow_missing)?;
    Ok(Some(lookup))
}

fn check_arity(name: &str, args: &[Expr], expected: usize) -> Result<(), TaqlError> {
    if args.len() != expected {
        return Err(TaqlError::ArgumentCount {
            name: name.to_owned(),
            expected: expected.to_string(),
            got: args.len(),
        });
    }
    Ok(())
}

fn constant_string(expr: &Expr, name: &str, style: IndexStyle) -> Result<String, TaqlError> {
    let value = eval_constant_string(expr, name, style)?;
    if value.is_empty() {
        return Err(TaqlError::TypeError {
            message: format!("{name} requires a nonempty metadata name"),
        });
    }
    Ok(value)
}

fn column_ref(name: &str) -> Expr {
    Expr::ColumnRef(ColumnRef {
        table: None,
        column: name.to_owned(),
    })
}

pub(super) fn open_subtable(table: &Table, name: &str) -> Result<Table, TaqlError> {
    let stored_path = table
        .keywords()
        .get(name)
        .and_then(Value::as_table_ref)
        .ok_or_else(|| TaqlError::Table(format!("subtable keyword {name:?} is missing")))?;
    let table_path = table.path().ok_or_else(|| {
        TaqlError::Table(format!(
            "cannot resolve subtable {name:?} without a table path"
        ))
    })?;
    Table::open(TableOptions::new(add_directory(stored_path, table_path))).map_err(table_error)
}

fn bind_column(
    original: &str,
    index: Expr,
    table: &Table,
    name: &str,
    allow_missing: bool,
) -> Result<Expr, TaqlError> {
    let schema = table
        .schema()
        .and_then(|schema| schema.column(name))
        .ok_or_else(|| TaqlError::ColumnNotFound {
            name: name.to_owned(),
        })?;
    if schema.column_type() == &ColumnType::Record {
        return Err(TaqlError::TypeError {
            message: format!("MSCAL metadata lookup cannot return record column {name:?}"),
        });
    }
    let column = table.column_accessor(name).map_err(table_error)?;
    let mut values: Vec<_> = match schema.column_type() {
        ColumnType::Scalar => column
            .scalar_cells_owned()
            .map_err(table_error)?
            .iter()
            .map(|value| value.as_ref().map_or(ExprValue::Null, ExprValue::from))
            .collect(),
        _ => column
            .array_cells_owned(&(0..table.row_count()).collect::<Vec<_>>())
            .map_err(table_error)?
            .into_iter()
            .map(|value| {
                value.map_or(ExprValue::Null, |array| {
                    ExprValue::from(&Value::Array(array))
                })
            })
            .collect(),
    };
    let mut missing = (allow_missing && schema.column_type() == &ColumnType::Scalar)
        .then(|| missing_value(schema));
    if let Some(unit) = column_unit(table, name)? {
        for value in values.iter_mut().chain(missing.iter_mut()) {
            apply_unit(value, &unit);
        }
    }
    Ok(Expr::BoundRowLookup {
        original: original.to_owned().into(),
        index: Box::new(index),
        values: values.into(),
        missing,
    })
}

fn column_unit(table: &Table, name: &str) -> Result<Option<Unit>, TaqlError> {
    use casa_core::{ArrayValue, ScalarValue};

    let Some(keywords) = table.column_keywords(name) else {
        return Ok(None);
    };
    // TableExprNodeColumn uses the first fixed unit or the legacy UNIT keyword.
    let unit = match keywords.get("QuantumUnits") {
        Some(Value::Array(ArrayValue::String(units))) => units.iter().next(),
        Some(_) => {
            return Err(TaqlError::TypeError {
                message: format!("QuantumUnits of metadata column {name:?} must be a string array"),
            });
        }
        None => match keywords.get("UNIT") {
            Some(Value::Scalar(ScalarValue::String(unit))) => Some(unit),
            _ => None,
        },
    };
    unit.filter(|unit| !unit.is_empty())
        .map(|unit| {
            Unit::new(unit).map_err(|err| TaqlError::TypeError {
                message: err.to_string(),
            })
        })
        .transpose()
}

fn apply_unit(value: &mut ExprValue, unit: &Unit) {
    match value {
        ExprValue::Float(number) => {
            *value = ExprValue::Quantity(Quantity::with_unit(*number, unit.clone()));
        }
        ExprValue::Int(number) => {
            *value = ExprValue::Quantity(Quantity::with_unit(*number as f64, unit.clone()));
        }
        ExprValue::Array(array) => {
            for element in &mut array.data {
                apply_unit(element, unit);
            }
        }
        _ => {}
    }
}

fn missing_value(schema: &ColumnSchema) -> ExprValue {
    match schema.data_type() {
        Some(PrimitiveType::Bool) => ExprValue::Bool(false),
        Some(PrimitiveType::String) => ExprValue::String(String::new()),
        Some(PrimitiveType::Float32 | PrimitiveType::Float64) => ExprValue::Float(0.),
        Some(PrimitiveType::Complex32 | PrimitiveType::Complex64) => ExprValue::Complex(0.0.into()),
        Some(_) => ExprValue::Int(0),
        None => ExprValue::Null,
    }
}

pub(super) fn table_error(err: TableError) -> TaqlError {
    TaqlError::Table(err.to_string())
}
