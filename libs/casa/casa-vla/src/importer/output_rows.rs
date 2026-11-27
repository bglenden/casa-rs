// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn seconds_to_nanos(seconds: f64) -> u64 {
    if !seconds.is_finite() || seconds <= 0.0 {
        return 0;
    }
    (seconds * 1_000_000_000.0).min(u64::MAX as f64) as u64
}

#[allow(clippy::too_many_arguments)]
pub(super) fn push_import_scalar_row(
    writer: &mut MeasurementSetWriteSession,
    antenna1: i32,
    antenna2: i32,
    array_id: i32,
    data_desc_id: i32,
    exposure: f64,
    field_id: i32,
    flag_row: bool,
    observation_id: i32,
    scan_number: i32,
    time: f64,
) -> Result<(), VlaError> {
    for (column, value) in [
        ("ANTENNA1", ScalarValue::Int32(antenna1)),
        ("ANTENNA2", ScalarValue::Int32(antenna2)),
        ("ARRAY_ID", ScalarValue::Int32(array_id)),
        ("DATA_DESC_ID", ScalarValue::Int32(data_desc_id)),
        ("EXPOSURE", ScalarValue::Float64(exposure)),
        ("FEED1", ScalarValue::Int32(0)),
        ("FEED2", ScalarValue::Int32(0)),
        ("FIELD_ID", ScalarValue::Int32(field_id)),
        ("FLAG_ROW", ScalarValue::Bool(flag_row)),
        ("INTERVAL", ScalarValue::Float64(exposure)),
        ("OBSERVATION_ID", ScalarValue::Int32(observation_id)),
        ("PROCESSOR_ID", ScalarValue::Int32(-1)),
        ("SCAN_NUMBER", ScalarValue::Int32(scan_number)),
        ("STATE_ID", ScalarValue::Int32(-1)),
        ("TIME", ScalarValue::Float64(time)),
        ("TIME_CENTROID", ScalarValue::Float64(time)),
    ] {
        writer
            .push_scalar_row(column, Some(value))
            .map_err(|error| VlaError::import(error.to_string()))?;
    }
    Ok(())
}

pub(super) fn push_import_complex32_row(
    writer: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
) -> Result<(), VlaError> {
    let ArrayValue::Complex32(array) = value else {
        return Err(VlaError::import(format!(
            "MAIN.{column} is not a Complex32 array"
        )));
    };
    let values = array_values_in_casa_order(array);
    writer
        .push_complex32_row(column, array.shape(), &values)
        .map_err(|error| VlaError::import(error.to_string()))
}

pub(super) fn push_import_bool_row(
    writer: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
) -> Result<(), VlaError> {
    let ArrayValue::Bool(array) = value else {
        return Err(VlaError::import(format!(
            "MAIN.{column} is not a Bool array"
        )));
    };
    let values = array_values_in_casa_order(array);
    writer
        .push_bool_row(column, array.shape(), &values)
        .map_err(|error| VlaError::import(error.to_string()))
}

pub(super) fn push_import_f32_row(
    writer: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
) -> Result<(), VlaError> {
    let ArrayValue::Float32(array) = value else {
        return Err(VlaError::import(format!(
            "MAIN.{column} is not a Float32 array"
        )));
    };
    let values = array_values_in_casa_order(array);
    writer
        .push_f32_row(column, array.shape(), &values)
        .map_err(|error| VlaError::import(error.to_string()))
}

pub(super) fn array_values_in_casa_order<T: Copy>(array: &ArrayD<T>) -> Cow<'_, [T]> {
    let casa_order = array.view().reversed_axes();
    match casa_order.to_slice() {
        Some(values) => Cow::Borrowed(values),
        None => Cow::Owned(casa_order.iter().copied().collect()),
    }
}

pub(super) fn align_direction_for_epoch(
    source_direction: [f64; 2],
    source_epoch: DirectionEpoch,
    target_epoch: DirectionEpoch,
) -> Result<[f64; 2], VlaError> {
    if source_epoch == target_epoch {
        return Ok(source_direction);
    }
    Err(VlaError::import(format!(
        "mixed direction epochs are not implemented yet: source={source_epoch:?} target={target_epoch:?}"
    )))
}

pub(super) fn normalize_longitude_radians(value: f64) -> f64 {
    let two_pi = 2.0 * PI;
    let wrapped = (value + PI).rem_euclid(two_pi) - PI;
    if wrapped == -PI { PI } else { wrapped }
}

pub(super) fn direction_measure_ref(epoch: DirectionEpoch) -> &'static str {
    match epoch {
        DirectionEpoch::J2000 => "J2000",
        DirectionEpoch::B1950Vla => "B1950_VLA",
        DirectionEpoch::Apparent => "APP",
        DirectionEpoch::Unknown(_) => "J2000",
    }
}

pub(super) fn set_direction_measure_reference(
    table: &mut Table,
    column: &str,
    epoch: DirectionEpoch,
) -> Result<(), VlaError> {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    let Some(Value::Record(mut measinfo)) = keywords.get("MEASINFO").cloned() else {
        return Err(VlaError::import(format!(
            "{column} column is missing MEASINFO keywords"
        )));
    };
    measinfo.upsert(
        "Ref",
        Value::Scalar(ScalarValue::String(
            direction_measure_ref(epoch).to_string(),
        )),
    );
    keywords.upsert("MEASINFO", Value::Record(measinfo));
    table.set_column_keywords(column, keywords);
    Ok(())
}

pub(super) fn set_epoch_measure_reference(
    table: &mut Table,
    column: &str,
    epoch_ref: EpochRef,
) -> Result<(), VlaError> {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    let Some(Value::Record(mut measinfo)) = keywords.get("MEASINFO").cloned() else {
        return Err(VlaError::import(format!(
            "{column} column is missing MEASINFO keywords"
        )));
    };
    measinfo.upsert(
        "Ref",
        Value::Scalar(ScalarValue::String(epoch_ref.to_string())),
    );
    keywords.upsert("MEASINFO", Value::Record(measinfo));
    table.set_column_keywords(column, keywords);
    Ok(())
}

pub(super) fn set_flag_category_names(table: &mut Table) -> Result<(), VlaError> {
    let mut keywords = table
        .column_keywords("FLAG_CATEGORY")
        .cloned()
        .unwrap_or_default();
    keywords.upsert(
        "CATEGORY",
        Value::Array(ArrayValue::from_string_vec(
            FLAG_CATEGORY_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )),
    );
    table.set_column_keywords("FLAG_CATEGORY", keywords);
    Ok(())
}

pub(super) fn make_row_from_columns(
    columns: &[casa_ms::ColumnDef],
    overrides: &[(&str, Value)],
) -> Result<RecordValue, VlaError> {
    let column_names: Vec<&str> = columns.iter().map(|column| column.name).collect();
    let all_columns: Vec<_> = columns.iter().collect();
    make_row_from_names(&column_names, &all_columns, overrides)
}

pub(super) fn make_row_from_names(
    schema_column_names: &[&str],
    all_columns: &[&casa_ms::ColumnDef],
    overrides: &[(&str, Value)],
) -> Result<RecordValue, VlaError> {
    let mut fields = Vec::with_capacity(schema_column_names.len());
    for &column_name in schema_column_names {
        if let Some((_, value)) = overrides.iter().find(|(name, _)| *name == column_name) {
            fields.push(RecordField::new(column_name, value.clone()));
            continue;
        }
        let definition = all_columns
            .iter()
            .copied()
            .find(|candidate| candidate.name == column_name)
            .ok_or_else(|| {
                VlaError::import(format!("missing column definition for {column_name}"))
            })?;
        fields.push(RecordField::new(
            column_name,
            default_value_for_column(definition)?,
        ));
    }
    Ok(RecordValue::new(fields))
}

pub(super) fn default_value_for_column(column: &casa_ms::ColumnDef) -> Result<Value, VlaError> {
    use casa_core::PrimitiveType;
    use casa_ms::ColumnKind;

    Ok(match column.column_kind {
        ColumnKind::Scalar => Value::Scalar(match column.data_type {
            PrimitiveType::Int32 => ScalarValue::Int32(0),
            PrimitiveType::Float32 => ScalarValue::Float32(0.0),
            PrimitiveType::Float64 => ScalarValue::Float64(0.0),
            PrimitiveType::Bool => ScalarValue::Bool(false),
            PrimitiveType::String => ScalarValue::String(String::new()),
            _ => {
                return Err(VlaError::import(format!(
                    "no default scalar value for {}",
                    column.name
                )));
            }
        }),
        ColumnKind::FixedArray { shape } => {
            let total: usize = shape.iter().product();
            Value::Array(match column.data_type {
                PrimitiveType::Float64 => ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(shape).f(), vec![0.0; total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Complex32 => ArrayValue::Complex32(
                    ArrayD::from_shape_vec(IxDyn(shape).f(), vec![Complex32::new(0.0, 0.0); total])
                        .map_err(|error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        })?,
                ),
                _ => {
                    return Err(VlaError::import(format!(
                        "no default fixed-array value for {}",
                        column.name
                    )));
                }
            })
        }
        ColumnKind::VariableArray { ndim } => {
            let shape = vec![1; ndim];
            let total: usize = shape.iter().product();
            Value::Array(match column.data_type {
                PrimitiveType::Bool => ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![false; total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Float32 => ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![0.0; total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Float64 => ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![0.0; total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Int32 => ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![0; total]).map_err(|error| {
                        VlaError::import(format!("shape default array {}: {error}", column.name))
                    })?,
                ),
                PrimitiveType::String => ArrayValue::String(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![String::new(); total]).map_err(
                        |error| {
                            VlaError::import(format!(
                                "shape default array {}: {error}",
                                column.name
                            ))
                        },
                    )?,
                ),
                PrimitiveType::Complex32 => ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        IxDyn(&shape).f(),
                        vec![Complex32::new(0.0, 0.0); total],
                    )
                    .map_err(|error| {
                        VlaError::import(format!("shape default array {}: {error}", column.name))
                    })?,
                ),
                _ => {
                    return Err(VlaError::import(format!(
                        "no default variable-array value for {}",
                        column.name
                    )));
                }
            })
        }
    })
}
