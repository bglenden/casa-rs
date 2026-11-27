// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Return the stored FIELD phase-direction measure without changing its frame.
///
/// Storage-owned spectral-range evaluation needs the original measure reference
/// because CASA applies dynamic references such as ITRF at every selected row
/// epoch instead of freezing them at the FIELD reference epoch.
pub fn raw_field_phase_direction(ms: &MeasurementSet, field_id: usize) -> MsResult<MDirection> {
    let field = ms.field()?;
    let raw = field.phase_dir(field_id)?;
    let (longitude, latitude) = phase_dir_constant(raw)?;
    let reference = resolve_direction_reference(field.table(), "FIELD", "PHASE_DIR", field_id)?;
    Ok(MDirection::from_angles(longitude, latitude, reference))
}

/// Return the stored FIELD reference direction in its original measure frame.
pub fn raw_field_reference_direction(ms: &MeasurementSet, field_id: usize) -> MsResult<MDirection> {
    let field = ms.field()?;
    let (longitude, latitude) = phase_dir_constant(field.reference_dir(field_id)?)?;
    let reference = resolve_direction_reference(field.table(), "FIELD", "REFERENCE_DIR", field_id)?;
    Ok(MDirection::from_angles(longitude, latitude, reference))
}

/// The epoch of `field_id`'s FIELD row, its `TIME` in that column's
/// reference (UTC when it names none): the origin of its direction
/// polynomial and the time casacore evaluates its ephemeris at when asked
/// for its direction at no time
/// ([`MsCalEngine::moving_field_reference_direction`]).
pub(super) fn field_reference_epoch(ms: &MeasurementSet, field_id: usize) -> MsResult<MEpoch> {
    let field = ms.field()?;
    Ok(MEpoch::from_mjd(
        selected_f64_value(field.table(), field_id, "TIME")? / 86_400.0,
        detect_epoch_reference(field.table(), "TIME", EpochRef::UTC),
    ))
}

pub(super) enum BorrowedMeasureReference<'a> {
    Fixed(&'a str),
    VariableInt {
        ref_column: &'a str,
        tab_ref_types: &'a ArrayD<String>,
        tab_ref_codes: BorrowedReferenceCodes<'a>,
    },
    VariableString {
        ref_column: &'a str,
    },
}

#[derive(Clone, Copy)]
pub(super) enum BorrowedReferenceCodes<'a> {
    Int32(&'a ArrayD<i32>),
    UInt32(&'a ArrayD<u32>),
    Int64(&'a ArrayD<i64>),
}

impl BorrowedReferenceCodes<'_> {
    fn position(self, code: i32) -> Option<usize> {
        match self {
            Self::Int32(values) => values.iter().position(|candidate| *candidate == code),
            Self::UInt32(values) => values
                .iter()
                .position(|candidate| *candidate as i32 == code),
            Self::Int64(values) => values
                .iter()
                .position(|candidate| *candidate as i32 == code),
        }
    }
}

pub(super) fn borrowed_measure_reference<'a>(
    table: &'a Table,
    column: &str,
) -> Option<BorrowedMeasureReference<'a>> {
    let Value::Record(measinfo) = table.column_keywords(column)?.get("MEASINFO")? else {
        return None;
    };
    let Value::Scalar(ScalarValue::String(measure_type)) = measinfo.get("type")? else {
        return None;
    };
    if ![
        "epoch",
        "direction",
        "position",
        "frequency",
        "doppler",
        "radialvelocity",
    ]
    .iter()
    .any(|candidate| measure_type.eq_ignore_ascii_case(candidate))
    {
        return None;
    }

    if let Some(Value::Scalar(ScalarValue::String(ref_column))) = measinfo.get("VarRefCol") {
        let tab_ref_types = match measinfo.get("TabRefTypes") {
            Some(Value::Array(ArrayValue::String(values))) => Some(values),
            _ => None,
        };
        let tab_ref_codes = match measinfo.get("TabRefCodes") {
            Some(Value::Array(ArrayValue::Int32(values))) => {
                Some(BorrowedReferenceCodes::Int32(values))
            }
            Some(Value::Array(ArrayValue::UInt32(values))) => {
                Some(BorrowedReferenceCodes::UInt32(values))
            }
            Some(Value::Array(ArrayValue::Int64(values))) => {
                Some(BorrowedReferenceCodes::Int64(values))
            }
            _ => None,
        };
        return match (tab_ref_types, tab_ref_codes) {
            (Some(tab_ref_types), Some(tab_ref_codes)) => {
                Some(BorrowedMeasureReference::VariableInt {
                    ref_column,
                    tab_ref_types,
                    tab_ref_codes,
                })
            }
            _ => Some(BorrowedMeasureReference::VariableString { ref_column }),
        };
    }
    match measinfo.get("Ref") {
        Some(Value::Scalar(ScalarValue::String(refer))) => {
            Some(BorrowedMeasureReference::Fixed(refer))
        }
        _ => None,
    }
}

pub(super) fn detect_time_reference(ms: &MeasurementSet) -> EpochRef {
    detect_epoch_reference(ms.main_table(), "TIME", EpochRef::UTC)
}

/// Detect the epoch reference frame from a table column's measure metadata.
pub fn detect_epoch_reference(table: &Table, column: &str, default: EpochRef) -> EpochRef {
    let Some(reference) = borrowed_measure_reference(table, column) else {
        return default;
    };
    match reference {
        BorrowedMeasureReference::Fixed(refer) => refer.parse::<EpochRef>().unwrap_or(default),
        BorrowedMeasureReference::VariableInt { .. }
        | BorrowedMeasureReference::VariableString { .. } => default,
    }
}

pub(super) fn phase_dir_constant(dir: &ArrayValue) -> MsResult<(f64, f64)> {
    match dir {
        ArrayValue::Float64(arr) => {
            let shape = arr.shape();
            if shape.len() != 2 || shape[0] != 2 || shape[1] == 0 {
                return Err(MsError::ColumnTypeMismatch {
                    column: "PHASE_DIR".to_string(),
                    table: "FIELD".to_string(),
                    expected: "Float64 array shaped [2, nPoly+1]".to_string(),
                    found: format!("Float64 array with shape {shape:?}"),
                });
            }
            Ok((arr[[0, 0]], arr[[1, 0]]))
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: "PHASE_DIR".to_string(),
            table: "FIELD".to_string(),
            expected: "Float64 array shaped [2, nPoly+1]".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

pub(super) fn selected_scalar_value(
    table: &Table,
    row: usize,
    column: &str,
) -> MsResult<ScalarValue> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(&[row])?
        .pop()
        .flatten()
        .ok_or_else(|| {
            MsError::InvalidInput(format!(
                "required selected metadata {column} row {row} is undefined"
            ))
        })
}

pub(super) fn selected_string_value(table: &Table, row: usize, column: &str) -> MsResult<String> {
    match selected_scalar_value(table, row, column)? {
        ScalarValue::String(value) => Ok(value),
        other => Err(MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: "metadata".to_string(),
            expected: "String scalar".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

pub(super) fn selected_f64_value(table: &Table, row: usize, column: &str) -> MsResult<f64> {
    match selected_scalar_value(table, row, column)? {
        ScalarValue::Float64(value) => Ok(value),
        other => Err(MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: "metadata".to_string(),
            expected: "Float64 scalar".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

/// Resolve the direction reference frame for a selected row without reading all rows.
pub fn resolve_direction_reference_selected(
    table: &Table,
    table_name: &str,
    column: &str,
    row: usize,
) -> MsResult<DirectionRef> {
    let Some(description) = borrowed_measure_reference(table, column) else {
        return Ok(DirectionRef::J2000);
    };
    let reference = match description {
        BorrowedMeasureReference::Fixed(refer) => Cow::Borrowed(refer),
        BorrowedMeasureReference::VariableInt {
            ref_column,
            tab_ref_types,
            tab_ref_codes,
        } => {
            let code = match selected_scalar_value(table, row, ref_column)? {
                ScalarValue::Int32(value) => value,
                ScalarValue::Int64(value) => {
                    i32::try_from(value).map_err(|_| MsError::ColumnTypeMismatch {
                        column: ref_column.to_string(),
                        table: table_name.to_string(),
                        expected: "an integer direction reference in the Int32 domain".to_string(),
                        found: value.to_string(),
                    })?
                }
                ScalarValue::UInt32(value) => {
                    i32::try_from(value).map_err(|_| MsError::ColumnTypeMismatch {
                        column: ref_column.to_string(),
                        table: table_name.to_string(),
                        expected: "an integer direction reference in the Int32 domain".to_string(),
                        found: value.to_string(),
                    })?
                }
                other => {
                    return Err(MsError::ColumnTypeMismatch {
                        column: ref_column.to_string(),
                        table: table_name.to_string(),
                        expected: "Int scalar".to_string(),
                        found: format!("{other:?}"),
                    });
                }
            };
            let Some(index) = tab_ref_codes.position(code) else {
                return Err(MsError::InvalidMeasureCode {
                    table: table_name.to_string(),
                    column: ref_column.to_string(),
                    code,
                });
            };
            tab_ref_types
                .iter()
                .nth(index)
                .map(|reference| Cow::Borrowed(reference.as_str()))
                .ok_or_else(|| MsError::ColumnTypeMismatch {
                    column: column.to_string(),
                    table: table_name.to_string(),
                    expected: "TabRefTypes index in bounds".to_string(),
                    found: format!("missing entry for TabRefCodes[{index}]"),
                })?
        }
        BorrowedMeasureReference::VariableString { ref_column } => {
            Cow::Owned(selected_string_value(table, row, ref_column)?)
        }
    };
    reference
        .parse::<DirectionRef>()
        .map_err(|_| MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: table_name.to_string(),
            expected: "a supported direction reference".to_string(),
            found: reference.into_owned(),
        })
}

pub(super) fn resolve_observatory_position(
    ms: &MeasurementSet,
    antenna_positions: &[MPosition],
    measures: &dyn MeasuresProvider,
) -> MPosition {
    let telescope = ms.observation().ok().and_then(|observation| {
        (0..observation.row_count()).find_map(|row| observation.string(row, "TELESCOPE_NAME").ok())
    });
    MPosition::casa_array_reference(telescope.as_deref(), antenna_positions.first(), measures)
}

pub(super) fn resolve_field_phase_direction_j2000_with_observatory(
    dir: MDirection,
    field: &crate::subtables::MsField<'_>,
    field_id: usize,
    observatory_position: &MPosition,
    measures: Arc<dyn MeasuresProvider>,
) -> MsResult<MDirection> {
    if dir.refer() == DirectionRef::J2000 {
        return Ok(dir);
    }
    let epoch_ref = detect_epoch_reference(field.table(), "TIME", EpochRef::UTC);
    let epoch = MEpoch::from_mjd(field.time(field_id)? / 86400.0, epoch_ref);
    let frame = MeasFrame::new()
        .with_epoch(epoch)
        .with_position(observatory_position.clone())
        .with_measures(measures);
    Ok(dir.convert_to(DirectionRef::J2000, &frame)?)
}

/// Resolve the direction reference frame for one row of a table direction column.
pub fn resolve_direction_reference(
    table: &Table,
    table_name: &str,
    column: &str,
    row: usize,
) -> MsResult<DirectionRef> {
    let Some(desc) = TableMeasDesc::reconstruct(table, column) else {
        return Ok(DirectionRef::J2000);
    };
    let refer = resolve_reference_string(table, table_name, &desc, row)?;
    refer
        .parse::<DirectionRef>()
        .map_err(|_| MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: table_name.to_string(),
            expected: "a supported direction reference".to_string(),
            found: refer,
        })
}

pub(super) fn resolve_reference_string(
    table: &Table,
    table_name: &str,
    desc: &TableMeasDesc,
    row: usize,
) -> MsResult<String> {
    match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => Ok(refer.clone()),
        MeasRefDesc::VariableInt {
            ref_column,
            tab_ref_types,
            tab_ref_codes,
        } => {
            let code = match table.cell_accessor(row, ref_column)?.scalar()? {
                &ScalarValue::Int32(value) => value,
                &ScalarValue::Int64(value) => {
                    i32::try_from(value).map_err(|_| MsError::ColumnTypeMismatch {
                        column: ref_column.clone(),
                        table: table_name.to_string(),
                        expected: "an integer direction reference in the Int32 domain".to_string(),
                        found: value.to_string(),
                    })?
                }
                &ScalarValue::UInt32(value) => {
                    i32::try_from(value).map_err(|_| MsError::ColumnTypeMismatch {
                        column: ref_column.clone(),
                        table: table_name.to_string(),
                        expected: "an integer direction reference in the Int32 domain".to_string(),
                        found: value.to_string(),
                    })?
                }
                other => {
                    return Err(MsError::ColumnTypeMismatch {
                        column: ref_column.clone(),
                        table: table_name.to_string(),
                        expected: "Int scalar".to_string(),
                        found: format!("{other:?}"),
                    });
                }
            };
            for (index, candidate) in tab_ref_codes.iter().enumerate() {
                if *candidate == code {
                    return tab_ref_types.get(index).cloned().ok_or_else(|| {
                        MsError::ColumnTypeMismatch {
                            column: desc.column_name().to_string(),
                            table: table_name.to_string(),
                            expected: "TabRefTypes index in bounds".to_string(),
                            found: format!("missing entry for TabRefCodes[{index}]"),
                        }
                    });
                }
            }
            Err(MsError::InvalidMeasureCode {
                table: table_name.to_string(),
                column: ref_column.clone(),
                code,
            })
        }
        MeasRefDesc::VariableString { ref_column } => {
            match table.cell_accessor(row, ref_column)?.scalar()? {
                ScalarValue::String(value) => Ok(value.clone()),
                other => Err(MsError::ColumnTypeMismatch {
                    column: ref_column.clone(),
                    table: table_name.to_string(),
                    expected: "String scalar".to_string(),
                    found: format!("{other:?}"),
                }),
            }
        }
    }
}
