// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::measures::MeasFrame;
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::{EpochRef, direction_from_record};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_measures::table_measures::{MeasOffsetDesc, MeasRefDesc, TableMeasDesc};
use casa_tables::{ColumnType, Table};

use crate::{MsError, MsResult};

/// A direction and the optional reference offset preserved by the CASA
/// measure record. The direction components are relative to its reference.
#[derive(Debug, Clone)]
pub struct MetadataDirection {
    /// The direction components and frame.
    pub direction: MDirection,
    /// A stored offset measure, including any offset in its own reference.
    pub offset: Option<Box<MetadataDirection>>,
}

impl MetadataDirection {
    pub(super) fn new(direction: MDirection) -> Self {
        Self {
            direction,
            offset: None,
        }
    }

    /// Apply the stored measure offset in the supplied observation frame.
    ///
    /// # Errors
    /// The offset cannot be converted to the direction's reference.
    pub fn resolve_offset(&self, frame: &MeasFrame) -> MsResult<MDirection> {
        let Some(offset) = &self.offset else {
            return Ok(self.direction.clone());
        };
        let reference = self.direction.refer();
        let offset = offset
            .resolve_offset(frame)?
            .convert_to(reference, frame)?
            .cosines();
        let direction = self.direction.cosines();
        let sum = std::array::from_fn(|axis| direction[axis] + offset[axis]);
        Ok(MDirection::from_cosines(sum, reference))
    }

    fn from_offset_record(record: &RecordValue) -> MsResult<Self> {
        let mut components = record.clone();
        let offset = match components.remove("offset") {
            Some(Value::Record(offset)) => Some(Box::new(Self::from_offset_record(&offset)?)),
            Some(_) => {
                return Err(MsError::InvalidInput(
                    "Direction offset must be a record".into(),
                ));
            }
            None => None,
        };
        let direction = direction_from_record(&components)?;
        Ok(Self { direction, offset })
    }
}

fn reference_value(
    table: &Table,
    column: &str,
    row: usize,
    element: usize,
) -> MsResult<ScalarValue> {
    let missing = || {
        MsError::InvalidInput(format!(
            "Missing measure reference value in {column} row {row}"
        ))
    };
    if table
        .schema()
        .and_then(|schema| schema.column(column))
        .is_some_and(|definition| *definition.column_type() == ColumnType::Scalar)
    {
        return table
            .column_accessor(column)?
            .scalar_cells_owned_for_rows(&[row])?
            .pop()
            .flatten()
            .ok_or_else(missing);
    }
    let value = table.cell_accessor(row, column)?.value()?;
    let scalar = match value {
        Some(Value::Scalar(value)) => Some(value.clone()),
        Some(Value::Array(ArrayValue::String(values))) => {
            values.iter().nth(element).cloned().map(ScalarValue::String)
        }
        Some(Value::Array(ArrayValue::Int32(values))) => {
            values.iter().nth(element).copied().map(ScalarValue::Int32)
        }
        Some(Value::Array(ArrayValue::Int64(values))) => {
            values.iter().nth(element).copied().map(ScalarValue::Int64)
        }
        Some(Value::Array(ArrayValue::UInt32(values))) => {
            values.iter().nth(element).copied().map(ScalarValue::UInt32)
        }
        _ => None,
    };
    scalar.ok_or_else(|| {
        MsError::InvalidInput(format!("Missing measure reference in {column} row {row}"))
    })
}

fn reference_name(
    table: &Table,
    desc: &TableMeasDesc,
    row: usize,
    element: usize,
    default_code: impl FnOnce(i32) -> Option<&'static str>,
) -> MsResult<String> {
    let (column, mapping) = match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => return Ok(refer.clone()),
        MeasRefDesc::VariableInt {
            ref_column,
            tab_ref_types,
            tab_ref_codes,
        } => (ref_column, Some((tab_ref_types, tab_ref_codes))),
        MeasRefDesc::VariableString { ref_column } => (ref_column, None),
    };
    let value = reference_value(table, column, row, element)?;
    let code = match value {
        ScalarValue::String(refer) if mapping.is_none() => return Ok(refer),
        ScalarValue::Int32(value) => Some(value),
        ScalarValue::Int64(value) => i32::try_from(value).ok(),
        ScalarValue::UInt32(value) => i32::try_from(value).ok(),
        _ => None,
    };
    let refer = code.and_then(|code| match mapping {
        Some((types, codes)) => codes
            .iter()
            .position(|&candidate| candidate == code)
            .and_then(|index| types.get(index).map(String::as_str)),
        None => default_code(code),
    });
    refer.map(String::from).ok_or_else(|| {
        MsError::InvalidInput(format!("Invalid measure reference in {column} row {row}"))
    })
}

pub(super) fn field_epoch_reference(table: &Table, row: usize) -> MsResult<EpochRef> {
    epoch_reference(table, "TIME", row, 0)
}

/// Resolve the epoch reference frame for `column` at `row` and `element`.
pub fn epoch_reference(
    table: &Table,
    column: &str,
    row: usize,
    element: usize,
) -> MsResult<EpochRef> {
    let Some(desc) = TableMeasDesc::reconstruct(table, column) else {
        return Ok(EpochRef::UTC);
    };
    let name = reference_name(table, &desc, row, element, |code| {
        EpochRef::ALL
            .iter()
            .find(|kind| kind.casacore_code() == code)
            .map(|kind| kind.casacore_name())
    })?;
    Ok(name.parse()?)
}

pub(super) fn direction_with_reference(
    table: &Table,
    column: &str,
    row: usize,
    element: usize,
    angles: [f64; 2],
) -> MsResult<MetadataDirection> {
    read_direction_reference(table, column, row, element, angles, &mut Vec::new())
}

fn read_direction_reference(
    table: &Table,
    column: &str,
    row: usize,
    element: usize,
    angles: [f64; 2],
    visited: &mut Vec<String>,
) -> MsResult<MetadataDirection> {
    let Some(desc) = TableMeasDesc::reconstruct(table, column) else {
        return Ok(MetadataDirection::new(MDirection::from_angles(
            angles[0],
            angles[1],
            DirectionRef::J2000,
        )));
    };
    let name = reference_name(table, &desc, row, element, |code| {
        DirectionRef::ALL
            .iter()
            .find(|kind| kind.casacore_code() == code)
            .map(|kind| kind.as_str())
    })?;
    let direction = MDirection::from_angles(angles[0], angles[1], name.parse()?);
    let offset = match desc.offset_desc() {
        Some(MeasOffsetDesc::Fixed { offset }) => {
            Some(MetadataDirection::from_offset_record(offset)?)
        }
        Some(MeasOffsetDesc::Variable { offset_column }) => {
            if visited.contains(offset_column) || offset_column == column {
                return Err(MsError::InvalidInput(
                    "Cyclic direction offset columns".into(),
                ));
            }
            visited.push(column.to_string());
            let values = table.cell_accessor(row, offset_column)?.array()?;
            let index = if values.shape().len() == 1 {
                0
            } else {
                element
            };
            let angles = direction_components(table, offset_column, values, index)?;
            let offset =
                read_direction_reference(table, offset_column, row, index, angles, visited)?;
            visited.pop();
            Some(offset)
        }
        None => None,
    };
    Ok(MetadataDirection {
        direction,
        offset: offset.map(Box::new),
    })
}

pub(super) fn direction_components(
    table: &Table,
    column: &str,
    values: &ArrayValue,
    index: usize,
) -> MsResult<[f64; 2]> {
    let ArrayValue::Float64(values) = values else {
        return Err(MsError::InvalidInput(format!(
            "{column} direction values must be double"
        )));
    };
    let units = super::metadata_field::direction_radians(table, column)?;
    let angle = |axis| {
        match values.shape() {
            [2] if index == 0 => values.get(ndarray::IxDyn(&[axis])),
            [2, _] => values.get(ndarray::IxDyn(&[axis, index])),
            _ => None,
        }
        .map(|value| value * units[axis])
        .ok_or_else(|| MsError::InvalidInput(format!("{column} has an invalid direction shape")))
    };
    Ok([angle(0)?, angle(1)?])
}
