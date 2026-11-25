// SPDX-License-Identifier: LGPL-3.0-or-later
//! Input arrays for selected MeasurementSet visibility writes.

use casa_tables::Table;
use casa_types::{ArrayValue, Complex32, RecordValue, ScalarValue, Value};

use super::{VisibilityChannels, input};
use crate::MsResult;

/// The stored columns accepted by the public MS tool's `putdata` method.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Column {
    Data,
    CorrectedData,
    ModelData,
    FloatData,
    Flag,
    FlagRow,
    Sigma,
    Weight,
}

impl Column {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Data => "DATA",
            Self::CorrectedData => "CORRECTED_DATA",
            Self::ModelData => "MODEL_DATA",
            Self::FloatData => "FLOAT_DATA",
            Self::Flag => "FLAG",
            Self::FlagRow => "FLAG_ROW",
            Self::Sigma => "SIGMA",
            Self::Weight => "WEIGHT",
        }
    }

    pub(super) fn cell_rank(self) -> usize {
        match self {
            Self::Data | Self::CorrectedData | Self::ModelData | Self::FloatData | Self::Flag => 2,
            Self::Sigma | Self::Weight => 1,
            Self::FlagRow => 0,
        }
    }

    pub(super) fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "DATA" => Some(Self::Data),
            "CORRECTED_DATA" => Some(Self::CorrectedData),
            "MODEL_DATA" => Some(Self::ModelData),
            "FLAG" => Some(Self::Flag),
            "FLAG_ROW" => Some(Self::FlagRow),
            "SIGMA" => Some(Self::Sigma),
            "WEIGHT" => Some(Self::Weight),
            _ => None,
        }
    }

    pub(super) fn from_legacy_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("FLOAT_DATA") {
            return Some(Self::FloatData);
        }
        Self::from_name(name)
    }

    pub(super) fn is_complex_data(self) -> bool {
        matches!(self, Self::Data | Self::CorrectedData | Self::ModelData)
    }

    pub(super) fn is_visibility_data(self) -> bool {
        self.is_complex_data() || self == Self::FloatData
    }

    pub(super) fn cell_bytes(self) -> usize {
        match self {
            Self::Data | Self::CorrectedData | Self::ModelData => size_of::<Complex32>(),
            Self::Sigma | Self::Weight | Self::FloatData => size_of::<f32>(),
            Self::Flag | Self::FlagRow => size_of::<bool>(),
        }
    }
}

pub(super) struct WriteColumn {
    pub(super) column: Column,
    pub(super) values: ArrayValue,
    pub(super) select_polarization: bool,
    pub(super) channels: Option<VisibilityChannels>,
}

pub(super) fn write_columns(
    main: &Table,
    items: RecordValue,
    legacy: bool,
) -> MsResult<Vec<WriteColumn>> {
    let lookup = if legacy {
        Column::from_legacy_name
    } else {
        Column::from_name
    };
    let mut columns = Vec::<WriteColumn>::new();
    for field in items.into_fields() {
        let Some(column) = lookup(&field.name) else {
            continue;
        };
        let absent = (matches!(column, Column::CorrectedData | Column::ModelData)
            || (legacy && column == Column::Data))
            && !main
                .schema()
                .is_some_and(|schema| schema.contains_column(column.name()));
        if absent && !legacy {
            continue;
        }
        let array = match field.value {
            Value::Array(array) => Some(array),
            Value::Scalar(value) if column == Column::FlagRow => {
                flag_scalar(value).map(|flag| ArrayValue::from_bool_vec(vec![flag]))
            }
            _ => None,
        };
        let values = array
            .and_then(|array| storage_precision(column, array))
            .ok_or_else(|| {
                input(format!(
                    "putdata field '{}' has an incompatible array type",
                    field.name
                ))
            })?;
        if absent {
            continue;
        }
        // Casacore permits case aliases in one record and writes them in
        // field order. The last such field owns the resulting column.
        if let Some(prior) = columns.iter_mut().find(|entry| entry.column == column) {
            prior.values = values;
        } else {
            columns.push(WriteColumn {
                column,
                values,
                select_polarization: true,
                channels: None,
            });
        }
    }
    Ok(columns)
}

fn flag_scalar(value: ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Bool(value) => Some(value),
        ScalarValue::UInt8(value) => Some(value != 0),
        ScalarValue::UInt16(value) => Some(value != 0),
        ScalarValue::UInt32(value) => Some(value != 0),
        ScalarValue::Int16(value) => Some(value != 0),
        ScalarValue::Int32(value) => Some(value != 0),
        ScalarValue::Int64(value) => Some(value != 0),
        _ => None,
    }
}

fn storage_precision(column: Column, array: ArrayValue) -> Option<ArrayValue> {
    macro_rules! numeric_array {
        ($value:expr, $variant:ident, $convert:expr) => {
            match $value {
                ArrayValue::UInt8(values) => Some(ArrayValue::$variant(
                    values.mapv(|value| $convert(f64::from(value))),
                )),
                ArrayValue::UInt16(values) => Some(ArrayValue::$variant(
                    values.mapv(|value| $convert(f64::from(value))),
                )),
                ArrayValue::UInt32(values) => Some(ArrayValue::$variant(
                    values.mapv(|value| $convert(f64::from(value))),
                )),
                ArrayValue::Int16(values) => Some(ArrayValue::$variant(
                    values.mapv(|value| $convert(f64::from(value))),
                )),
                ArrayValue::Int32(values) => Some(ArrayValue::$variant(
                    values.mapv(|value| $convert(f64::from(value))),
                )),
                ArrayValue::Int64(values) => Some(ArrayValue::$variant(
                    values.mapv(|value| $convert(value as f64)),
                )),
                ArrayValue::Float32(values) => Some(ArrayValue::$variant(
                    values.mapv(|value| $convert(f64::from(value))),
                )),
                ArrayValue::Float64(values) => Some(ArrayValue::$variant(values.mapv($convert))),
                _ => None,
            }
        };
    }
    match (column, array) {
        (Column::Flag | Column::FlagRow, ArrayValue::Bool(values)) => {
            Some(ArrayValue::Bool(values))
        }
        (Column::Flag | Column::FlagRow, ArrayValue::Float32(_) | ArrayValue::Float64(_)) => None,
        (Column::Flag | Column::FlagRow, array) => {
            numeric_array!(array, Bool, |value| value != 0.0)
        }
        (Column::Sigma | Column::Weight | Column::FloatData, ArrayValue::Float32(values)) => {
            Some(ArrayValue::Float32(values))
        }
        (Column::Sigma | Column::Weight | Column::FloatData, array) => {
            numeric_array!(array, Float32, |value| value as f32)
        }
        (_, ArrayValue::Complex32(values)) => Some(ArrayValue::Complex32(values)),
        (_, ArrayValue::Complex64(values)) => {
            Some(ArrayValue::Complex32(values.mapv(|value| {
                Complex32::new(value.re as f32, value.im as f32)
            })))
        }
        (_, array) => numeric_array!(array, Complex32, |value| Complex32::new(value as f32, 0.0)),
    }
}
