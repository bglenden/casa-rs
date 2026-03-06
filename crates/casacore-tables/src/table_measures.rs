// SPDX-License-Identifier: LGPL-3.0-or-later
//! Measure column support: storing/reading measures in table columns via MEASINFO.
//!
//! This module provides the Rust counterparts of C++ `TableMeasDesc`,
//! `ScalarMeasColumn`, and `ArrayMeasColumn` from the casacore
//! `measures/TableMeasures` subsystem.
//!
//! A "measure column" is a regular numeric array column whose cells carry
//! physical measure metadata. The metadata is stored in the column's keyword
//! record under the `MEASINFO` sub-record:
//!
//! - **Fixed reference**: `{ type: "epoch", Ref: "UTC" }`
//! - **Variable Int reference**: `{ type: "epoch", VarRefCol: "TimeRef",
//!   TabRefTypes: ["LAST","LMST",...], TabRefCodes: [0, 1, ...] }`
//! - **Variable String reference**: `{ type: "epoch", VarRefCol: "TimeRefStr" }`
//!
//! Optional offset fields (`RefOffMsr` for fixed, `RefOffCol` for variable)
//! encode measure offsets.
//!
//! # Relationship to `table_quantum`
//!
//! MEASINFO and `QuantumUnits` coexist in the same column keyword record.
//! A column can have both measure metadata (reference frame) and quantum
//! metadata (physical units). The `table_quantum` module handles units;
//! this module handles reference frames.
//!
//! # Examples
//!
//! ```rust
//! use casacore_tables::{Table, TableSchema, ColumnSchema, table_measures::*};
//! use casacore_types::*;
//!
//! // Create a table with an epoch column
//! let schema = TableSchema::new(vec![
//!     ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
//! ]).unwrap();
//! let mut table = Table::with_schema(schema);
//!
//! // Attach fixed-reference MEASINFO
//! let desc = TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "UTC");
//! desc.write(&mut table).unwrap();
//! assert!(TableMeasDesc::has_measinfo(&table, "TIME"));
//! ```

use crate::table::{Table, TableError};
use casacore_types::measures::direction::{DirectionRef, MDirection};
use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::epoch::{EpochRef, MEpoch, MjdHighPrec};
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};
use casacore_types::measures::position::{MPosition, PositionRef};
use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casacore_types::{ArrayValue, RecordValue, ScalarValue, Value};

// ─── Keyword names (matching C++ casacore) ──────────────────────────────────

const MEASINFO_KW: &str = "MEASINFO";
const TYPE_KW: &str = "type";
const REF_KW: &str = "Ref";
const VAR_REF_COL_KW: &str = "VarRefCol";
const TAB_REF_TYPES_KW: &str = "TabRefTypes";
const TAB_REF_CODES_KW: &str = "TabRefCodes";
const REF_OFF_MSR_KW: &str = "RefOffMsr";
const REF_OFF_COL_KW: &str = "RefOffCol";

// ─── MeasureType ────────────────────────────────────────────────────────────

/// The kind of measure stored in a column.
///
/// Corresponds to the `type` field in the `MEASINFO` sub-record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasureType {
    /// Epoch (time instant).
    Epoch,
    /// Sky direction.
    Direction,
    /// 3D spatial position.
    Position,
    /// Spectral frequency.
    Frequency,
    /// Doppler shift.
    Doppler,
    /// Radial velocity.
    RadialVelocity,
}

impl MeasureType {
    /// Returns the C++ casacore string name for this measure type.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Epoch => "epoch",
            Self::Direction => "direction",
            Self::Position => "position",
            Self::Frequency => "frequency",
            Self::Doppler => "doppler",
            Self::RadialVelocity => "radialvelocity",
        }
    }

    /// Parses a measure type from its C++ casacore string name.
    pub fn from_str_casacore(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "epoch" => Some(Self::Epoch),
            "direction" => Some(Self::Direction),
            "position" => Some(Self::Position),
            "frequency" => Some(Self::Frequency),
            "doppler" => Some(Self::Doppler),
            "radialvelocity" => Some(Self::RadialVelocity),
            _ => None,
        }
    }

    /// Returns the number of Double values per measure for this type.
    ///
    /// Epoch=1, Direction=2, Position=3, Frequency=1, Doppler=1,
    /// RadialVelocity=1.
    pub fn values_per_measure(self) -> usize {
        match self {
            Self::Epoch | Self::Frequency | Self::Doppler | Self::RadialVelocity => 1,
            Self::Direction => 2,
            Self::Position => 3,
        }
    }

    /// Returns the default unit strings for this measure type.
    ///
    /// C++ casacore's `TableMeasDesc::write` writes these via `TableQuantumDesc`
    /// so that `ArrayMeasColumn` can find them.
    pub fn default_units(self) -> Vec<String> {
        match self {
            Self::Epoch => vec!["d".into()],
            Self::Direction => vec!["rad".into(), "rad".into()],
            Self::Position => vec!["m".into(), "m".into(), "m".into()],
            Self::Frequency => vec!["Hz".into()],
            Self::Doppler => vec!["m/s".into()],
            Self::RadialVelocity => vec!["m/s".into()],
        }
    }
}

// ─── MeasRefDesc ────────────────────────────────────────────────────────────

/// Describes how the reference frame for a measure column is determined.
#[derive(Debug, Clone)]
pub enum MeasRefDesc {
    /// All rows share a single fixed reference frame.
    Fixed {
        /// The reference frame name (e.g. "UTC", "J2000").
        refer: String,
    },
    /// Each row has an integer code stored in a companion column, with
    /// explicit mapping tables.
    VariableInt {
        /// Name of the companion Int column.
        ref_column: String,
        /// String names for each code (e.g. `["LAST", "LMST", ..., "UTC", ...]`).
        tab_ref_types: Vec<String>,
        /// Integer codes for each entry (e.g. `[0, 1, ..., 6, ...]`).
        tab_ref_codes: Vec<i32>,
    },
    /// Each row has a string reference stored in a companion column.
    VariableString {
        /// Name of the companion String column.
        ref_column: String,
    },
}

// ─── MeasOffsetDesc ─────────────────────────────────────────────────────────

/// Describes how the offset for a measure column is determined.
#[derive(Debug, Clone)]
pub enum MeasOffsetDesc {
    /// A fixed offset stored as a serialized measure record.
    Fixed {
        /// The serialized measure (same format as `MeasureHolder`).
        offset: RecordValue,
    },
    /// A variable offset read from a companion column.
    Variable {
        /// Name of the companion column holding the offset values.
        offset_column: String,
    },
}

// ─── TableMeasDesc ──────────────────────────────────────────────────────────

/// Descriptor for a measure column's MEASINFO metadata.
///
/// Corresponds to C++ `casa::TableMeasDesc`. This descriptor records the
/// measure type, reference frame mode (fixed, variable-int, variable-string),
/// and optional offset for a table column.
#[derive(Debug, Clone)]
pub struct TableMeasDesc {
    column_name: String,
    measure_type: MeasureType,
    ref_desc: MeasRefDesc,
    offset_desc: Option<MeasOffsetDesc>,
}

impl TableMeasDesc {
    /// Creates a descriptor with a fixed reference frame.
    pub fn new_fixed(column: &str, measure_type: MeasureType, refer: &str) -> Self {
        Self {
            column_name: column.to_owned(),
            measure_type,
            ref_desc: MeasRefDesc::Fixed {
                refer: refer.to_owned(),
            },
            offset_desc: None,
        }
    }

    /// Creates a descriptor with a variable integer reference column.
    ///
    /// Returns an error if `tab_ref_types` and `tab_ref_codes` have different lengths.
    pub fn new_variable_int(
        column: &str,
        measure_type: MeasureType,
        ref_column: &str,
        tab_ref_types: Vec<String>,
        tab_ref_codes: Vec<i32>,
    ) -> Result<Self, TableError> {
        if tab_ref_types.len() != tab_ref_codes.len() {
            return Err(TableError::Storage(format!(
                "TabRefTypes length ({}) != TabRefCodes length ({}) for column '{column}'",
                tab_ref_types.len(),
                tab_ref_codes.len()
            )));
        }
        Ok(Self {
            column_name: column.to_owned(),
            measure_type,
            ref_desc: MeasRefDesc::VariableInt {
                ref_column: ref_column.to_owned(),
                tab_ref_types,
                tab_ref_codes,
            },
            offset_desc: None,
        })
    }

    /// Creates a descriptor with a variable string reference column.
    pub fn new_variable_string(column: &str, measure_type: MeasureType, ref_column: &str) -> Self {
        Self {
            column_name: column.to_owned(),
            measure_type,
            ref_desc: MeasRefDesc::VariableString {
                ref_column: ref_column.to_owned(),
            },
            offset_desc: None,
        }
    }

    /// Adds a fixed offset to this descriptor.
    pub fn with_fixed_offset(mut self, offset: RecordValue) -> Self {
        self.offset_desc = Some(MeasOffsetDesc::Fixed { offset });
        self
    }

    /// Adds a variable offset column to this descriptor.
    pub fn with_variable_offset(mut self, offset_column: &str) -> Self {
        self.offset_desc = Some(MeasOffsetDesc::Variable {
            offset_column: offset_column.to_owned(),
        });
        self
    }

    /// Returns `true` if the column has a `MEASINFO` sub-record in its keywords.
    pub fn has_measinfo(table: &Table, column: &str) -> bool {
        table
            .column_keywords(column)
            .and_then(|kw| kw.get(MEASINFO_KW))
            .is_some()
    }

    /// Reconstructs a descriptor from a column's persisted keywords.
    ///
    /// Returns `None` if the column has no `MEASINFO` sub-record.
    pub fn reconstruct(table: &Table, column: &str) -> Option<Self> {
        let kw = table.column_keywords(column)?;
        let measinfo = match kw.get(MEASINFO_KW) {
            Some(Value::Record(r)) => r,
            _ => return None,
        };

        // Extract type
        let type_str = match measinfo.get(TYPE_KW) {
            Some(Value::Scalar(ScalarValue::String(s))) => s.as_str(),
            _ => return None,
        };
        let measure_type = MeasureType::from_str_casacore(type_str)?;

        // Determine reference mode
        let ref_desc = if let Some(Value::Scalar(ScalarValue::String(var_col))) =
            measinfo.get(VAR_REF_COL_KW)
        {
            // Variable reference. Check for TabRefTypes/TabRefCodes (Int mode).
            let tab_ref_types = measinfo.get(TAB_REF_TYPES_KW).and_then(|v| {
                if let Value::Array(ArrayValue::String(a)) = v {
                    Some(a.iter().cloned().collect::<Vec<_>>())
                } else {
                    None
                }
            });
            let tab_ref_codes = measinfo.get(TAB_REF_CODES_KW).and_then(|v| match v {
                Value::Array(ArrayValue::Int32(a)) => Some(a.iter().copied().collect::<Vec<i32>>()),
                Value::Array(ArrayValue::UInt32(a)) => {
                    Some(a.iter().map(|&x| x as i32).collect::<Vec<i32>>())
                }
                Value::Array(ArrayValue::Int64(a)) => {
                    Some(a.iter().map(|&x| x as i32).collect::<Vec<i32>>())
                }
                _ => None,
            });

            if let (Some(types), Some(codes)) = (tab_ref_types, tab_ref_codes) {
                MeasRefDesc::VariableInt {
                    ref_column: var_col.clone(),
                    tab_ref_types: types,
                    tab_ref_codes: codes,
                }
            } else {
                MeasRefDesc::VariableString {
                    ref_column: var_col.clone(),
                }
            }
        } else if let Some(Value::Scalar(ScalarValue::String(refer))) = measinfo.get(REF_KW) {
            MeasRefDesc::Fixed {
                refer: refer.clone(),
            }
        } else {
            return None;
        };

        // Check for offset
        let offset_desc = if let Some(Value::Record(off_rec)) = measinfo.get(REF_OFF_MSR_KW) {
            Some(MeasOffsetDesc::Fixed {
                offset: off_rec.clone(),
            })
        } else if let Some(Value::Scalar(ScalarValue::String(off_col))) =
            measinfo.get(REF_OFF_COL_KW)
        {
            Some(MeasOffsetDesc::Variable {
                offset_column: off_col.clone(),
            })
        } else {
            None
        };

        Some(Self {
            column_name: column.to_owned(),
            measure_type,
            ref_desc,
            offset_desc,
        })
    }

    /// Writes the MEASINFO sub-record to the column's keyword record.
    pub fn write(&self, table: &mut Table) -> Result<(), TableError> {
        let mut kw = table
            .column_keywords(&self.column_name)
            .cloned()
            .unwrap_or_default();

        let mut measinfo = RecordValue::default();
        measinfo.upsert(
            TYPE_KW,
            Value::Scalar(ScalarValue::String(self.measure_type.as_str().to_owned())),
        );

        match &self.ref_desc {
            MeasRefDesc::Fixed { refer } => {
                measinfo.upsert(REF_KW, Value::Scalar(ScalarValue::String(refer.clone())));
            }
            MeasRefDesc::VariableInt {
                ref_column,
                tab_ref_types,
                tab_ref_codes,
            } => {
                measinfo.upsert(
                    VAR_REF_COL_KW,
                    Value::Scalar(ScalarValue::String(ref_column.clone())),
                );
                measinfo.upsert(
                    TAB_REF_TYPES_KW,
                    Value::Array(ArrayValue::from_string_vec(tab_ref_types.clone())),
                );
                measinfo.upsert(
                    TAB_REF_CODES_KW,
                    Value::Array(ArrayValue::from_i32_vec(tab_ref_codes.clone())),
                );
            }
            MeasRefDesc::VariableString { ref_column } => {
                measinfo.upsert(
                    VAR_REF_COL_KW,
                    Value::Scalar(ScalarValue::String(ref_column.clone())),
                );
            }
        }

        if let Some(ref offset) = self.offset_desc {
            match offset {
                MeasOffsetDesc::Fixed { offset: off_rec } => {
                    measinfo.upsert(REF_OFF_MSR_KW, Value::Record(off_rec.clone()));
                }
                MeasOffsetDesc::Variable { offset_column } => {
                    measinfo.upsert(
                        REF_OFF_COL_KW,
                        Value::Scalar(ScalarValue::String(offset_column.clone())),
                    );
                }
            }
        }

        kw.upsert(MEASINFO_KW, Value::Record(measinfo));

        // C++ casacore also writes QuantumUnits via TableQuantumDesc so that
        // ArrayMeasColumn can reconstruct units. Write default units if not
        // already present.
        if kw.get("QuantumUnits").is_none() {
            kw.upsert(
                "QuantumUnits",
                Value::Array(ArrayValue::from_string_vec(
                    self.measure_type.default_units(),
                )),
            );
        }

        table.set_column_keywords(&self.column_name, kw);
        Ok(())
    }

    /// Returns the data column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Returns the measure type.
    pub fn measure_type(&self) -> MeasureType {
        self.measure_type
    }

    /// Returns the reference descriptor.
    pub fn ref_desc(&self) -> &MeasRefDesc {
        &self.ref_desc
    }

    /// Returns the offset descriptor, if any.
    pub fn offset_desc(&self) -> Option<&MeasOffsetDesc> {
        self.offset_desc.as_ref()
    }
}

// ─── MeasureValue ───────────────────────────────────────────────────────────

/// A typed measure value read from a table column.
///
/// Wraps the specific measure type so callers can extract the one they need.
#[derive(Debug, Clone)]
pub enum MeasureValue {
    /// An epoch measure.
    Epoch(MEpoch),
    /// A direction measure.
    Direction(MDirection),
    /// A position measure.
    Position(MPosition),
    /// A frequency measure.
    Frequency(MFrequency),
    /// A Doppler measure.
    Doppler(MDoppler),
    /// A radial velocity measure.
    RadialVelocity(MRadialVelocity),
}

impl MeasureValue {
    /// Returns the epoch, or an error if this is a different type.
    pub fn as_epoch(&self) -> Result<&MEpoch, TableError> {
        match self {
            Self::Epoch(e) => Ok(e),
            _ => Err(TableError::Storage("expected Epoch measure".to_owned())),
        }
    }

    /// Returns the direction, or an error if this is a different type.
    pub fn as_direction(&self) -> Result<&MDirection, TableError> {
        match self {
            Self::Direction(d) => Ok(d),
            _ => Err(TableError::Storage("expected Direction measure".to_owned())),
        }
    }

    /// Returns the position, or an error if this is a different type.
    pub fn as_position(&self) -> Result<&MPosition, TableError> {
        match self {
            Self::Position(p) => Ok(p),
            _ => Err(TableError::Storage("expected Position measure".to_owned())),
        }
    }

    /// Returns the frequency, or an error if this is a different type.
    pub fn as_frequency(&self) -> Result<&MFrequency, TableError> {
        match self {
            Self::Frequency(f) => Ok(f),
            _ => Err(TableError::Storage("expected Frequency measure".to_owned())),
        }
    }

    /// Returns the Doppler, or an error if this is a different type.
    pub fn as_doppler(&self) -> Result<&MDoppler, TableError> {
        match self {
            Self::Doppler(d) => Ok(d),
            _ => Err(TableError::Storage("expected Doppler measure".to_owned())),
        }
    }

    /// Returns the radial velocity, or an error if this is a different type.
    pub fn as_radial_velocity(&self) -> Result<&MRadialVelocity, TableError> {
        match self {
            Self::RadialVelocity(rv) => Ok(rv),
            _ => Err(TableError::Storage(
                "expected RadialVelocity measure".to_owned(),
            )),
        }
    }
}

// ─── Reference resolution helpers ───────────────────────────────────────────

/// Resolves the reference string for a given row based on the descriptor.
fn resolve_ref_string(
    table: &Table,
    desc: &TableMeasDesc,
    row: usize,
) -> Result<String, TableError> {
    match &desc.ref_desc {
        MeasRefDesc::Fixed { refer } => Ok(refer.clone()),
        MeasRefDesc::VariableInt {
            ref_column,
            tab_ref_types,
            tab_ref_codes,
        } => {
            let code = match table.get_scalar_cell(row, ref_column)? {
                ScalarValue::Int32(v) => *v,
                ScalarValue::Int64(v) => *v as i32,
                ScalarValue::UInt32(v) => *v as i32,
                _ => {
                    return Err(TableError::Storage(format!(
                        "ref column '{ref_column}' at row {row}: expected Int"
                    )));
                }
            };
            // Find this code in the mapping table
            for (i, &c) in tab_ref_codes.iter().enumerate() {
                if c == code {
                    return tab_ref_types.get(i).cloned().ok_or_else(|| {
                        TableError::Storage(format!(
                            "TabRefTypes index {i} out of bounds (len={}) for column '{}'",
                            tab_ref_types.len(),
                            desc.column_name
                        ))
                    });
                }
            }
            Err(TableError::Storage(format!(
                "ref code {code} not found in TabRefCodes for column '{}'",
                desc.column_name
            )))
        }
        MeasRefDesc::VariableString { ref_column } => {
            match table.get_scalar_cell(row, ref_column)? {
                ScalarValue::String(s) => Ok(s.clone()),
                _ => Err(TableError::Storage(format!(
                    "ref column '{ref_column}' at row {row}: expected String"
                ))),
            }
        }
    }
}

/// Creates a MeasureValue from raw f64 values and a reference string.
fn make_measure(
    measure_type: MeasureType,
    values: &[f64],
    refer_str: &str,
) -> Result<MeasureValue, TableError> {
    let err = |msg: String| TableError::Storage(msg);
    match measure_type {
        MeasureType::Epoch => {
            let r: EpochRef = refer_str
                .parse()
                .map_err(|e| err(format!("invalid epoch ref '{refer_str}': {e}")))?;
            Ok(MeasureValue::Epoch(MEpoch::new(
                MjdHighPrec::from_mjd(values[0]),
                r,
            )))
        }
        MeasureType::Direction => {
            let r: DirectionRef = refer_str
                .parse()
                .map_err(|e| err(format!("invalid direction ref '{refer_str}': {e}")))?;
            Ok(MeasureValue::Direction(MDirection::from_angles(
                values[0], values[1], r,
            )))
        }
        MeasureType::Position => {
            let r: PositionRef = refer_str
                .parse()
                .map_err(|e| err(format!("invalid position ref '{refer_str}': {e}")))?;
            Ok(MeasureValue::Position(match r {
                PositionRef::ITRF => MPosition::new_itrf(values[0], values[1], values[2]),
                PositionRef::WGS84 => MPosition::new_wgs84(values[0], values[1], values[2]),
            }))
        }
        MeasureType::Frequency => {
            let r: FrequencyRef = refer_str
                .parse()
                .map_err(|e| err(format!("invalid frequency ref '{refer_str}': {e}")))?;
            Ok(MeasureValue::Frequency(MFrequency::new(values[0], r)))
        }
        MeasureType::Doppler => {
            let r: DopplerRef = refer_str
                .parse()
                .map_err(|e| err(format!("invalid doppler ref '{refer_str}': {e}")))?;
            Ok(MeasureValue::Doppler(MDoppler::new(values[0], r)))
        }
        MeasureType::RadialVelocity => {
            let r: RadialVelocityRef = refer_str
                .parse()
                .map_err(|e| err(format!("invalid radial velocity ref '{refer_str}': {e}")))?;
            Ok(MeasureValue::RadialVelocity(MRadialVelocity::new(
                values[0], r,
            )))
        }
    }
}

/// Extracts the raw f64 values from a MeasureValue.
fn measure_values(mv: &MeasureValue) -> Vec<f64> {
    match mv {
        MeasureValue::Epoch(e) => vec![e.value().as_mjd()],
        MeasureValue::Direction(d) => {
            let (lon, lat) = d.as_angles();
            vec![lon, lat]
        }
        MeasureValue::Position(p) => p.values().to_vec(),
        MeasureValue::Frequency(f) => vec![f.hz()],
        MeasureValue::Doppler(d) => vec![d.value()],
        MeasureValue::RadialVelocity(rv) => vec![rv.ms()],
    }
}

/// Returns the reference string for a MeasureValue.
fn measure_ref_string(mv: &MeasureValue) -> String {
    match mv {
        MeasureValue::Epoch(e) => e.refer().as_str().to_owned(),
        MeasureValue::Direction(d) => d.refer().as_str().to_owned(),
        MeasureValue::Position(p) => p.refer().as_str().to_owned(),
        MeasureValue::Frequency(f) => f.refer().as_str().to_owned(),
        MeasureValue::Doppler(d) => d.refer().as_str().to_owned(),
        MeasureValue::RadialVelocity(rv) => rv.refer().as_str().to_owned(),
    }
}

/// Resolves the casacore integer code for a ref string and measure type.
fn ref_string_to_code(measure_type: MeasureType, refer: &str) -> Result<i32, TableError> {
    let err = |msg: String| TableError::Storage(msg);
    match measure_type {
        MeasureType::Epoch => {
            let r: EpochRef = refer
                .parse()
                .map_err(|e| err(format!("invalid epoch ref '{refer}': {e}")))?;
            Ok(r.casacore_code())
        }
        MeasureType::Direction => {
            let r: DirectionRef = refer
                .parse()
                .map_err(|e| err(format!("invalid direction ref '{refer}': {e}")))?;
            Ok(r.casacore_code())
        }
        MeasureType::Position => {
            let r: PositionRef = refer
                .parse()
                .map_err(|e| err(format!("invalid position ref '{refer}': {e}")))?;
            Ok(r.casacore_code())
        }
        MeasureType::Frequency => {
            let r: FrequencyRef = refer
                .parse()
                .map_err(|e| err(format!("invalid frequency ref '{refer}': {e}")))?;
            Ok(r.casacore_code())
        }
        MeasureType::Doppler => {
            let r: DopplerRef = refer
                .parse()
                .map_err(|e| err(format!("invalid doppler ref '{refer}': {e}")))?;
            Ok(r.casacore_code())
        }
        MeasureType::RadialVelocity => {
            let r: RadialVelocityRef = refer
                .parse()
                .map_err(|e| err(format!("invalid radial velocity ref '{refer}': {e}")))?;
            Ok(r.casacore_code())
        }
    }
}

// ─── ScalarMeasColumn (read-only) ──────────────────────────────────────────

/// Read-only accessor for a scalar measure column.
///
/// Corresponds to C++ `casa::ScalarMeasColumn<M>`. A "scalar" measure column
/// has one measure per row (e.g. one epoch, one direction). The underlying
/// data column is an `ArrayColumn<Double>` with shape `[N]` where N is the
/// number of values per measure (1 for epoch, 2 for direction, 3 for position).
pub struct ScalarMeasColumn<'a> {
    table: &'a Table,
    column_name: String,
    desc: TableMeasDesc,
}

impl<'a> ScalarMeasColumn<'a> {
    /// Attaches to a scalar measure column.
    ///
    /// Returns an error if the column has no MEASINFO keywords.
    pub fn new(table: &'a Table, column: &str) -> Result<Self, TableError> {
        let desc = TableMeasDesc::reconstruct(table, column).ok_or_else(|| {
            TableError::Storage(format!("column '{column}' has no MEASINFO keywords"))
        })?;
        Ok(Self {
            table,
            column_name: column.to_owned(),
            desc,
        })
    }

    /// Returns the cached descriptor.
    pub fn desc(&self) -> &TableMeasDesc {
        &self.desc
    }

    /// Reads the measure at `row`.
    pub fn get(&self, row: usize) -> Result<MeasureValue, TableError> {
        let refer = resolve_ref_string(self.table, &self.desc, row)?;
        let values = read_array_values(self.table, row, &self.column_name)?;
        make_measure(self.desc.measure_type, &values, &refer)
    }

    /// Convenience: reads the epoch at `row`.
    pub fn get_epoch(&self, row: usize) -> Result<MEpoch, TableError> {
        self.get(row)?.as_epoch().cloned()
    }

    /// Convenience: reads the direction at `row`.
    pub fn get_direction(&self, row: usize) -> Result<MDirection, TableError> {
        self.get(row)?.as_direction().cloned()
    }
}

// ─── ArrayMeasColumn (read-only) ───────────────────────────────────────────

/// Read-only accessor for an array measure column.
///
/// Corresponds to C++ `casa::ArrayMeasColumn<M>`. An "array" measure column
/// has multiple measures per row (e.g. an array of directions). The underlying
/// data column is an `ArrayColumn<Double>` with shape `[N, M]` where N is
/// values-per-measure and M is the number of measures per cell.
pub struct ArrayMeasColumn<'a> {
    table: &'a Table,
    column_name: String,
    desc: TableMeasDesc,
}

impl<'a> ArrayMeasColumn<'a> {
    /// Attaches to an array measure column.
    pub fn new(table: &'a Table, column: &str) -> Result<Self, TableError> {
        let desc = TableMeasDesc::reconstruct(table, column).ok_or_else(|| {
            TableError::Storage(format!("column '{column}' has no MEASINFO keywords"))
        })?;
        Ok(Self {
            table,
            column_name: column.to_owned(),
            desc,
        })
    }

    /// Returns the cached descriptor.
    pub fn desc(&self) -> &TableMeasDesc {
        &self.desc
    }

    /// Reads all measures at `row` as a vector.
    pub fn get(&self, row: usize) -> Result<Vec<MeasureValue>, TableError> {
        let refer = resolve_ref_string(self.table, &self.desc, row)?;
        let all_values = read_array_values(self.table, row, &self.column_name)?;
        let n = self.desc.measure_type.values_per_measure();
        let mut result = Vec::with_capacity(all_values.len() / n);
        for chunk in all_values.chunks(n) {
            result.push(make_measure(self.desc.measure_type, chunk, &refer)?);
        }
        Ok(result)
    }
}

// ─── ScalarMeasColumnMut (write) ───────────────────────────────────────────

/// Mutable accessor for writing scalar measures to a column.
///
/// Corresponds to the write path of C++ `casa::ScalarMeasColumn<M>`.
pub struct ScalarMeasColumnMut<'a> {
    table: &'a mut Table,
    column_name: String,
    desc: TableMeasDesc,
}

impl<'a> ScalarMeasColumnMut<'a> {
    /// Attaches to a scalar measure column for writing.
    pub fn new(table: &'a mut Table, column: &str) -> Result<Self, TableError> {
        let desc = TableMeasDesc::reconstruct(table, column).ok_or_else(|| {
            TableError::Storage(format!("column '{column}' has no MEASINFO keywords"))
        })?;
        Ok(Self {
            table,
            column_name: column.to_owned(),
            desc,
        })
    }

    /// Returns the cached descriptor.
    pub fn desc(&self) -> &TableMeasDesc {
        &self.desc
    }

    /// Writes a measure at `row`.
    pub fn put(&mut self, row: usize, mv: &MeasureValue) -> Result<(), TableError> {
        let vals = measure_values(mv);
        self.table.set_cell(
            row,
            &self.column_name,
            Value::Array(ArrayValue::from_f64_vec(vals)),
        )?;
        self.write_ref(row, mv)?;
        Ok(())
    }

    /// Convenience: writes an epoch at `row`.
    pub fn put_epoch(&mut self, row: usize, epoch: &MEpoch) -> Result<(), TableError> {
        self.put(row, &MeasureValue::Epoch(epoch.clone()))
    }

    /// Convenience: writes a direction at `row`.
    pub fn put_direction(&mut self, row: usize, dir: &MDirection) -> Result<(), TableError> {
        self.put(row, &MeasureValue::Direction(dir.clone()))
    }

    fn write_ref(&mut self, row: usize, mv: &MeasureValue) -> Result<(), TableError> {
        let refer = measure_ref_string(mv);
        match &self.desc.ref_desc {
            MeasRefDesc::Fixed { .. } => Ok(()),
            MeasRefDesc::VariableInt { ref_column, .. } => {
                let code = ref_string_to_code(self.desc.measure_type, &refer)?;
                let ref_column = ref_column.clone();
                self.table
                    .set_cell(row, &ref_column, Value::Scalar(ScalarValue::Int32(code)))?;
                Ok(())
            }
            MeasRefDesc::VariableString { ref_column } => {
                let ref_column = ref_column.clone();
                self.table
                    .set_cell(row, &ref_column, Value::Scalar(ScalarValue::String(refer)))?;
                Ok(())
            }
        }
    }
}

// ─── ArrayMeasColumnMut (write) ────────────────────────────────────────────

/// Mutable accessor for writing arrays of measures to a column.
///
/// Corresponds to the write path of C++ `casa::ArrayMeasColumn<M>`.
pub struct ArrayMeasColumnMut<'a> {
    table: &'a mut Table,
    column_name: String,
    desc: TableMeasDesc,
}

impl<'a> ArrayMeasColumnMut<'a> {
    /// Attaches to an array measure column for writing.
    pub fn new(table: &'a mut Table, column: &str) -> Result<Self, TableError> {
        let desc = TableMeasDesc::reconstruct(table, column).ok_or_else(|| {
            TableError::Storage(format!("column '{column}' has no MEASINFO keywords"))
        })?;
        Ok(Self {
            table,
            column_name: column.to_owned(),
            desc,
        })
    }

    /// Writes an array of measures at `row`.
    ///
    /// All measures must have the same reference frame. For variable-ref
    /// columns, the reference of the first measure is written.
    pub fn put(&mut self, row: usize, measures: &[MeasureValue]) -> Result<(), TableError> {
        // Validate all measures share the same reference
        if measures.len() > 1 {
            let first_ref = measure_ref_string(&measures[0]);
            for (i, mv) in measures.iter().enumerate().skip(1) {
                let r = measure_ref_string(mv);
                if r != first_ref {
                    return Err(TableError::Storage(format!(
                        "ArrayMeasColumnMut::put: measure[{i}] has reference '{r}' \
                         but measure[0] has '{first_ref}'; all measures in a row \
                         must share the same reference"
                    )));
                }
            }
        }
        let n = self.desc.measure_type.values_per_measure();
        let mut all_values = Vec::with_capacity(measures.len() * n);
        for mv in measures {
            all_values.extend_from_slice(&measure_values(mv));
        }
        self.table.set_cell(
            row,
            &self.column_name,
            Value::Array(ArrayValue::from_f64_vec(all_values)),
        )?;
        // Write reference from first measure if variable
        if !measures.is_empty() {
            let refer = measure_ref_string(&measures[0]);
            match &self.desc.ref_desc {
                MeasRefDesc::Fixed { .. } => {}
                MeasRefDesc::VariableInt { ref_column, .. } => {
                    let code = ref_string_to_code(self.desc.measure_type, &refer)?;
                    let ref_column = ref_column.clone();
                    self.table.set_cell(
                        row,
                        &ref_column,
                        Value::Scalar(ScalarValue::Int32(code)),
                    )?;
                }
                MeasRefDesc::VariableString { ref_column } => {
                    let ref_column = ref_column.clone();
                    self.table.set_cell(
                        row,
                        &ref_column,
                        Value::Scalar(ScalarValue::String(refer)),
                    )?;
                }
            }
        }
        Ok(())
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn read_array_values(table: &Table, row: usize, column: &str) -> Result<Vec<f64>, TableError> {
    let arr = table.get_array_cell(row, column)?;
    match arr {
        ArrayValue::Float64(a) => Ok(a.iter().copied().collect()),
        ArrayValue::Float32(a) => Ok(a.iter().map(|&v| v as f64).collect()),
        ArrayValue::Int32(a) => Ok(a.iter().map(|&v| v as f64).collect()),
        ArrayValue::Int64(a) => Ok(a.iter().map(|&v| v as f64).collect()),
        _ => Err(TableError::Storage(format!(
            "column '{column}' at row {row}: expected numeric array"
        ))),
    }
}

// ─── Default TabRefTypes / TabRefCodes generators ───────────────────────────

/// Returns the default `(TabRefTypes, TabRefCodes)` for epoch variable-int columns.
///
/// This matches C++ casacore's default mapping for `MEpoch::Types`.
pub fn default_epoch_ref_map() -> (Vec<String>, Vec<i32>) {
    let types: Vec<String> = EpochRef::ALL
        .iter()
        .map(|r| r.casacore_name().to_owned())
        .collect();
    let codes: Vec<i32> = EpochRef::ALL.iter().map(|r| r.casacore_code()).collect();
    (types, codes)
}

/// Returns the default `(TabRefTypes, TabRefCodes)` for direction variable-int columns.
pub fn default_direction_ref_map() -> (Vec<String>, Vec<i32>) {
    let types: Vec<String> = DirectionRef::ALL
        .iter()
        .map(|r| r.as_str().to_owned())
        .collect();
    let codes: Vec<i32> = DirectionRef::ALL
        .iter()
        .map(|r| r.casacore_code())
        .collect();
    (types, codes)
}

/// Returns the default `(TabRefTypes, TabRefCodes)` for frequency variable-int columns.
pub fn default_frequency_ref_map() -> (Vec<String>, Vec<i32>) {
    let types: Vec<String> = FrequencyRef::ALL
        .iter()
        .map(|r| r.as_str().to_owned())
        .collect();
    let codes: Vec<i32> = FrequencyRef::ALL
        .iter()
        .map(|r| r.casacore_code())
        .collect();
    (types, codes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use casacore_types::measures::record;
    use casacore_types::*;

    use crate::{ColumnSchema, TableSchema};

    fn make_epoch_table(fixed_ref: &str) -> Table {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "TIME",
            PrimitiveType::Float64,
            vec![1],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let desc = TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, fixed_ref);
        desc.write(&mut table).unwrap();
        table
    }

    #[test]
    fn rr_epoch_fixed_ref() {
        let mut table = make_epoch_table("UTC");
        let epochs = [
            MEpoch::from_mjd(51544.5, EpochRef::UTC),
            MEpoch::from_mjd(51545.0, EpochRef::UTC),
            MEpoch::from_mjd(51546.5, EpochRef::UTC),
        ];
        for e in &epochs {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "TIME",
                    Value::Array(ArrayValue::from_f64_vec(vec![e.value().as_mjd()])),
                )]))
                .unwrap();
        }

        let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
        for (i, expected) in epochs.iter().enumerate() {
            let got = col.get_epoch(i).unwrap();
            assert_eq!(got.refer(), expected.refer());
            assert!((got.value().as_mjd() - expected.value().as_mjd()).abs() < 1e-12);
        }
    }

    #[test]
    fn rr_epoch_var_int_ref() {
        let schema = TableSchema::new(vec![
            ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
            ColumnSchema::scalar("TimeRef", PrimitiveType::Int32),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);

        let (types, codes) = default_epoch_ref_map();
        let desc =
            TableMeasDesc::new_variable_int("TIME", MeasureType::Epoch, "TimeRef", types, codes)
                .unwrap();
        desc.write(&mut table).unwrap();

        // Add rows with different refs
        let data = [
            (51544.5, EpochRef::UTC),
            (51545.0, EpochRef::TAI),
            (51546.5, EpochRef::TT),
        ];
        for &(mjd, refer) in &data {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("TIME", Value::Array(ArrayValue::from_f64_vec(vec![mjd]))),
                    RecordField::new(
                        "TimeRef",
                        Value::Scalar(ScalarValue::Int32(refer.casacore_code())),
                    ),
                ]))
                .unwrap();
        }

        let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
        for (i, &(mjd, refer)) in data.iter().enumerate() {
            let got = col.get_epoch(i).unwrap();
            assert_eq!(got.refer(), refer, "row {i}");
            assert!((got.value().as_mjd() - mjd).abs() < 1e-12);
        }
    }

    #[test]
    fn rr_epoch_var_str_ref() {
        let schema = TableSchema::new(vec![
            ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
            ColumnSchema::scalar("TimeRefStr", PrimitiveType::String),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);

        let desc = TableMeasDesc::new_variable_string("TIME", MeasureType::Epoch, "TimeRefStr");
        desc.write(&mut table).unwrap();

        let data = [(51544.5, "UTC"), (51545.0, "TAI"), (51546.5, "TT")];
        for &(mjd, refer_str) in &data {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("TIME", Value::Array(ArrayValue::from_f64_vec(vec![mjd]))),
                    RecordField::new(
                        "TimeRefStr",
                        Value::Scalar(ScalarValue::String(refer_str.to_owned())),
                    ),
                ]))
                .unwrap();
        }

        let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
        for (i, &(mjd, refer_str)) in data.iter().enumerate() {
            let got = col.get_epoch(i).unwrap();
            assert_eq!(got.refer().as_str(), refer_str, "row {i}");
            assert!((got.value().as_mjd() - mjd).abs() < 1e-12);
        }
    }

    #[test]
    fn rr_direction_fixed_ref() {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "DIR",
            PrimitiveType::Float64,
            vec![2],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let desc = TableMeasDesc::new_fixed("DIR", MeasureType::Direction, "J2000");
        desc.write(&mut table).unwrap();

        let dirs = [
            MDirection::from_angles(1.0, 0.5, DirectionRef::J2000),
            MDirection::from_angles(2.0, -0.3, DirectionRef::J2000),
            MDirection::from_angles(0.0, 1.5, DirectionRef::J2000),
        ];
        for d in &dirs {
            let (lon, lat) = d.as_angles();
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "DIR",
                    Value::Array(ArrayValue::from_f64_vec(vec![lon, lat])),
                )]))
                .unwrap();
        }

        let col = ScalarMeasColumn::new(&table, "DIR").unwrap();
        for (i, expected) in dirs.iter().enumerate() {
            let got = col.get_direction(i).unwrap();
            assert_eq!(got.refer(), expected.refer());
            let (e_lon, e_lat) = expected.as_angles();
            let (g_lon, g_lat) = got.as_angles();
            assert!((e_lon - g_lon).abs() < 1e-12);
            assert!((e_lat - g_lat).abs() < 1e-12);
        }
    }

    #[test]
    fn rr_direction_array() {
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "DIRS",
            PrimitiveType::Float64,
            Some(1),
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let desc = TableMeasDesc::new_fixed("DIRS", MeasureType::Direction, "GALACTIC");
        desc.write(&mut table).unwrap();

        let dirs = vec![
            MDirection::from_angles(0.1, 0.2, DirectionRef::GALACTIC),
            MDirection::from_angles(0.3, 0.4, DirectionRef::GALACTIC),
        ];
        let mut values = Vec::new();
        for d in &dirs {
            let (lon, lat) = d.as_angles();
            values.push(lon);
            values.push(lat);
        }
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DIRS",
                Value::Array(ArrayValue::from_f64_vec(values)),
            )]))
            .unwrap();

        let col = ArrayMeasColumn::new(&table, "DIRS").unwrap();
        let got = col.get(0).unwrap();
        assert_eq!(got.len(), 2);
        for (i, (expected, got_mv)) in dirs.iter().zip(got.iter()).enumerate() {
            let d = got_mv.as_direction().unwrap();
            let (e_lon, e_lat) = expected.as_angles();
            let (g_lon, g_lat) = d.as_angles();
            assert!((e_lon - g_lon).abs() < 1e-12, "dir {i} lon");
            assert!((e_lat - g_lat).abs() < 1e-12, "dir {i} lat");
        }
    }

    #[test]
    fn rr_measinfo_reconstruct() {
        let table = make_epoch_table("TAI");
        let desc = TableMeasDesc::reconstruct(&table, "TIME").unwrap();
        assert_eq!(desc.measure_type(), MeasureType::Epoch);
        match desc.ref_desc() {
            MeasRefDesc::Fixed { refer } => assert_eq!(refer, "TAI"),
            _ => panic!("expected Fixed ref"),
        }
    }

    #[test]
    fn rr_epoch_fixed_offset() {
        let mut table = make_epoch_table("TAI");
        // Overwrite with offset
        let offset_rec = record::epoch_to_record(&MEpoch::from_mjd(51544.0, EpochRef::UTC));
        let desc = TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "TAI")
            .with_fixed_offset(offset_rec.clone());
        desc.write(&mut table).unwrap();

        let reconstructed = TableMeasDesc::reconstruct(&table, "TIME").unwrap();
        assert!(reconstructed.offset_desc().is_some());
        match reconstructed.offset_desc().unwrap() {
            MeasOffsetDesc::Fixed { offset } => {
                let epoch = record::epoch_from_record(offset).unwrap();
                assert_eq!(epoch.refer(), EpochRef::UTC);
                assert!((epoch.value().as_mjd() - 51544.0).abs() < 1e-12);
            }
            _ => panic!("expected Fixed offset"),
        }
    }

    #[test]
    fn rr_quantum_units_coexist() {
        use crate::table_quantum::TableQuantumDesc;

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "TIME",
            PrimitiveType::Float64,
            vec![1],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);

        // Write quantum units
        TableQuantumDesc::with_unit("TIME", "d")
            .write(&mut table)
            .unwrap();
        // Write measinfo
        TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "UTC")
            .write(&mut table)
            .unwrap();

        // Both should be present
        assert!(TableQuantumDesc::has_quanta(&table, "TIME"));
        assert!(TableMeasDesc::has_measinfo(&table, "TIME"));
    }

    #[test]
    fn rr_scalar_meas_column_mut() {
        let schema = TableSchema::new(vec![
            ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
            ColumnSchema::scalar("TimeRef", PrimitiveType::String),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);

        let desc = TableMeasDesc::new_variable_string("TIME", MeasureType::Epoch, "TimeRef");
        desc.write(&mut table).unwrap();

        // Add empty rows
        for _ in 0..3 {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("TIME", Value::Array(ArrayValue::from_f64_vec(vec![0.0]))),
                    RecordField::new("TimeRef", Value::Scalar(ScalarValue::String(String::new()))),
                ]))
                .unwrap();
        }

        // Write measures
        {
            let mut col = ScalarMeasColumnMut::new(&mut table, "TIME").unwrap();
            col.put_epoch(0, &MEpoch::from_mjd(51544.5, EpochRef::UTC))
                .unwrap();
            col.put_epoch(1, &MEpoch::from_mjd(51545.0, EpochRef::TAI))
                .unwrap();
            col.put_epoch(2, &MEpoch::from_mjd(51546.5, EpochRef::TT))
                .unwrap();
        }

        // Read back
        let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
        let e0 = col.get_epoch(0).unwrap();
        assert_eq!(e0.refer(), EpochRef::UTC);
        assert!((e0.value().as_mjd() - 51544.5).abs() < 1e-12);

        let e1 = col.get_epoch(1).unwrap();
        assert_eq!(e1.refer(), EpochRef::TAI);

        let e2 = col.get_epoch(2).unwrap();
        assert_eq!(e2.refer(), EpochRef::TT);
    }

    #[test]
    fn measure_type_values_per_measure() {
        assert_eq!(MeasureType::Epoch.values_per_measure(), 1);
        assert_eq!(MeasureType::Direction.values_per_measure(), 2);
        assert_eq!(MeasureType::Position.values_per_measure(), 3);
        assert_eq!(MeasureType::Frequency.values_per_measure(), 1);
        assert_eq!(MeasureType::Doppler.values_per_measure(), 1);
        assert_eq!(MeasureType::RadialVelocity.values_per_measure(), 1);
    }

    #[test]
    fn default_ref_maps() {
        let (types, codes) = default_epoch_ref_map();
        assert_eq!(types.len(), 12);
        assert_eq!(codes.len(), 12);
        assert_eq!(types[6], "UTC");
        assert_eq!(codes[6], 6);

        let (types, codes) = default_direction_ref_map();
        assert_eq!(types.len(), 19);
        // J2000 should be code 0
        let j2000_idx = types.iter().position(|t| t == "J2000").unwrap();
        assert_eq!(codes[j2000_idx], 0);

        let (types, codes) = default_frequency_ref_map();
        assert_eq!(types.len(), 9);
        assert_eq!(types[0], "REST");
        assert_eq!(codes[0], 0);
    }

    #[test]
    fn array_meas_put_rejects_mixed_references() {
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "DIRS",
            PrimitiveType::Float64,
            Some(1),
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let desc = TableMeasDesc::new_fixed("DIRS", MeasureType::Direction, "J2000");
        desc.write(&mut table).unwrap();

        table.add_row(RecordValue::default()).unwrap();

        let measures = vec![
            MeasureValue::Direction(MDirection::from_angles(1.0, 0.5, DirectionRef::J2000)),
            MeasureValue::Direction(MDirection::from_angles(2.0, -0.3, DirectionRef::GALACTIC)),
        ];
        let mut col = ArrayMeasColumnMut::new(&mut table, "DIRS").unwrap();
        let err = col.put(0, &measures).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("measure[1]") && msg.contains("GALACTIC") && msg.contains("J2000"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn new_variable_int_rejects_mismatched_lengths() {
        let types = vec!["UTC".to_owned(), "TAI".to_owned()];
        let codes = vec![0]; // length mismatch
        let err =
            TableMeasDesc::new_variable_int("TIME", MeasureType::Epoch, "TimeRef", types, codes)
                .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("TabRefTypes length (2)") && msg.contains("TabRefCodes length (1)"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn resolve_ref_string_returns_error_for_corrupted_measinfo() {
        // Construct a table with a variable-int ref column but
        // with a manually corrupted TabRefTypes/TabRefCodes mapping
        let schema = TableSchema::new(vec![
            ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
            ColumnSchema::scalar("TimeRef", PrimitiveType::Int32),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);

        // Use a valid ref map to write MEASINFO
        let (types, codes) = default_epoch_ref_map();
        let desc =
            TableMeasDesc::new_variable_int("TIME", MeasureType::Epoch, "TimeRef", types, codes)
                .unwrap();
        desc.write(&mut table).unwrap();

        // Add a row with a code that doesn't exist in the mapping
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TIME",
                    Value::Array(ArrayValue::from_f64_vec(vec![51544.5])),
                ),
                RecordField::new("TimeRef", Value::Scalar(ScalarValue::Int32(9999))),
            ]))
            .unwrap();

        let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
        let err = col.get_epoch(0).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("ref code 9999 not found"),
            "unexpected error: {msg}"
        );
    }
}
