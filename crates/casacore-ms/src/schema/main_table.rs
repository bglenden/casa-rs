// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column and keyword definitions for the MS main table.
//!
//! The main table holds one row per visibility sample (baseline x time).
//! Required columns include antenna IDs, time, UVW, flags, and weights.
//! Data columns (DATA, CORRECTED_DATA, etc.) are optional.
//!
//! Cf. C++ `MSMainEnums.h` and `MSTableImpl::colMapDef` for the main table.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind, KeywordDef, KeywordValueType};
use crate::schema::SubtableId;

/// Required columns of the MS main table.
///
/// These 21 columns must be present in every valid MeasurementSet.
/// Cf. C++ `MSMainEnums::PredefinedColumns` (required subset).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "ANTENNA1",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID of first antenna in interferometer",
    },
    ColumnDef {
        name: "ANTENNA2",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID of second antenna in interferometer",
    },
    ColumnDef {
        name: "ARRAY_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID of array or subarray",
    },
    ColumnDef {
        name: "DATA_DESC_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The data description table index",
    },
    ColumnDef {
        name: "EXPOSURE",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: None,
        measure_ref: "",
        comment: "The effective integration time",
    },
    ColumnDef {
        name: "FEED1",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The feed index for ANTENNA1",
    },
    ColumnDef {
        name: "FEED2",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The feed index for ANTENNA2",
    },
    ColumnDef {
        name: "FIELD_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Unique id for this pointing",
    },
    ColumnDef {
        name: "FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The data flags, array of bools with same shape as data",
    },
    ColumnDef {
        name: "FLAG_CATEGORY",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::VariableArray { ndim: 3 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The flag category, NUM_CAT flags for each datum",
    },
    ColumnDef {
        name: "FLAG_ROW",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Row flag - flag all data in this row if True",
    },
    ColumnDef {
        name: "INTERVAL",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: None,
        measure_ref: "",
        comment: "The sampling interval",
    },
    ColumnDef {
        name: "OBSERVATION_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID for this observation, index in OBSERVATION table",
    },
    ColumnDef {
        name: "PROCESSOR_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Id for backend processor, index in PROCESSOR table",
    },
    ColumnDef {
        name: "SCAN_NUMBER",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Sequential scan number from on-line system",
    },
    ColumnDef {
        name: "SIGMA",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Estimated rms noise for channel with unity bandpass response",
    },
    ColumnDef {
        name: "STATE_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID for this observing state",
    },
    ColumnDef {
        name: "TIME",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Modified Julian Day",
    },
    ColumnDef {
        name: "TIME_CENTROID",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Modified Julian Day for centroid",
    },
    ColumnDef {
        name: "UVW",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[3] },
        unit: "m",
        // C++ uses Muvw measure type, but we map to Direction as the
        // closest available. The UVW measure reference is handled specially.
        measure_type: None,
        measure_ref: "",
        comment: "Vector with uvw coordinates (in meters)",
    },
    ColumnDef {
        name: "WEIGHT",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Weight for each polarization spectrum",
    },
];

/// Optional columns of the MS main table.
///
/// Cf. C++ `MSMainEnums::PredefinedColumns` (optional subset).
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "ANTENNA3",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID of third antenna in triple correlations",
    },
    ColumnDef {
        name: "BASELINE_REF",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Reference antenna for this baseline, True for ANTENNA1",
    },
    ColumnDef {
        name: "CORRECTED_DATA",
        data_type: PrimitiveType::Complex32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The corrected data column",
    },
    ColumnDef {
        name: "DATA",
        data_type: PrimitiveType::Complex32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The data column, i.e. the raw data",
    },
    ColumnDef {
        name: "FEED3",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Feed id on ANTENNA3",
    },
    ColumnDef {
        name: "FLOAT_DATA",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Floating point data - for single dish",
    },
    ColumnDef {
        name: "IMAGING_WEIGHT",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Weight set by imaging task (e.g. uniform weighting)",
    },
    ColumnDef {
        name: "LAG_DATA",
        data_type: PrimitiveType::Complex32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The lag data column (NUM_CORR, NUM_LAG)",
    },
    ColumnDef {
        name: "MODEL_DATA",
        data_type: PrimitiveType::Complex32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The model data column",
    },
    ColumnDef {
        name: "PHASE_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Id for phase switching",
    },
    ColumnDef {
        name: "PULSAR_BIN",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Pulsar pulse-phase bin for this DATA",
    },
    ColumnDef {
        name: "PULSAR_GATE_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID for this gate, index into PULSAR_GATE table",
    },
    ColumnDef {
        name: "SIGMA_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Estimated rms noise for each data point",
    },
    ColumnDef {
        name: "TIME_EXTRA_PREC",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: None,
        measure_ref: "",
        comment: "Additional precision for TIME",
    },
    ColumnDef {
        name: "UVW2",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[3] },
        unit: "m",
        measure_type: None,
        measure_ref: "",
        comment: "UVW for second pair of triple correlation",
    },
    ColumnDef {
        name: "VIDEO_POINT",
        data_type: PrimitiveType::Complex32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Zero frequency point for lag transform",
    },
    ColumnDef {
        name: "WEIGHT_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Weight for each data point",
    },
    ColumnDef {
        name: "CORRECTED_WEIGHT_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Weight for each corrected data point",
    },
];

/// Enum-backed selector for optional MS main-table columns.
///
/// This replaces stringly typed column selection in
/// [`MeasurementSetBuilder`](crate::builder::MeasurementSetBuilder) so callers
/// get compile-time validation and deterministic output ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OptionalMainColumn {
    /// ID of third antenna in triple correlations.
    Antenna3,
    /// Reference antenna flag for this baseline.
    BaselineRef,
    /// Corrected complex visibility data.
    CorrectedData,
    /// Raw complex visibility data.
    Data,
    /// Feed index for the third antenna.
    Feed3,
    /// Single-dish float visibility data.
    FloatData,
    /// Imaging weight column.
    ImagingWeight,
    /// Lag-domain visibility data.
    LagData,
    /// Model visibility data.
    ModelData,
    /// ID for this phase.
    PhaseId,
    /// Pulsar bin number.
    PulsarBin,
    /// Pulsar gate ID.
    PulsarGateId,
    /// Per-channel sigma estimates.
    SigmaSpectrum,
    /// Extra precision term for TIME.
    TimeExtraPrec,
    /// Secondary UVW coordinates.
    Uvw2,
    /// Video-point visibility flag.
    VideoPoint,
    /// Per-channel weights.
    WeightSpectrum,
    /// Per-channel corrected weights.
    CorrectedWeightSpectrum,
}

impl OptionalMainColumn {
    /// All optional main-table columns in canonical MS schema order.
    pub const ALL: &[Self] = &[
        Self::Antenna3,
        Self::BaselineRef,
        Self::CorrectedData,
        Self::Data,
        Self::Feed3,
        Self::FloatData,
        Self::ImagingWeight,
        Self::LagData,
        Self::ModelData,
        Self::PhaseId,
        Self::PulsarBin,
        Self::PulsarGateId,
        Self::SigmaSpectrum,
        Self::TimeExtraPrec,
        Self::Uvw2,
        Self::VideoPoint,
        Self::WeightSpectrum,
        Self::CorrectedWeightSpectrum,
    ];

    /// The on-disk column name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Antenna3 => "ANTENNA3",
            Self::BaselineRef => "BASELINE_REF",
            Self::CorrectedData => "CORRECTED_DATA",
            Self::Data => "DATA",
            Self::Feed3 => "FEED3",
            Self::FloatData => "FLOAT_DATA",
            Self::ImagingWeight => "IMAGING_WEIGHT",
            Self::LagData => "LAG_DATA",
            Self::ModelData => "MODEL_DATA",
            Self::PhaseId => "PHASE_ID",
            Self::PulsarBin => "PULSAR_BIN",
            Self::PulsarGateId => "PULSAR_GATE_ID",
            Self::SigmaSpectrum => "SIGMA_SPECTRUM",
            Self::TimeExtraPrec => "TIME_EXTRA_PREC",
            Self::Uvw2 => "UVW2",
            Self::VideoPoint => "VIDEO_POINT",
            Self::WeightSpectrum => "WEIGHT_SPECTRUM",
            Self::CorrectedWeightSpectrum => "CORRECTED_WEIGHT_SPECTRUM",
        }
    }

    /// The schema definition for this optional column.
    pub const fn column_def(self) -> &'static ColumnDef {
        match self {
            Self::Antenna3 => &OPTIONAL_COLUMNS[0],
            Self::BaselineRef => &OPTIONAL_COLUMNS[1],
            Self::CorrectedData => &OPTIONAL_COLUMNS[2],
            Self::Data => &OPTIONAL_COLUMNS[3],
            Self::Feed3 => &OPTIONAL_COLUMNS[4],
            Self::FloatData => &OPTIONAL_COLUMNS[5],
            Self::ImagingWeight => &OPTIONAL_COLUMNS[6],
            Self::LagData => &OPTIONAL_COLUMNS[7],
            Self::ModelData => &OPTIONAL_COLUMNS[8],
            Self::PhaseId => &OPTIONAL_COLUMNS[9],
            Self::PulsarBin => &OPTIONAL_COLUMNS[10],
            Self::PulsarGateId => &OPTIONAL_COLUMNS[11],
            Self::SigmaSpectrum => &OPTIONAL_COLUMNS[12],
            Self::TimeExtraPrec => &OPTIONAL_COLUMNS[13],
            Self::Uvw2 => &OPTIONAL_COLUMNS[14],
            Self::VideoPoint => &OPTIONAL_COLUMNS[15],
            Self::WeightSpectrum => &OPTIONAL_COLUMNS[16],
            Self::CorrectedWeightSpectrum => &OPTIONAL_COLUMNS[17],
        }
    }
}

/// Enum-backed selector for the complex visibility data columns.
///
/// These columns all share the same typed accessor family and differ only by
/// which optional main-table column is selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum VisibilityDataColumn {
    /// Corrected complex visibility data.
    CorrectedData,
    /// Raw complex visibility data.
    Data,
    /// Model visibility data.
    ModelData,
}

impl VisibilityDataColumn {
    /// All complex visibility data columns in canonical schema order.
    pub const ALL: &[Self] = &[Self::CorrectedData, Self::Data, Self::ModelData];

    /// The on-disk column name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::CorrectedData => "CORRECTED_DATA",
            Self::Data => "DATA",
            Self::ModelData => "MODEL_DATA",
        }
    }

    /// The corresponding optional main-table column selector.
    pub const fn optional_column(self) -> OptionalMainColumn {
        match self {
            Self::CorrectedData => OptionalMainColumn::CorrectedData,
            Self::Data => OptionalMainColumn::Data,
            Self::ModelData => OptionalMainColumn::ModelData,
        }
    }
}

impl From<VisibilityDataColumn> for OptionalMainColumn {
    fn from(value: VisibilityDataColumn) -> Self {
        value.optional_column()
    }
}

/// Required keywords of the MS main table.
///
/// Includes the 12 required subtable references plus MS_VERSION.
/// Cf. C++ `MSMainEnums::PredefinedKeywords`.
pub const REQUIRED_KEYWORDS: &[KeywordDef] = &[
    KeywordDef {
        name: "MS_VERSION",
        value_type: KeywordValueType::Float,
        required: true,
        comment: "MS version number",
    },
    KeywordDef {
        name: SubtableId::Antenna.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Antenna subtable",
    },
    KeywordDef {
        name: SubtableId::DataDescription.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Data description subtable",
    },
    KeywordDef {
        name: SubtableId::Feed.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Feed subtable",
    },
    KeywordDef {
        name: SubtableId::Field.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Field subtable",
    },
    KeywordDef {
        name: SubtableId::FlagCmd.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Flag command subtable",
    },
    KeywordDef {
        name: SubtableId::History.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "History subtable",
    },
    KeywordDef {
        name: SubtableId::Observation.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Observation subtable",
    },
    KeywordDef {
        name: SubtableId::Pointing.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Pointing subtable",
    },
    KeywordDef {
        name: SubtableId::Polarization.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Polarization subtable",
    },
    KeywordDef {
        name: SubtableId::Processor.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Processor subtable",
    },
    KeywordDef {
        name: SubtableId::SpectralWindow.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "Spectral window subtable",
    },
    KeywordDef {
        name: SubtableId::State.name(),
        value_type: KeywordValueType::Table,
        required: true,
        comment: "State subtable",
    },
];

/// Optional keywords of the MS main table.
pub const OPTIONAL_KEYWORDS: &[KeywordDef] = &[
    KeywordDef {
        name: SubtableId::Doppler.name(),
        value_type: KeywordValueType::Table,
        required: false,
        comment: "Doppler subtable",
    },
    KeywordDef {
        name: SubtableId::FreqOffset.name(),
        value_type: KeywordValueType::Table,
        required: false,
        comment: "Frequency offset subtable",
    },
    KeywordDef {
        name: SubtableId::Source.name(),
        value_type: KeywordValueType::Table,
        required: false,
        comment: "Source subtable",
    },
    KeywordDef {
        name: SubtableId::SysCal.name(),
        value_type: KeywordValueType::Table,
        required: false,
        comment: "System calibration subtable",
    },
    KeywordDef {
        name: SubtableId::Weather.name(),
        value_type: KeywordValueType::Table,
        required: false,
        comment: "Weather subtable",
    },
    KeywordDef {
        name: "SORT_COLUMNS",
        value_type: KeywordValueType::String,
        required: false,
        comment: "Columns on which the MS is sorted",
    },
    KeywordDef {
        name: "SORT_ORDER",
        value_type: KeywordValueType::String,
        required: false,
        comment: "Sort order of the MS",
    },
    KeywordDef {
        name: "SORTED_TABLES",
        value_type: KeywordValueType::Table,
        required: false,
        comment: "Sorted table references",
    },
    KeywordDef {
        name: "CAL_TABLES",
        value_type: KeywordValueType::Table,
        required: false,
        comment: "Calibration table references",
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use std::collections::HashSet;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 21);
    }

    #[test]
    fn no_duplicate_required_column_names() {
        let names: HashSet<&str> = REQUIRED_COLUMNS.iter().map(|c| c.name).collect();
        assert_eq!(names.len(), REQUIRED_COLUMNS.len());
    }

    #[test]
    fn no_duplicate_optional_column_names() {
        let names: HashSet<&str> = OPTIONAL_COLUMNS.iter().map(|c| c.name).collect();
        assert_eq!(names.len(), OPTIONAL_COLUMNS.len());
    }

    #[test]
    fn no_overlap_required_optional() {
        let req: HashSet<&str> = REQUIRED_COLUMNS.iter().map(|c| c.name).collect();
        for col in OPTIONAL_COLUMNS {
            assert!(!req.contains(col.name), "overlap: {}", col.name);
        }
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("required schema should build");
    }

    #[test]
    fn optional_main_column_enum_matches_schema() {
        assert_eq!(OptionalMainColumn::ALL.len(), OPTIONAL_COLUMNS.len());
        for column in OptionalMainColumn::ALL {
            assert_eq!(column.column_def().name, column.name());
        }
    }

    #[test]
    fn visibility_data_columns_map_to_optional_columns() {
        assert_eq!(
            VisibilityDataColumn::Data.optional_column(),
            OptionalMainColumn::Data
        );
        assert_eq!(
            VisibilityDataColumn::CorrectedData.optional_column().name(),
            "CORRECTED_DATA"
        );
        assert_eq!(
            VisibilityDataColumn::ModelData.optional_column().name(),
            "MODEL_DATA"
        );
    }

    #[test]
    fn required_keywords_include_ms_version() {
        assert!(REQUIRED_KEYWORDS.iter().any(|k| k.name == "MS_VERSION"));
    }

    #[test]
    fn required_keywords_include_all_required_subtables() {
        for id in super::SubtableId::ALL_REQUIRED {
            assert!(
                REQUIRED_KEYWORDS.iter().any(|k| k.name == id.name()),
                "missing keyword for subtable {}",
                id.name()
            );
        }
    }
}
