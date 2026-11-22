// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared calibration-table schema, metadata, and persistence lifecycle.

use std::path::Path;

use casa_ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{
    ColumnSchema, Table, TableError, TableInfo, TableOptions, TableSchema, strip_directory,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};
use thiserror::Error;

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_FPARAM, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
    COL_WEIGHT, KEY_CASA_VERSION, KEY_MS_NAME, KEY_PAR_TYPE, KEY_POL_BASIS, KEY_VIS_CAL,
    STANDARD_SUBTABLE_KEYWORDS, TABLE_INFO_TYPE,
};
use crate::output::CalibrationOutput;
use crate::{CalibrationOutputMode, output_merge};

/// Common on-disk identity for one calibration-table family.
pub(crate) struct CalibrationTableDescriptor<'a> {
    pub output: &'a Path,
    pub schema: TableSchema,
    pub subtype: &'a str,
    pub parameter_type: Option<&'a str>,
    /// The MeasurementSet solved on, as a path or a name; the table records
    /// its base name as `MSName`, as CASA's `NewCalTable::setMetaInfo`
    /// does, and CASA appends only to a table whose `MSName` is the one it
    /// would write.
    pub measurement_set_name: String,
    pub include_polarization_basis: bool,
    pub time_extra_precision_column: Option<&'a str>,
}

/// Common calibration-table writer failure.
#[derive(Debug, Error)]
pub enum CalibrationTableWriteError {
    #[error("failed to prepare output path {path}: {reason}")]
    Prepare { path: String, reason: String },
    #[error("failed to append calibration row to {path}: {source}")]
    Append {
        path: String,
        #[source]
        source: Box<TableError>,
    },
    #[error("failed to save calibration table {path}: {source}")]
    Save {
        path: String,
        #[source]
        source: Box<TableError>,
    },
    #[error("failed to copy {subtable} subtable into {path}: {source}")]
    CopySubtable {
        subtable: String,
        path: String,
        #[source]
        source: Box<TableError>,
    },
    #[error("failed to publish staged calibration table {staging} as {output}: {reason}")]
    Publish {
        staging: String,
        output: String,
        reason: String,
    },
}

/// Typed owner of common calibration-table construction and final persistence.
pub(crate) struct CalibrationTableWriter<'a> {
    measurement_set: &'a MeasurementSet,
    output: CalibrationOutput,
    table: Table,
    mode: CalibrationOutputMode,
    sort_for_casa: bool,
}

impl<'a> CalibrationTableWriter<'a> {
    pub(crate) fn create_simulated(
        measurement_set: &'a MeasurementSet,
        descriptor: CalibrationTableDescriptor<'_>,
    ) -> Result<Self, CalibrationTableWriteError> {
        let output = CalibrationOutput::prepare_simulated(descriptor.output)?;
        let mut writer = Self::staged(
            measurement_set,
            descriptor,
            output,
            CalibrationOutputMode::Replace,
        );
        // Simulation supplies its established row order and budgets one
        // materialized table. Do not duplicate it for task storeNCT sorting.
        writer.preserve_row_order();
        Ok(writer)
    }

    pub(crate) fn preserve_row_order(&mut self) {
        self.sort_for_casa = false;
    }

    pub(crate) fn with_mode(
        measurement_set: &'a MeasurementSet,
        descriptor: CalibrationTableDescriptor<'_>,
        mode: CalibrationOutputMode,
    ) -> Result<Self, CalibrationTableWriteError> {
        let output = CalibrationOutput::prepare(descriptor.output, mode)?;
        Ok(Self::staged(measurement_set, descriptor, output, mode))
    }

    pub(crate) fn staged(
        measurement_set: &'a MeasurementSet,
        descriptor: CalibrationTableDescriptor<'_>,
        output: CalibrationOutput,
        mode: CalibrationOutputMode,
    ) -> Self {
        let mut table = Table::with_schema(descriptor.schema);
        table.set_info(TableInfo {
            table_type: TABLE_INFO_TYPE.to_string(),
            sub_type: descriptor.subtype.to_string(),
            readme: Vec::new(),
        });
        let measurement_set_name = Path::new(&descriptor.measurement_set_name)
            .file_name()
            .map_or(descriptor.measurement_set_name.clone(), |name| {
                name.to_string_lossy().into_owned()
            });
        for (key, value) in [
            (KEY_VIS_CAL, descriptor.subtype.to_string()),
            (KEY_MS_NAME, measurement_set_name),
            (KEY_CASA_VERSION, "casa-rs".to_string()),
        ] {
            table
                .keywords_mut()
                .upsert(key, Value::Scalar(ScalarValue::String(value)));
        }
        if let Some(parameter_type) = descriptor.parameter_type {
            table.keywords_mut().upsert(
                KEY_PAR_TYPE,
                Value::Scalar(ScalarValue::String(parameter_type.to_string())),
            );
        }
        if descriptor.include_polarization_basis {
            table.keywords_mut().upsert(
                KEY_POL_BASIS,
                Value::Scalar(ScalarValue::String("unknown".to_string())),
            );
        }
        set_fixed_unit_keyword(&mut table, COL_TIME, &["s"]);
        set_measinfo_keyword(&mut table, COL_TIME, "epoch", Some("UTC"));
        set_fixed_unit_keyword(&mut table, COL_INTERVAL, &["s"]);
        if let Some(column) = descriptor.time_extra_precision_column {
            set_fixed_unit_keyword(&mut table, column, &["s"]);
        }
        for name in STANDARD_SUBTABLE_KEYWORDS {
            table.keywords_mut().upsert(
                *name,
                Value::table_ref(strip_directory(&output.staging.join(name), &output.staging)),
            );
        }
        Self {
            measurement_set,
            output,
            table,
            mode,
            sort_for_casa: true,
        }
    }

    /// Staging root used for family-specific subtables before publication.
    pub(crate) fn output_path(&self) -> &Path {
        &self.output.staging
    }

    pub(crate) fn table_mut(&mut self) -> &mut Table {
        &mut self.table
    }

    pub(crate) fn append(&mut self, row: RecordValue) -> Result<(), CalibrationTableWriteError> {
        self.table
            .add_row(row)
            .map_err(|source| CalibrationTableWriteError::Append {
                path: self.output.staging.display().to_string(),
                source: Box::new(source),
            })
    }

    pub(crate) fn copy_subtables(
        &self,
        subtables: &[(SubtableId, &'static str)],
    ) -> Result<(), CalibrationTableWriteError> {
        for &(id, name) in subtables {
            self.measurement_set
                .subtable(id)
                .expect("required calibration subtable available")
                .save(TableOptions::new(self.output.staging.join(name)))
                .map_err(|source| CalibrationTableWriteError::CopySubtable {
                    subtable: name.to_string(),
                    path: self.output.staging.display().to_string(),
                    source: Box::new(source),
                })?;
        }
        Ok(())
    }

    pub(crate) fn save_main(mut self) -> Result<(), CalibrationTableWriteError> {
        if self.mode == CalibrationOutputMode::Append && self.output.destination.exists() {
            if matches!(
                self.table.info().sub_type.as_str(),
                "BPOLY" | "GSPLINE" | "EP Jones"
            ) {
                let mut old = output_merge::open(&self.output.destination)?;
                output_merge::append_legacy(
                    &mut old,
                    &mut self.table,
                    &self.output.destination,
                    &self.output.staging,
                )?;
                self.table = old;
            } else {
                let spw = output_merge::merge_spw(&self.output.destination, &self.output.staging)?;
                let mut old = output_merge::open(&self.output.destination)?;
                output_merge::append(&mut old, &self.table, &self.output.destination)?;
                output_merge::preserve_subtables(&self.output.destination, &self.output.staging)?;
                spw.save(TableOptions::new(
                    self.output.staging.join("SPECTRAL_WINDOW"),
                ))
                .map_err(|e| output_merge::invalid(&self.output.staging, e.to_string()))?;
                self.table = old;
            }
        }
        if self.sort_for_casa {
            output_merge::save_sorted(&mut self.table, &self.output.staging)?;
        } else {
            self.table
                .save(
                    TableOptions::new(&self.output.staging)
                        .with_data_manager(casa_tables::DataManagerKind::StandardStMan),
                )
                .map_err(|source| CalibrationTableWriteError::Save {
                    path: self.output.staging.display().to_string(),
                    source: Box::new(source),
                })?;
        }
        self.output.commit()
    }
}

pub(crate) fn set_fixed_unit_keyword(table: &mut Table, column: &str, units: &[&str]) {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    keywords.upsert(
        "QuantumUnits",
        Value::Array(ArrayValue::from_string_vec(
            units.iter().map(|unit| (*unit).to_string()).collect(),
        )),
    );
    table.set_column_keywords(column, keywords);
}

pub(crate) fn set_measinfo_keyword(
    table: &mut Table,
    column: &str,
    measure_type: &str,
    measure_ref: Option<&str>,
) {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    let mut fields = vec![RecordField::new(
        "type",
        Value::Scalar(ScalarValue::String(measure_type.to_string())),
    )];
    if let Some(measure_ref) = measure_ref {
        fields.push(RecordField::new(
            "Ref",
            Value::Scalar(ScalarValue::String(measure_ref.to_string())),
        ));
    }
    keywords.upsert("MEASINFO", Value::Record(RecordValue::new(fields)));
    table.set_column_keywords(column, keywords);
}

pub(crate) enum SingleChannelFrequency {
    MiddleChannel,
    Mean,
}

/// Reduce every spectral window of the SPECTRAL_WINDOW subtable at `path`
/// to one channel, as CASA's `NewCalTable::makeSpwSingleChan` does for a
/// term that does not depend on frequency: the middle channel's frequency
/// (`CHAN_FREQ[n/2]`), and CHAN_WIDTH, EFFECTIVE_BW and RESOLUTION the
/// window's TOTAL_BANDWIDTH. CASA's `applycal` refuses a table whose
/// windows keep the MeasurementSet's channels.
pub(crate) fn make_windows_single_channel(
    path: &Path,
    frequency: SingleChannelFrequency,
) -> Result<(), CalibrationTableWriteError> {
    let failed = |source: TableError| CalibrationTableWriteError::Save {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut table = Table::open(TableOptions::new(path)).map_err(failed)?;
    for row in 0..table.row_count() {
        let value = |column: &str| -> Result<Value, TableError> {
            Ok(table
                .cell_accessor(row, column)?
                .value()?
                .cloned()
                .unwrap_or_else(|| Value::Scalar(ScalarValue::Int32(0))))
        };
        let frequencies = match value("CHAN_FREQ").map_err(failed)? {
            Value::Array(ArrayValue::Float64(frequencies)) => frequencies,
            _ => continue,
        };
        let channels = frequencies.len();
        if channels <= 1 {
            continue;
        }
        let bandwidth = match value("TOTAL_BANDWIDTH").map_err(failed)? {
            Value::Scalar(ScalarValue::Float64(bandwidth)) => bandwidth,
            _ => continue,
        };
        let one = |value: f64| {
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[1]), vec![value]).expect("one channel"),
            ))
        };
        let frequency = match frequency {
            SingleChannelFrequency::MiddleChannel => frequencies
                .iter()
                .nth(channels / 2)
                .copied()
                .unwrap_or_default(),
            SingleChannelFrequency::Mean => frequencies.iter().sum::<f64>() / channels as f64,
        };
        for (column, cell) in [
            ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(1))),
            ("CHAN_FREQ", one(frequency)),
            ("CHAN_WIDTH", one(bandwidth)),
            ("EFFECTIVE_BW", one(bandwidth)),
            ("RESOLUTION", one(bandwidth)),
        ] {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut accessor| accessor.set(cell))
                .map_err(failed)?;
        }
    }
    table.flush().map_err(failed)
}

pub(crate) fn new_calibration_table_schema(complex: bool) -> TableSchema {
    let (column, parameter) = if complex {
        (COL_CPARAM, PrimitiveType::Complex32)
    } else {
        (COL_FPARAM, PrimitiveType::Float32)
    };
    TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, PrimitiveType::Int32),
        ColumnSchema::array_variable(column, parameter, None),
        ColumnSchema::array_variable(COL_PARAMERR, PrimitiveType::Float32, None),
        ColumnSchema::array_variable(COL_FLAG, PrimitiveType::Bool, None),
        ColumnSchema::array_variable(COL_SNR, PrimitiveType::Float32, None),
        ColumnSchema::array_variable(COL_WEIGHT, PrimitiveType::Float32, None),
    ])
    .expect("valid simulated calibration schema")
}

#[cfg(test)]
mod tests {

    use tempfile::tempdir;

    use crate::CalibrationOutputMode;
    use crate::output::CalibrationOutput;
    use std::fs;

    #[test]
    fn output_collision_is_rejected_without_modifying_existing_data() {
        let temp = tempdir().expect("tempdir");
        let output = temp.path().join("gain.cal");
        fs::create_dir(&output).expect("create existing output");
        let sentinel = output.join("sentinel");
        fs::write(&sentinel, b"keep").expect("write sentinel");

        let error = CalibrationOutput::prepare(&output, CalibrationOutputMode::Create)
            .expect_err("collision must fail");

        assert!(error.to_string().contains("output already exists"));
        assert_eq!(fs::read(&sentinel).expect("sentinel remains"), b"keep");
    }

    #[test]
    fn interrupted_staging_output_is_visible_and_blocks_reuse() {
        let temp = tempdir().expect("tempdir");
        let output = temp.path().join("bandpass.cal");
        let staging = CalibrationOutput::prepare(&output, CalibrationOutputMode::Create)
            .expect("reserve staging output");
        fs::write(staging.staging.join("partial"), b"incomplete").expect("write partial output");

        let error = CalibrationOutput::prepare(&output, CalibrationOutputMode::Create)
            .expect_err("staging collision must fail");

        assert!(!output.exists());
        assert_eq!(
            fs::read(staging.staging.join("partial")).expect("partial remains"),
            b"incomplete"
        );
        assert!(
            error
                .to_string()
                .contains("another writer or interrupted publication")
        );
    }
}
