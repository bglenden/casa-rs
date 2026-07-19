// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared calibration-table schema, metadata, and persistence lifecycle.

use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{DataManagerKind, Table, TableError, TableInfo, TableOptions, TableSchema};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use thiserror::Error;

use crate::constants::{
    COL_INTERVAL, COL_TIME, KEY_CASA_VERSION, KEY_MS_NAME, KEY_PAR_TYPE, KEY_POL_BASIS,
    KEY_VIS_CAL, STANDARD_SUBTABLE_KEYWORDS, TABLE_INFO_TYPE,
};

/// Common on-disk identity for one calibration-table family.
pub(crate) struct CalibrationTableDescriptor<'a> {
    pub output: &'a Path,
    pub schema: TableSchema,
    pub subtype: &'a str,
    pub parameter_type: Option<&'a str>,
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
    final_output: PathBuf,
    output: PathBuf,
    table: Table,
}

impl<'a> CalibrationTableWriter<'a> {
    pub(crate) fn create(
        measurement_set: &'a MeasurementSet,
        descriptor: CalibrationTableDescriptor<'_>,
    ) -> Result<Self, CalibrationTableWriteError> {
        let output = prepare_output_root(descriptor.output)?;
        let mut table = Table::with_schema(descriptor.schema);
        table.set_info(TableInfo {
            table_type: TABLE_INFO_TYPE.to_string(),
            sub_type: descriptor.subtype.to_string(),
            readme: Vec::new(),
        });
        for (key, value) in [
            (KEY_VIS_CAL, descriptor.subtype.to_string()),
            (KEY_MS_NAME, descriptor.measurement_set_name),
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
                Value::table_ref(subtable_keyword_value(&output, &output.join(name))),
            );
        }
        Ok(Self {
            measurement_set,
            final_output: descriptor.output.to_path_buf(),
            output,
            table,
        })
    }

    /// Staging root used for family-specific subtables before publication.
    pub(crate) fn output_path(&self) -> &Path {
        &self.output
    }

    pub(crate) fn table_mut(&mut self) -> &mut Table {
        &mut self.table
    }

    pub(crate) fn append(&mut self, row: RecordValue) -> Result<(), CalibrationTableWriteError> {
        self.table
            .add_row(row)
            .map_err(|source| CalibrationTableWriteError::Append {
                path: self.output.display().to_string(),
                source: Box::new(source),
            })
    }

    pub(crate) fn finish(
        self,
        subtables: &[(SubtableId, &'static str)],
    ) -> Result<(), CalibrationTableWriteError> {
        self.copy_subtables(subtables)?;
        self.save_main()
    }

    pub(crate) fn copy_subtables(
        &self,
        subtables: &[(SubtableId, &'static str)],
    ) -> Result<(), CalibrationTableWriteError> {
        for &(id, name) in subtables {
            self.measurement_set
                .subtable(id)
                .expect("required calibration subtable available")
                .save(TableOptions::new(self.output.join(name)))
                .map_err(|source| CalibrationTableWriteError::CopySubtable {
                    subtable: name.to_string(),
                    path: self.output.display().to_string(),
                    source: Box::new(source),
                })?;
        }
        Ok(())
    }

    pub(crate) fn save_main(self) -> Result<(), CalibrationTableWriteError> {
        self.table
            .save(TableOptions::new(&self.output).with_data_manager(DataManagerKind::StandardStMan))
            .map_err(|source| CalibrationTableWriteError::Save {
                path: self.output.display().to_string(),
                source: Box::new(source),
            })?;
        if self.final_output.exists() {
            return Err(CalibrationTableWriteError::Publish {
                staging: self.output.display().to_string(),
                output: self.final_output.display().to_string(),
                reason: "destination appeared while the table was being built".to_string(),
            });
        }
        fs::rename(&self.output, &self.final_output).map_err(|error| {
            CalibrationTableWriteError::Publish {
                staging: self.output.display().to_string(),
                output: self.final_output.display().to_string(),
                reason: error.to_string(),
            }
        })
    }
}

fn prepare_output_root(path: &Path) -> Result<PathBuf, CalibrationTableWriteError> {
    if path.exists() {
        return Err(CalibrationTableWriteError::Prepare {
            path: path.display().to_string(),
            reason: "output already exists".to_string(),
        });
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent).map_err(|error| CalibrationTableWriteError::Prepare {
        path: path.display().to_string(),
        reason: error.to_string(),
    })?;
    let name = path
        .file_name()
        .ok_or_else(|| CalibrationTableWriteError::Prepare {
            path: path.display().to_string(),
            reason: "output path must name a table".to_string(),
        })?
        .to_string_lossy();
    let staging = parent.join(format!(".{name}.casa-rs-incomplete-{}", std::process::id()));
    fs::create_dir(&staging).map_err(|error| CalibrationTableWriteError::Prepare {
        path: path.display().to_string(),
        reason: format!(
            "cannot reserve staging path {} (an interrupted or concurrent write may exist): {error}",
            staging.display()
        ),
    })?;
    Ok(staging)
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

pub(crate) fn subtable_keyword_value(base_path: &Path, subtable_path: &Path) -> String {
    if let Ok(relative) = subtable_path.strip_prefix(base_path) {
        return format!("././{}", relative.to_string_lossy());
    }
    if let Some(parent) = base_path.parent()
        && let Ok(relative) = subtable_path.strip_prefix(parent)
    {
        return format!("./{}", relative.to_string_lossy());
    }
    if subtable_path.is_relative() {
        return format!(
            "././{}",
            subtable_path.to_string_lossy().trim_start_matches("./")
        );
    }
    subtable_path.to_string_lossy().to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::prepare_output_root;

    #[test]
    fn output_collision_is_rejected_without_modifying_existing_data() {
        let temp = tempdir().expect("tempdir");
        let output = temp.path().join("gain.cal");
        fs::create_dir(&output).expect("create existing output");
        let sentinel = output.join("sentinel");
        fs::write(&sentinel, b"keep").expect("write sentinel");

        let error = prepare_output_root(&output).expect_err("collision must fail");

        assert!(error.to_string().contains("output already exists"));
        assert_eq!(fs::read(&sentinel).expect("sentinel remains"), b"keep");
    }

    #[test]
    fn interrupted_staging_output_is_visible_and_blocks_reuse() {
        let temp = tempdir().expect("tempdir");
        let output = temp.path().join("bandpass.cal");
        let staging = prepare_output_root(&output).expect("reserve staging output");
        fs::write(staging.join("partial"), b"incomplete").expect("write partial output");

        let error = prepare_output_root(&output).expect_err("staging collision must fail");

        assert!(!output.exists());
        assert_eq!(
            fs::read(staging.join("partial")).expect("partial remains"),
            b"incomplete"
        );
        assert!(
            error
                .to_string()
                .contains("interrupted or concurrent write")
        );
    }
}
