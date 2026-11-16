// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publication policy for scientific calibration products.

use std::fs;
use std::path::{Path, PathBuf};

use crate::CalibrationTableWriteError;

/// How a calibration operation treats an existing destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalibrationOutputMode {
    /// Refuse an existing product.
    Create,
    /// Publish a replacement after the complete new product is written.
    Replace,
    /// Append compatible solution rows, retaining the original metadata.
    Append,
    /// Add specified corrections to an existing generated calibration table.
    Update,
}

/// Owns only this operation's staging directory. An interrupted predecessor
/// is refused; errors clean up our own stage and leave the destination intact.
#[derive(Debug)]
pub(crate) struct CalibrationOutput {
    pub(crate) destination: PathBuf,
    pub(crate) staging: PathBuf,
    mode: CalibrationOutputMode,
    table_type: &'static str,
}

impl CalibrationOutput {
    pub(crate) fn prepare(
        path: &Path,
        mode: CalibrationOutputMode,
    ) -> Result<Self, CalibrationTableWriteError> {
        Self::prepare_type(path, mode, crate::constants::TABLE_INFO_TYPE)
    }

    pub(crate) fn prepare_measurement_set(
        path: &Path,
        overwrite: bool,
    ) -> Result<Self, CalibrationTableWriteError> {
        Self::prepare_type(
            path,
            if overwrite {
                CalibrationOutputMode::Replace
            } else {
                CalibrationOutputMode::Create
            },
            "Measurement Set",
        )
    }

    fn prepare_type(
        path: &Path,
        mode: CalibrationOutputMode,
        table_type: &'static str,
    ) -> Result<Self, CalibrationTableWriteError> {
        let fail = |reason| CalibrationTableWriteError::Prepare {
            path: path.display().to_string(),
            reason,
        };
        if path.exists() {
            if mode == CalibrationOutputMode::Create {
                return Err(fail(
                    "output already exists; use append or replacement explicitly".to_string(),
                ));
            }
            validate_destination(path, table_type)?;
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent).map_err(|e| fail(e.to_string()))?;
        let staging = sibling(path, "casa-rs-incomplete")?;
        fs::create_dir(&staging).map_err(|e| fail(format!("cannot reserve staging path {} (an interrupted or concurrent write may exist): {e}", staging.display())))?;
        Ok(Self {
            destination: path.to_path_buf(),
            staging,
            mode,
            table_type,
        })
    }

    pub(crate) fn commit(&self) -> Result<(), CalibrationTableWriteError> {
        let fail = |reason| CalibrationTableWriteError::Publish {
            staging: self.staging.display().to_string(),
            output: self.destination.display().to_string(),
            reason,
        };
        if !self.destination.exists() {
            return fs::rename(&self.staging, &self.destination).map_err(|e| fail(e.to_string()));
        }
        if self.mode == CalibrationOutputMode::Create {
            return Err(fail(
                "destination appeared while the table was being built".to_string(),
            ));
        }
        validate_destination(&self.destination, self.table_type)?;
        let replaced = sibling(&self.destination, "casa-rs-replaced")?;
        if replaced.exists() {
            return Err(fail(format!(
                "backup {} already exists; prior product is unchanged",
                replaced.display()
            )));
        }
        fs::rename(&self.destination, &replaced).map_err(|e| fail(e.to_string()))?;
        if let Err(error) = fs::rename(&self.staging, &self.destination) {
            let restored = fs::rename(&replaced, &self.destination);
            return Err(fail(format!(
                "{error}; prior product {}",
                if restored.is_ok() {
                    "restored".to_string()
                } else {
                    format!("left at {}", replaced.display())
                }
            )));
        }
        fs::remove_dir_all(&replaced).map_err(|e| {
            fail(format!(
                "new product published; prior product left at {}: {e}",
                replaced.display()
            ))
        })
    }
}

impl Drop for CalibrationOutput {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.staging);
    }
}

pub(crate) fn validate_destination(
    path: &Path,
    table_type: &str,
) -> Result<(), CalibrationTableWriteError> {
    let table = casa_tables::Table::open(casa_tables::TableOptions::new(path));
    if table
        .as_ref()
        .is_ok_and(|t| t.info().table_type == table_type)
    {
        return Ok(());
    }
    Err(CalibrationTableWriteError::Prepare {
        path: path.display().to_string(),
        reason: format!("exists and is not a {table_type} table; prior product is unchanged"),
    })
}

fn sibling(path: &Path, what: &str) -> Result<PathBuf, CalibrationTableWriteError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| CalibrationTableWriteError::Prepare {
            path: path.display().to_string(),
            reason: "output path must name a table".to_string(),
        })?
        .to_string_lossy();
    Ok(parent.join(format!(".{name}.{what}-{}", std::process::id())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_tables::{Table, TableInfo, TableOptions};

    #[test]
    fn a_missing_completed_stage_restores_the_previous_product() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("old.cal");
        let mut table = Table::with_schema(
            casa_tables::TableSchema::new(vec![casa_tables::ColumnSchema::scalar(
                "TIME",
                casa_types::PrimitiveType::Float64,
            )])
            .unwrap(),
        );
        table.set_info(TableInfo {
            table_type: "Calibration".to_string(),
            sub_type: "G Jones".to_string(),
            readme: vec![],
        });
        table.save(TableOptions::new(&destination)).unwrap();
        let original = std::fs::read(destination.join("table.dat")).unwrap();
        let output =
            CalibrationOutput::prepare(&destination, CalibrationOutputMode::Replace).unwrap();
        std::fs::remove_dir(&output.staging).unwrap();
        assert!(output.commit().is_err());
        assert_eq!(
            std::fs::read(destination.join("table.dat")).unwrap(),
            original
        );
    }
}
