// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publication policy for scientific calibration products.

use std::fs;
use std::path::{Path, PathBuf};

use crate::table::CalibrationTableWriteError;

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
pub struct CalibrationOutput {
    pub(crate) destination: PathBuf,
    /// Temporary sibling path that owns the unpublished calibration output.
    pub staging: PathBuf,
    mode: CalibrationOutputMode,
    table_type: Option<&'static str>,
    table_subtype: Option<&'static str>,
    _reservation: Reservation,
    simulated_marker: bool,
    replace_empty_directory: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplacementException {
    None,
    SimulatedMarker,
    EmptyDirectory,
}

/// A destination-wide claim survives a killed writer. Only its owner removes it.
#[derive(Debug)]
struct Reservation(PathBuf);

impl Drop for Reservation {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.0);
    }
}

impl CalibrationOutput {
    pub(crate) fn prepare(
        path: &Path,
        mode: CalibrationOutputMode,
    ) -> Result<Self, CalibrationTableWriteError> {
        Self::prepare_type(
            path,
            mode,
            Some(crate::constants::TABLE_INFO_TYPE),
            None,
            ReplacementException::None,
        )
    }

    /// CASA single-dish solves also allow explicit replacement of a separate MS.
    pub(crate) fn prepare_single_dish_solve(
        path: &Path,
        mode: CalibrationOutputMode,
    ) -> Result<Self, CalibrationTableWriteError> {
        if mode == CalibrationOutputMode::Replace
            && fs::read_to_string(path.join("table.info")).is_ok_and(|text| {
                casa_tables::TableInfo::parse(&text).table_type == "Measurement Set"
            })
        {
            Self::prepare_measurement_set(path, true)
        } else {
            Self::prepare(path, mode)
        }
    }

    /// Simulation already validates the destination against its MS and has
    /// historically replaced an incomplete table identified by table.dat.
    pub(crate) fn prepare_simulated(path: &Path) -> Result<Self, CalibrationTableWriteError> {
        Self::prepare_type(
            path,
            CalibrationOutputMode::Replace,
            Some(crate::constants::TABLE_INFO_TYPE),
            None,
            ReplacementException::SimulatedMarker,
        )
    }

    /// Reserve and stage a MeasurementSet, optionally replacing an existing MS.
    pub fn prepare_measurement_set(
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
            Some("Measurement Set"),
            None,
            ReplacementException::None,
        )
    }

    /// Atmospheric correction can replace an explicitly supplied empty output directory.
    pub fn prepare_atmosphere(
        path: &Path,
        overwrite: bool,
    ) -> Result<Self, CalibrationTableWriteError> {
        if !overwrite {
            return Self::prepare_measurement_set(path, false);
        }
        Self::prepare_type(
            path,
            CalibrationOutputMode::Replace,
            Some("Measurement Set"),
            None,
            ReplacementException::EmptyDirectory,
        )
    }

    /// Detached imports replace any readable casacore destination table.
    pub fn prepare_import(path: &Path) -> Result<Self, CalibrationTableWriteError> {
        Self::prepare_type(
            path,
            CalibrationOutputMode::Replace,
            None,
            None,
            ReplacementException::None,
        )
    }

    /// Reserve and stage a CASA single-dish baseline ApplyTable.
    pub fn prepare_baseline_table(
        path: &Path,
        overwrite: bool,
    ) -> Result<Self, CalibrationTableWriteError> {
        let mode = if overwrite {
            CalibrationOutputMode::Replace
        } else {
            CalibrationOutputMode::Create
        };
        Self::prepare_type(
            path,
            mode,
            Some("ApplyTable"),
            None,
            ReplacementException::None,
        )
    }

    /// The normalized publication destination.
    pub fn destination(&self) -> &Path {
        &self.destination
    }

    /// Prepares an ephemeris subtable for the staged output before it is published.
    pub fn prepare_ephemeris_table(
        path: &Path,
        overwrite: bool,
    ) -> Result<Self, CalibrationTableWriteError> {
        let mode = if overwrite {
            CalibrationOutputMode::Replace
        } else {
            CalibrationOutputMode::Create
        };
        Self::prepare_type(
            path,
            mode,
            Some("IERS"),
            Some("Comet"),
            ReplacementException::None,
        )
    }

    fn prepare_type(
        path: &Path,
        mode: CalibrationOutputMode,
        table_type: Option<&'static str>,
        table_subtype: Option<&'static str>,
        replacement: ReplacementException,
    ) -> Result<Self, CalibrationTableWriteError> {
        let fail = |reason| CalibrationTableWriteError::Prepare {
            path: path.display().to_string(),
            reason,
        };
        let destination = crate::paths::normalized(path).map_err(|e| fail(e.to_string()))?;
        let path = destination.as_path();
        let parent = path
            .parent()
            .ok_or_else(|| fail("output path must name a table".to_string()))?;
        fs::create_dir_all(parent).map_err(|e| fail(e.to_string()))?;
        let name = path
            .file_name()
            .ok_or_else(|| fail("output path must name a table".to_string()))?
            .to_string_lossy();
        let reservation = parent.join(format!(".{name}.casa-rs-reservation"));
        fs::create_dir(&reservation).map_err(|e| {
            fail(format!(
                "cannot reserve {}: another writer or interrupted publication may exist: {e}",
                reservation.display()
            ))
        })?;
        let reservation = Reservation(reservation);
        for entry in fs::read_dir(parent).map_err(|e| fail(e.to_string()))? {
            let entry = entry.map_err(|e| fail(e.to_string()))?;
            let entry_name = entry.file_name();
            let entry_name = entry_name.to_string_lossy();
            if ["casa-rs-incomplete", "casa-rs-replaced"]
                .iter()
                .any(|kind| entry_name.starts_with(&format!(".{name}.{kind}-")))
            {
                return Err(fail(format!(
                    "interrupted publication at {}; preserve and recover the prior product before retrying",
                    entry.path().display()
                )));
            }
        }
        let simulated_marker = replacement == ReplacementException::SimulatedMarker;
        let replace_empty_directory = replacement == ReplacementException::EmptyDirectory
            && fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_none());
        if path.exists() {
            if mode == CalibrationOutputMode::Create {
                return Err(fail(
                    "output already exists; use append or replacement explicitly".to_string(),
                ));
            }
            if !replace_empty_directory {
                validate_product(path, table_type, table_subtype, simulated_marker)?;
            }
        }
        let staging = sibling(path, "casa-rs-incomplete")?;
        fs::create_dir(&staging).map_err(|e| fail(format!("cannot reserve staging path {} (an interrupted or concurrent write may exist): {e}", staging.display())))?;
        Ok(Self {
            destination,
            staging,
            mode,
            table_type,
            table_subtype,
            _reservation: reservation,
            simulated_marker,
            replace_empty_directory,
        })
    }

    /// Publishes the staged calibration table using the selected output mode. Rename or destination
    /// conflicts return a writer error.
    pub fn commit(&self) -> Result<(), CalibrationTableWriteError> {
        let fail = |reason| CalibrationTableWriteError::Publish {
            staging: self.staging.display().to_string(),
            output: self.destination.display().to_string(),
            reason,
        };
        if !self.destination.exists() || self.replace_empty_directory {
            // A directory rename can only replace an empty directory. If another
            // process added content during the run, the existing directory survives.
            return fs::rename(&self.staging, &self.destination).map_err(|e| fail(e.to_string()));
        }
        if self.mode == CalibrationOutputMode::Create {
            return Err(fail(
                "destination appeared while the table was being built".to_string(),
            ));
        }
        validate_product(
            &self.destination,
            self.table_type,
            self.table_subtype,
            self.simulated_marker,
        )?;
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
    validate_product(path, Some(table_type), None, false)
}

fn validate_product(
    path: &Path,
    table_type: Option<&str>,
    table_subtype: Option<&str>,
    simulated_marker: bool,
) -> Result<(), CalibrationTableWriteError> {
    let table = casa_tables::Table::open(casa_tables::TableOptions::new(path));
    if table.as_ref().is_ok_and(|table| {
        let info = table.info();
        table_type.is_none_or(|kind| info.table_type == kind)
            && table_subtype.is_none_or(|subtype| info.sub_type == subtype)
    }) {
        return Ok(());
    }
    if simulated_marker && table.is_err() && path.is_dir() && path.join("table.dat").is_file() {
        return Ok(());
    }
    Err(CalibrationTableWriteError::Prepare {
        path: path.display().to_string(),
        reason: if simulated_marker && !path.join("table.dat").is_file() {
            "exists and is not a table; prior product is unchanged".to_string()
        } else {
            let table_type = table_type.unwrap_or("readable casacore");
            let kind = table_subtype.map_or_else(
                || table_type.to_owned(),
                |subtype| format!("{table_type} {subtype}"),
            );
            format!("exists and is not a {kind} table; prior product is unchanged")
        },
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
                casa_core::PrimitiveType::Float64,
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

    #[test]
    fn destination_reservation_is_shared_by_writers_and_released_by_its_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.cal");
        let first = CalibrationOutput::prepare(&path, CalibrationOutputMode::Create).unwrap();
        assert!(CalibrationOutput::prepare(&path, CalibrationOutputMode::Create).is_err());
        assert!(dir.path().join(".out.cal.casa-rs-reservation").is_dir());
        drop(first);
        assert!(!dir.path().join(".out.cal.casa-rs-reservation").exists());
        assert!(CalibrationOutput::prepare(&path, CalibrationOutputMode::Create).is_ok());
    }
}
