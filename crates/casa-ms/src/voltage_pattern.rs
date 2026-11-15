// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the default telescope record in a table saved by CASA VPManager.

use std::path::{Path, PathBuf};

use casa_numerics::PrimaryBeam;
use casa_tables::{Table, TableOptions};
use casa_types::{
    RecordValue, ScalarValue, Value,
    quanta::{Quantity, Unit},
};

/// Failure to read a saved voltage-pattern table or interpret its record.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct VoltagePatternError(String);

impl VoltagePatternError {
    fn read(error: impl ToString) -> Self {
        Self(error.to_string())
    }
}

/// The root of a CASA VPManager table and its telescope defaults subtable.
pub struct VoltagePatternTable {
    root: Table,
    defaults: PathBuf,
}

impl VoltagePatternTable {
    /// Estimate peak bytes needed to open one VPManager table. The caller
    /// holds this reservation while its decoded records are in use.
    ///
    /// # Errors
    /// The table header or directory entries could not be read.
    pub fn reading_bytes(path: &Path) -> Result<u64, VoltagePatternError> {
        let rows = Table::rows_on_disk(path).map_err(VoltagePatternError::read)? as u64;
        let mut bytes = rows.saturating_mul(4096).saturating_add(64 << 10);
        for entry in std::fs::read_dir(path).map_err(VoltagePatternError::read)? {
            let metadata = entry
                .and_then(|entry| entry.metadata())
                .map_err(VoltagePatternError::read)?;
            if metadata.is_file() {
                bytes = bytes.saturating_add(metadata.len().saturating_mul(4));
            }
        }
        Ok(bytes)
    }

    /// Open the descriptions and locate the defaults table via its CASA
    /// keyword. This does not open the defaults table.
    ///
    /// # Errors
    /// The root table is unreadable or has no `VPLIST_DEFAULTS` table keyword.
    pub fn open(path: &Path) -> Result<Self, VoltagePatternError> {
        let root = Table::open(TableOptions::new(path)).map_err(VoltagePatternError::read)?;
        let Some(Value::TableRef(reference)) = root.keywords().get("VPLIST_DEFAULTS") else {
            return Err(VoltagePatternError::read(
                "missing VPLIST_DEFAULTS subtable",
            ));
        };
        let defaults = path.join(reference);
        Ok(Self { root, defaults })
    }

    /// Estimate peak bytes needed to read the defaults subtable.
    ///
    /// # Errors
    /// The defaults subtable cannot be sized.
    pub fn defaults_reading_bytes(&self) -> Result<u64, VoltagePatternError> {
        Self::reading_bytes(&self.defaults)
    }

    /// Read the last matching default row, as `VPManager::loadfromtable`
    /// updates the telescope map. A saved index of `-1` selects a built-in
    /// pattern and returns `None`.
    ///
    /// # Errors
    /// The telescope default is missing, invalid, or names an unreadable row.
    pub fn default_record(
        &self,
        telescope: &str,
    ) -> Result<Option<RecordValue>, VoltagePatternError> {
        let defaults =
            Table::open(TableOptions::new(&self.defaults)).map_err(VoltagePatternError::read)?;
        let mut selected = None;
        for row in 0..defaults.row_count() {
            let name = defaults
                .cell_accessor(row, "tel_and_anttype")
                .and_then(|cell| cell.scalar())
                .map_err(VoltagePatternError::read)?;
            if matches!(name, ScalarValue::String(name) if name == telescope) {
                let ScalarValue::Int32(index) = defaults
                    .cell_accessor(row, "vplistnum")
                    .and_then(|cell| cell.scalar())
                    .map_err(VoltagePatternError::read)?
                else {
                    return Err(VoltagePatternError::read(
                        "VPLIST_DEFAULTS.vplistnum is not an integer",
                    ));
                };
                selected = Some(*index);
            }
        }
        let selected = selected.ok_or_else(|| {
            VoltagePatternError::read(format!("no default for telescope {telescope}"))
        })?;
        if selected == -1 {
            return Ok(None);
        }
        let index = usize::try_from(selected).map_err(VoltagePatternError::read)?;
        if index >= self.root.row_count() {
            return Err(VoltagePatternError::read(
                "VPLIST_DEFAULTS selects an absent beam",
            ));
        }
        self.root
            .cell_accessor(index, "pbdescription")
            .and_then(|cell| cell.record())
            .map(Some)
            .map_err(VoltagePatternError::read)
    }
}

/// Decode the circular Gaussian and Airy laws shared by imaging and
/// simulation. CASA stores radii at the record's reference frequency.
/// The result scales them to the arcmin-GHz argument of PBMath1D.
///
/// # Errors
/// The record is invalid or names a law other than `GAUSS` or `AIRY`.
pub fn radial_pattern(record: &RecordValue) -> Result<PrimaryBeam, VoltagePatternError> {
    let Some(Value::Scalar(ScalarValue::String(name))) = record.get("name") else {
        return Err(VoltagePatternError::read("missing string name"));
    };
    let ghz = quantity(record, "reffreq", "GHz")?;
    let radius = quantity(record, "maxrad", "arcmin")? * ghz;
    if !ghz.is_finite() || ghz <= 0. || !radius.is_finite() || radius <= 0. {
        return Err(VoltagePatternError::read(
            "reference frequency and support must be finite and positive",
        ));
    }
    match name.as_str() {
        "AIRY" => {
            let diameter = quantity(record, "dishdiam", "m")?;
            let blockage = quantity(record, "blockagediam", "m")?;
            if !diameter.is_finite()
                || diameter <= 0.
                || !blockage.is_finite()
                || blockage < 0.
                || blockage >= diameter
            {
                return Err(VoltagePatternError::read(
                    "invalid dish or blockage diameter",
                ));
            }
            Ok(PrimaryBeam::Airy {
                dish_diameter_m: diameter,
                blockage_diameter_m: blockage,
                maximum_radius_arcmin_ghz: radius,
            })
        }
        "GAUSS" => {
            let width = quantity(record, "halfwidth", "arcmin")? * ghz;
            if !width.is_finite() || width <= 0. {
                return Err(VoltagePatternError::read(
                    "Gaussian HWHM must be finite and positive",
                ));
            }
            let Some(Value::Scalar(ScalarValue::Bool(is_voltage))) = record.get("isthisvp") else {
                return Err(VoltagePatternError::read("missing boolean isthisvp"));
            };
            Ok(PrimaryBeam::Gaussian {
                half_width_arcmin_ghz: width,
                maximum_radius_arcmin_ghz: radius,
                is_voltage: *is_voltage,
            })
        }
        _ => Err(VoltagePatternError::read(format!(
            "saved radial law {name} (#1307)"
        ))),
    }
}

fn quantity(record: &RecordValue, field: &str, unit: &str) -> Result<f64, VoltagePatternError> {
    let Some(Value::Record(value)) = record.get(field) else {
        return Err(VoltagePatternError::read(format!(
            "missing quantity {field}"
        )));
    };
    let Some(Value::Scalar(ScalarValue::Float64(number))) = value.get("value") else {
        return Err(VoltagePatternError::read(format!(
            "non-scalar quantity {field}"
        )));
    };
    let Some(Value::Scalar(ScalarValue::String(saved_unit))) = value.get("unit") else {
        return Err(VoltagePatternError::read("missing string unit"));
    };
    Quantity::new(*number, saved_unit)
        .and_then(|q| q.get_value_in(&Unit::new(unit)?))
        .map_err(VoltagePatternError::read)
}
