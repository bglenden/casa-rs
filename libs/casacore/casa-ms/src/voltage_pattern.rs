// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the default telescope record in a table saved by CASA VPManager.

use std::path::{Path, PathBuf};

use casa_core::{
    RecordValue, ScalarValue, Value,
    measures::direction_from_record,
    quanta::{Quantity, Unit},
};
use casa_numerics::PrimaryBeam;
use casa_tables::{Table, TableOptions};

mod curves;
pub use curves::{radial_curve_storage, radial_lookup};

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

    /// Read all VP definitions after checking the saved default indices.
    ///
    /// # Errors
    /// A definition or default row is unreadable, or a default index is invalid.
    pub fn definitions(&self) -> Result<Vec<RecordValue>, VoltagePatternError> {
        let defaults =
            Table::open(TableOptions::new(&self.defaults)).map_err(VoltagePatternError::read)?;
        for row in 0..defaults.row_count() {
            let name = default_cell(&defaults, row, "tel_and_anttype")?;
            if !matches!(name, ScalarValue::String(_)) {
                return Err(VoltagePatternError::read(
                    "VPLIST_DEFAULTS.tel_and_anttype is not a string",
                ));
            }
            let index = default_index(&defaults, row)?;
            if index < -1 || usize::try_from(index).is_ok_and(|i| i >= self.root.row_count()) {
                return Err(VoltagePatternError::read(
                    "VPLIST_DEFAULTS selects an absent beam",
                ));
            }
        }
        (0..self.root.row_count())
            .map(|row| self.definition(row))
            .collect()
    }

    /// Copy the root and the defaults subtable into an independently owned table.
    /// The caller can update the returned copy and save it at `path`.
    ///
    /// # Errors
    /// The table could not be read or copied.
    pub fn copy_to(&self, path: &Path) -> Result<Table, VoltagePatternError> {
        let defaults =
            Table::open(TableOptions::new(&self.defaults)).map_err(VoltagePatternError::read)?;
        let output_defaults =
            std::path::absolute(path.join("VPLIST_DEFAULTS")).map_err(VoltagePatternError::read)?;
        self.root
            .deep_copy(TableOptions::new(path))
            .map_err(VoltagePatternError::read)?;
        defaults
            .deep_copy(TableOptions::new(&output_defaults))
            .map_err(VoltagePatternError::read)?;
        let mut copy = Table::open(TableOptions::new(path)).map_err(VoltagePatternError::read)?;
        copy.keywords_mut().upsert(
            "VPLIST_DEFAULTS",
            Value::TableRef(output_defaults.to_string_lossy().into_owned()),
        );
        Ok(copy)
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
            let name = default_cell(&defaults, row, "tel_and_anttype")?;
            if matches!(name, ScalarValue::String(name) if name == telescope) {
                selected = Some(default_index(&defaults, row)?);
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
        self.definition(index).map(Some)
    }

    fn definition(&self, row: usize) -> Result<RecordValue, VoltagePatternError> {
        self.root
            .cell_accessor(row, "pbdescription")
            .and_then(|cell| cell.record())
            .map_err(VoltagePatternError::read)
    }
}

fn default_cell<'a>(
    table: &'a Table,
    row: usize,
    column: &str,
) -> Result<&'a ScalarValue, VoltagePatternError> {
    table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
        .map_err(VoltagePatternError::read)
}

fn default_index(table: &Table, row: usize) -> Result<i32, VoltagePatternError> {
    match default_cell(table, row, "vplistnum")? {
        ScalarValue::Int32(index) => Ok(*index),
        _ => Err(VoltagePatternError::read(
            "VPLIST_DEFAULTS.vplistnum is not an integer",
        )),
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
    let (radius, ghz) = reference_radius(record)?;
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

fn reference_radius(record: &RecordValue) -> Result<(f64, f64), VoltagePatternError> {
    let ghz = quantity(record, "reffreq", "GHz")?;
    let radius = quantity(record, "maxrad", "arcmin")? * ghz;
    if !ghz.is_finite() || ghz <= 0. || !radius.is_finite() || radius <= 0. {
        return Err(VoltagePatternError::read(
            "reference frequency and support must be finite and positive",
        ));
    }
    Ok((radius, ghz))
}

/// Read a stored radial pattern's squint in tangent-angle radians and Hz.
/// An omitted or zero direction leaves the beam centre unchanged.
///
/// # Errors
/// A supplied direction or reference frequency is invalid.
pub fn radial_squint(record: &RecordValue) -> Result<Option<([f64; 2], f64)>, VoltagePatternError> {
    let Some(direction) = record.get("squintdir") else {
        return Ok(None);
    };
    let Value::Record(direction) = direction else {
        return Err(VoltagePatternError::read(
            "squintdir must be a direction record",
        ));
    };
    let direction = direction_from_record(direction).map_err(VoltagePatternError::read)?;
    // PBMath reads a signed tangent offset, even when the direction was
    // serialized with its longitude in [0, 2π).
    let [x, y, _] = direction.cosines();
    let offset = [y.atan2(x), direction.latitude_rad()];
    if offset == [0.; 2] {
        return Ok(None);
    }
    let frequency = quantity(record, "squintreffreq", "Hz")?;
    if !offset.iter().all(|value| value.is_finite()) || !frequency.is_finite() || frequency <= 0. {
        return Err(VoltagePatternError::read(
            "invalid radial beam squint or reference frequency",
        ));
    }
    Ok(Some((offset, frequency)))
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
