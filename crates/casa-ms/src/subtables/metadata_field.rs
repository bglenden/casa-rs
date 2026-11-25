// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native FIELD directions for MeasurementSet metadata.

use std::path::{Path, PathBuf};

use casa_measures_data::CometTable;
use casa_tables::Table;
use casa_types::measures::direction::MDirection;
use casa_types::measures::ephemeris::offset_true_angle;
use casa_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casa_types::measures::{EpochRef, MEpoch, MeasFrame};
use casa_types::quanta::casacore::casacore_unit;
use casa_types::quanta::{Dimension, UnitDim};
use casa_types::{ArrayValue, ScalarValue, Value};

use crate::ephemeris::{PhaseCentreEphemerisError, find_ephemeris_in_field};
use crate::{MeasurementSet, MsError, MsResult, SubTable, open_measures_runtime};

use super::MsField;
use super::metadata_measure::{
    MetadataDirection, direction_components, direction_with_reference, field_epoch_reference,
};

/// The FIELD direction to evaluate at an epoch.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FieldDirectionColumn {
    /// The delay direction, with its FIELD offset when attached to an ephemeris.
    Delay,
    /// The phase centre, with its FIELD offset when attached to an ephemeris.
    Phase,
    /// The reference direction, with its FIELD offset when attached to an ephemeris.
    Reference,
    /// The apparent ephemeris direction without a FIELD offset, or the
    /// reference direction when the field has no ephemeris.
    Ephemeris,
}

enum DirectionTime<'a> {
    Epoch(Option<&'a MEpoch>),
    FieldSeconds(f64),
}

impl MeasurementSet {
    /// Evaluate the FIELD phase centre or reference direction as a metadata
    /// measure. The stored frame is retained for a fixed or polynomial field.
    /// An attached ephemeris provides its own frame and FIELD offsets apply
    /// as true angles. Without an epoch, use the constant polynomial term or
    /// the ephemeris at FIELD.TIME.
    ///
    /// # Errors
    ///
    /// The FIELD row, measure record or attached ephemeris is invalid, or the
    /// epoch is outside the ephemeris interpolation interval.
    pub fn metadata_field_direction(
        &self,
        row: usize,
        reference: bool,
        epoch: Option<&MEpoch>,
    ) -> MsResult<MetadataDirection> {
        let column = if reference {
            FieldDirectionColumn::Reference
        } else {
            FieldDirectionColumn::Phase
        };
        self.field()?
            .metadata_direction(row, column, DirectionTime::Epoch(epoch))
    }
}

impl MsField<'_> {
    /// Return the FIELD direction at `seconds` in the row's FIELD.TIME frame.
    /// A zero epoch uses the stored FIELD time. Coefficients and measure
    /// references follow the chosen direction column.
    ///
    /// # Errors
    /// The row or measure is invalid, or the requested epoch has no
    /// ephemeris sample.
    pub fn metadata_direction_at_seconds(
        &self,
        row: usize,
        column: FieldDirectionColumn,
        seconds: f64,
    ) -> MsResult<MetadataDirection> {
        self.metadata_direction(row, column, DirectionTime::FieldSeconds(seconds))
    }

    pub(crate) fn metadata_radial_velocity_at_seconds(
        &self,
        row: usize,
        seconds: f64,
    ) -> MsResult<Option<MRadialVelocity>> {
        let Some(path) = self.ephemeris_path(row)? else {
            return Ok(None);
        };
        let (seconds, origin, reference) =
            self.direction_time(row, DirectionTime::FieldSeconds(seconds))?;
        let mjd = ephemeris_mjd(seconds, origin, reference)?;
        let table =
            CometTable::open(&path).map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let velocity = table
            .radial_velocity_m_s(mjd)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?
            .ok_or_else(|| {
                MsError::InvalidInput(format!("No valid field ephemeris entry for MJD {mjd}"))
            })?;
        let reference = if table.topocentre_m().is_some() {
            RadialVelocityRef::TOPO
        } else {
            RadialVelocityRef::GEO
        };
        Ok(Some(MRadialVelocity::new(velocity, reference)))
    }

    fn metadata_direction(
        &self,
        row: usize,
        kind: FieldDirectionColumn,
        time: DirectionTime<'_>,
    ) -> MsResult<MetadataDirection> {
        if row >= self.row_count() {
            return Err(MsError::InvalidIndex {
                context: "FIELD".into(),
                index: row,
                max: self.row_count(),
            });
        }
        let order = usize::try_from(self.num_poly(row)?).map_err(|_| {
            MsError::InvalidInput("FIELD polynomial order must be nonnegative".into())
        })?;
        let ephemeris = if order == 0 || kind == FieldDirectionColumn::Ephemeris {
            self.ephemeris_path(row)?
        } else {
            None
        };
        if kind == FieldDirectionColumn::Ephemeris
            && let Some(path) = ephemeris.as_ref()
        {
            let (seconds, origin, reference) = self.direction_time(row, time)?;
            return ephemeris_direction(path, seconds, origin, reference)
                .map(MetadataDirection::new);
        }

        let (column, values) = match kind {
            FieldDirectionColumn::Delay => ("DELAY_DIR", self.delay_dir(row)?),
            FieldDirectionColumn::Phase => ("PHASE_DIR", self.phase_dir(row)?),
            FieldDirectionColumn::Reference | FieldDirectionColumn::Ephemeris => {
                ("REFERENCE_DIR", self.reference_dir(row)?)
            }
        };
        let coefficient = |index| {
            let angles = direction_components(self.table(), column, values, index)?;
            direction_with_reference(self.table(), column, row, index, angles)
        };
        let mut direction = coefficient(0)?;
        if order == 0 && ephemeris.is_none() {
            return Ok(direction);
        }
        let (seconds, origin, reference) = self.direction_time(row, time)?;
        if order != 0 {
            if seconds < 1. || (seconds - origin).abs() <= 1e-13 {
                return Ok(direction);
            }
            let delta = seconds - origin;
            let mut angles = signed_angles(&direction.direction);
            let mut factor = 1.0;
            for term in 1..=order {
                factor *= delta;
                let term = signed_angles(&coefficient(term)?.direction);
                angles[0] += term[0] * factor;
                angles[1] += term[1] * factor;
            }
            direction.direction =
                MDirection::from_angles(angles[0], angles[1], direction.direction.refer());
            return Ok(direction);
        }
        let path = ephemeris.expect("a constant field without ephemeris already returned");
        let sample = ephemeris_direction(&path, seconds, origin, reference)?;
        Ok(MetadataDirection::new(offset_true_angle(
            &sample,
            signed_angles(&direction.direction),
        )?))
    }

    fn ephemeris_path(&self, row: usize) -> MsResult<Option<PathBuf>> {
        let Some(id) = self.ephemeris_id(row)?.filter(|&id| id >= 0) else {
            return Ok(None);
        };
        let field_path = self.table().path().ok_or_else(|| {
            MsError::InvalidInput("field ephemeris requires an on-disk MeasurementSet".into())
        })?;
        let result = find_ephemeris_in_field(field_path, id);
        if matches!(
            result,
            Err(PhaseCentreEphemerisError::MissingEphemerisTable { .. })
        ) && let Some(source) = self.table().physical_row_path(row)?
            && source != field_path
        {
            return find_ephemeris_in_field(&source, id)
                .map(Some)
                .map_err(|error| MsError::InvalidInput(error.to_string()));
        }
        result
            .map(Some)
            .map_err(|error| MsError::InvalidInput(error.to_string()))
    }

    fn direction_time(
        &self,
        row: usize,
        time: DirectionTime<'_>,
    ) -> MsResult<(f64, f64, EpochRef)> {
        let reference = field_epoch_reference(self.table(), row)?;
        let origin = self.time(row)?;
        let seconds = match time {
            DirectionTime::FieldSeconds(seconds) => seconds,
            DirectionTime::Epoch(Some(epoch))
                if epoch.refer() == reference && epoch.offset().is_none() && !epoch.is_razing() =>
            {
                epoch.value().as_mjd() * 86400.
            }
            DirectionTime::Epoch(Some(epoch)) => {
                let frame = MeasFrame::new().with_measures(open_measures_runtime()?);
                epoch.convert_to(reference, &frame)?.value().as_mjd() * 86400.
            }
            DirectionTime::Epoch(None) => 0.,
        };
        Ok((seconds, origin, reference))
    }
}

fn ephemeris_direction(
    path: &Path,
    seconds: f64,
    origin: f64,
    reference: EpochRef,
) -> MsResult<MDirection> {
    let table = CometTable::open(path).map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let utc_mjd = ephemeris_mjd(seconds, origin, reference)?;
    table
        .direction_at(utc_mjd)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?
        .map(|sample| sample.direction)
        .ok_or_else(|| {
            MsError::InvalidInput(format!("No valid field ephemeris entry for MJD {utc_mjd}"))
        })
}

fn ephemeris_mjd(seconds: f64, origin: f64, reference: EpochRef) -> MsResult<f64> {
    let time = if seconds == 0. { origin } else { seconds };
    if reference == EpochRef::UTC {
        return Ok(time / 86_400.);
    }
    let frame = MeasFrame::new().with_measures(open_measures_runtime()?);
    Ok(MEpoch::from_mjd_seconds(time, reference)
        .convert_to(EpochRef::UTC, &frame)?
        .value()
        .as_mjd())
}

pub(super) fn signed_angles(direction: &MDirection) -> [f64; 2] {
    let [x, y, z] = direction.cosines();
    [if x == 0. && y == 0. { 0. } else { y.atan2(x) }, z.asin()]
}

pub(super) fn direction_radians(table: &Table, column: &str) -> MsResult<[f64; 2]> {
    let units = table
        .column_keywords(column)
        .and_then(|metadata| metadata.get("QuantumUnits"));
    let scale = |axis| {
        let unit = match units {
            Some(Value::Array(ArrayValue::String(values))) => values
                .iter()
                .nth(axis)
                .or_else(|| values.first())
                .map(String::as_str),
            Some(Value::Scalar(ScalarValue::String(value))) => Some(value.as_str()),
            _ => None,
        }
        .unwrap_or("rad");
        if unit == "rad" {
            return Ok(1.0);
        }
        let unit = casacore_unit(unit).map_err(|error| MsError::InvalidInput(error.to_string()))?;
        if unit.val().dim != UnitDim::basis(Dimension::Angle) {
            return Err(MsError::InvalidInput(format!(
                "{column} requires angular units"
            )));
        }
        Ok(unit.val().factor)
    };
    Ok([scale(0)?, scale(1)?])
}
