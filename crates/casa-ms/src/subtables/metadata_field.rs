// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native phase and reference directions for MeasurementSet metadata.

use casa_measures_data::CometTable;
use casa_tables::Table;
use casa_types::measures::direction::MDirection;
use casa_types::measures::ephemeris::offset_true_angle;
use casa_types::measures::{EpochRef, MEpoch, MeasFrame};
use casa_types::quanta::casacore::casacore_unit;
use casa_types::quanta::{Dimension, UnitDim};
use casa_types::{ArrayValue, ScalarValue, Value};

use crate::derived::engine::{detect_epoch_reference, resolve_direction_reference};
use crate::ephemeris::find_ephemeris_in_field;
use crate::{MeasurementSet, MsError, MsResult, SubTable, open_measures_runtime};

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
    ) -> MsResult<MDirection> {
        let field = self.field()?;
        if row >= field.row_count() {
            return Err(MsError::InvalidIndex {
                context: "FIELD".into(),
                index: row,
                max: field.row_count(),
            });
        }
        let column = if reference {
            "REFERENCE_DIR"
        } else {
            "PHASE_DIR"
        };
        let values = if reference {
            field.reference_dir(row)?
        } else {
            field.phase_dir(row)?
        };
        let ArrayValue::Float64(values) = values else {
            return Err(MsError::InvalidInput(format!(
                "FIELD.{column} must be double"
            )));
        };
        let units = direction_radians(field.table(), column)?;
        let refer = resolve_direction_reference(field.table(), "FIELD", column, row)?;
        let coefficient = |index| {
            let angle = |axis| {
                match values.shape() {
                    [2] if index == 0 => values.get(ndarray::IxDyn(&[axis])),
                    [2, _] => values.get(ndarray::IxDyn(&[axis, index])),
                    _ => None,
                }
                .map(|value| value * units[axis])
                .ok_or_else(|| {
                    MsError::InvalidInput(format!("FIELD.{column} has an invalid direction shape"))
                })
            };
            Ok::<_, MsError>(MDirection::from_angles(angle(0)?, angle(1)?, refer))
        };
        let direction = coefficient(0)?;
        let order = usize::try_from(field.num_poly(row)?).map_err(|_| {
            MsError::InvalidInput("FIELD polynomial order must be nonnegative".into())
        })?;
        let ephemeris_id = field.ephemeris_id(row)?.filter(|&id| id >= 0);
        if order == 0 && ephemeris_id.is_none() {
            return Ok(direction);
        }
        let time_reference = detect_epoch_reference(field.table(), "TIME", EpochRef::UTC);
        let origin = field.time(row)?;
        let seconds = match epoch {
            Some(epoch)
                if epoch.refer() == time_reference
                    && epoch.offset().is_none()
                    && !epoch.is_razing() =>
            {
                epoch.value().as_mjd() * 86400.
            }
            Some(epoch) => {
                let frame = MeasFrame::new().with_measures(open_measures_runtime()?);
                epoch.convert_to(time_reference, &frame)?.value().as_mjd() * 86400.
            }
            None => 0.,
        };
        if order != 0 {
            if seconds < 1. || (seconds - origin).abs() <= 1e-13 {
                return Ok(direction);
            }
            let delta = seconds - origin;
            let mut angles = signed_angles(&direction);
            let mut factor = 1.0;
            for term in 1..=order {
                factor *= delta;
                let term = signed_angles(&coefficient(term)?);
                angles[0] += term[0] * factor;
                angles[1] += term[1] * factor;
            }
            return Ok(MDirection::from_angles(
                angles[0],
                angles[1],
                direction.refer(),
            ));
        }
        let id = ephemeris_id.expect("constant field without ephemeris already returned");
        let field_path = field.table().path().ok_or_else(|| {
            MsError::InvalidInput("field ephemeris requires an on-disk MeasurementSet".into())
        })?;
        let path = find_ephemeris_in_field(field_path, id)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let table =
            CometTable::open(&path).map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let time = if seconds == 0. { origin } else { seconds };
        let utc_mjd = if time_reference == EpochRef::UTC {
            time / 86400.
        } else {
            let frame = MeasFrame::new().with_measures(open_measures_runtime()?);
            MEpoch::from_mjd_seconds(time, time_reference)
                .convert_to(EpochRef::UTC, &frame)?
                .value()
                .as_mjd()
        };
        let sample = table
            .direction_at(utc_mjd)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?
            .ok_or_else(|| {
                MsError::InvalidInput(format!("No valid field ephemeris entry for MJD {utc_mjd}"))
            })?;
        Ok(offset_true_angle(
            &sample.direction,
            signed_angles(&direction),
        )?)
    }
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
