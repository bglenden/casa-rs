// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the legacy imager's radial voltage pattern on the model image.

use std::path::PathBuf;

use casa_coordinates::DirectionCoordinate;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, voltage_pattern};
use casa_numerics::PrimaryBeam;
use casa_types::measures::{MeasFrame, direction::MDirection};
use ndarray::Array2;

use super::invalid;

/// A radial voltage pattern selected for the imager's next prediction.
#[derive(Clone, Debug, Default)]
pub struct ImagerVoltagePattern {
    /// Override the attached MeasurementSet's telescope name.
    pub telescope: Option<String>,
    /// Custom pattern table. With no table, use the telescope's default.
    pub table: Option<PathBuf>,
}

#[derive(Clone)]
pub(super) struct PreparedVoltagePattern {
    pub(super) pattern: PrimaryBeam,
    centre: MDirection,
    frame: MeasFrame,
}

impl ImagerVoltagePattern {
    pub(super) fn prepare(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        epoch: Option<f64>,
    ) -> MsResult<PreparedVoltagePattern> {
        let first = &selected.selected_rows[0];
        let telescope = match &self.telescope {
            Some(telescope) => telescope.clone(),
            None => ms.observation()?.string(0, "TELESCOPE_NAME")?,
        };
        let pattern = if let Some(path) = &self.table {
            let table = voltage_pattern::VoltagePatternTable::open(path).map_err(invalid)?;
            let record = table
                .default_record(&telescope)
                .map_err(invalid)?
                .ok_or_else(|| invalid(format!("no voltage pattern for {telescope}")))?;
            voltage_pattern::radial_pattern(&record).map_err(invalid)?
        } else {
            let frequency = ms.spectral_window()?.ref_frequency(0)?;
            PrimaryBeam::for_telescope(&telescope, frequency)
                .ok_or_else(|| invalid(format!("no default voltage pattern for {telescope}")))?
        };
        let engine = MsCalEngine::new(ms)?;
        let time = epoch
            .filter(|time| *time > 0.)
            .unwrap_or(first.time_mjd_seconds);
        let field = usize::try_from(first.field_id).map_err(invalid)?;
        let centre = engine.observation_direction_j2000(time, field)?;
        let frame = engine.spectral_frame_observatory_direction(time, centre.clone())?;
        Ok(PreparedVoltagePattern {
            pattern,
            centre,
            frame,
        })
    }
}

impl PreparedVoltagePattern {
    pub(super) fn separations(
        &self,
        direction: &DirectionCoordinate,
        [nx, ny]: [usize; 2],
    ) -> MsResult<Array2<f64>> {
        let centre = self
            .centre
            .convert_to(direction.direction_ref(), &self.frame)
            .map_err(invalid)?;
        let (lon0, lat0) = centre.as_angles();
        Ok(Array2::from_shape_fn((ny, nx), |(y, x)| {
            let Ok([lon, lat]) = direction.pixel_to_world([x as f64, y as f64]) else {
                return f64::INFINITY;
            };
            let separation = ((lat - lat0) * 0.5).sin().powi(2)
                + lat.cos() * lat0.cos() * ((lon - lon0) * 0.5).sin().powi(2);
            2.0 * separation.clamp(0., 1.).sqrt().asin()
        }))
    }
}
