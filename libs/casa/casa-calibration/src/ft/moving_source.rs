// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observe a legacy FTMachine moving direction at a visibility epoch.

use std::sync::Arc;

use casa_core::ScalarValue;
use casa_core::measures::MeasFrame;
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::position::MPosition;
use casa_measures::PlanetaryTable;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection};

use super::invalid;

#[derive(Clone)]
pub(super) struct MovingSourceAnchor {
    pub(super) source: Arc<ObservedSource>,
    pub(super) frame: MeasFrame,
}

pub(crate) struct ObservedSource {
    model: MDirection,
    planets: Option<Box<PlanetaryTable>>,
}

impl ObservedSource {
    pub(crate) fn new(model: MDirection) -> MsResult<Self> {
        let reference = model.refer();
        if reference == DirectionRef::COMET {
            return Err(invalid(
                "No MeasFrame comet specified for conversion of Direction",
            ));
        }
        let planets = (reference.casacore_code() >= 32)
            .then(PlanetaryTable::open_discovered)
            .transpose()
            .map_err(invalid)?
            .map(Box::new);
        Ok(Self { model, planets })
    }

    pub(crate) fn model(&self) -> &MDirection {
        &self.model
    }

    pub(crate) fn direction(
        &self,
        frame: &MeasFrame,
        target: DirectionRef,
    ) -> MsResult<MDirection> {
        // The legacy machine goes through AZEL to apply topocentric
        // parallax, then interprets that line of sight in the image frame.
        let azel = match &self.planets {
            Some(planets) => {
                let (geocentric, distance) = planets
                    .geocentric_direction(&self.model, frame)
                    .map_err(invalid)?;
                geocentric.convert_with_parallax(DirectionRef::AZEL, frame, distance)?
            }
            None => self.model.convert_to(DirectionRef::AZEL, frame)?,
        };
        Ok(azel.convert_to(target, frame)?)
    }
}

pub(super) fn first_tracking_frame(
    ms: &MeasurementSet,
    selection: &ResolvedMsSelection,
    location: MPosition,
) -> MsResult<MeasFrame> {
    let first = first_imager_row(ms, selection)?;
    let engine = MsCalEngine::new(ms)?;
    let frame =
        engine.spectral_frame_observatory(first.time_mjd_seconds, first.field_id as usize)?;
    Ok(frame.with_position(location))
}

pub(super) fn first_imager_row<'a>(
    ms: &MeasurementSet,
    selection: &'a ResolvedMsSelection,
) -> MsResult<&'a casa_ms::ResolvedMsSelectionRow> {
    let arrays = ms.main_table().column_accessor("ARRAY_ID")?;
    let mut first = None;
    for rows in selection.selected_rows.chunks(1024) {
        let row_ids: Vec<_> = rows.iter().map(|row| row.row_index).collect();
        for (row, array) in rows
            .iter()
            .zip(arrays.scalar_cells_owned_for_rows(&row_ids)?)
        {
            let Some(ScalarValue::Int32(array)) = array else {
                return Err(invalid("ARRAY_ID must be a defined integer"));
            };
            // Imager::makeVisSet sorts the old iterator in this order.
            let key = (array, row.field_id, row.data_desc_id, row.time_mjd_seconds);
            if first.as_ref().is_none_or(|(old, _)| key < *old) {
                first = Some((key, row));
            }
        }
    }
    first
        .map(|(_, row)| row)
        .ok_or_else(|| invalid("the imager needs a selected visibility"))
}
