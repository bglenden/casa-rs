// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the OBSERVATION subtable.
//!
//! Cf. C++ `MSObservationColumns`.

use casa_types::measures::{MPosition, MeasuresProvider};

use crate::schema::{self, SubtableId};
use crate::{MsError, MsResult};

crate::subtables::define_generic_subtable!(
    read = MsObservation,
    write = MsObservationMut,
    id = SubtableId::Observation,
    required = schema::observation::REQUIRED_COLUMNS,
    optional = schema::observation::OPTIONAL_COLUMNS,
    table_name = "OBSERVATION",
    read_doc = "Read-only typed wrapper for the OBSERVATION subtable.",
    write_doc = "Mutable typed wrapper for the OBSERVATION subtable."
);

impl MsObservation<'_> {
    /// Resolve the observatory at `row` using the supplied Measures catalogue.
    /// CASA validates every telescope name before returning the requested position.
    pub fn observatory_position(
        &self,
        row: usize,
        measures: &dyn MeasuresProvider,
    ) -> MsResult<MPosition> {
        if row >= self.row_count() {
            return Err(MsError::InvalidInput("Observation ID out of range".into()));
        }
        let mut position = None;
        for index in 0..self.row_count() {
            let name = self.string(index, "TELESCOPE_NAME")?;
            if name.is_empty() {
                return Err(MsError::InvalidInput(
                    "The name of the telescope is not stored in the measurement set".into(),
                ));
            }
            let current = MPosition::casa_observatory(&name, measures).ok_or_else(|| {
                MsError::InvalidInput(format!("Telescope {name} is not recognized by CASA"))
            })?;
            if index == row {
                position = Some(current);
            }
        }
        position.ok_or_else(|| MsError::InvalidInput("Observation ID out of range".into()))
    }
}
