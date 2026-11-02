// SPDX-License-Identifier: LGPL-3.0-or-later
//! The engine of an observation's catalogue, whatever its store
//! ([`MsCalEngine::from_catalogue`]).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use casa_observation::Catalogue;
use casa_types::measures::MeasuresProvider;
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::epoch::EpochRef;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::position::MPosition;

use super::{FieldMotion, MsCalEngine};
use crate::error::MsResult;

impl MsCalEngine {
    /// An engine over the observation `catalogue` describes, whatever its
    /// store, evaluating every frame conversion with `measures`: what
    /// [`Self::with_measures`] and [`Self::with_field_ephemerides`] read from
    /// a MeasurementSet's subtables, read from the catalogue. Its antennas
    /// and their mounts, the observatory its first observation's telescope
    /// names (else its first antenna, as CASA's array reference), its
    /// fields' phase centres, the epochs they are stated at and the
    /// ephemerides they follow, and the time scale of its partitions' times
    /// (`UTC` without a partition).
    ///
    /// For a MeasurementSet's catalogue this is the engine
    /// [`Self::with_measures`] builds, field ephemerides attached: the MSv2
    /// adapter reads the same subtables the same way (law
    /// `the_catalogue_engine_is_the_measurement_set_engine`).
    ///
    /// # Errors
    ///
    /// A field's phase centre that cannot be converted to J2000.
    pub fn from_catalogue(
        catalogue: &Catalogue,
        measures: Arc<dyn MeasuresProvider>,
    ) -> MsResult<Self> {
        let antenna_positions = catalogue
            .antennas
            .iter()
            .map(|antenna| {
                let [x, y, z] = antenna.position.as_itrf();
                MPosition::new_itrf(x, y, z)
            })
            .collect::<Vec<_>>();
        let antenna_mount_alt_az = catalogue
            .antennas
            .iter()
            .map(casa_observation::AntennaEntry::alt_az)
            .collect::<Vec<_>>();
        let observatory_position = MPosition::casa_array_reference(
            catalogue
                .observations
                .first()
                .map(|observation| observation.telescope.as_str()),
            antenna_positions.first(),
            measures.as_ref(),
        );
        let mut field_directions = Vec::with_capacity(catalogue.fields.len());
        let mut field_phase_offsets = Vec::with_capacity(catalogue.fields.len());
        let mut field_reference_epochs = Vec::with_capacity(catalogue.fields.len());
        let mut field_motions = Vec::with_capacity(catalogue.fields.len());
        for field in &catalogue.fields {
            let (longitude, latitude) = field.phase_centre.as_angles();
            field_phase_offsets.push([longitude, latitude]);
            field_directions.push(if field.phase_centre.refer() == DirectionRef::J2000 {
                field.phase_centre.clone()
            } else {
                let frame = MeasFrame::new()
                    .with_position(observatory_position.clone())
                    .with_measures(Arc::clone(&measures));
                let frame = match &field.reference_epoch {
                    Some(epoch) => frame.with_epoch(epoch.clone()),
                    None => frame,
                };
                field.phase_centre.convert_to(DirectionRef::J2000, &frame)?
            });
            field_reference_epochs.push(field.reference_epoch.clone());
            field_motions.push(match &field.ephemeris {
                Some(ephemeris) => FieldMotion::Ephemeris(Arc::clone(ephemeris)),
                None => FieldMotion::Fixed,
            });
        }
        let time_reference = catalogue
            .partitions
            .first()
            .map_or(EpochRef::UTC, |partition| partition.time_scale);
        Ok(Self {
            antenna_positions: antenna_positions.into_boxed_slice(),
            antenna_mount_alt_az: antenna_mount_alt_az.into_boxed_slice(),
            field_directions: field_directions.into_boxed_slice(),
            field_phase_offsets: field_phase_offsets.into_boxed_slice(),
            field_motions: field_motions.into_boxed_slice(),
            field_reference_epochs: field_reference_epochs.into_boxed_slice(),
            observatory_position,
            time_reference,
            measures: Some(measures),
            azel_cache: RwLock::new(HashMap::new()),
            hadec_cache: RwLock::new(HashMap::new()),
            parallactic_angle_cache: RwLock::new(HashMap::new()),
        })
    }
}
