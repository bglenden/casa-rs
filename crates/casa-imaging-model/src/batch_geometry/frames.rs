// SPDX-License-Identifier: LGPL-3.0-or-later
//! The reference frames batch geometry converts in: the observatory and
//! each antenna at a time, the fields' J2000 directions, and the
//! parallactic angles.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use casa_observation::{AntennaId, Catalogue, FieldId};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::{MPosition, MeasFrame, MeasureError, MeasuresProvider};
use casa_types::quanta::Unit;

use super::BatchGeometryError;
use crate::selected_observation_sample::AntennaResponseClass;

/// The observatory, antennas and fields of one observation, in the frames
/// geometry needs, and the parallactic angles computed so far.
pub(super) struct Frames {
    measures: Arc<dyn MeasuresProvider>,
    observatory: MPosition,
    antennas: Vec<Antenna>,
    /// Each field's phase centre in J2000.
    fields: Vec<MDirection>,
    /// Each antenna's parallactic angle by time and field, NaN where not
    /// yet computed: an antenna's angle at a time is the same in every
    /// partition, and the batches visit the partitions one after another.
    parallactic: Mutex<ParallacticAngles>,
}

/// Every antenna's parallactic angle by (time bits, time scale, field).
type ParallacticAngles = HashMap<ParallacticKey, Box<[f64]>>;

type ParallacticKey = (u64, EpochRef, FieldId);

struct Antenna {
    position: MPosition,
    alt_az: bool,
    dish_diameter_m: f64,
}

impl Frames {
    /// The frames of `catalogue`: the observatory named by its first
    /// observation's telescope ([`MPosition::casa_observatory`], else the
    /// first antenna's position), and its fields' phase centres in J2000.
    /// A centre in another frame is converted from the observatory at its
    /// reference epoch, or with no epoch where the field states none (an
    /// MSv4 field: ICRS or another frame whose tie to J2000 is fixed).
    pub(super) fn new(
        catalogue: &Catalogue,
        measures: Arc<dyn MeasuresProvider>,
    ) -> Result<Self, BatchGeometryError> {
        let metres = Unit::new("m").expect("metres are a unit");
        let antennas = catalogue
            .antennas
            .iter()
            .map(|antenna| {
                Ok(Antenna {
                    position: antenna.position.clone(),
                    alt_az: antenna.alt_az(),
                    dish_diameter_m: antenna.dish_diameter.get_value_in(&metres).map_err(|_| {
                        BatchGeometryError::Catalogue("states a dish diameter that is not a length")
                    })?,
                })
            })
            .collect::<Result<Vec<_>, BatchGeometryError>>()?;
        let observatory = MPosition::casa_array_reference(
            catalogue
                .observations
                .first()
                .map(|observation| observation.telescope.as_str()),
            antennas.first().map(|antenna| &antenna.position),
            measures.as_ref(),
        );
        let fields = catalogue
            .fields
            .iter()
            .map(|field| {
                if field.phase_centre.refer() == DirectionRef::J2000 {
                    return Ok(field.phase_centre.clone());
                }
                let mut frame = MeasFrame::new();
                if let Some(epoch) = field.reference_epoch.clone() {
                    frame = frame.with_epoch(epoch);
                }
                let frame = frame
                    .with_position(observatory.clone())
                    .with_measures(Arc::clone(&measures));
                Ok(field.phase_centre.convert_to(DirectionRef::J2000, &frame)?)
            })
            .collect::<Result<Vec<_>, BatchGeometryError>>()?;
        Ok(Self {
            measures,
            observatory,
            antennas,
            fields,
            parallactic: Mutex::new(HashMap::new()),
        })
    }

    /// `field`'s phase centre in J2000.
    pub(super) fn field(&self, field: FieldId) -> &MDirection {
        &self.fields[field.0 as usize]
    }

    /// The observatory's frame at `time_mjd_s` on `scale`.
    pub(super) fn observatory_at(&self, time_mjd_s: f64, scale: EpochRef) -> MeasFrame {
        MeasFrame::new()
            .with_epoch(MEpoch::from_mjd_seconds(time_mjd_s, scale))
            .with_position(self.observatory.clone())
            .with_measures(Arc::clone(&self.measures))
    }

    /// `angles_rad` in `frame` converted to J2000 at `time_mjd_s`, from the
    /// observatory; J2000 angles unchanged.
    pub(super) fn j2000(
        &self,
        time_mjd_s: f64,
        scale: EpochRef,
        angles_rad: [f64; 2],
        frame: DirectionRef,
    ) -> Result<[f64; 2], BatchGeometryError> {
        if frame == DirectionRef::J2000 {
            return Ok(angles_rad);
        }
        let direction = MDirection::from_angles(angles_rad[0], angles_rad[1], frame);
        let context = self
            .observatory_at(time_mjd_s, scale)
            .with_direction(direction.clone());
        let (longitude, latitude) = direction
            .convert_to(DirectionRef::J2000, &context)?
            .as_angles();
        Ok([longitude, latitude])
    }

    /// Whether Measures names `name` ([`MDirection::check_source_name`]),
    /// which [`Self::named`] then resolves at any time.
    pub(super) fn check_named(&self, name: &str) -> Result<(), MeasureError> {
        let frame = MeasFrame::new()
            .with_position(self.observatory.clone())
            .with_measures(Arc::clone(&self.measures));
        MDirection::check_source_name(name, &frame)
    }

    /// The J2000 direction of the solar-system body or catalogued source
    /// `name` at `time_mjd_s`, seen from the observatory.
    pub(super) fn named(
        &self,
        time_mjd_s: f64,
        scale: EpochRef,
        name: &str,
    ) -> Result<MDirection, BatchGeometryError> {
        let frame = self.observatory_at(time_mjd_s, scale);
        Ok(MDirection::from_source_name(name, &frame)?.convert_to(DirectionRef::J2000, &frame)?)
    }

    /// The parallactic angles of `field` at `time_mjd_s` for every antenna,
    /// computed for `antennas` (zero for a mount that is not alt-az; NaN
    /// for an antenna no batch has asked for at this time):
    /// [`MDirection::parallactic_angle`] in each antenna's frame. The cache
    /// is consulted once a time, not once a sample.
    pub(super) fn parallactic_angles(
        &self,
        time_mjd_s: f64,
        scale: EpochRef,
        field: FieldId,
        antennas: &[AntennaId],
    ) -> Result<Box<[f64]>, BatchGeometryError> {
        let key = (time_mjd_s.to_bits(), scale, field);
        let mut angles = self
            .parallactic
            .lock()
            .expect("parallactic angle cache poisoned")
            .get(&key)
            .cloned()
            .unwrap_or_else(|| vec![f64::NAN; self.antennas.len()].into_boxed_slice());
        let mut computed = false;
        for antenna in antennas {
            let slot = antenna.0 as usize;
            if !angles[slot].is_nan() {
                continue;
            }
            let site = &self.antennas[slot];
            angles[slot] = if site.alt_az {
                let frame = MeasFrame::new()
                    .with_epoch(MEpoch::from_mjd_seconds(time_mjd_s, scale))
                    .with_position(site.position.clone())
                    .with_measures(Arc::clone(&self.measures));
                self.field(field).parallactic_angle(&frame)?
            } else {
                0.0
            };
            computed = true;
        }
        if computed {
            self.parallactic
                .lock()
                .expect("parallactic angle cache poisoned")
                .insert(key, angles.clone());
        }
        Ok(angles)
    }

    /// At most the bytes the parallactic angles computed so far hold: one
    /// angle an antenna for each (time, field) a batch has asked for, and
    /// the table that finds them. The cache holds every (time, field) of
    /// the observation once its batches have all been read, and no more.
    pub(super) fn parallactic_bytes(&self) -> usize {
        let cache = self
            .parallactic
            .lock()
            .expect("parallactic angle cache poisoned");
        let angles = cache.len() * self.antennas.len() * size_of::<f64>();
        let capacity = cache.capacity();
        if capacity == 0 {
            return angles;
        }
        // The standard table keeps at most capacity × 8/7 + 1 buckets, each
        // an entry and a control byte, plus one group of control bytes and
        // its alignment (at most 16 bytes each).
        let buckets = (capacity * 8).div_ceil(7) + 1;
        angles + buckets * (size_of::<(ParallacticKey, Box<[f64]>)>() + 1) + 32
    }

    /// CASA's aperture class of each antenna
    /// ([`AntennaResponseClass::casa_alma_aca`]) when every observation is
    /// an ALMA or ACA one; `None` throughout for another telescope.
    pub(super) fn alma_aca_classes(
        &self,
        catalogue: &Catalogue,
    ) -> Vec<Option<AntennaResponseClass>> {
        let alma = !catalogue.observations.is_empty()
            && catalogue.observations.iter().all(|observation| {
                AntennaResponseClass::casa_alma_aca_telescope(&observation.telescope)
            });
        self.antennas
            .iter()
            .map(|antenna| {
                if alma {
                    AntennaResponseClass::casa_alma_aca(antenna.dish_diameter_m)
                } else {
                    None
                }
            })
            .collect()
    }
}
