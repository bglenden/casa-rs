// SPDX-License-Identifier: LGPL-3.0-or-later
//! The reference frames batch geometry converts in: the observatory and
//! each antenna at a time, the fields' J2000 directions, and the
//! parallactic angle.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use casa_observation::{AntennaId, Catalogue, FieldId};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::{MPosition, MeasFrame, MeasuresProvider};
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
    /// Parallactic angles by (time bits, field, antenna): an antenna's
    /// angle at a time is the same in every partition.
    parallactic: RwLock<HashMap<(u64, FieldId, AntennaId), f64>>,
}

struct Antenna {
    position: MPosition,
    alt_az: bool,
    dish_diameter_m: f64,
}

impl Frames {
    /// The frames of `catalogue`: the observatory named by its first
    /// observation's telescope ([`MPosition::casa_observatory`], else the
    /// first antenna's position), and its fields' phase centres in J2000, a centre in
    /// another frame converted at its reference epoch from the observatory.
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
                    alt_az: antenna
                        .mount
                        .as_bytes()
                        .get(.."alt-az".len())
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"alt-az")),
                    dish_diameter_m: antenna.dish_diameter.get_value_in(&metres).map_err(|_| {
                        BatchGeometryError::Catalogue("a dish diameter is not a length")
                    })?,
                })
            })
            .collect::<Result<Vec<_>, BatchGeometryError>>()?;
        let observatory = catalogue
            .observations
            .first()
            .and_then(|observation| {
                MPosition::casa_observatory(&observation.telescope, measures.as_ref())
            })
            .or_else(|| antennas.first().map(|antenna| antenna.position.clone()))
            .unwrap_or_else(|| MPosition::new_itrf(0.0, 0.0, 0.0));
        let fields = catalogue
            .fields
            .iter()
            .map(|field| {
                if field.phase_centre.refer() == DirectionRef::J2000 {
                    return Ok(field.phase_centre.clone());
                }
                let epoch = field
                    .reference_epoch
                    .clone()
                    .ok_or(BatchGeometryError::Catalogue(
                        "a field outside J2000 states no reference epoch",
                    ))?;
                let frame = MeasFrame::new()
                    .with_epoch(epoch)
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
            parallactic: RwLock::new(HashMap::new()),
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

    /// The parallactic angle of `field` at `antenna` and `time_mjd_s` (zero
    /// for a mount that is not alt-az): the position angle of the celestial
    /// pole seen from the field, both in the antenna's `AZEL`.
    ///
    /// Cf. C++ `MSCalEngine::getPA`.
    pub(super) fn parallactic_angle(
        &self,
        time_mjd_s: f64,
        scale: EpochRef,
        field: FieldId,
        antenna: AntennaId,
    ) -> Result<f64, BatchGeometryError> {
        let key = (time_mjd_s.to_bits(), field, antenna);
        if let Some(angle) = self
            .parallactic
            .read()
            .expect("parallactic angle cache poisoned")
            .get(&key)
        {
            return Ok(*angle);
        }
        let site = &self.antennas[antenna.0 as usize];
        let angle = if site.alt_az {
            let frame = MeasFrame::new()
                .with_epoch(MEpoch::from_mjd_seconds(time_mjd_s, scale))
                .with_position(site.position.clone())
                .with_measures(Arc::clone(&self.measures));
            let source = self.field(field).convert_to(DirectionRef::AZEL, &frame)?;
            let pole =
                MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_2, DirectionRef::HADEC)
                    .convert_to(DirectionRef::AZEL, &frame)?;
            source.position_angle(&pole)
        } else {
            0.0
        };
        self.parallactic
            .write()
            .expect("parallactic angle cache poisoned")
            .insert(key, angle);
        Ok(angle)
    }

    /// CASA's aperture class of each antenna, when every observation is an
    /// ALMA or ACA one: 12 m dishes within 0.5 m, 7 m dishes within 1 m;
    /// `None` for another dish or another telescope.
    pub(super) fn alma_aca_classes(
        &self,
        catalogue: &Catalogue,
    ) -> Vec<Option<AntennaResponseClass>> {
        let alma = !catalogue.observations.is_empty()
            && catalogue
                .observations
                .iter()
                .all(|observation| matches!(observation.telescope.trim(), "ALMA" | "ACA"));
        self.antennas
            .iter()
            .map(|antenna| {
                let diameter = antenna.dish_diameter_m;
                if !alma {
                    None
                } else if (diameter - 12.0).abs() < 0.5 {
                    Some(AntennaResponseClass::CasaAlma12m)
                } else if (diameter - 7.0).abs() < 1.0 {
                    Some(AntennaResponseClass::CasaAca7m)
                } else {
                    None
                }
            })
            .collect()
    }
}
