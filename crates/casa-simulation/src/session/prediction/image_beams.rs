// SPDX-License-Identifier: LGPL-3.0-or-later
//! Retain a sky once and prepare its Jones-applied grids per pointing.
use super::*;
use crate::predict::jones::JonesImage;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition};

impl Simulator {
    pub(super) fn prepare_jones_image(
        &self,
        model: SkyModel,
        operator: ModelOperator,
        image: Arc<JonesImage>,
        kernels: Reservation,
        phase_centres: Option<&super::super::from_ms::polynomial::PhaseCentres>,
    ) -> SimulatorResult<Prediction> {
        if self.beam_angle_increment_rad < std::f64::consts::TAU {
            return Err(SimulatorError::not_yet_implemented(
                "Jones-image grid refresh within a scan (#1297)",
            ));
        }
        let resources = &self.options.resources;
        let (host, policy) = (resources.host(), resources.policy());
        let admitted = |phase, memory| {
            admit(host, policy, &Demand { phase, memory }).map_err(SimulatorError::admission)
        };
        charge_process_caches(host, policy).map_err(SimulatorError::admission)?;
        let reservation = admitted(
            MODEL_PHASE,
            model.held_bytes()
                + size_of::<JonesFieldModel>() as u64
                + (self.scans.len() * size_of::<(([u64; 2], usize), f64)>()) as u64
                + (16 << 10),
        )?;
        let alt_az = super::super::voltage_patterns::squint_mount(self)?;
        let [x, y, z] = self.antennas[0].position_itrf_m;
        let initial = self
            .prediction_frame(phase_centres)
            .with_position(MPosition::new_itrf(x, y, z));
        let mut centres = Vec::with_capacity(self.scans.len());
        for scan in &self.scans {
            let key = (scan.pointing_rad.map(f64::to_bits), scan.field_id);
            if centres.iter().any(|(known, _)| *known == key) {
                continue;
            }
            let mut frame = initial.clone();
            if let Some(time) = super::super::from_ms::polynomial::epoch(phase_centres, scan) {
                frame = frame.with_epoch(MEpoch::from_mjd(time / 86400., EpochRef::UTC));
            }
            let centre = super::super::from_ms::polynomial::centre(phase_centres, scan);
            let direction = MDirection::from_angles(centre[0], centre[1], DirectionRef::J2000);
            let angle = if alt_az {
                f64::from(
                    direction
                        .parallactic_angle(&frame)
                        .map_err(|e| SimulatorError::raised(e.to_string()))?
                        as f32,
                )
            } else {
                0.
            };
            centres.push((key, angle));
        }
        centres.sort_unstable_by_key(|(key, _)| *key);
        drop(admitted(
            PREPARATION_PHASE,
            operator.grid_bytes() + operator.jones_scratch_bytes(&model),
        )?);
        let reading = admitted(PREPARATION_PHASE, model.plane_bytes() + model.cache_bytes())?;
        let held = model.held().map_err(model_error)?;
        let tangent = model.tangent_direction().clone();
        drop(model);
        drop(reading);
        Ok(Prediction {
            model: operator,
            tangent,
            frame_measures: self.options.measures.clone(),
            frame_position_itrf_m: self.prediction_location_itrf_m,
            phase_centres: None,
            focus: None,
            distance_m: 0.,
            grids: StepGrids::Jones(Box::new(JonesFieldModel {
                model: held,
                image,
                centres,
            })),
            pointings: Vec::new(),
            antenna_types: Vec::new(),
            _reservation: reservation,
            _kernels: kernels,
        })
    }
}
