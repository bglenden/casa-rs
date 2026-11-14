// SPDX-License-Identifier: LGPL-3.0-or-later
//! Retain a sky once and prepare its Jones-applied grids per pointing.
use super::super::observe::{Scan, ScanRows};
use super::*;
use crate::predict::jones::JonesImage;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition};
use std::collections::HashMap;

pub(super) type VisitKey = ([u64; 2], usize, i32, usize, u64, bool);

pub(super) fn visit_key(scan: &Scan) -> VisitKey {
    let (row, on_disk) = match &scan.rows {
        ScanRows::OnDisk(rows) => (rows.start as u64, true),
        ScanRows::Planned(rows) => (rows.start_s.to_bits(), false),
    };
    (
        scan.pointing_rad.map(f64::to_bits),
        scan.field_id,
        scan.array_id,
        scan.data_description_id,
        row,
        on_disk,
    )
}

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
                + (self.scans.len()
                    * (size_of::<(([u64; 2], usize), f64)>()
                        + size_of::<(VisitKey, usize)>()
                        + 160)) as u64
                + (16 << 10),
        )?;
        let alt_az = super::super::voltage_patterns::squint_mount(self)?;
        let [x, y, z] = self.antennas[0].position_itrf_m;
        let initial = self
            .prediction_frame(phase_centres)
            .with_position(MPosition::new_itrf(x, y, z));
        let selection = self.data_plan.selection();
        let mut first = HashMap::<VisitKey, f64>::new();
        if selection.has_query() {
            self.visit_selected_headers(selection, |scan, header| {
                first
                    .entry(visit_key(scan))
                    .and_modify(|time| *time = time.min(header.time_s))
                    .or_insert(header.time_s);
                Ok(())
            })?;
        }
        let mut centres = Vec::with_capacity(self.scans.len());
        let mut visits = Vec::with_capacity(self.scans.len());
        let mut previous = None;
        for (index, scan) in self.scans.iter().enumerate() {
            if !selection.contains_scan(scan) || scan.row_count() == 0 {
                continue;
            }
            let visit = visit_key(scan);
            let time = if selection.has_query() {
                let Some(&time) = first.get(&visit) else {
                    continue;
                };
                time
            } else {
                match &scan.rows {
                    ScanRows::OnDisk(_) => {
                        self.opened.as_ref().expect("opened").scan_times_s[index][0]
                    }
                    ScanRows::Planned(rows) => rows.integration_times_s().next().unwrap_or(0.),
                }
            };
            let key = (scan.pointing_rad.map(f64::to_bits), scan.field_id);
            if let Some((known, grid)) = previous
                && known == key
            {
                visits.push((visit, grid));
                continue;
            }
            let frame = initial
                .clone()
                .with_epoch(MEpoch::from_mjd(time / 86400., EpochRef::UTC));
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
            let grid = centres
                .iter()
                .position(|(known, known_angle): &(_, f64)| {
                    *known == key && known_angle.to_bits() == angle.to_bits()
                })
                .unwrap_or_else(|| {
                    let grid = centres.len();
                    centres.push((key, angle));
                    grid
                });
            visits.push((visit, grid));
            previous = Some((key, grid));
        }
        visits.sort_unstable_by_key(|(key, _)| *key);
        visits.dedup_by_key(|(key, _)| *key);
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
                visits,
            })),
            pointings: Vec::new(),
            antenna_types: Vec::new(),
            _reservation: reservation,
            _kernels: kernels,
        })
    }
}
