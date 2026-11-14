// SPDX-License-Identifier: LGPL-3.0-or-later
//! Retain a sky once and prepare its Jones-applied grids per pointing.
use super::super::observe::{Scan, ScanRows};
use super::*;
use crate::predict::image_beam::ImageBeam;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition};
use std::collections::HashMap;

pub(super) type VisitKey = ([u64; 2], usize, i32, usize, u64, bool);

pub(super) type BeamFieldKey = ([u64; 2], usize, i32);
pub(super) type BeamVisit = ((VisitKey, f64), usize);
type BeamVisits = (Vec<(BeamFieldKey, f64)>, Vec<BeamVisit>);

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
    pub(in crate::session) fn transformed_image_beam(
        &self,
        model: &SkyModel,
        correlations: &[Correlation],
        selected: Option<&super::super::voltage_patterns::ResolvedBeam>,
    ) -> SimulatorResult<Option<ImageBeam>> {
        use super::super::voltage_patterns::BeamSource;
        let Some(selected) = selected.filter(|_| !self.mosaic) else {
            return Ok(None);
        };
        if let Some(image) = selected.pattern.image() {
            return Ok(Some(ImageBeam::Jones(image.clone())));
        }
        let Some((offset, reference_hz)) = selected.squint else {
            return Ok(None);
        };
        if model.is_stokes_i_only()
            || !correlations.iter().any(|correlation| {
                matches!(
                    correlation,
                    Correlation::RR | Correlation::RL | Correlation::LR | Correlation::LL
                )
            })
        {
            return Ok(None);
        }
        let pattern = match &selected.pattern {
            BeamSource::Tabulated(pattern) => pattern.clone(),
            BeamSource::Configured(beam) => {
                let resources = &self.options.resources;
                let reservation = admit(
                    resources.host(),
                    resources.policy(),
                    &Demand {
                        phase: MODEL_PHASE,
                        memory: BeamPattern::BYTES + 128,
                    },
                )
                .map_err(SimulatorError::admission)?;
                Arc::new(casa_imaging_runtime::Admitted {
                    value: beam.pattern(),
                    reservation,
                })
            }
            BeamSource::Image(_) => unreachable!("Jones images were handled above"),
        };
        Ok(Some(ImageBeam::Squinted {
            pattern,
            offset,
            reference_hz,
        }))
    }

    pub(super) fn mosaic_jones_angles(
        &self,
        phase_centres: Option<&super::super::from_ms::polynomial::PhaseCentres>,
    ) -> SimulatorResult<Vec<(u32, f64)>> {
        let resources = &self.options.resources;
        let _temporary = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: MOSAIC_STATE_PHASE,
                memory: (self.scans.len() as u64)
                    .saturating_mul(160)
                    .saturating_add(16 << 10),
            },
        )
        .map_err(SimulatorError::admission)?;
        let alt_az = super::super::voltage_patterns::squint_mount(self)?;
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
        let [x, y, z] = self.antennas[0].position_itrf_m;
        let initial = self
            .prediction_frame(phase_centres)
            .with_position(MPosition::new_itrf(x, y, z));
        // The enclosing mosaic-state admission owns the returned window
        // list until kernel construction drops its inputs.
        let mut angles = Vec::with_capacity(self.spectral_windows.len());
        let mut previous: Option<(_, f64)> = None;
        for (index, scan) in self.scans.iter().enumerate() {
            if !selection.contains_scan(scan) || scan.row_count() == 0 {
                continue;
            }
            let time = if selection.has_query() {
                let Some(&time) = first.get(&visit_key(scan)) else {
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
            let key = (
                scan.array_id,
                scan.field_id,
                scan.pointing_rad.map(f64::to_bits),
            );
            let angle = if let Some((known, angle)) = previous
                && known == key
                && self.beam_angle_increment_rad >= std::f64::consts::TAU
            {
                angle
            } else {
                let frame = initial
                    .clone()
                    .with_epoch(MEpoch::from_mjd(time / 86400., EpochRef::UTC));
                let centre = super::super::from_ms::polynomial::centre(phase_centres, scan);
                let current = if alt_az {
                    f64::from(
                        MDirection::from_angles(centre[0], centre[1], DirectionRef::J2000)
                            .parallactic_angle(&frame)
                            .map_err(|error| SimulatorError::raised(error.to_string()))?
                            as f32,
                    )
                } else {
                    0.
                };
                match previous {
                    Some((known, angle))
                        if known == key
                            && (current - angle).abs() <= self.beam_angle_increment_rad =>
                    {
                        angle
                    }
                    _ => current,
                }
            };
            previous = Some((key, angle));
            let window = scan.spectral_window_id as u32;
            if !angles.iter().any(|(known, _)| *known == window) {
                angles.push((window, angle));
            }
        }
        Ok(angles)
    }

    pub(super) fn prepare_image_beam(
        &self,
        model: SkyModel,
        operator: ModelOperator,
        image: ImageBeam,
        kernels: Reservation,
        phase_centres: Option<&super::super::from_ms::polynomial::PhaseCentres>,
    ) -> SimulatorResult<Prediction> {
        let resources = &self.options.resources;
        let (host, policy) = (resources.host(), resources.policy());
        let admitted = |phase, memory| {
            admit(host, policy, &Demand { phase, memory }).map_err(SimulatorError::admission)
        };
        charge_process_caches(host, policy).map_err(SimulatorError::admission)?;
        let kept = model.held_bytes() + size_of::<ImageBeamModel>() as u64 + (16 << 10);
        let mut reservation = admitted(MODEL_PHASE, kept)?;
        let (centres, visits) = self.image_beam_visits(phase_centres, &mut reservation, kept)?;
        drop(admitted(
            PREPARATION_PHASE,
            operator.grid_bytes() + operator.image_beam_scratch_bytes(&model),
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
            grids: StepGrids::ImageBeam(Box::new(ImageBeamModel {
                model: held,
                image,
                centres,
                visits,
                timed: self.beam_angle_increment_rad < std::f64::consts::TAU,
            })),
            pointings: Vec::new(),
            antenna_types: Vec::new(),
            _reservation: reservation,
            _kernels: kernels,
        })
    }

    fn image_beam_visits(
        &self,
        phase_centres: Option<&super::super::from_ms::polynomial::PhaseCentres>,
        reservation: &mut Reservation,
        kept: u64,
    ) -> SimulatorResult<BeamVisits> {
        let resources = &self.options.resources;
        let alt_az = super::super::voltage_patterns::squint_mount(self)?;
        let [x, y, z] = self.antennas[0].position_itrf_m;
        let initial = self
            .prediction_frame(phase_centres)
            .with_position(MPosition::new_itrf(x, y, z));
        let selection = self.data_plan.selection();
        let mut centres: Vec<(BeamFieldKey, f64)> = Vec::new();
        let mut visits: Vec<BeamVisit> = Vec::new();
        let mut indices = HashMap::new();
        let mut previous: Option<(BeamFieldKey, usize)> = None;
        let mut last_sample = None;
        let mut capacity = 0;
        let increment = self.beam_angle_increment_rad;
        let mut capture = |scan: &Scan, time: f64| -> SimulatorResult<()> {
            let visit = visit_key(scan);
            if last_sample == Some((visit, time.to_bits())) {
                return Ok(());
            }
            last_sample = Some((visit, time.to_bits()));
            let key = (
                scan.pointing_rad.map(f64::to_bits),
                scan.field_id,
                scan.array_id,
            );
            let same = previous
                .filter(|(known, _)| *known == key)
                .map(|(_, grid)| grid);
            let angle = if let Some(grid) = same
                && increment >= std::f64::consts::TAU
            {
                centres[grid].1
            } else {
                let frame = initial
                    .clone()
                    .with_epoch(MEpoch::from_mjd(time / 86400., EpochRef::UTC));
                let centre = super::super::from_ms::polynomial::centre(phase_centres, scan);
                let angle = if alt_az {
                    f64::from(
                        MDirection::from_angles(centre[0], centre[1], DirectionRef::J2000)
                            .parallactic_angle(&frame)
                            .map_err(|e| SimulatorError::raised(e.to_string()))?
                            as f32,
                    )
                } else {
                    0.
                };
                same.map(|grid| centres[grid].1)
                    .filter(|previous| (angle - previous).abs() <= increment)
                    .unwrap_or(angle)
            };
            let identity = (key, angle.to_bits());
            let known = indices.get(&identity).copied();
            if visits
                .last()
                .is_some_and(|((prior, _), grid)| *prior == visit && Some(*grid) == known)
            {
                previous = Some((key, known.expect("same grid")));
                return Ok(());
            }
            if visits.len() == capacity {
                let next = (capacity * 2).max(4);
                // Admission precedes growth of both retained vectors and
                // the temporary hash index; all share this entry bound.
                reservation.join(
                    admit(
                        resources.host(),
                        resources.policy(),
                        &Demand {
                            phase: MODEL_PHASE,
                            memory: ((next - capacity) * 256) as u64,
                        },
                    )
                    .map_err(SimulatorError::admission)?,
                );
                visits.reserve_exact(next - visits.len());
                centres.reserve_exact(next - centres.len());
                indices.reserve(next - indices.len());
                capacity = next;
            }
            let grid = *indices.entry(identity).or_insert_with(|| {
                let grid = centres.len();
                centres.push((key, angle));
                grid
            });
            visits.push(((visit, time), grid));
            previous = Some((key, grid));
            Ok(())
        };
        if increment < std::f64::consts::TAU || selection.has_query() {
            self.visit_selected_headers(selection, |scan, header| capture(scan, header.time_s))?;
        } else {
            for (index, scan) in self.scans.iter().enumerate() {
                if !selection.contains_scan(scan) || scan.row_count() == 0 {
                    continue;
                }
                let time = match &scan.rows {
                    ScanRows::OnDisk(_) => {
                        self.opened.as_ref().expect("opened").scan_times_s[index][0]
                    }
                    ScanRows::Planned(rows) => rows.integration_times_s().next().unwrap_or(0.),
                };
                capture(scan, time)?;
            }
        }
        drop(capture);
        drop(indices);
        visits.sort_unstable_by(|(a, _), (b, _)| a.0.cmp(&b.0).then_with(|| a.1.total_cmp(&b.1)));
        reservation.retain(
            kept + (centres.capacity() * size_of::<(BeamFieldKey, f64)>()
                + visits.capacity() * size_of::<BeamVisit>()) as u64,
        );
        Ok((centres, visits))
    }
}
