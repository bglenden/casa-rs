// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.predict` after `setoptions(ftmachine='sd')`: total power, sampled
//! from the model in the image plane as CASA's `SDGrid` samples it
//! (`crate::predict` documents the sampling).
//!
//! The model is sampled when `predict` is called, at each distinct pointing
//! of the scans observed so far (a scan's phase centre, which `observemany`
//! can move off its field), one sample per pointing, window, channel and
//! correlation; `close` adds each scan's pointing's samples to the
//! autocorrelations of the scans the step covers. The
//! `PB` kernel is the default pattern of OBSERVATION row 0's telescope,
//! whatever `setvp` says; `setvp(dovp=True)` also applies that pattern to
//! the model first, as CASA's `SkyEquation` does.

use casa_imaging_runtime::{Demand, admit};

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::predict::{
    BeamPattern, PrimaryBeam, SingleDishKernel, SingleDishPointings, SingleDishPrediction,
    SingleDishWindow, SkyModel,
};

type BeamKey = (i32, usize, [u64; 2]);

/// The phase a total-power prediction is admitted as.
const SINGLE_DISH_PHASE: &str = "the simulator's total-power samples";

impl Simulator {
    /// Sample `model` with `kernel` for the scans observed so far; `beam` is
    /// `setvp`'s. The caller records the step after every model is prepared.
    /// The model is dropped, with its tile cache, before the
    /// preparation's admission is released.
    ///
    /// # Errors
    ///
    /// - Refused: a telescope without a default PB, or an image too small
    ///   for its selected kernel, as in CASA.
    /// - Raised: the samples do not fit the session's memory budget, or the
    ///   model's pixels could not be read.
    pub(super) fn prepare_single_dish(
        &self,
        model: SkyModel,
        kernel: SingleDishKernel,
        beam: Option<&super::voltage_patterns::ResolvedBeam>,
    ) -> SimulatorResult<SingleDishPrediction> {
        let telescope = self.observing_telescope();
        // The selected telescope supplies the PB kernel independently of setvp.
        let kernel_beam = match kernel {
            SingleDishKernel::Pb => Some(
                PrimaryBeam::for_telescope(telescope, self.reference_frequency_hz()).ok_or_else(
                    || {
                        SimulatorError::refused(format!(
                            "No default primary beam for telescope {telescope}"
                        ))
                    },
                )?,
            ),
            SingleDishKernel::Box
            | SingleDishKernel::Spheroidal
            | SingleDishKernel::Gauss
            | SingleDishKernel::Gjinc => None,
        };
        let resources = &self.options.resources;
        let _window_memory = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "the simulator's total-power channel axes",
                memory: BeamPattern::BYTES
                    + self
                        .data_descriptions
                        .iter()
                        .map(|description| {
                            size_of::<SingleDishWindow>() as u64
                                + self.spectral_windows[description.window].channel_count as u64 * 8
                                + self.polarizations[description.polarization].len() as u64 * 4
                        })
                        .sum::<u64>(),
            },
        )
        .map_err(SimulatorError::admission)?;
        let correlations = self
            .correlations_for_scan(&self.scans[0])
            .iter()
            .map(|&code| {
                casa_types::Correlation::from_casacore_code(code).expect("validated correlation")
            })
            .collect::<Vec<_>>();
        let transformed = self.transformed_image_beam(&model, &correlations, beam)?;
        let radial = if transformed.is_none() {
            beam.map(|beam| beam.pattern.pattern()).transpose()?
        } else {
            None
        };
        let windows: Vec<SingleDishWindow> = self
            .data_descriptions
            .iter()
            .map(|description| SingleDishWindow {
                frequencies_hz: self.spectral_windows[description.window].channel_frequencies_hz(),
                correlations: self.polarizations[description.polarization].clone(),
            })
            .collect();
        // Total power samples where the antennas point: the POINTING table
        // casa-rs writes.
        let _pointing_memory = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "the simulator's total-power pointing pixels",
                memory: if Self::local_total_power_frame(&model) || transformed.is_some() {
                    (self.planned_rows() as u64)
                        .saturating_mul(384)
                        .saturating_add(256 << 10)
                } else {
                    self.scans.len() as u64 * 64
                },
            },
        )
        .map_err(SimulatorError::admission)?;
        let pointings = self.total_power_pointings(&model, transformed.is_some())?;
        let (kernel_pattern, voltage_pattern) =
            (kernel_beam.map(PrimaryBeam::pattern), radial.as_ref());
        let held = SingleDishPrediction::bytes(&windows, &pointings);
        let mut reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: SINGLE_DISH_PHASE,
                memory: held
                    + SingleDishPrediction::preparation_bytes(
                        &model,
                        kernel,
                        transformed.is_some(),
                        kernel_pattern.as_ref(),
                        (&windows, &pointings),
                    ),
            },
        )
        .map_err(SimulatorError::admission)?;
        let preparation = reservation.split(reservation.memory() - held);
        let count = pointings.directions_rad.len();
        let prediction = SingleDishPrediction::new(
            &model,
            kernel,
            (
                kernel_pattern.as_ref(),
                voltage_pattern,
                transformed.as_ref(),
            ),
            (&windows, pointings),
            reservation,
        );
        // The model's tile cache is charged to the preparation, so the model
        // goes first.
        drop(model);
        drop(preparation);
        let prediction = prediction.map_err(|error| {
            if matches!(error, crate::predict::PredictError::InvalidModel(_)) {
                SimulatorError::refused(format!("Caught exception: {error}"))
            } else {
                SimulatorError::raised(format!("Caught exception: {error}"))
            }
        })?;
        tracing::info!(
            ?kernel,
            pointings = count,
            "sampled the model for total power"
        );
        Ok(prediction)
    }
    fn local_total_power_frame(model: &SkyModel) -> bool {
        use casa_types::measures::direction::DirectionRef::*;
        !matches!(
            model.tangent_direction().refer(),
            J2000 | B1950 | B1950Vla | GALACTIC | ICRS | SUPERGAL | ECLIPTIC
        )
    }

    fn total_power_pointings(
        &self,
        model: &SkyModel,
        transformed: bool,
    ) -> SimulatorResult<SingleDishPointings> {
        if !Self::local_total_power_frame(model) && !transformed {
            return Ok(SingleDishPointings::of_scans(
                self.scans.iter().map(|scan| scan.pointing_rad),
            ));
        }
        use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};
        let capacity = self.planned_rows();
        let indices: std::collections::HashMap<_, _> = self
            .scans
            .iter()
            .enumerate()
            .map(|(i, scan)| (scan as *const _ as usize, i))
            .collect();
        let mut directions_rad = Vec::with_capacity(capacity);
        let mut pixels = Vec::with_capacity(capacity);
        let mut visits = Vec::with_capacity(capacity);
        let alt_az = transformed && super::voltage_patterns::squint_mount(self)?;
        let mut angles = Vec::with_capacity(capacity);
        let mut prepared = std::collections::HashMap::new();
        let mut previous: Option<(BeamKey, f64)> = None;
        let mut of_scan = vec![0; self.scans.len()];
        self.visit_selected_headers(self.data_plan.selection(), |scan, row| {
            if row.antennas[0] != row.antennas[1] {
                return Ok(());
            }
            let index = indices[&(scan as *const _ as usize)];
            let [x, y, z] = self.antennas[row.antennas[0]].position_itrf_m;
            let frame = MeasFrame::new()
                .with_measures(self.options.measures.clone())
                .with_epoch(MEpoch::from_mjd(row.time_s / 86400., EpochRef::UTC))
                .with_position(MPosition::new_itrf(x, y, z));
            let pixel = model
                .pixel_of_with_frame(scan.pointing_rad, &frame)
                .map_err(|error| {
                    SimulatorError::raised(format!("total-power image direction: {error}"))
                })?;
            let key = (
                scan.array_id,
                scan.field_id,
                scan.pointing_rad.map(f64::to_bits),
            );
            let [x, y, z] = self.antennas[0].position_itrf_m;
            let current = if alt_az {
                f64::from(
                    casa_types::measures::direction::MDirection::from_angles(
                        scan.phase_center_rad[0],
                        scan.phase_center_rad[1],
                        casa_types::measures::direction::DirectionRef::J2000,
                    )
                    .parallactic_angle(&frame.clone().with_position(MPosition::new_itrf(x, y, z)))
                    .map_err(|error| SimulatorError::raised(error.to_string()))?
                        as f32,
                )
            } else {
                0.
            };
            let angle = match previous {
                Some((known, old))
                    if known == key && (current - old).abs() <= self.beam_angle_increment_rad =>
                {
                    old
                }
                _ => current,
            };
            previous = Some((key, angle));
            let identity = (
                scan.pointing_rad.map(f64::to_bits),
                pixel.map(f64::to_bits),
                angle.to_bits(),
            );
            let sample = *prepared.entry(identity).or_insert_with(|| {
                let sample = pixels.len();
                directions_rad.push(scan.pointing_rad);
                pixels.push(pixel);
                angles.push(angle);
                sample
            });
            of_scan[index] = sample;
            visits.push(((index, row.time_s.to_bits(), row.antennas[0]), sample));
            Ok(())
        })?;
        visits.sort_unstable_by_key(|v| v.0);
        Ok(SingleDishPointings {
            directions_rad,
            of_scan,
            pixels: Some(pixels),
            visits,
            angles,
        })
    }
}
