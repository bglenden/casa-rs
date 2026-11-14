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
    PrimaryBeam, SingleDishKernel, SingleDishPointings, SingleDishPrediction, SingleDishWindow,
    SkyModel,
};

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
    /// - Not yet implemented: a `PB` kernel for a telescope whose default
    ///   pattern casa-rs does not have.
    /// - Raised: the samples do not fit the session's memory budget, or the
    ///   model's pixels could not be read.
    pub(super) fn prepare_single_dish(
        &self,
        model: SkyModel,
        kernel: SingleDishKernel,
        beam: Option<PrimaryBeam>,
    ) -> SimulatorResult<SingleDishPrediction> {
        if self
            .fields
            .last()
            .is_some_and(|field| field.distance_m != 0.)
            || model.distance_m != 0.
        {
            return Err(SimulatorError::not_yet_implemented(
                "finite-distance total-power image prediction is not yet implemented in casa-rs (#1299)",
            ));
        }
        if model.tangent_direction().refer() != casa_types::measures::direction::DirectionRef::J2000
        {
            return Err(SimulatorError::not_yet_implemented(
                "single-dish prediction in other image direction frames is not yet implemented in casa-rs (#1299)",
            ));
        }
        if !model.is_stokes_i_only() {
            return Err(SimulatorError::not_yet_implemented(
                "single-dish prediction of polarized image models is not yet implemented in casa-rs (#1299)",
            ));
        }
        let telescope = self.observing_telescope();
        // The PB kernel is compared with CASA for ALMA's and ACA's patterns;
        // the VLA's and EVLA's are not yet.
        let kernel_beam = match kernel {
            SingleDishKernel::Pb => Some(
                PrimaryBeam::for_telescope(telescope, self.reference_frequency_hz())
                    .filter(|beam| *beam == PrimaryBeam::ALMA || *beam == PrimaryBeam::ACA)
                    .ok_or_else(|| {
                        SimulatorError::not_yet_implemented(format!(
                            "setoptions(ftmachine='sd', gridfunction='PB') for telescope \
                             {telescope} is not yet implemented in casa-rs (#1053); ALMA's and \
                             ACA's patterns are"
                        ))
                    })?,
            ),
            SingleDishKernel::Box
            | SingleDishKernel::Spheroidal
            | SingleDishKernel::Gauss
            | SingleDishKernel::Gjinc => None,
        };
        let windows: Vec<SingleDishWindow> = self
            .spectral_windows
            .iter()
            .map(|window| SingleDishWindow {
                frequencies_hz: window.channel_frequencies_hz(),
                correlations: self.correlations_for_scan(&self.scans[0]).to_vec(),
            })
            .collect();
        // Total power samples where the antennas point: the POINTING table
        // casa-rs writes.
        let pointings =
            SingleDishPointings::of_scans(self.scans.iter().map(|scan| scan.pointing_rad));
        let (kernel_pattern, voltage_pattern) = (
            kernel_beam.map(PrimaryBeam::pattern),
            beam.map(PrimaryBeam::pattern),
        );
        let held = SingleDishPrediction::bytes(&windows, &pointings);
        let resources = &self.options.resources;
        let mut reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: SINGLE_DISH_PHASE,
                memory: held
                    + SingleDishPrediction::preparation_bytes(
                        &model,
                        kernel,
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
            (kernel_pattern.as_ref(), voltage_pattern.as_ref()),
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
}
