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

use std::sync::Arc;

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
    /// Sample `model` with `kernel` for the scans observed so far and record
    /// the step: replacing what came before unless `incremental`; `beam` is
    /// `setvp`'s.
    ///
    /// # Errors
    ///
    /// - Not yet implemented: a `PB` kernel for a telescope whose default
    ///   pattern casa-rs does not have.
    /// - Raised: the samples do not fit the session's memory budget, or the
    ///   model's pixels could not be read.
    pub(super) fn predict_single_dish(
        &mut self,
        model: &SkyModel,
        kernel: SingleDishKernel,
        beam: Option<PrimaryBeam>,
        incremental: bool,
    ) -> SimulatorResult<()> {
        let telescope = self.observing_telescope();
        let kernel_beam = match kernel {
            SingleDishKernel::Pb => Some(PrimaryBeam::for_telescope(telescope).ok_or_else(|| {
                SimulatorError::not_yet_implemented(format!(
                    "setoptions(ftmachine='sd', gridfunction='PB') for telescope {telescope} is \
                     not yet implemented in casa-rs (#841); ALMA's and ACA's patterns are"
                ))
            })?),
            SingleDishKernel::Box | SingleDishKernel::Spheroidal => None,
        };
        let windows: Vec<SingleDishWindow> = self
            .spectral_windows
            .iter()
            .map(|window| SingleDishWindow {
                frequencies_hz: window.channel_frequencies_hz(),
                correlations: window.correlations.clone(),
            })
            .collect();
        // A scan is pointed at its phase centre (the POINTING table casa-rs
        // writes).
        let pointings =
            SingleDishPointings::of_scans(self.scans.iter().map(|scan| scan.phase_center_rad));
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
                        model,
                        kernel_pattern.as_ref(),
                        (&windows, &pointings),
                    ),
            },
        )
        .map_err(SimulatorError::admission)?;
        let preparation = reservation.split(reservation.memory() - held);
        let count = pointings.directions_rad.len();
        let prediction = SingleDishPrediction::new(
            model,
            kernel,
            (kernel_pattern.as_ref(), voltage_pattern.as_ref()),
            (&windows, pointings),
            reservation,
        )
        .map_err(|error| SimulatorError::raised(format!("Caught exception: {error}")))?;
        drop(preparation);
        tracing::info!(
            ?kernel,
            pointings = count,
            "sampled the model for total power"
        );
        if !incremental {
            // Every step so far covers a prefix of the scans this one covers.
            self.data_plan.clear();
        }
        self.data_plan
            .predict_single_dish(Arc::new(prediction), self.scans.len());
        Ok(())
    }
}
