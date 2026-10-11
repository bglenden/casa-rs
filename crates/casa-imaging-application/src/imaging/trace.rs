// SPDX-License-Identifier: LGPL-3.0-or-later
//! The science trace (`science_trace`): what the clean controller reads and
//! decides at each cycle, as `info` events under [`SCIENCE_TRACE`]. Each
//! event is O(1); none walks an image.

use casa_imaging_deconvolution::{CycleControls, ResidualStatistics};
use casa_imaging_runtime::SCIENCE_TRACE;

use crate::NativeMinorCycleOutcome;

/// The residual statistics a cleaning decision reads after `major_cycle`
/// major cycles (CASA's minor-cycle initialisation record).
pub(super) fn residual(major_cycle: usize, statistics: &ResidualStatistics) {
    tracing::info!(
        target: SCIENCE_TRACE,
        major_cycle,
        peak_jy = statistics.peak,
        peak_no_mask_jy = statistics.peak_no_mask,
        psf_sidelobe = statistics.psf_sidelobe,
        nsigma_threshold_jy = statistics.nsigma_threshold,
        mask_pixels = statistics.mask_sum,
        "residual"
    );
}

/// The controls minor cycle `cycle` runs under.
pub(super) fn cycle_controls(cycle: usize, controls: &CycleControls) {
    tracing::info!(
        target: SCIENCE_TRACE,
        cycle,
        iterations = controls.iterations,
        threshold_jy = controls.threshold,
        threshold_reached = controls.threshold_reached,
        gain = controls.gain,
        nsigma = controls.nsigma,
        "minor cycle controls"
    );
}

/// What one minor cycle cleaned.
pub(super) fn minor_cycle(record: &NativeMinorCycleOutcome) {
    tracing::info!(
        target: SCIENCE_TRACE,
        cycle = record.cycle,
        iterations = record.iterations,
        components = record.actual_iterations,
        total_iterations = record.total_iterations,
        absolute_flux_jy = record.total_flux,
        initial_peak_jy = record.initial_peak_flux,
        final_peak_jy = record.final_peak_flux,
        noise_rms_jy = %Optional(record.noise_rms),
        effective_threshold_jy = record.effective_threshold,
        global_threshold_jy = record.global_threshold,
        cycle_threshold_jy = %Optional(record.cycle_threshold),
        stop = ?record.stop_reason,
        clark_refreshes = record.clark_refreshes,
        "minor cycle"
    );
}

/// A quantity the cycle may not have measured, logged as its value or
/// `none`.
struct Optional(Option<f64>);

impl std::fmt::Display for Optional {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(value) => value.fmt(formatter),
            None => formatter.write_str("none"),
        }
    }
}
