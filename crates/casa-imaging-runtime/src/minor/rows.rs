// SPDX-License-Identifier: LGPL-3.0-or-later
//! Each plane's row of a minor cycle, which CASA's `summaryminor` reports.

use casa_imaging_deconvolution::{
    CycleControls, FinalNormalState, ImageDomainReconstructionMasks, PlaneOutcome, PlaneStatistics,
    PlaneStop, blank_plane_stop,
};

use super::PlaneKey;

/// One plane's part of a minor cycle: what `SDAlgorithmBase::deconvolve`
/// hands `addSummaryMinor`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlaneCycleRow {
    /// The plane's image domain.
    pub domain: usize,
    /// The plane's absolute output channel; a Taylor family's first.
    pub channel: usize,
    /// The plane's polarization.
    pub polarization: usize,
    /// Iterations charged to the plane.
    pub iterations: usize,
    /// Peak residual inside the mask on entry; zero with no mask.
    pub start_peak: f64,
    /// Peak residual inside the mask at the end; zero with no mask.
    pub peak: f64,
    /// Peak residual over the plane's pixels on entry.
    pub start_peak_no_mask: f64,
    /// Peak residual over the plane's pixels at the end.
    pub peak_no_mask: f64,
    /// Signed first-term flux the plane added to the model.
    pub model_flux: f64,
    /// Pixels in the plane's mask.
    pub mask_sum: usize,
    /// Why the plane stopped.
    pub stop: PlaneStop,
}

/// The row of a plane with no data.
pub(super) fn blank_row(
    key: PlaneKey,
    statistics: &PlaneStatistics,
    controls: &CycleControls,
) -> PlaneCycleRow {
    PlaneCycleRow {
        domain: key.domain,
        channel: key.channel,
        polarization: key.polarization,
        iterations: 0,
        start_peak: 0.0,
        peak: 0.0,
        start_peak_no_mask: 0.0,
        peak_no_mask: 0.0,
        model_flux: 0.0,
        mask_sum: statistics.mask_sum,
        stop: blank_plane_stop(controls, statistics),
    }
}

/// A plane with no data as CASA measures it: zero peaks, and its mask's
/// pixels, which its zero residual doesn't change.
pub(super) fn blank_statistics(
    normal: &FinalNormalState,
    masks: &ImageDomainReconstructionMasks,
    key: PlaneKey,
    automask: bool,
) -> PlaneStatistics {
    let mask = masks
        .get(key.domain)
        .and_then(|mask| {
            mask.plane(
                key.channel - normal.slab().core_range().start,
                key.polarization,
            )
        })
        .expect("a mask for every plane of every image domain");
    PlaneStatistics {
        peak_in_mask: 0.0,
        peak_no_mask: 0.0,
        mask_sum: mask.iter().filter(|masked| **masked).count(),
        noise: None,
        automask,
    }
}

/// The row of a plane that `outcome` cleaned or skipped. A plane with no mask
/// reports zero peaks inside it, and stops on that zero
/// (`SDAlgorithmBase::deconvolve`, CAS-14201).
pub(super) fn cycle_row(
    key: PlaneKey,
    statistics: &PlaneStatistics,
    outcome: &PlaneOutcome,
    peak_no_mask: f64,
    controls: &CycleControls,
) -> PlaneCycleRow {
    let masked = |peak: f64| if statistics.mask_sum > 0 { peak } else { 0.0 };
    PlaneCycleRow {
        domain: key.domain,
        channel: key.channel,
        polarization: key.polarization,
        iterations: outcome.iterations,
        start_peak: masked(outcome.start_peak),
        peak: masked(outcome.peak),
        start_peak_no_mask: statistics.peak_no_mask,
        peak_no_mask,
        model_flux: outcome.model_flux,
        mask_sum: statistics.mask_sum,
        stop: if statistics.mask_sum > 0 {
            outcome.stop
        } else {
            blank_plane_stop(controls, statistics)
        },
    }
}

#[cfg(test)]
mod tests {
    use casa_imaging_deconvolution::Delta;

    use super::*;

    fn controls() -> CycleControls {
        CycleControls {
            iterations: 5,
            threshold: 0.1,
            threshold_reached: false,
            gain: 0.1,
            nsigma: 0.0,
        }
    }

    fn statistics(mask_sum: usize) -> PlaneStatistics {
        PlaneStatistics {
            peak_in_mask: if mask_sum > 0 { 1.0 } else { 0.0 },
            peak_no_mask: 2.0,
            mask_sum,
            noise: None,
            automask: false,
        }
    }

    fn outcome(stop: PlaneStop) -> PlaneOutcome {
        PlaneOutcome {
            delta: Delta::new(1),
            iterations: 3,
            components: 3,
            absolute_flux: 1.0,
            model_flux: 0.5,
            start_peak: 2.0,
            peak: 1.5,
            stop,
            trace: Vec::new(),
            refreshes: 0,
            end_residual: Vec::new(),
        }
    }

    const KEY: PlaneKey = PlaneKey {
        domain: 1,
        channel: 2,
        polarization: 3,
    };

    /// A cleaned plane's row is its own outcome.
    #[test]
    fn a_cleaned_planes_row_holds_its_outcome() {
        let row = cycle_row(
            KEY,
            &statistics(4),
            &outcome(PlaneStop::Iterations),
            0.75,
            &controls(),
        );
        assert_eq!(
            [
                row.domain,
                row.channel,
                row.polarization,
                row.iterations,
                row.mask_sum
            ],
            [1, 2, 3, 3, 4]
        );
        assert_eq!(
            [
                row.start_peak,
                row.peak,
                row.start_peak_no_mask,
                row.peak_no_mask,
                row.model_flux
            ],
            [2.0, 1.5, 2.0, 0.75, 0.5]
        );
        assert_eq!(row.stop, PlaneStop::Iterations);
    }

    /// CAS-14201: a plane with no mask holds zero peaks inside it, and
    /// `checkStop` on that zero stops it at the threshold, though its data
    /// peak above it, as `SDAlgorithmBase::deconvolve` does.
    #[test]
    fn a_plane_with_no_mask_stops_on_a_zero_peak() {
        let row = cycle_row(
            KEY,
            &statistics(0),
            &outcome(PlaneStop::ZeroMask),
            2.0,
            &controls(),
        );
        assert_eq!([row.start_peak, row.peak], [0.0, 0.0]);
        assert_eq!(row.start_peak_no_mask, 2.0);
        assert_eq!(row.stop, PlaneStop::CycleThreshold);
    }

    /// A plane with no data holds zero rows and its mask's pixels.
    #[test]
    fn a_blank_planes_row_is_empty_with_its_mask() {
        let row = blank_row(KEY, &statistics(7), &controls());
        assert_eq!(row.iterations, 0);
        assert_eq!(row.mask_sum, 7);
        assert_eq!(
            [row.peak, row.peak_no_mask, row.model_flux],
            [0.0, 0.0, 0.0]
        );
        assert_eq!(row.stop, PlaneStop::CycleThreshold);
    }
}
