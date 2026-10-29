// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA 6.7.6.14's `tclean` returns (#931): the summary record of
//! `imager.getSummary`, built from the run's report.
//! `tools/casa-compat/record_casa_returns.py` records the structure of
//! CASA's own returns, which the task tests compare this with.
//!
//! CASA's `summaryminor` holds one entry per outlier field, channel and
//! Stokes plane; casa-rs's report sums each minor cycle over the planes, so
//! the record holds one, `[0][0][0]`. Values the report does not hold are
//! `nan`: the PSF sidelobe (`maxpsfsidelobe`), a dirty run's peak residual,
//! the peaks outside the mask (`startPeakResNM`, `peakResNM`), and the cycle
//! threshold of a run that ran no minor cycle. The model flux is the sum of
//! the components' magnitudes, which is CASA's signed model flux when no
//! component is negative.
//!
//! A dirty run, `niter=0`, holds CASA's `construct_residual_dict` values:
//! integer zeros for the iteration controls, `nsigma` 0, and `threshold` as
//! it was given. A run that iterates but runs no minor cycle (its threshold
//! above the peak, or an empty mask) holds an empty `summaryminor`, `{0:
//! {}}`, as CASA's does.

use casa_imaging_application::ImagingRequest;
use casa_provider_contracts::{CasaEntry, CasaKey, CasaValue};
use serde_json::{Map, Value};

use crate::{
    ImagerCleanStopReason, ImagerMinorCycleDiagnostic, ImagerMinorCycleStopReason, ImagerRunReport,
};

/// `tclean`'s return for `request`, made from tclean's resolved `values`
/// and run as `run` reports; `fullsummary` adds each minor cycle's full
/// detail.
pub fn tclean_record(
    request: &ImagingRequest,
    run: &ImagerRunReport,
    values: &Map<String, Value>,
) -> CasaValue {
    let fullsummary = values.get("fullsummary").and_then(Value::as_bool) == Some(true);
    let cycles = &run.minor_cycles;
    let dirty = request.niter == 0;
    // CASA's dirty summary holds integer zeros in place of these floats.
    let float_or_zero = |value: f64| {
        if dirty {
            CasaValue::Int { value: 0 }
        } else {
            CasaValue::float(value)
        }
    };
    // CASA keeps the iteration controls as `float` (single precision).
    let single = |value: f64| f64::from(value as f32);
    let iterations = int(run.minor_iterations);
    let (stopcode, description) = stop(run, dirty);
    let mut major = vec![0_i64];
    major.extend(cycles.iter().map(|cycle| count(cycle.total_iterations)));
    let threshold = if dirty {
        given_threshold(values)
    } else {
        CasaValue::float(single(request.threshold))
    };
    CasaValue::dict([
        ("cleanstate", CasaValue::string("running")),
        ("cyclefactor", CasaValue::float(request.cyclefactor)),
        ("cycleiterdone", int(0)),
        (
            "cycleniter",
            int(if dirty { 0 } else { request.minor_cycle_length }),
        ),
        (
            "cyclethreshold",
            float_or_zero(
                cycles
                    .last()
                    .map_or(f64::NAN, |cycle| cycle.effective_threshold),
            ),
        ),
        ("interactiveiterdone", iterations.clone()),
        ("interactivemode", CasaValue::Bool { value: false }),
        ("interactiveniter", int(0)),
        ("interactivethreshold", float_or_zero(0.0)),
        ("iterdone", iterations),
        ("loopgain", float_or_zero(single(request.gain))),
        (
            "maxpsffraction",
            float_or_zero(single(request.maxpsffraction)),
        ),
        ("maxpsfsidelobe", float_or_zero(f64::NAN)),
        (
            "minpsffraction",
            float_or_zero(single(request.minpsffraction)),
        ),
        ("niter", int(request.niter)),
        ("nmajordone", int(run.major_cycles.max(1))),
        (
            "nsigma",
            CasaValue::float(if dirty { 0.0 } else { request.nsigma }),
        ),
        ("stopcode", CasaValue::Int { value: stopcode }),
        ("summarymajor", CasaValue::int64_vector(major)),
        ("summaryminor", summary_minor(cycles, dirty, fullsummary)),
        ("threshold", threshold),
        ("stopDescription", CasaValue::string(description)),
    ])
}

/// A dirty run's `threshold`, as CASA's echoes the parameter: the
/// quantity's text as the catalog normalized it (CASA keeps the caller's
/// own spelling, `'1mJy'` where casa-rs holds `'0.001Jy'`), or the number
/// `0.0` for tclean's default.
fn given_threshold(values: &Map<String, Value>) -> CasaValue {
    match values.get("threshold") {
        Some(Value::String(text)) if text.trim_end_matches("Jy").parse::<f64>() != Ok(0.0) => {
            CasaValue::string(text)
        }
        Some(Value::Number(number)) => CasaValue::float(number.as_f64().unwrap_or(0.0)),
        _ => CasaValue::float(0.0),
    }
}

/// CASA's `stopcode` and its description (`imager_base.getStopDescription`).
fn stop(run: &ImagerRunReport, dirty: bool) -> (i64, &'static str) {
    if dirty {
        return (3, "Zero iterations performed");
    }
    match run.clean_stop_reason {
        Some(ImagerCleanStopReason::IterationLimitReached) => (1, "iteration limit"),
        Some(ImagerCleanStopReason::GlobalThresholdReached) | None => (2, "threshold"),
        // CASA 6.7.6.14 stops an empty mask so (probed).
        Some(ImagerCleanStopReason::NoCleanablePixels) => (7, "zero mask"),
        Some(ImagerCleanStopReason::DivergenceDetected) => {
            (4, "no change in peak residual across two major cycles")
        }
        Some(ImagerCleanStopReason::NsigmaThresholdReached) => (
            8,
            "any combination of n-sigma and other valid exit criterion",
        ),
        Some(ImagerCleanStopReason::MajorCycleLimitReached) => (9, "reached nmajor"),
    }
}

/// `summaryminor`: `{field: {channel: {stokes: {row: [one per cycle]}}}}`;
/// `{0: {}}` for a run that iterates but ran no minor cycle.
fn summary_minor(
    cycles: &[ImagerMinorCycleDiagnostic],
    dirty: bool,
    fullsummary: bool,
) -> CasaValue {
    let keyed = |value: CasaValue| CasaValue::Dict {
        entries: vec![CasaEntry {
            key: CasaKey::Int(0),
            value,
        }],
    };
    if cycles.is_empty() && !dirty {
        return keyed(CasaValue::Dict {
            entries: Vec::new(),
        });
    }
    let plane = CasaValue::dict(Rows::of(cycles).entries(fullsummary));
    keyed(keyed(keyed(plane)))
}

/// The rows of `summaryminor`, one value per minor cycle.
struct Rows {
    dirty: bool,
    start_iterations: Vec<f64>,
    iterations: Vec<f64>,
    start_peaks: Vec<f64>,
    peaks: Vec<f64>,
    start_flux: Vec<f64>,
    model_flux: Vec<f64>,
    thresholds: Vec<f64>,
    mask_sums: Vec<f64>,
    stop_codes: Vec<f64>,
}

impl Rows {
    fn of(cycles: &[ImagerMinorCycleDiagnostic]) -> Self {
        if cycles.is_empty() {
            // A dirty run's one zero cycle, as CASA's initial summary.
            return Self {
                dirty: true,
                start_iterations: vec![0.0],
                iterations: vec![0.0],
                start_peaks: vec![f64::NAN],
                peaks: vec![f64::NAN],
                start_flux: vec![0.0],
                model_flux: vec![0.0],
                thresholds: vec![0.0],
                mask_sums: vec![0.0],
                stop_codes: vec![3.0],
            };
        }
        let of = |field: fn(&ImagerMinorCycleDiagnostic) -> f64| {
            cycles.iter().map(field).collect::<Vec<_>>()
        };
        let mut flux = 0.0;
        let mut start_flux = Vec::with_capacity(cycles.len());
        let mut model_flux = Vec::with_capacity(cycles.len());
        for cycle in cycles {
            start_flux.push(flux);
            flux += cycle.total_flux;
            model_flux.push(flux);
        }
        Self {
            dirty: false,
            start_iterations: of(|cycle| cycle.iterations_entering as f64),
            iterations: of(|cycle| cycle.iterations as f64),
            start_peaks: of(|cycle| cycle.initial_peak_flux),
            peaks: of(|cycle| cycle.final_peak_flux),
            start_flux,
            model_flux,
            thresholds: of(|cycle| cycle.effective_threshold),
            mask_sums: of(|cycle| {
                cycle
                    .mask_support
                    .iter()
                    .filter(|&&cleanable| cleanable)
                    .count() as f64
            }),
            stop_codes: of(|cycle| f64::from(minor_stop_code(cycle.stop_reason))),
        }
    }

    /// The rows in CASA's order: four, or the full summary's 13. casa-rs
    /// runs on one server (`mpiServer` 0) and holds no peaks outside the
    /// mask.
    fn entries(self, fullsummary: bool) -> Vec<(&'static str, CasaValue)> {
        // CASA's dirty summary holds an integer zero model flux.
        let model_flux = if self.dirty && !fullsummary {
            CasaValue::List {
                items: vec![int(0)],
            }
        } else {
            floats(self.model_flux)
        };
        if !fullsummary {
            return vec![
                ("iterDone", floats(self.iterations)),
                ("peakRes", floats(self.peaks)),
                ("modelFlux", model_flux),
                ("cycleThresh", floats(self.thresholds)),
            ];
        }
        let each = |value: f64| floats(std::iter::repeat_n(value, self.iterations.len()));
        vec![
            ("startIterDone", floats(self.start_iterations.clone())),
            ("iterDone", floats(self.iterations.clone())),
            ("startPeakRes", floats(self.start_peaks)),
            ("peakRes", floats(self.peaks)),
            ("startModelFlux", floats(self.start_flux)),
            ("modelFlux", model_flux),
            ("startPeakResNM", each(f64::NAN)),
            ("peakResNM", each(f64::NAN)),
            ("cycleThresh", floats(self.thresholds)),
            ("cycleStartIters", floats(self.start_iterations)),
            ("masksum", floats(self.mask_sums)),
            ("mpiServer", each(0.0)),
            ("stopCode", floats(self.stop_codes)),
        ]
    }
}

/// CASA's minor-cycle stop code (`imager_return_dict`): 1 at cycleniter,
/// 2 at the cycle threshold, 4 on divergence.
const fn minor_stop_code(reason: ImagerMinorCycleStopReason) -> u8 {
    match reason {
        ImagerMinorCycleStopReason::IterationBound => 1,
        ImagerMinorCycleStopReason::ThresholdReached => 2,
        ImagerMinorCycleStopReason::Diverged => 4,
    }
}

fn floats(values: impl IntoIterator<Item = f64>) -> CasaValue {
    CasaValue::List {
        items: values.into_iter().map(CasaValue::float).collect(),
    }
}

fn count(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn int(value: usize) -> CasaValue {
    CasaValue::Int {
        value: count(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(clean_stop_reason: Option<ImagerCleanStopReason>) -> ImagerRunReport {
        ImagerRunReport {
            gridded_samples: 0,
            major_cycles: 1,
            minor_iterations: 0,
            actual_minor_iterations: 0,
            clean_stop_reason,
            minor_cycles: Vec::new(),
            visibility_products: None,
            elapsed_ns: 0,
        }
    }

    /// CASA 6.7.6.14 stops a run whose mask holds no pixel at stopcode 7
    /// (probed: a box outside the image), not at its threshold.
    #[test]
    fn an_empty_mask_stops_at_casas_zero_mask() {
        assert_eq!(
            stop(
                &report(Some(ImagerCleanStopReason::NoCleanablePixels)),
                false
            ),
            (7, "zero mask")
        );
        assert_eq!(
            stop(
                &report(Some(ImagerCleanStopReason::GlobalThresholdReached)),
                false
            ),
            (2, "threshold")
        );
        assert_eq!(stop(&report(None), true), (3, "Zero iterations performed"));
    }
}
