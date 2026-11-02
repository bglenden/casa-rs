// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA 6.7.6.14's `tclean` returns (#931): the summary record of
//! `imager.getSummary`, built from the run's report.
//! `tools/casa-compat/record_casa_returns.py` records the structure of
//! CASA's own returns, which the task tests compare this with.
//!
//! Values the report does not hold are `nan`, CASA's floats marked
//! unavailable rather than given another value:
//! - the PSF sidelobe (`maxpsfsidelobe`), a dirty run's peak residual and the
//!   peaks outside the mask (`startPeakResNM`, `peakResNM`);
//! - the top-level `cyclethreshold`, which CASA computes for the cycle after
//!   the last major cycle;
//! - the model flux of a run that starts from a model (`startmodel`), whose
//!   own flux the report does not hold; otherwise the model flux is the
//!   components' signed first-term flux, as CASA sums the model;
//! - every row of a run of more than one plane: CASA's `summaryminor` holds
//!   one entry per outlier field, channel and Stokes plane, and casa-rs's
//!   report sums each minor cycle over the planes, so each plane's entry
//!   holds `nan` rather than the sum.
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
        // CASA's is the threshold it computes after the last major cycle,
        // for a cycle the run does not run, which the report does not hold.
        ("cyclethreshold", float_or_zero(f64::NAN)),
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
        (
            "summaryminor",
            summary_minor(run, dirty, fullsummary, request.startmodel.is_some()),
        ),
        ("threshold", threshold),
        ("stopDescription", CasaValue::string(description)),
    ])
}

/// A dirty run's `threshold`, as CASA's echoes the parameter: as the caller
/// gave it, a number as a float and text as its text (`'1mJy'`, `'0Jy'`);
/// `values` holds tclean's default 0.0 where it was not given.
fn given_threshold(values: &Map<String, Value>) -> CasaValue {
    match values.get("threshold") {
        Some(Value::String(text)) => CasaValue::string(text),
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
        Some(ImagerCleanStopReason::NoChange) => {
            (4, "no change in peak residual across two major cycles")
        }
        Some(ImagerCleanStopReason::DivergedFromPrevious) => (
            5,
            "peak residual increased by more than 3 times from the previous major cycle",
        ),
        Some(ImagerCleanStopReason::DivergedFromMinimum) => (
            6,
            "peak residual increased by more than 3 times from the minimum reached",
        ),
        Some(ImagerCleanStopReason::NsigmaThresholdReached) => (
            8,
            "any combination of n-sigma and other valid exit criterion",
        ),
        Some(ImagerCleanStopReason::MajorCycleLimitReached) => (9, "reached nmajor"),
    }
}

/// `summaryminor`: `{field: {channel: {stokes: {row: [one per cycle]}}}}`,
/// with an entry for every plane of `run`; `{0: {}}` for a run that
/// iterates but ran no minor cycle. The report sums each minor cycle over
/// the planes, so a run of more than one plane holds `nan` rows; one that
/// starts from a model (`seeded`) holds `nan` model fluxes.
fn summary_minor(run: &ImagerRunReport, dirty: bool, fullsummary: bool, seeded: bool) -> CasaValue {
    let keyed = |keys: usize, value: &CasaValue| CasaValue::Dict {
        entries: (0..keys)
            .map(|key| CasaEntry {
                key: CasaKey::Int(i64::try_from(key).unwrap_or(i64::MAX)),
                value: value.clone(),
            })
            .collect(),
    };
    let cycles = &run.minor_cycles;
    if cycles.is_empty() && !dirty {
        return keyed(1, &CasaValue::dict(Vec::<(&str, CasaValue)>::new()));
    }
    let [fields, channels, stokes] = run.planes;
    let one_plane = fields * channels * stokes == 1;
    let rows = if dirty {
        dirty_rows(fullsummary, seeded)
    } else {
        let rows = Rows::of(cycles, seeded).entries(fullsummary);
        if one_plane { rows } else { unavailable(rows) }
    };
    let plane = CasaValue::dict(rows);
    keyed(fields, &keyed(channels, &keyed(stokes, &plane)))
}

/// `rows` with each value `nan`: per-plane rows the report does not hold.
fn unavailable(rows: Vec<(&'static str, CasaValue)>) -> Vec<(&'static str, CasaValue)> {
    rows.into_iter()
        .map(|(name, value)| match value {
            CasaValue::List { items } => (name, floats(items.iter().map(|_| f64::NAN))),
            other => (name, other),
        })
        .collect()
}

/// The rows of `summaryminor`, one value per minor cycle.
struct Rows {
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
    /// The rows of `cycles`, at least one; the model fluxes are `nan` when
    /// the run started from a model (`seeded`), whose flux the report does
    /// not hold.
    fn of(cycles: &[ImagerMinorCycleDiagnostic], seeded: bool) -> Self {
        let of = |field: fn(&ImagerMinorCycleDiagnostic) -> f64| {
            cycles.iter().map(field).collect::<Vec<_>>()
        };
        let mut flux = if seeded { f64::NAN } else { 0.0 };
        let mut start_flux = Vec::with_capacity(cycles.len());
        let mut model_flux = Vec::with_capacity(cycles.len());
        for cycle in cycles {
            start_flux.push(flux);
            flux += cycle.model_flux;
            model_flux.push(flux);
        }
        Self {
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
        if !fullsummary {
            return vec![
                ("iterDone", floats(self.iterations)),
                ("peakRes", floats(self.peaks)),
                ("modelFlux", floats(self.model_flux)),
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
            ("modelFlux", floats(self.model_flux)),
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

/// A dirty run's one row of each, as CASA's `construct_residual_dict` makes
/// them: in its own order, with integer zero model fluxes and stop code 3
/// (probed with CASA 6.7.6.14, `fullsummary` or not). The report holds no
/// dirty peak, so the peaks are `nan`, and no start model's flux, so a
/// run that starts from one (`seeded`) has `nan` model fluxes, where CASA
/// sums the model.
fn dirty_rows(fullsummary: bool, seeded: bool) -> Vec<(&'static str, CasaValue)> {
    let zero = || floats([0.0]);
    let nan = || floats([f64::NAN]);
    let int = |value: i64| CasaValue::List {
        items: vec![CasaValue::Int { value }],
    };
    let model_flux = || if seeded { nan() } else { int(0) };
    let mut rows = vec![
        ("iterDone", zero()),
        ("peakRes", nan()),
        ("modelFlux", model_flux()),
        ("cycleThresh", zero()),
    ];
    if fullsummary {
        rows.extend([
            ("cycleStartIters", zero()),
            ("startIterDone", zero()),
            ("startPeakRes", nan()),
            ("startModelFlux", model_flux()),
            ("startPeakResNM", nan()),
            ("peakResNM", nan()),
            ("masksum", zero()),
            ("mpiServer", zero()),
            ("stopCode", int(3)),
        ]);
    }
    rows
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
            planes: [1, 1, 1],
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

    /// Astra's #1015 F4: the controller's three divergence stops keep
    /// CASA's three codes and descriptions through the report.
    #[test]
    fn each_divergence_stop_keeps_its_casa_code() {
        use casa_imaging_application::CleanStop;
        for (stop_reason, code) in [
            (CleanStop::NoChange, 4),
            (CleanStop::DivergedFromPrevious, 5),
            (CleanStop::DivergedFromMinimum, 6),
        ] {
            let reason = ImagerCleanStopReason::from(stop_reason);
            let (returned, description) = stop(&report(Some(reason)), false);
            assert_eq!(returned, code, "{stop_reason:?}");
            assert_eq!(i64::from(stop_reason.code()), code);
            assert!(!description.is_empty());
        }
    }

    /// A minor cycle that adds `model_flux` to the model.
    fn cycle(model_flux: f64) -> ImagerMinorCycleDiagnostic {
        ImagerMinorCycleDiagnostic {
            cycle: 1,
            iterations_entering: 0,
            iterations: 1,
            total_iterations: 1,
            actual_iterations_entering: 0,
            actual_iterations: 1,
            total_actual_iterations: 1,
            total_flux: model_flux.abs(),
            model_flux,
            initial_peak_flux: 1.0,
            final_peak_flux: 0.5,
            noise_rms: None,
            effective_threshold: 0.1,
            global_threshold: 0.0,
            cycle_threshold: None,
            stop_reason: ImagerMinorCycleStopReason::IterationBound,
            clark_refreshes: 0,
            associated_replay_ordinal: 1,
            components: Vec::new(),
            mask_support: vec![true; 4],
            auto_mask: None,
        }
    }

    fn at<'a>(value: &'a CasaValue, key: &CasaKey) -> &'a CasaValue {
        let CasaValue::Dict { entries } = value else {
            panic!("not a dictionary: {value:?}");
        };
        &entries
            .iter()
            .find(|entry| &entry.key == key)
            .unwrap_or_else(|| panic!("no {key:?}"))
            .value
    }

    /// `summary`'s plane at `[field, channel, stokes]`.
    fn plane(summary: &CasaValue, keys: [i64; 3]) -> &CasaValue {
        keys.iter()
            .fold(summary, |value, key| at(value, &CasaKey::Int(*key)))
    }

    /// `name`'s row of `summary`'s plane at `keys`, as numbers.
    fn row(summary: &CasaValue, keys: [i64; 3], name: &str) -> Vec<f64> {
        let CasaValue::List { items } = at(plane(summary, keys), &CasaKey::String(name.into()))
        else {
            panic!("{name} is not a list");
        };
        items
            .iter()
            .map(|item| match item {
                CasaValue::Float { value } => value.0,
                CasaValue::Int { value } => *value as f64,
                other => panic!("{other:?}"),
            })
            .collect()
    }

    /// Astra's #1015 F2: the model flux is signed, and unavailable where the
    /// run starts from a model whose flux the report does not hold.
    #[test]
    fn the_model_flux_is_signed_and_unavailable_from_a_start_model() {
        let mut run = report(Some(ImagerCleanStopReason::IterationLimitReached));
        run.minor_cycles = vec![cycle(0.5), cycle(-0.2)];
        let summary = summary_minor(&run, false, true, false);
        assert_eq!(row(&summary, [0, 0, 0], "startModelFlux"), [0.0, 0.5]);
        let flux = row(&summary, [0, 0, 0], "modelFlux");
        assert!((flux[0] - 0.5).abs() < 1e-12 && (flux[1] - 0.3).abs() < 1e-12);
        let seeded = summary_minor(&run, false, true, true);
        for name in ["startModelFlux", "modelFlux"] {
            assert!(
                row(&seeded, [0, 0, 0], name)
                    .iter()
                    .all(|flux| flux.is_nan())
            );
        }
        let dirty = summary_minor(&report(None), true, false, true);
        assert!(row(&dirty, [0, 0, 0], "modelFlux")[0].is_nan());
    }

    /// Astra's #1015 F3: each plane of a cube has its entry, and none holds
    /// the sum over the planes.
    #[test]
    fn each_plane_of_a_cube_has_its_entry_without_the_planes_sum() {
        let mut run = report(Some(ImagerCleanStopReason::IterationLimitReached));
        run.minor_cycles = vec![cycle(0.5)];
        run.planes = [1, 3, 1];
        let summary = summary_minor(&run, false, false, false);
        for channel in 0..3 {
            for name in ["iterDone", "peakRes", "modelFlux", "cycleThresh"] {
                let values = row(&summary, [0, channel, 0], name);
                assert!(values.len() == 1 && values[0].is_nan(), "{channel} {name}");
            }
        }
        // A dirty cube's planes hold CASA's dirty rows.
        let mut dirty = report(None);
        dirty.planes = [1, 2, 1];
        let summary = summary_minor(&dirty, true, false, false);
        assert_eq!(row(&summary, [0, 1, 0], "iterDone"), [0.0]);
    }

    /// Astra's #1015 F9: a dirty run's full summary holds CASA's residual
    /// rows, in their order and types (probed with CASA 6.7.6.14).
    #[test]
    fn a_dirty_full_summary_holds_casas_residual_rows() {
        let summary = summary_minor(&report(None), true, true, false);
        let CasaValue::Dict { entries } = plane(&summary, [0, 0, 0]) else {
            panic!("a plane");
        };
        let names = entries
            .iter()
            .map(|entry| match &entry.key {
                CasaKey::String(name) => name.as_str(),
                CasaKey::Int(_) => panic!("a row name"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "iterDone",
                "peakRes",
                "modelFlux",
                "cycleThresh",
                "cycleStartIters",
                "startIterDone",
                "startPeakRes",
                "startModelFlux",
                "startPeakResNM",
                "peakResNM",
                "masksum",
                "mpiServer",
                "stopCode"
            ]
        );
        for (name, value) in [("modelFlux", 0), ("startModelFlux", 0), ("stopCode", 3)] {
            assert_eq!(
                at(plane(&summary, [0, 0, 0]), &CasaKey::String(name.into())),
                &CasaValue::List {
                    items: vec![CasaValue::Int { value }]
                },
                "{name}"
            );
        }
    }
}
