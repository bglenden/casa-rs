// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA 6.7.6.14's `tclean` returns (#931): the summary record of
//! `imager.getSummary`, built from the run's report.
//! `tools/casa-compat/record_casa_returns.py` records the structure of
//! CASA's own returns, which the task tests compare this with.
//!
//! `summaryminor` holds a row for every plane of every minor cycle, in
//! CASA's order of image domain, channel and polarization. casa-rs runs on
//! one server, so `mpiServer` is 0.
//!
//! A dirty run, `niter=0`, holds CASA's `construct_residual_dict` values:
//! integer zeros for the iteration controls, `nsigma` 0, and `threshold` as
//! it was given; its rows read the products it wrote ([`dirty`]). A run that
//! iterates but runs no minor cycle (its threshold
//! above the peak, or an empty mask) holds an empty `summaryminor`, `{0:
//! {}}`, as CASA's does.

mod dirty;

use casa_imaging_application::{ImagingRequest, PlaneCycleRow, PlaneRows, ResidualStatistics};
use casa_provider_contracts::{CasaEntry, CasaKey, CasaValue};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

use crate::{ImagerCleanStopReason, ImagerMinorCycleDiagnostic, ImagerRunReport};

/// What a run hands [`tclean_record`] beside its report.
#[derive(Clone, Debug, Default)]
pub struct RunReturn {
    /// The final convergence check's measured state, including when cleaning
    /// stops before executing a minor cycle.
    pub statistics: Option<ResidualStatistics>,
    /// Each plane's part of each minor cycle.
    pub plane_rows: PlaneRows,
}

/// `tclean`'s return for `request`, made from tclean's resolved `values`
/// and run as `run` reports; `fullsummary` adds each minor cycle's full
/// detail.
pub fn tclean_record(
    request: &ImagingRequest,
    run: &ImagerRunReport,
    values: &Map<String, Value>,
    held: &RunReturn,
) -> CasaValue {
    let statistics = held.statistics.as_ref();
    let fullsummary = values.get("fullsummary").and_then(Value::as_bool) == Some(true);
    let cycles = &run.minor_cycles;
    let (max_psf_sidelobe, cycle_threshold) =
        statistics.map_or((f64::NAN, f64::NAN), |statistics| {
            final_scalars(
                statistics,
                request.cyclefactor,
                request.minpsffraction,
                request.maxpsffraction,
            )
        });
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
    let major = major_summary(dirty, request.calcres, cycles);
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
        ("cyclethreshold", float_or_zero(cycle_threshold)),
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
        ("maxpsfsidelobe", float_or_zero(max_psf_sidelobe)),
        (
            "minpsffraction",
            float_or_zero(single(request.minpsffraction)),
        ),
        ("niter", int(request.niter)),
        // CASA's dirty summary counts one major cycle; a clean run counts those
        // it ran, none for a residual it read (`calcres=False`).
        ("nmajordone", int(if dirty { 1 } else { run.major_cycles })),
        (
            "nsigma",
            CasaValue::float(if dirty { 0.0 } else { request.nsigma }),
        ),
        ("stopcode", CasaValue::Int { value: stopcode }),
        ("summarymajor", CasaValue::int64_vector(major)),
        (
            "summaryminor",
            if dirty {
                dirty::summary_minor(
                    &request.image_names(),
                    request.mask_image.as_deref(),
                    fullsummary,
                )
            } else {
                summary_minor(run, &held.plane_rows, fullsummary)
            },
        ),
        ("threshold", threshold),
        (
            "stopDescription",
            description.map_or(CasaValue::None, CasaValue::string),
        ),
    ])
}

/// CASA `getDetailsRecord` recomputes the raw automatic threshold. The
/// global threshold clamps only `getMinorCycleControls`' local copy, not
/// this summary value. Reproduce the reporting state's Float arithmetic
/// without changing the native controller's scientific computations.
fn final_scalars(
    statistics: &ResidualStatistics,
    cyclefactor: f64,
    minimum: f64,
    maximum: f64,
) -> (f64, f64) {
    let sidelobe = statistics.psf_sidelobe as f32;
    let fraction = (sidelobe * cyclefactor as f32)
        .max(minimum as f32)
        .min(maximum as f32);
    let threshold = statistics.peak as f32 * fraction;
    (f64::from(sidelobe), f64::from(threshold))
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

/// Each major-cycle completion appends its cumulative iteration count. A
/// read residual and a minor cycle that cleans nothing run no major cycle.
fn major_summary(dirty: bool, calcres: bool, cycles: &[ImagerMinorCycleDiagnostic]) -> Vec<i64> {
    let mut major = if dirty || calcres {
        vec![0]
    } else {
        Vec::new()
    };
    if !dirty {
        major.extend(
            cycles
                .iter()
                .filter(|cycle| cycle.iterations > 0)
                .map(|cycle| count(cycle.total_iterations)),
        );
    }
    major
}

/// CASA's `stopcode` and its description (`imager_base.getStopDescription`).
fn stop(run: &ImagerRunReport, dirty: bool) -> (i64, Option<&'static str>) {
    if dirty {
        return (3, Some("Zero iterations performed"));
    }
    let (code, description) = match run.clean_stop_reason {
        Some(ImagerCleanStopReason::IterationLimitReached) => (1, "iteration limit"),
        Some(ImagerCleanStopReason::GlobalThresholdReached) => (2, "threshold"),
        None => return (0, None),
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
    };
    (code, Some(description))
}

/// `summaryminor`: `{field: {channel: {stokes: {row: [one per cycle]}}}}`,
/// with an entry for every plane; `{0: {}}` for a run that iterates but ran
/// no minor cycle. Planes run in sequence in CASA: a plane's `startIterDone`
/// counts the planes before it, and a field's `cycleStartIters` the fields
/// before it.
fn summary_minor(run: &ImagerRunReport, rows: &PlaneRows, fullsummary: bool) -> CasaValue {
    if rows.cycles.iter().all(Vec::is_empty) {
        return keyed([(0, CasaValue::dict(Vec::<(&str, CasaValue)>::new()))]);
    }
    let mut planes = BTreeMap::new();
    for (cycle, cycle_rows) in run.minor_cycles.iter().zip(&rows.cycles) {
        let mut done = cycle.iterations_entering;
        let mut field = (usize::MAX, done);
        for row in cycle_rows {
            if field.0 != row.domain {
                field = (row.domain, done);
            }
            planes
                .entry([row.domain, row.channel, row.polarization])
                .or_insert_with_key(|key| {
                    Rows::starting_from(start_flux(rows, *key, row.polarizations))
                })
                .push(row, cycle, done, field.1);
            done += row.iterations;
        }
    }
    nested(
        planes
            .into_iter()
            .map(|(key, rows)| (key, CasaValue::dict(rows.entries(fullsummary)))),
    )
}

/// The plane's start-model flux; zero when it has none.
fn start_flux(rows: &PlaneRows, key: [usize; 3], polarizations: usize) -> f64 {
    (key[2]..key[2] + polarizations)
        .map(|polarization| {
            let plane = [key[0], key[1], polarization];
            rows.start_model_flux
                .binary_search_by_key(&plane, |(key, _)| *key)
                .map_or(0.0, |index| rows.start_model_flux[index].1)
        })
        .sum()
}

/// `planes`, sorted by `[field, channel, stokes]`, as nested dictionaries.
fn nested(planes: impl IntoIterator<Item = ([usize; 3], CasaValue)>) -> CasaValue {
    let mut fields: BTreeMap<usize, BTreeMap<usize, Vec<(usize, CasaValue)>>> = BTreeMap::new();
    for ([field, channel, stokes], plane) in planes {
        fields
            .entry(field)
            .or_default()
            .entry(channel)
            .or_default()
            .push((stokes, plane));
    }
    keyed(fields.into_iter().map(|(field, channels)| {
        let channels = channels
            .into_iter()
            .map(|(channel, stokes)| (channel, keyed(stokes)));
        (field, keyed(channels))
    }))
}

/// A dictionary with integer keys.
fn keyed(entries: impl IntoIterator<Item = (usize, CasaValue)>) -> CasaValue {
    CasaValue::Dict {
        entries: entries
            .into_iter()
            .map(|(key, value)| CasaEntry {
                key: CasaKey::Int(count(key)),
                value,
            })
            .collect(),
    }
}

/// One plane's rows of `summaryminor`, one value per minor cycle.
#[derive(Default)]
struct Rows {
    flux: f64,
    start_iterations: Vec<f64>,
    cycle_start_iterations: Vec<f64>,
    iterations: Vec<f64>,
    start_peaks: Vec<f64>,
    peaks: Vec<f64>,
    start_flux: Vec<f64>,
    model_flux: Vec<f64>,
    start_peaks_no_mask: Vec<f64>,
    peaks_no_mask: Vec<f64>,
    thresholds: Vec<f64>,
    mask_sums: Vec<f64>,
    stop_codes: Vec<f64>,
}

impl Rows {
    fn starting_from(flux: f64) -> Self {
        Self {
            flux,
            ..Self::default()
        }
    }

    /// Add `row`, of `cycle`, which starts at `done` iterations in a field
    /// that started at `field_done`.
    fn push(
        &mut self,
        row: &PlaneCycleRow,
        cycle: &ImagerMinorCycleDiagnostic,
        done: usize,
        field_done: usize,
    ) {
        self.start_iterations.push(done as f64);
        self.cycle_start_iterations.push(field_done as f64);
        self.iterations.push(row.iterations as f64);
        self.start_peaks.push(row.start_peak);
        self.peaks.push(row.peak);
        self.start_flux.push(self.flux);
        self.flux += row.model_flux;
        self.model_flux.push(self.flux);
        self.start_peaks_no_mask.push(row.start_peak_no_mask);
        self.peaks_no_mask.push(row.peak_no_mask);
        self.thresholds
            .push(cycle.cycle_threshold.unwrap_or(cycle.effective_threshold));
        self.mask_sums.push(row.mask_sum as f64);
        self.stop_codes.push(f64::from(row.stop.code()));
    }

    /// The rows in CASA's order: four, or the full summary's 13.
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
        let servers = each(0.0);
        vec![
            ("startIterDone", floats(self.start_iterations)),
            ("iterDone", floats(self.iterations)),
            ("startPeakRes", floats(self.start_peaks)),
            ("peakRes", floats(self.peaks)),
            ("startModelFlux", floats(self.start_flux)),
            ("modelFlux", floats(self.model_flux)),
            ("startPeakResNM", floats(self.start_peaks_no_mask)),
            ("peakResNM", floats(self.peaks_no_mask)),
            ("cycleThresh", floats(self.thresholds)),
            ("cycleStartIters", floats(self.cycle_start_iterations)),
            ("masksum", floats(self.mask_sums)),
            ("mpiServer", servers),
            ("stopCode", floats(self.stop_codes)),
        ]
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

    /// Pinned CASA 6.7.6.14 values, with final peaks read from its saved
    /// residuals. The final iteration-limit peak differs from the preceding
    /// minor cycle's peak; an empty mask uses the valid unmasked peak.
    #[test]
    fn final_scalar_summary_uses_casa_float_arithmetic_and_final_peaks() {
        for (peak, mask_sum, expected) in [
            (0.20589078962802887, 4096, 0.16471263766288757),
            (0.7290002703666687, 4096, 0.583200216293335),
            (1.0000007152557373, 4096, 0.8000006079673767),
            (1.0000007152557373, 0, 0.8000006079673767),
        ] {
            let statistics = ResidualStatistics {
                peak,
                peak_no_mask: peak,
                psf_sidelobe: 0.9974840879440308,
                nsigma_threshold: 0.0,
                mask_sum,
            };
            assert_eq!(
                final_scalars(&statistics, 1.0, 0.05, 0.8),
                (0.9974840879440308, expected)
            );
        }
    }

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
            (7, Some("zero mask"))
        );
        assert_eq!(
            stop(
                &report(Some(ImagerCleanStopReason::GlobalThresholdReached)),
                false
            ),
            (2, Some("threshold"))
        );
        assert_eq!(
            stop(&report(None), true),
            (3, Some("Zero iterations performed"))
        );
    }

    /// CASA leaves the global stop unset when a minor cycle cleans nothing
    /// but no global convergence condition applies.
    #[test]
    fn no_global_stop_is_not_reported_as_threshold() {
        assert_eq!(stop(&report(None), false), (0, None));
    }

    /// CASA appends a summary entry only after a real major cycle, including
    /// the initial residual if formed, but excluding an empty minor cycle.
    #[test]
    fn major_summary_records_only_major_cycles_that_ran() {
        let mut cleaned = cycle(0);
        cleaned.total_iterations = 4;
        let mut empty = cycle(4);
        empty.iterations = 0;
        empty.total_iterations = 4;
        assert_eq!(
            major_summary(false, false, &[cleaned.clone(), empty.clone()]),
            [4]
        );
        assert_eq!(major_summary(false, true, &[cleaned, empty]), [0, 4]);
        assert!(major_summary(false, false, &[]).is_empty());
        assert_eq!(major_summary(true, true, &[]), [0]);
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
            assert!(!description.expect("description").is_empty());
        }
    }

    /// A minor cycle that starts after `entering` iterations.
    fn cycle(entering: usize) -> ImagerMinorCycleDiagnostic {
        ImagerMinorCycleDiagnostic {
            cycle: 1,
            iterations_entering: entering,
            iterations: 1,
            total_iterations: 1,
            actual_iterations_entering: 0,
            actual_iterations: 1,
            total_actual_iterations: 1,
            total_flux: 0.0,
            model_flux: 0.0,
            initial_peak_flux: 1.0,
            final_peak_flux: 0.5,
            noise_rms: None,
            effective_threshold: 0.1,
            global_threshold: 0.0,
            cycle_threshold: None,
            stop_reason: crate::ImagerMinorCycleStopReason::IterationBound,
            clark_refreshes: 0,
            associated_replay_ordinal: 1,
            components: Vec::new(),
            mask_support: vec![true; 4],
            auto_mask: None,
        }
    }

    /// A row of `[domain, channel, polarization]` with `iterations` adding
    /// `model_flux`.
    fn plane_row(key: [usize; 3], iterations: usize, model_flux: f64) -> PlaneCycleRow {
        PlaneCycleRow {
            domain: key[0],
            channel: key[1],
            polarization: key[2],
            polarizations: 1,
            iterations,
            start_peak: 1.0,
            peak: 0.5,
            start_peak_no_mask: 2.0,
            peak_no_mask: 0.75,
            model_flux,
            mask_sum: 4,
            stop: casa_imaging_deconvolution::PlaneStop::Iterations,
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

    /// `name`'s row of `summary`'s plane at `keys`, as numbers.
    fn row(summary: &CasaValue, keys: [i64; 3], name: &str) -> Vec<f64> {
        let plane = keys
            .iter()
            .fold(summary, |value, key| at(value, &CasaKey::Int(*key)));
        let CasaValue::List { items } = at(plane, &CasaKey::String(name.into())) else {
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

    /// A plane's model flux accumulates signed from its start model's, each
    /// plane's its own, as `SIImageStore::getModelFlux` reads it.
    #[test]
    fn the_model_flux_accumulates_from_each_planes_start_model() {
        let mut run = report(Some(ImagerCleanStopReason::IterationLimitReached));
        run.minor_cycles = vec![cycle(0), cycle(2)];
        let rows = PlaneRows {
            start_model_flux: vec![([0, 0, 0], 3.0)],
            cycles: vec![
                vec![plane_row([0, 0, 0], 1, 0.5), plane_row([0, 1, 0], 1, 0.25)],
                vec![plane_row([0, 0, 0], 1, -0.2), plane_row([0, 1, 0], 1, 0.0)],
            ],
        };
        let summary = summary_minor(&run, &rows, true);
        assert_eq!(row(&summary, [0, 0, 0], "startModelFlux"), [3.0, 3.5]);
        assert_eq!(row(&summary, [0, 0, 0], "modelFlux"), [3.5, 3.3]);
        assert_eq!(row(&summary, [0, 1, 0], "startModelFlux"), [0.0, 0.25]);
        assert_eq!(row(&summary, [0, 1, 0], "modelFlux"), [0.25, 0.25]);
    }

    /// A joint Clark row starts from all its polarizations' model flux,
    /// including non-first polarization start models and restarts (#1140).
    #[test]
    fn joint_polarization_row_starts_from_the_whole_groups_model_flux() {
        let mut run = report(Some(ImagerCleanStopReason::IterationLimitReached));
        run.minor_cycles = vec![cycle(0), cycle(1)];
        let mut first = plane_row([0, 0, 0], 1, 0.5);
        first.polarizations = 2;
        let mut second = plane_row([0, 0, 0], 1, -0.2);
        second.polarizations = 2;
        let rows = PlaneRows {
            start_model_flux: vec![([0, 0, 0], 3.0), ([0, 0, 1], -1.0), ([0, 1, 0], 9.0)],
            cycles: vec![vec![first], vec![second]],
        };
        let summary = summary_minor(&run, &rows, true);
        assert_eq!(row(&summary, [0, 0, 0], "startModelFlux"), [2.0, 2.5]);
        assert_eq!(row(&summary, [0, 0, 0], "modelFlux"), [2.5, 2.3]);
    }

    /// Planes run in sequence in CASA, so `startIterDone` counts the planes
    /// before each and `cycleStartIters` the fields before each, as probed on
    /// CASA 6.7.6.14's outlier and cube runs.
    #[test]
    fn iteration_counts_run_through_planes_and_fields_in_order() {
        let mut run = report(Some(ImagerCleanStopReason::IterationLimitReached));
        run.minor_cycles = vec![cycle(10)];
        let rows = PlaneRows {
            start_model_flux: Vec::new(),
            cycles: vec![vec![
                plane_row([0, 0, 0], 3, 0.0),
                plane_row([0, 0, 1], 4, 0.0),
                plane_row([0, 1, 0], 5, 0.0),
                plane_row([1, 0, 0], 6, 0.0),
            ]],
        };
        let summary = summary_minor(&run, &rows, true);
        for (plane, start, field_start) in [
            ([0, 0, 0], 10.0, 10.0),
            ([0, 0, 1], 13.0, 10.0),
            ([0, 1, 0], 17.0, 10.0),
            ([1, 0, 0], 22.0, 22.0),
        ] {
            assert_eq!(row(&summary, plane, "startIterDone"), [start]);
            assert_eq!(row(&summary, plane, "cycleStartIters"), [field_start]);
        }
        assert_eq!(row(&summary, [0, 1, 0], "iterDone"), [5.0]);
        assert_eq!(row(&summary, [0, 1, 0], "peakResNM"), [0.75]);
        assert_eq!(row(&summary, [0, 1, 0], "startPeakResNM"), [2.0]);
        assert_eq!(row(&summary, [0, 1, 0], "masksum"), [4.0]);
        assert_eq!(row(&summary, [0, 1, 0], "stopCode"), [1.0]);
    }

    /// A run that iterates and cleans nothing holds CASA's empty record.
    #[test]
    fn a_run_with_no_minor_cycle_holds_an_empty_summary() {
        let summary = summary_minor(&report(None), &PlaneRows::default(), true);
        let CasaValue::Dict { entries } = at(&summary, &CasaKey::Int(0)) else {
            panic!("a dictionary");
        };
        assert!(entries.is_empty());
    }
}
