// SPDX-License-Identifier: LGPL-3.0-or-later
//! MSv2/MSv4 trial, example B (`docs/measurement-set/trial/README.md`): the
//! production gain accumulation and solver fed from
//! `casa_observation::VisibilityBatch`es of both stores, with 60 s solution
//! groups that cross batch boundaries.
//!
//! `cargo test -p casa-calibration --lib batch_trial -- --nocapture` prints
//! the measurements the evidence records.

//!
//! The solve itself is [`feed`], which sees only the shared observation
//! interface; this module opens the stores, runs the production solve, and
//! compares.

mod feed;

use std::fs::File;
use std::path::{Path, PathBuf};

use casa_ms::ms::MeasurementSet;
use casa_ms::observation_io::{GroupClosure, OpenOptions};
use casa_ms::selection::MsSelection;
use casa_observation::{
    AccessRequest, Binding, BindingTolerance, CorrelationRequest, Grouping, Observation, Roles,
};

use super::kernel::SolutionRow;
use super::{
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
    GainType, RefAntSelector, solve_gain, solve_gain_rows,
};
use feed::{solve_concurrent, solve_sequential};

fn fixture(root: &Path, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../casa-ms/tests/fixtures/ms_trial")
        .join(format!("{name}.tgz"));
    tar::Archive::new(flate2::read::GzDecoder::new(
        File::open(archive).expect("fixture"),
    ))
    .unpack(root)
    .expect("unpack");
    root.join(name)
}

/// `gaincal(gaintype='G', calmode='ap', solint='60s', refant=antenna 0)`
/// against a 1 Jy point source.
fn gain_request(output_table: PathBuf) -> GainSolveRequest {
    GainSolveRequest {
        selection: MsSelection::new(),
        output_table,
        gain_type: GainType::G,
        solve_mode: GainSolveMode::AmplitudePhase,
        solve_interval: GainSolveInterval::Seconds(60.0),
        combine: GainSolveCombine::default(),
        refant: RefAntSelector::AntennaId(0),
        prior_calibration_tables: Vec::new(),
        parang: false,
        model_source: GainSolveModelSource::PointSource,
        normalize_average_amplitude: false,
        min_snr: 3.0,
        min_baselines_per_antenna: 4,
        smodel: [1.0, 0.0, 0.0, 0.0],
    }
}

/// The traversal the solve binds: every partition, channel and role (time
/// centroid, interval and scan included), 60 s bins per field and scan,
/// `times` whole times per batch.
fn access(observation: &dyn Observation, times: usize) -> AccessRequest {
    let request = AccessRequest {
        partitions: observation.catalogue().partition_ids(),
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels: 0..8,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::TimeBins {
            width_s: 60.0,
            combine_fields: false,
            combine_scans: false,
        },
        batch_bytes: usize::MAX,
    };
    let estimate = observation.estimate(&request).expect("estimate");
    let partition = &estimate.partitions[0];
    AccessRequest {
        batch_bytes: partition.held_bytes_per_time() * times,
        ..request
    }
}

/// `rows`, renumbered from their observation's antennas and fields to the
/// production MeasurementSet's through the catalogue binding.
fn renumbered(rows: Vec<SolutionRow>, binding: &Binding) -> Vec<SolutionRow> {
    let mut rows: Vec<SolutionRow> = rows
        .into_iter()
        .map(|mut row| {
            row.antenna_id = binding.antennas[row.antenna_id as usize].0 as i32;
            row.refant_id = binding.antennas[row.refant_id as usize].0 as i32;
            row.field_id = binding.fields[row.field_id as usize].0 as i32;
            row
        })
        .collect();
    rows.sort_by(|a, b| {
        (a.time_seconds, a.antenna_id)
            .partial_cmp(&(b.time_seconds, b.antenna_id))
            .expect("finite")
    });
    rows
}

/// The largest differences between two solutions, row by row. Every
/// difference is NaN-safe: a NaN in either solution is the largest
/// difference, never a silent zero.
#[derive(Debug, Default, PartialEq)]
struct Agreement {
    rows: usize,
    gain: f32,
    snr_relative: f32,
    weight_relative: f32,
    time_s: f64,
    flags_equal: bool,
}

/// The larger difference, a NaN counting as the largest.
fn worse<T: num_traits_free::Float>(a: T, b: T) -> T {
    if a.is_nan() || b.is_nan() {
        T::INFINITY
    } else if b > a {
        b
    } else {
        a
    }
}

/// The two float widths the comparison uses, without a numeric-traits
/// dependency.
mod num_traits_free {
    pub(super) trait Float: Copy + PartialOrd {
        const INFINITY: Self;
        fn is_nan(self) -> bool;
    }
    impl Float for f32 {
        const INFINITY: Self = f32::INFINITY;
        fn is_nan(self) -> bool {
            f32::is_nan(self)
        }
    }
    impl Float for f64 {
        const INFINITY: Self = f64::INFINITY;
        fn is_nan(self) -> bool {
            f64::is_nan(self)
        }
    }
}

/// `|x - y| / |x|`, infinite unless both are finite and comparable.
fn relative(x: f32, y: f32) -> f32 {
    if !(x.is_finite() && y.is_finite()) {
        f32::INFINITY
    } else if x == y {
        0.0
    } else {
        ((x - y) / x.abs().max(f32::MIN_POSITIVE)).abs()
    }
}

/// The agreement of `actual` with `expected`, which must have the same
/// rows, identities, scans, intervals and solution shapes.
///
/// # Panics
///
/// If the row counts, a row's identities or scan, or a solution's length
/// differ.
fn agreement(expected: &[SolutionRow], actual: &[SolutionRow]) -> Agreement {
    assert_eq!(expected.len(), actual.len(), "solution rows");
    let mut worst = Agreement {
        rows: expected.len(),
        flags_equal: true,
        ..Agreement::default()
    };
    for (a, b) in expected.iter().zip(actual) {
        let identity = |row: &SolutionRow| {
            (
                row.antenna_id,
                row.field_id,
                row.spw_id,
                row.refant_id,
                row.observation_id,
                row.scan_number,
            )
        };
        assert_eq!(identity(a), identity(b), "row identities and scan");
        assert_eq!(
            (a.gains.len(), a.snrs.len(), a.weights.len(), a.flags.len()),
            (b.gains.len(), b.snrs.len(), b.weights.len(), b.flags.len()),
            "solution shapes"
        );
        worst.time_s = worse(worst.time_s, (a.time_seconds - b.time_seconds).abs());
        worst.flags_equal &= a.flags == b.flags;
        for (x, y) in a.gains.iter().zip(&b.gains) {
            let difference = if x.is_finite() && y.is_finite() {
                (x - y).norm()
            } else {
                f32::INFINITY
            };
            worst.gain = worse(worst.gain, difference);
        }
        for (x, y) in a.snrs.iter().zip(&b.snrs) {
            worst.snr_relative = worse(worst.snr_relative, relative(*x, *y));
        }
        for (x, y) in a.weights.iter().zip(&b.weights) {
            worst.weight_relative = worse(worst.weight_relative, relative(*x, *y));
        }
    }
    worst
}

/// The trial's two observations, opened through the one store-choosing
/// entry point with group closure as `closure` says, the production
/// MeasurementSet, and each observation's rebinding to it.
struct Stores {
    observations: Vec<(&'static str, Box<dyn Observation>)>,
    ms: MeasurementSet,
}

fn open(root: &Path, closure: GroupClosure) -> Stores {
    let v2 = fixture(root, "ms_trial.ms");
    let options = OpenOptions {
        group_closure: closure,
        ..OpenOptions::default()
    };
    let open = |path: PathBuf| casa_ms::observation_io::open(path, &options).expect("open");
    Stores {
        observations: vec![
            ("v2", open(v2.clone())),
            ("v4", open(fixture(root, "ms_trial.ps.zarr"))),
        ],
        ms: MeasurementSet::open(&v2).expect("ms"),
    }
}

impl Stores {
    /// The binding of `observation` to the production numbering: the v2
    /// trial observation's catalogue numbers like the MeasurementSet.
    /// Identity is checked content (station and position, phase centre),
    /// never names or list order: XRADIO renames antennas and fields, and
    /// an unmatched or ambiguous entry is a failure.
    fn binding(&self, observation: &dyn Observation) -> Binding {
        observation
            .catalogue()
            .bind_to(self.observations[0].1.catalogue(), &BindingTolerance::EXACT)
            .expect("binds by content")
    }
}

/// The production solve on the MSv2 writes its caltable, and its rows are
/// what the batch-fed solves are compared with.
fn production(ms: &MeasurementSet, root: &Path) -> Vec<SolutionRow> {
    let request = gain_request(root.join("production.G"));
    let report = solve_gain(ms, &request).expect("production gaincal");
    assert_eq!(report.solution_row_count, 10, "5 antennas x 2 intervals");
    solve_gain_rows(ms, &request).expect("production rows").rows
}

/// The production refant (antenna 0 of the MeasurementSet) in `binding`'s
/// observation numbering.
fn refant(binding: &Binding) -> i32 {
    binding
        .antennas
        .iter()
        .position(|id| id.0 == 0)
        .expect("refant binds") as i32
}

/// Sequentially fed from either store, at any batch size and under either
/// closure announcement, the solve reproduces the production `gaincal` on
/// the MSv2: the same accumulation in the same order, so the same
/// solutions, with the solutions' times, intervals and scans from the
/// batches' own time centroids, intervals and scans. A 60 s group spans up
/// to sixty of the smallest batches; only the live groups' sufficient
/// statistics are kept, never their visibilities.
#[test]
fn batch_fed_solves_equal_the_production_gaincal() {
    let root = tempfile::tempdir().expect("root");
    let request = gain_request(root.path().join("unused.G"));
    let expected = {
        let stores = open(root.path(), GroupClosure::AtDelivery);
        let expected = production(&stores.ms, root.path());
        // The solver iterates hash maps in a per-run order, so production
        // agrees with itself only to rounding in its SNR and weights.
        let again = solve_gain_rows(&stores.ms, &request)
            .expect("production rows")
            .rows;
        println!(
            "production against itself: {:?}",
            agreement(&expected, &again)
        );
        expected
    };
    for closure in [GroupClosure::AtDelivery, GroupClosure::Deferred] {
        let stores = open(root.path(), closure);
        for (store, observation) in &stores.observations {
            let binding = stores.binding(&**observation);
            for times in [1, 3, 4, 7, 12] {
                let (rows, trace) = solve_sequential(
                    &**observation,
                    &request,
                    &access(&**observation, times),
                    refant(&binding),
                );
                let agreement = agreement(&expected, &renumbered(rows, &binding));
                println!(
                    "{store} sequential {closure:?} {times} times/batch: {agreement:?}; {trace:?}"
                );
                assert!(agreement.flags_equal, "{store}");
                assert!(
                    agreement.gain < 1e-5
                        && agreement.snr_relative < 1e-4
                        && agreement.weight_relative < 1e-4,
                    "{store} {agreement:?}"
                );
                // The solution time is a weighted mean over baselines summed
                // in hash-map order, production's too: it agrees to a few
                // ulps of a time in MJD seconds (about 1 µs each).
                let ulp = expected[0].time_seconds.abs() * f64::EPSILON;
                assert!(agreement.time_s <= 4.0 * ulp, "{store} {agreement:?}");
                assert!(trace.max_live_groups <= 2);
                match closure {
                    GroupClosure::AtDelivery => {
                        assert_eq!((trace.closed_by_fragment, trace.closed_by_end), (2, 0))
                    }
                    GroupClosure::Deferred => {
                        assert_eq!((trace.closed_by_fragment, trace.closed_by_end), (0, 2))
                    }
                }
            }
        }
    }
}

/// Fragments accumulated as concurrent work units, the first held back:
/// its group's input completes with work pending, and the group is solved
/// only after that work joins. Merged partial sums agree with the
/// production solve to rounding.
#[test]
fn concurrent_fragments_close_only_after_their_work_joins() {
    let root = tempfile::tempdir().expect("root");
    let request = gain_request(root.path().join("unused.G"));
    let expected = production(&open(root.path(), GroupClosure::AtDelivery).ms, root.path());
    for closure in [GroupClosure::AtDelivery, GroupClosure::Deferred] {
        let stores = open(root.path(), closure);
        for (store, observation) in &stores.observations {
            let binding = stores.binding(&**observation);
            let (rows, trace) = solve_concurrent(
                &**observation,
                &request,
                &access(&**observation, 3),
                refant(&binding),
            );
            let agreement = agreement(&expected, &renumbered(rows, &binding));
            println!("{store} concurrent {closure:?} 3 times/batch: {agreement:?}; {trace:?}");
            assert!(agreement.flags_equal);
            assert!(
                agreement.gain < 1e-4
                    && agreement.snr_relative < 1e-3
                    && agreement.weight_relative < 1e-3,
                "{store} {agreement:?}"
            );
            assert!(
                trace.closed_with_pending >= 1,
                "{store}: a closure waited for work: {trace:?}"
            );
        }
    }
}

/// The comparison fails rather than passes on what it cannot compare: a
/// NaN gain or weight is the worst difference, and differing rows,
/// identities or scans are refused.
#[test]
fn agreement_never_passes_a_nan_or_a_mismatch() {
    let root = tempfile::tempdir().expect("root");
    let expected = production(&open(root.path(), GroupClosure::AtDelivery).ms, root.path());
    assert_eq!(agreement(&expected, &expected).gain, 0.0);
    let mut poisoned = expected.clone();
    poisoned[3].gains[1] = num_complex::Complex32::new(f32::NAN, 0.0);
    assert_eq!(agreement(&expected, &poisoned).gain, f32::INFINITY);
    let mut poisoned = expected.clone();
    poisoned[0].weights[0] = f32::NAN;
    assert_eq!(
        agreement(&expected, &poisoned).weight_relative,
        f32::INFINITY
    );
    let mut rescanned = expected.clone();
    rescanned[0].scan_number += 1;
    assert!(std::panic::catch_unwind(|| agreement(&expected, &rescanned)).is_err());
    assert!(std::panic::catch_unwind(|| agreement(&expected, &expected[1..])).is_err());
}

/// The reusable consumer module sees only the shared interface.
#[test]
fn the_feed_imports_no_store_type() {
    let feed = include_str!("batch_trial/feed.rs");
    for store in ["casa_ms", "Msv2", "Msv4", "MeasurementSet"] {
        assert!(!feed.contains(store), "feed.rs names {store}");
    }
}

/// The solutions recover the known gains the fixture applied, referred to
/// antenna 0: a science check on the fixture, loose because the model omits
/// the 0.1 Jy offset source.
#[test]
fn solutions_recover_the_fixture_gains() {
    let root = tempfile::tempdir().expect("root");
    let expected = production(&open(root.path(), GroupClosure::AtDelivery).ms, root.path());
    let manifest: serde_json::Value = serde_json::from_reader(
        File::open(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../casa-ms/tests/fixtures/ms_trial/manifest.json"),
        )
        .expect("manifest"),
    )
    .expect("json");
    let gains = &manifest["corruption"]["true_gains"];
    let truth = |interval: usize, antenna: usize, receptor: usize| {
        num_complex::Complex32::new(
            gains["re"][interval][antenna][receptor]
                .as_f64()
                .expect("re") as f32,
            gains["im"][interval][antenna][receptor]
                .as_f64()
                .expect("im") as f32,
        )
    };
    let start = expected
        .iter()
        .map(|row| row.time_seconds)
        .fold(f64::INFINITY, f64::min);
    let (mut phase, mut amplitude) = (0.0_f32, 0.0_f32);
    for row in &expected {
        let interval = usize::from(row.time_seconds > start + 1.0);
        let antenna = row.antenna_id as usize;
        for receptor in 0..2 {
            let reference = truth(interval, 0, receptor);
            let true_gain =
                truth(interval, antenna, receptor) * reference.conj() / reference.norm();
            let solved = row.gains[receptor];
            phase = worse(phase, (solved / true_gain).arg().abs());
            amplitude = worse(amplitude, (solved.norm() / true_gain.norm() - 1.0).abs());
        }
    }
    println!("worst phase error {phase:.4} rad, worst amplitude error {amplitude:.4}");
    assert!(phase < 0.2 && amplitude < 0.2);
}
