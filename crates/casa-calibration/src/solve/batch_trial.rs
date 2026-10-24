// SPDX-License-Identifier: LGPL-3.0-or-later
//! MSv2/MSv4 trial, example B (`docs/measurement-set/trial/README.md`): the
//! production gain accumulation and solver fed from
//! `casa_observation::VisibilityBatch`es of both stores, with 60 s solution
//! groups that cross batch boundaries.
//!
//! `cargo test -p casa-calibration --lib batch_trial -- --nocapture` prints
//! the measurements the evidence records.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use casa_ms::ms::MeasurementSet;
use casa_ms::observation_io::{Msv2Observation, Msv4Observation};
use casa_ms::selection::MsSelection;
use casa_observation::{
    AccessRequest, BatchBuffers, Closure, Fill, GroupKey, Grouping, IoCounters, Layout,
    ObservationError, OwnedEvent, PartitionId, Precision, ReadEvent, Realization, Roles,
    VisibilityBatch, VisibilityCursor, WeightView,
};
use ndarray::{Axis, s};

use super::grouping::{SolveAccumulator, SolveSampleRow, SolveSamples, SolveWeights};
use super::kernel::SolutionRow;
use super::{
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
    GainType, RefAntSelector, apply_flex_refant, solve_closed_group, solve_gain, solve_gain_rows,
};

/// The fixtures' integration time. The batch carries neither `INTERVAL`
/// nor `EXPOSURE` yet, so the trial supplies it (finding: the contract's
/// time auxiliaries are not in the trial batch).
const INTERVAL_S: f64 = 10.0;

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

/// The traversal the solve binds: every channel and role, 60 s bins.
fn access(max_samples: usize, closure: Closure) -> AccessRequest {
    AccessRequest {
        partition: PartitionId(0),
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels: 0..8,
        layout: Layout::SampleMajor,
        precision: Precision::Complex32 {
            allow_narrowing: false,
        },
        grouping: Grouping::TimeBins {
            width_s: 60.0,
            combine_fields: false,
        },
        closure,
        max_samples,
    }
}

enum Observation {
    V2(Box<Msv2Observation>),
    V4(Msv4Observation),
}

enum Cursor<'o> {
    V2(casa_ms::observation_io::Msv2Cursor<'o>),
    V4(casa_ms::observation_io::Msv4Cursor<'o>),
}

impl VisibilityCursor for Cursor<'_> {
    fn realization(&self) -> &Realization {
        match self {
            Self::V2(cursor) => cursor.realization(),
            Self::V4(cursor) => cursor.realization(),
        }
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<Option<Fill>, ObservationError> {
        match self {
            Self::V2(cursor) => cursor.fill(buffers),
            Self::V4(cursor) => cursor.fill(buffers),
        }
    }

    fn counters(&self) -> IoCounters {
        match self {
            Self::V2(cursor) => cursor.counters(),
            Self::V4(cursor) => cursor.counters(),
        }
    }
}

impl Observation {
    fn bind(&self, request: &AccessRequest) -> Cursor<'_> {
        match self {
            Self::V2(observation) => Cursor::V2(observation.bind_read(request).expect("bind")),
            Self::V4(observation) => Cursor::V4(observation.bind_read(request).expect("bind")),
        }
    }

    fn antennas(&self) -> BTreeSet<i32> {
        let catalogue = match self {
            Self::V2(observation) => observation.catalogue(),
            Self::V4(observation) => observation.catalogue(),
        };
        (0..catalogue.antennas.len() as i32).collect()
    }
}

/// Accumulate `batch`'s samples `samples` into `group`: views of the batch,
/// transposed to the `[correlation, channel]` order the accumulator reads,
/// nothing copied.
fn observe(
    group: &mut SolveAccumulator,
    batch: &VisibilityBatch<'_>,
    samples: Range<usize>,
    request: &GainSolveRequest,
) {
    let values = batch
        .group
        .values
        .expect("values")
        .as_complex32()
        .expect("c64");
    let flags = batch.group.flags.expect("flags");
    let weights = batch.group.weights.expect("weights");
    let codes: Vec<i32> = batch
        .correlations
        .iter()
        .map(|c| c.casacore_code())
        .collect();
    let channels: Vec<usize> = (0..batch.channel_count()).collect();
    let origin_s = batch.topology.time().origin().day() * 86_400.0;
    for sample in samples {
        let coordinate = batch.topology.sample(sample);
        if coordinate.baseline.is_auto() {
            continue;
        }
        let time_seconds = origin_s + coordinate.time_offset_s;
        group
            .observe_samples(
                &SolveSamples {
                    row: SolveSampleRow {
                        data_desc_id: 0,
                        antenna1: coordinate.baseline.antenna1.0 as i32,
                        antenna2: coordinate.baseline.antenna2.0 as i32,
                        time_seconds,
                        // The batch has no TIME_CENTROID; the fixtures' equals TIME.
                        time_centroid_seconds: time_seconds,
                        interval_seconds: INTERVAL_S,
                        // Nor a scan: the solution rows' scan is not compared.
                        scan_number: 0,
                    },
                    data: values.slice(s![sample, .., ..]).reversed_axes(),
                    flags: flags.slice(s![sample, .., ..]).reversed_axes(),
                    weights: match weights {
                        WeightView::PerCorrelation(weights) => {
                            SolveWeights::Row(weights.index_axis_move(Axis(0), sample))
                        }
                        WeightView::Spectral(weights) => SolveWeights::Spectrum(
                            weights.slice_move(s![sample, .., ..]).reversed_axes(),
                        ),
                    },
                    model: None,
                    correlation_types: &codes,
                    selected_channels: &channels,
                },
                request.gain_type,
                request.model_source,
                request.smodel[0],
            )
            .expect("observe");
    }
}

/// What a batch-fed solve measured.
#[derive(Debug, Default)]
struct SolveTrace {
    batches: usize,
    fragments: usize,
    /// Groups live at once, and the most accumulator entries they held.
    max_live_groups: usize,
    max_state_entries: usize,
    /// Groups closed by a fragment marker, by `GroupEnd`, and closed with
    /// fragment work still outstanding.
    closed_by_fragment: usize,
    closed_by_end: usize,
    closed_with_pending: usize,
    buffer_bytes: usize,
}

/// Solve one closed group into the rows.
fn finish(
    group: SolveAccumulator,
    available: &BTreeSet<i32>,
    request: &GainSolveRequest,
    rows: &mut Vec<SolutionRow>,
) {
    let mut group = group;
    group.finalize_for_solve(request.solve_mode);
    rows.extend(solve_closed_group(group, available, request, 0).expect("solve"));
}

/// Sequential realization: one batch at a time, fragments accumulated in
/// delivery order, each group solved when its closure arrives.
fn solve_sequential(
    observation: &Observation,
    request: &GainSolveRequest,
    access: &AccessRequest,
) -> (Vec<SolutionRow>, SolveTrace) {
    let available = observation.antennas();
    let mut cursor = observation.bind(access);
    let mut buffers = BatchBuffers::default();
    let mut live = BTreeMap::<GroupKey, SolveAccumulator>::new();
    let mut rows = Vec::new();
    let mut trace = SolveTrace::default();
    while let Some(event) = cursor.next(&mut buffers).expect("read") {
        let closed: Vec<GroupKey> = match event {
            ReadEvent::GroupEnd(key) => {
                trace.closed_by_end += 1;
                vec![key]
            }
            ReadEvent::Batch(batch) => {
                trace.batches += 1;
                let mut closed = Vec::new();
                for fragment in batch.fragments {
                    trace.fragments += 1;
                    let group = live
                        .entry(fragment.key)
                        .or_insert_with(|| SolveAccumulator::new(0, 0, 0));
                    observe(group, &batch, fragment.samples.clone(), request);
                    if fragment.position.closes() {
                        trace.closed_by_fragment += 1;
                        closed.push(fragment.key);
                    }
                }
                trace.max_live_groups = trace.max_live_groups.max(live.len());
                trace.max_state_entries = trace
                    .max_state_entries
                    .max(live.values().map(SolveAccumulator::state_entries).sum());
                closed
            }
        };
        for key in closed {
            finish(
                live.remove(&key).expect("live group"),
                &available,
                request,
                &mut rows,
            );
        }
    }
    trace.buffer_bytes = buffers.capacity_bytes();
    assert!(live.is_empty(), "every group closed");
    apply_flex_refant(&mut rows, 0);
    (rows, trace)
}

/// Batch buffers the concurrent realization circulates.
const CONCURRENT_POOL: usize = 4;

/// Concurrent realization: each fragment is a work unit on its own thread,
/// accumulating a partial group from a shared batch (`Arc<FilledBatch>`).
/// The first is held back, so its group's closure arrives while its work
/// is pending: the group is solved only when every fragment's work has
/// joined. A batch's buffers return to the pool when its last fragment's
/// work lets go of them.
fn solve_concurrent(
    observation: &Observation,
    request: &GainSolveRequest,
    access: &AccessRequest,
) -> (Vec<SolutionRow>, SolveTrace) {
    let available = observation.antennas();
    let mut cursor = observation.bind(access);
    let mut trace = SolveTrace::default();
    let mut rows = Vec::new();
    let mut partial = BTreeMap::<GroupKey, SolveAccumulator>::new();
    let mut pending = BTreeMap::<GroupKey, usize>::new();
    let mut closed = BTreeSet::<GroupKey>::new();
    let (done_tx, done_rx) = mpsc::channel::<(GroupKey, SolveAccumulator)>();
    let (free_tx, free_rx) = mpsc::channel::<BatchBuffers>();
    // Join finished work; solve every closed group with none pending.
    let settle = |partial: &mut BTreeMap<GroupKey, SolveAccumulator>,
                  pending: &mut BTreeMap<GroupKey, usize>,
                  closed: &mut BTreeSet<GroupKey>,
                  rows: &mut Vec<SolutionRow>| {
        while let Ok((key, group)) = done_rx.try_recv() {
            *pending.get_mut(&key).expect("pending") -= 1;
            match partial.get_mut(&key) {
                Some(total) => total.merge(group),
                None => {
                    partial.insert(key, group);
                }
            }
        }
        let ready: Vec<GroupKey> = closed
            .iter()
            .filter(|key| pending.get(key).copied().unwrap_or(0) == 0)
            .copied()
            .collect();
        for key in ready {
            closed.remove(&key);
            pending.remove(&key);
            finish(
                partial.remove(&key).expect("group"),
                &available,
                request,
                rows,
            );
        }
    };
    std::thread::scope(|scope| {
        let mut allocated = 0;
        let mut spare: Option<BatchBuffers> = None;
        let mut first = true;
        loop {
            let buffers = match spare.take().or_else(|| free_rx.try_recv().ok()) {
                Some(buffers) => buffers,
                None if allocated < CONCURRENT_POOL => {
                    allocated += 1;
                    BatchBuffers::default()
                }
                None => free_rx.recv().expect("a buffer comes back"),
            };
            let Some(event) = cursor.next_owned(buffers).expect("read") else {
                break;
            };
            let filled = match event {
                OwnedEvent::GroupEnd(key, returned) => {
                    spare = Some(returned);
                    trace.closed_by_end += 1;
                    if pending.get(&key).copied().unwrap_or(0) > 0 {
                        trace.closed_with_pending += 1;
                    }
                    closed.insert(key);
                    settle(&mut partial, &mut pending, &mut closed, &mut rows);
                    continue;
                }
                OwnedEvent::Batch(filled) => Arc::new(filled),
            };
            trace.batches += 1;
            trace.buffer_bytes = trace.buffer_bytes.max(filled.capacity_bytes());
            for fragment in filled.batch().fragments.to_vec() {
                trace.fragments += 1;
                *pending.entry(fragment.key).or_default() += 1;
                let (filled, done_tx, free_tx) = (filled.clone(), done_tx.clone(), free_tx.clone());
                let delay = std::mem::take(&mut first);
                scope.spawn(move || {
                    if delay {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    let mut group = SolveAccumulator::new(0, 0, 0);
                    observe(
                        &mut group,
                        &filled.batch(),
                        fragment.samples.clone(),
                        request,
                    );
                    let _ = done_tx.send((fragment.key, group));
                    if let Some(filled) = Arc::into_inner(filled) {
                        let _ = free_tx.send(filled.into_buffers());
                    }
                });
                if fragment.position.closes() {
                    trace.closed_by_fragment += 1;
                    if pending[&fragment.key] > 1 {
                        trace.closed_with_pending += 1;
                    }
                    closed.insert(fragment.key);
                }
            }
            trace.max_live_groups = trace.max_live_groups.max(pending.len());
            if let Some(filled) = Arc::into_inner(filled) {
                spare = Some(filled.into_buffers());
            }
            settle(&mut partial, &mut pending, &mut closed, &mut rows);
        }
        while !pending.is_empty() {
            std::thread::sleep(Duration::from_millis(1));
            settle(&mut partial, &mut pending, &mut closed, &mut rows);
        }
    });
    rows.sort_by(|a, b| {
        (a.time_seconds, a.antenna_id)
            .partial_cmp(&(b.time_seconds, b.antenna_id))
            .expect("finite")
    });
    apply_flex_refant(&mut rows, 0);
    (rows, trace)
}

/// The largest differences between two solutions, row by row.
#[derive(Debug, Default, PartialEq)]
struct Agreement {
    rows: usize,
    gain: f32,
    snr_relative: f32,
    weight_relative: f32,
    time_s: f64,
    flags_equal: bool,
}

fn agreement(expected: &[SolutionRow], actual: &[SolutionRow]) -> Agreement {
    assert_eq!(expected.len(), actual.len(), "solution rows");
    let mut worst = Agreement {
        rows: expected.len(),
        flags_equal: true,
        ..Agreement::default()
    };
    for (a, b) in expected.iter().zip(actual) {
        assert_eq!(
            (
                a.antenna_id,
                a.field_id,
                a.spw_id,
                a.refant_id,
                a.observation_id
            ),
            (
                b.antenna_id,
                b.field_id,
                b.spw_id,
                b.refant_id,
                b.observation_id
            )
        );
        assert_eq!(a.interval_seconds, b.interval_seconds);
        worst.time_s = worst.time_s.max((a.time_seconds - b.time_seconds).abs());
        worst.flags_equal &= a.flags == b.flags;
        for (x, y) in a.gains.iter().zip(&b.gains) {
            worst.gain = worst.gain.max((x - y).norm());
        }
        let relative = |x: f32, y: f32| {
            if x == y {
                0.0
            } else {
                ((x - y) / x.abs().max(f32::MIN_POSITIVE)).abs()
            }
        };
        for (x, y) in a.snrs.iter().zip(&b.snrs) {
            worst.snr_relative = worst.snr_relative.max(relative(*x, *y));
        }
        for (x, y) in a.weights.iter().zip(&b.weights) {
            worst.weight_relative = worst.weight_relative.max(relative(*x, *y));
        }
    }
    worst
}

fn open(root: &Path) -> (Observation, Observation, MeasurementSet) {
    let v2 = fixture(root, "ms_trial.ms");
    (
        Observation::V2(Box::new(Msv2Observation::open(&v2).expect("v2"))),
        Observation::V4(Msv4Observation::open(fixture(root, "ms_trial.ps.zarr")).expect("v4")),
        MeasurementSet::open(&v2).expect("ms"),
    )
}

/// The production solve on the MSv2 writes its caltable, and its rows are
/// what the batch-fed solves are compared with.
fn production(ms: &MeasurementSet, root: &Path) -> Vec<SolutionRow> {
    let request = gain_request(root.join("production.G"));
    let report = solve_gain(ms, &request).expect("production gaincal");
    assert_eq!(report.solution_row_count, 10, "5 antennas x 2 intervals");
    solve_gain_rows(ms, &request).expect("production rows").0
}

/// Sequentially fed from either store, at any batch size and under either
/// closure announcement, the solve reproduces the production `gaincal` on
/// the MSv2: the same accumulation in the same order, so the same
/// solutions. A 60 s group spans up to six of the smallest batches; only
/// the live groups' sufficient statistics are kept, never their
/// visibilities.
#[test]
fn batch_fed_solves_equal_the_production_gaincal() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4, ms) = open(root.path());
    let expected = production(&ms, root.path());
    let request = gain_request(root.path().join("unused.G"));
    // The solver iterates hash maps in a per-run order, so production
    // agrees with itself only to rounding in its SNR and weights.
    let again = solve_gain_rows(&ms, &request).expect("production rows").0;
    println!(
        "production against itself: {:?}",
        agreement(&expected, &again)
    );
    for (store, observation, sizes) in [
        ("v2", &v2, [1, 7, 25, 60, 120]),
        ("v4", &v4, [10, 30, 40, 70, 120]),
    ] {
        for closure in [Closure::AtDelivery, Closure::Deferred] {
            for max_samples in sizes {
                let (rows, trace) =
                    solve_sequential(observation, &request, &access(max_samples, closure));
                let agreement = agreement(&expected, &rows);
                println!(
                    "{store} sequential {closure:?} {max_samples}/batch: {agreement:?}; {trace:?}"
                );
                assert!(agreement.flags_equal, "{store}");
                assert!(
                    agreement.gain < 1e-5 && agreement.snr_relative < 1e-4,
                    "{store} {agreement:?}"
                );
                assert!(agreement.time_s < 1e-6);
                assert!(trace.max_live_groups <= 2);
                match closure {
                    Closure::AtDelivery => {
                        assert_eq!((trace.closed_by_fragment, trace.closed_by_end), (2, 0))
                    }
                    Closure::Deferred => {
                        assert_eq!((trace.closed_by_fragment, trace.closed_by_end), (0, 2))
                    }
                }
            }
        }
    }
}

/// Fragments accumulated as concurrent work units, the first held back:
/// its group's closure arrives with work pending, and the group is solved
/// only after that work joins. Merged partial sums agree with the
/// production solve to rounding.
#[test]
fn concurrent_fragments_close_only_after_their_work_joins() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4, ms) = open(root.path());
    let expected = production(&ms, root.path());
    let request = gain_request(root.path().join("unused.G"));
    for (store, observation, max_samples) in [("v2", &v2, 25), ("v4", &v4, 30)] {
        for closure in [Closure::AtDelivery, Closure::Deferred] {
            let (rows, trace) =
                solve_concurrent(observation, &request, &access(max_samples, closure));
            let agreement = agreement(&expected, &rows);
            println!(
                "{store} concurrent {closure:?} {max_samples}/batch: {agreement:?}; {trace:?}"
            );
            assert!(agreement.flags_equal);
            assert!(
                agreement.gain < 1e-4 && agreement.snr_relative < 1e-3,
                "{store} {agreement:?}"
            );
            assert!(
                trace.closed_with_pending >= 1,
                "{store}: a closure waited for work: {trace:?}"
            );
        }
    }
}

/// The solutions recover the known gains the fixture applied, referred to
/// antenna 0: a science check on the fixture, loose because the model omits
/// the 0.1 Jy offset source.
#[test]
fn solutions_recover_the_fixture_gains() {
    let root = tempfile::tempdir().expect("root");
    let (_, _, ms) = open(root.path());
    let expected = production(&ms, root.path());
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
            phase = phase.max((solved / true_gain).arg().abs());
            amplitude = amplitude.max((solved.norm() / true_gain.norm() - 1.0).abs());
        }
    }
    println!("worst phase error {phase:.4} rad, worst amplitude error {amplitude:.4}");
    assert!(phase < 0.2 && amplitude < 0.2);
}
