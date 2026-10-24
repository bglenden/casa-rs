// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA solve bookkeeping that the per-mode tests in `gain_solve.rs` and
//! `bandpass_solve.rs` do not pin: which solution intervals a solve keeps
//! (`Calibrater::genericGatherAndSolve` keeps an interval when its solve
//! succeeds, before the SNR threshold), the TIME and INTERVAL CASA writes
//! (`SolvableVisCal::keepNCT`), flex reference-antenna selection across an
//! interval that lacks the reference (`SolvableVisJones::applyRefAnt`), and
//! the exclusion of `FLAG_ROW` rows across gain types, modes, intervals and
//! prior tables (`VisEquation::divideCorrByModel`).

mod common;

use std::path::{Path, PathBuf};

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, BandpassSolveCombine,
    BandpassSolveError, BandpassSolveReport, BandpassSolveRequest, BandpassType,
    CalibrationDataset, CalibrationError, CalibrationSolveRequest, CalibrationSolveResult,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveReport,
    GainSolveRequest, GainType, RefAntSelector, execute_apply_from_path, solve_calibration,
};
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use common::{SyntheticGainFixtureKind, SyntheticGainSolutionRow, SyntheticGainTimeCluster};
use ndarray::ArrayD;
use tempfile::TempDir;

/// MAIN rows per integration in the three-antenna fixtures, in the order
/// baselines 0-1, 0-2, 1-2.
const BASELINES: [(i32, i32); 3] = [(0, 1), (0, 2), (1, 2)];

fn run_gain_solve(
    path: &Path,
    request: &GainSolveRequest,
) -> Result<GainSolveReport, CalibrationError> {
    match solve_calibration(
        CalibrationDataset::path(path),
        CalibrationSolveRequest::Gain(request.clone()),
    )? {
        CalibrationSolveResult::Gain(report) => Ok(report),
        CalibrationSolveResult::Bandpass(_) => unreachable!("gain request result"),
    }
}

fn run_bandpass_solve(
    path: &Path,
    request: &BandpassSolveRequest,
) -> Result<BandpassSolveReport, CalibrationError> {
    match solve_calibration(
        CalibrationDataset::path(path),
        CalibrationSolveRequest::Bandpass(request.clone()),
    )? {
        CalibrationSolveResult::Bandpass(report) => Ok(report),
        CalibrationSolveResult::Gain(_) => unreachable!("bandpass request result"),
    }
}

fn gain_request(
    output_table: PathBuf,
    gain_type: GainType,
    solve_mode: GainSolveMode,
    solve_interval: GainSolveInterval,
) -> GainSolveRequest {
    GainSolveRequest {
        selection: MsSelection::new(),
        output_table,
        gain_type,
        solve_mode,
        solve_interval,
        combine: GainSolveCombine::default(),
        refant: RefAntSelector::AntennaId(0),
        prior_calibration_tables: Vec::new(),
        parang: false,
        model_source: GainSolveModelSource::PointSource,
        normalize_average_amplitude: false,
        min_snr: 0.0,
        min_baselines_per_antenna: 0,
        smodel: [1.0, 0.0, 0.0, 0.0],
    }
}

fn bandpass_request(
    output_table: PathBuf,
    prior_calibration_tables: Vec<ApplyCalibrationTableSpec>,
) -> BandpassSolveRequest {
    BandpassSolveRequest {
        selection: MsSelection::new().field(&[0]).spw(&[0]),
        output_table,
        refant: RefAntSelector::AntennaId(0),
        prior_calibration_tables,
        parang: false,
        combine: BandpassSolveCombine::default(),
        band_type: BandpassType::B,
        normalize_average_amplitude: false,
        amplitude_degree: 3,
        phase_degree: 3,
        smodel: [1.0, 0.0, 0.0, 0.0],
    }
}

/// A prior gain table pre-applied the way CASA's gaincal and bandpass tasks
/// pre-apply `gaintable`, with `calwt=True`.
fn calwt_prior(path: &Path, rows: &[SyntheticGainSolutionRow]) -> ApplyCalibrationTableSpec {
    let table = common::create_apply_gain_caltable(path, &["TARGET0"], rows);
    let mut spec = ApplyCalibrationTableSpec::new(table);
    spec.calwt = true;
    spec
}

fn prior_row(
    time_seconds: f64,
    antenna_id: i32,
    gains: [Complex32; 2],
) -> SyntheticGainSolutionRow {
    SyntheticGainSolutionRow {
        time_seconds,
        field_id: 0,
        spectral_window_id: 0,
        antenna_id,
        gains: gains.to_vec(),
        flags: vec![false, false],
    }
}

fn unit_prior_rows(time_seconds: f64) -> Vec<SyntheticGainSolutionRow> {
    (0..3)
        .map(|antenna_id| prior_row(time_seconds, antenna_id, [Complex32::new(1.0, 0.0); 2]))
        .collect()
}

/// One three-antenna fixture integration with the same per-antenna gain on
/// both polarizations.
fn cluster(time_seconds: f64, gains: [Complex32; 3]) -> SyntheticGainTimeCluster {
    SyntheticGainTimeCluster {
        time_seconds,
        scan_number: 1,
        gains: gains.map(|gain| [gain; 2]),
    }
}

/// One caltable row, as the assertions below need it.
#[derive(Debug)]
struct CalRow {
    time: f64,
    interval: f64,
    antenna: i32,
    refant: i32,
    gains: Vec<Complex32>,
    flags: Vec<bool>,
}

fn read_cal_rows(path: &Path) -> Vec<CalRow> {
    let table = Table::open(TableOptions::new(path)).expect("open caltable");
    let scalar = |row, column| {
        table
            .cell_accessor(row, column)
            .and_then(|cell| cell.scalar())
            .expect("caltable scalar cell")
            .clone()
    };
    let array = |row, column| {
        table
            .cell_accessor(row, column)
            .and_then(|cell| cell.array())
            .expect("caltable array cell")
            .clone()
    };
    (0..table.row_count())
        .map(|row| {
            let (ScalarValue::Float64(time), ScalarValue::Float64(interval)) =
                (scalar(row, "TIME"), scalar(row, "INTERVAL"))
            else {
                panic!("TIME and INTERVAL are Float64");
            };
            let (ScalarValue::Int32(antenna), ScalarValue::Int32(refant)) =
                (scalar(row, "ANTENNA1"), scalar(row, "ANTENNA2"))
            else {
                panic!("ANTENNA1 and ANTENNA2 are Int32");
            };
            let (ArrayValue::Complex32(gains), ArrayValue::Bool(flags)) =
                (array(row, "CPARAM"), array(row, "FLAG"))
            else {
                panic!("CPARAM is Complex32 and FLAG is Bool");
            };
            CalRow {
                time,
                interval,
                antenna,
                refant,
                gains: gains.iter().copied().collect(),
                flags: flags.iter().copied().collect(),
            }
        })
        .collect()
}

fn solution_times(rows: &[CalRow]) -> Vec<f64> {
    let mut times = rows.iter().map(|row| row.time).collect::<Vec<_>>();
    times.dedup();
    times
}

/// Rewrite `column` on the MAIN rows `rows` of the MS at `ms_path`.
fn set_main_cells(ms_path: &Path, column: &str, rows: &[usize], value: impl Fn(usize) -> Value) {
    let mut ms = MeasurementSet::open(ms_path).expect("open measurement set");
    let table = ms.main_table_mut();
    for &row in rows {
        table
            .column_accessor_mut(column)
            .and_then(|mut accessor| accessor.set(row, value(row)))
            .expect("write MAIN cell");
    }
    table
        .prepare_write()
        .save_selected_rows(&[column], rows)
        .expect("save MAIN cells");
}

fn flag_cells(value: bool) -> Value {
    Value::Array(ArrayValue::Bool(ArrayD::from_elem(vec![2, 2], value)))
}

fn per_correlation(value: f32) -> Value {
    Value::Array(ArrayValue::Float32(ArrayD::from_elem(vec![2], value)))
}

/// MAIN rows of integration `integration` (0-based) whose baseline includes
/// `antenna`.
fn rows_with_antenna(integration: usize, antenna: i32) -> Vec<usize> {
    BASELINES
        .iter()
        .enumerate()
        .filter(|(_, (a1, a2))| *a1 == antenna || *a2 == antenna)
        .map(|(offset, _)| integration * BASELINES.len() + offset)
        .collect()
}

fn assert_close(actual: f64, expected: f64, what: &str) {
    assert!(
        (actual - expected).abs() <= 1.0e-6,
        "{what}: expected {expected}, got {actual}"
    );
}

fn assert_gain_close(actual: Complex32, expected: Complex32, what: &str) {
    assert!(
        (actual - expected).norm() <= 1.0e-4,
        "{what}: expected {expected}, got {actual}"
    );
}

fn apply(ms_path: &Path, calibration_tables: Vec<ApplyCalibrationTableSpec>) {
    execute_apply_from_path(
        ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables,
        },
    )
    .expect("apply solved tables");
}

// ---------------------------------------------------------------------------
// Which intervals a solve keeps (#903)

#[test]
fn an_interval_with_no_unflagged_data_is_omitted_rather_than_given_a_unit_refant() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(dir.path(), SyntheticGainFixtureKind::G);
    set_main_cells(&ms_path, "FLAG", &[3, 4, 5], |_| flag_cells(true));
    let caltable = dir.path().join("omitted.gcal");

    let report = run_gain_solve(
        &ms_path,
        &gain_request(
            caltable.clone(),
            GainType::G,
            GainSolveMode::Phase,
            GainSolveInterval::Integration,
        ),
    )
    .expect("solve the two integrations that have data");

    assert_eq!(report.solution_row_count, 6);
    let rows = read_cal_rows(&caltable);
    assert_eq!(solution_times(&rows), vec![100.0, 102.0]);
    assert!(rows.iter().all(|row| row.flags.iter().all(|flag| !flag)));
}

#[test]
fn an_interval_whose_antennas_cascade_below_minblperant_is_omitted() {
    // At t=101 baseline 1-2 is flagged: antennas 1 and 2 keep one baseline
    // each, fall below minblperant=2 and are removed, which leaves antenna 0
    // with none. CASA's verifyConstraints then fails the whole interval. The
    // other integrations sit exactly on the boundary (two baselines each).
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(dir.path(), SyntheticGainFixtureKind::G);
    set_main_cells(&ms_path, "FLAG", &[5], |_| flag_cells(true));
    let caltable = dir.path().join("minblperant.gcal");
    let mut request = gain_request(
        caltable.clone(),
        GainType::G,
        GainSolveMode::Phase,
        GainSolveInterval::Integration,
    );
    request.min_baselines_per_antenna = 2;

    let report = run_gain_solve(&ms_path, &request).expect("solve the boundary integrations");

    assert_eq!(report.solution_row_count, 6);
    let rows = read_cal_rows(&caltable);
    assert_eq!(solution_times(&rows), vec![100.0, 102.0]);
    assert!(rows.iter().all(|row| row.flags.iter().all(|flag| !flag)));
}

#[test]
fn an_antenna_whose_prior_solution_is_flagged_is_flagged_inside_a_kept_interval() {
    // CASA flags the data of an antenna whose pre-applied solution is
    // flagged; the interval still solves from the other baselines and keeps
    // a flagged slot for that antenna.
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(dir.path(), SyntheticGainFixtureKind::G);
    let mut prior_rows = unit_prior_rows(100.0);
    prior_rows[2].flags = vec![true, true];
    let prior = calwt_prior(&dir.path().join("prior.gcal"), &prior_rows);
    let caltable = dir.path().join("partial.gcal");
    let mut request = gain_request(
        caltable.clone(),
        GainType::G,
        GainSolveMode::Phase,
        GainSolveInterval::Infinite,
    );
    request.prior_calibration_tables = vec![prior.clone()];

    let report = run_gain_solve(&ms_path, &request).expect("solve without antenna 2");

    assert_eq!(report.solution_row_count, 3);
    for row in read_cal_rows(&caltable) {
        let flagged = row.antenna == 2;
        assert!(
            row.flags.iter().all(|flag| *flag == flagged),
            "antenna {} flags {:?}",
            row.antenna,
            row.flags
        );
    }
    apply(
        &ms_path,
        vec![prior, ApplyCalibrationTableSpec::new(&caltable)],
    );
    let antenna2_rows = (0..3)
        .flat_map(|integration| rows_with_antenna(integration, 2))
        .collect::<Vec<_>>();
    common::assert_corrected_rows_are_unit_model_except(&ms_path, &antenna2_rows);
}

#[test]
fn solutions_flagged_by_minsnr_keep_their_interval() {
    // A solve that succeeds and is then flagged by the SNR threshold is
    // still written: CASA marks the interval good before applying minsnr.
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(dir.path(), SyntheticGainFixtureKind::G);
    // A small unflagged phase error gives the fit a finite SNR.
    let ArrayValue::Complex32(data) = MeasurementSet::open(&ms_path)
        .expect("open measurement set")
        .main_table()
        .cell_accessor(0, "DATA")
        .and_then(|cell| cell.array())
        .expect("DATA cell")
        .clone()
    else {
        panic!("expected complex DATA");
    };
    let rotated = data.mapv(|value| value * Complex32::from_polar(1.0, 0.05));
    set_main_cells(&ms_path, "DATA", &[0], |_| {
        Value::Array(ArrayValue::Complex32(rotated.clone()))
    });
    let caltable = dir.path().join("minsnr.gcal");
    let mut request = gain_request(
        caltable.clone(),
        GainType::G,
        GainSolveMode::Phase,
        GainSolveInterval::Infinite,
    );
    request.min_snr = 1.0e9;

    let report = run_gain_solve(&ms_path, &request).expect("solve and flag by SNR");

    assert_eq!(report.solution_row_count, 3);
    let rows = read_cal_rows(&caltable);
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row.flags.iter().all(|flag| *flag)));
}

#[test]
fn bandpass_with_no_unflagged_data_writes_no_table() {
    let dir = TempDir::new().expect("tempdir");
    let clusters = [100.0, 101.0].map(|time_seconds| common::SyntheticBandpassTimeCluster {
        field_id: 0,
        time_seconds,
        scan_number: 1,
        prior_gains: [[Complex32::new(1.0, 0.0); 2]; 3],
        bandpass_gains: [[[Complex32::new(1.0, 0.0); 2]; 2]; 3],
    });
    let ms_path = common::create_bandpass_solve_fixture_ms(dir.path(), &clusters);
    set_main_cells(&ms_path, "FLAG", &[0, 1, 2, 3, 4, 5], |_| flag_cells(true));
    let caltable = dir.path().join("empty.bcal");

    let result = run_bandpass_solve(&ms_path, &bandpass_request(caltable.clone(), Vec::new()));

    match result {
        Err(CalibrationError::Bandpass(error))
            if matches!(*error, BandpassSolveError::NoSolutions) => {}
        other => panic!("expected NoSolutions, got {other:?}"),
    }
    assert!(!caltable.exists());
}

// ---------------------------------------------------------------------------
// Solution TIME and INTERVAL (#904)

#[test]
fn solution_time_is_the_sigma_weighted_centroid_and_interval_is_zero() {
    // CASA resets the solve weights from SIGMA (resetWeightsUsingSigma), so
    // WEIGHT is ignored: t=102 carries weight 4 and t=100's WEIGHT of 100
    // has no effect. TIME = (100 + 101 + 4*102) / 6.
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(dir.path(), SyntheticGainFixtureKind::G);
    set_main_cells(&ms_path, "SIGMA", &[6, 7, 8], |_| per_correlation(0.5));
    set_main_cells(&ms_path, "WEIGHT", &[0, 1, 2], |_| per_correlation(100.0));
    let caltable = dir.path().join("centroid.gcal");

    run_gain_solve(
        &ms_path,
        &gain_request(
            caltable.clone(),
            GainType::G,
            GainSolveMode::Phase,
            GainSolveInterval::Infinite,
        ),
    )
    .expect("solve G");

    for row in read_cal_rows(&caltable) {
        assert_close(row.time, 101.5, "TIME");
        assert_close(row.interval, 0.0, "INTERVAL");
    }
}

#[test]
fn phase_only_solution_time_weights_each_baseline_by_its_squared_amplitude() {
    // Baseline 0-1 is flagged at t=102, so its time centroid is 100.5 while
    // the other two baselines' is 101, each per-correlation weight being the
    // unflagged sample count (4, 6, 6). Amplitude-phase solves weight the
    // baseline centroids by those weights alone. Phase-only solves first
    // multiply each weight by |V|^2 (SolveDataBuffer::enforceAPonData),
    // which with antenna amplitudes 1, 2 and 0.5 is 4, 0.25 and 1. A 0.1 rad
    // closure-phase error on baseline 1-2 gives chi-square a floor, as noise
    // would; without one CASA's convergence test (|dchi2| < 0.1 chi2 in
    // VisCalSolver2::converged) can fail on noise-free data that converge
    // linearly.
    let gains = [
        Complex32::new(1.0, 0.0),
        Complex32::from_polar(2.0, 0.3),
        Complex32::from_polar(0.5, -0.2),
    ];
    let amplitude_phase_time = (4.0 * 100.5 + 6.0 * 101.0 + 6.0 * 101.0) / 16.0;
    let phase_time = (16.0 * 100.5 + 1.5 * 101.0 + 6.0 * 101.0) / 23.5;
    for (gain_type, solve_mode, expected) in [
        (
            GainType::G,
            GainSolveMode::AmplitudePhase,
            amplitude_phase_time,
        ),
        (GainType::G, GainSolveMode::Phase, phase_time),
        (GainType::T, GainSolveMode::Phase, phase_time),
    ] {
        let dir = TempDir::new().expect("tempdir");
        let clusters = [100.0, 101.0, 102.0].map(|time| cluster(time, gains));
        let ms_path = common::create_gain_solve_fixture_ms_from_clusters(dir.path(), &clusters);
        set_main_cells(&ms_path, "FLAG", &[6], |_| flag_cells(true));
        set_main_cells(&ms_path, "DATA", &[2, 5, 8], |_| {
            Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                vec![2, 2],
                gains[1] * gains[2].conj() * Complex32::from_polar(1.0, 0.1),
            )))
        });
        let caltable = dir.path().join("weighted.gcal");

        run_gain_solve(
            &ms_path,
            &gain_request(
                caltable.clone(),
                gain_type,
                solve_mode,
                GainSolveInterval::Infinite,
            ),
        )
        .unwrap_or_else(|error| panic!("{gain_type:?} {solve_mode:?}: {error}"));

        for row in read_cal_rows(&caltable) {
            assert_close(
                row.time,
                expected,
                &format!("{gain_type:?} {solve_mode:?} TIME"),
            );
        }
    }
}

#[test]
fn calwt_prior_tables_scale_the_solve_weights() {
    // The data and the prior both carry amplitude 2 per antenna at t=102,
    // so the corrected data are constant. With calwt the prior scales the
    // t=102 weights by |2*2|^2 = 16: TIME = (100 + 101 + 16*102) / 18.
    // Without calwt the weights stay equal and TIME is the plain mean.
    let base = [
        Complex32::new(1.0, 0.0),
        Complex32::from_polar(1.0, 0.3),
        Complex32::from_polar(1.0, -0.2),
    ];
    for (calwt, expected) in [
        (true, (100.0 + 101.0 + 16.0 * 102.0) / 18.0),
        (false, 101.0),
    ] {
        let dir = TempDir::new().expect("tempdir");
        let clusters = [(100.0, 1.0), (101.0, 1.0), (102.0, 2.0)]
            .map(|(time, scale)| cluster(time, base.map(|gain| gain * scale)));
        let ms_path = common::create_gain_solve_fixture_ms_from_clusters(dir.path(), &clusters);
        let prior_rows = [(100.0, 1.0), (101.0, 1.0), (102.0, 2.0)]
            .into_iter()
            .flat_map(|(time, scale)| {
                (0..3).map(move |antenna| prior_row(time, antenna, [Complex32::new(scale, 0.0); 2]))
            })
            .collect::<Vec<_>>();
        let mut prior = calwt_prior(&dir.path().join("prior.gcal"), &prior_rows);
        prior.calwt = calwt;
        let caltable = dir.path().join("calwt.gcal");
        let mut request = gain_request(
            caltable.clone(),
            GainType::G,
            GainSolveMode::AmplitudePhase,
            GainSolveInterval::Infinite,
        );
        request.prior_calibration_tables = vec![prior];

        run_gain_solve(&ms_path, &request).expect("solve G through the prior");

        for row in read_cal_rows(&caltable) {
            assert_close(row.time, expected, &format!("calwt={calwt} TIME"));
            assert_gain_close(row.gains[0], base[row.antenna as usize], "solved gain");
        }
    }
}

// ---------------------------------------------------------------------------
// Flex reference antenna

#[test]
fn an_interval_without_the_refant_is_referred_through_the_nearest_antenna() {
    // Antenna 0 (the refant) has no data at t=101. CASA refers that
    // interval through the antenna nearest the refant that is good in both
    // it and the previous interval -- antenna 2 here, placed next to
    // antenna 0 -- and keeps that antenna for t=102, because antenna 0 was
    // not good at t=101. Phases stay continuous and no later interval is
    // flagged.
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(dir.path(), SyntheticGainFixtureKind::G);
    {
        let antenna_path = ms_path.join("ANTENNA");
        let mut antenna = Table::open(TableOptions::new(&antenna_path)).expect("open ANTENNA");
        antenna
            .cell_accessor_mut(2, "POSITION")
            .and_then(|mut cell| {
                cell.set(Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], vec![0.1, 1.0, 0.0]).expect("position"),
                )))
            })
            .expect("move antenna 2 next to antenna 0");
        antenna
            .save(TableOptions::new(&antenna_path))
            .expect("save ANTENNA");
    }
    set_main_cells(&ms_path, "FLAG", &rows_with_antenna(1, 0), |_| {
        flag_cells(true)
    });
    let caltable = dir.path().join("flex.gcal");

    run_gain_solve(
        &ms_path,
        &gain_request(
            caltable.clone(),
            GainType::G,
            GainSolveMode::Phase,
            GainSolveInterval::Integration,
        ),
    )
    .expect("solve G per integration");

    let rows = read_cal_rows(&caltable);
    assert_eq!(rows.len(), 9);
    let first = rows
        .iter()
        .filter(|row| row.time == 100.0)
        .collect::<Vec<_>>();
    for row in &rows {
        let refant_absent = row.time == 101.0 && row.antenna == 0;
        assert!(
            row.flags.iter().all(|flag| *flag == refant_absent),
            "t={} antenna {} flags {:?}",
            row.time,
            row.antenna,
            row.flags
        );
        let expected_refant = if row.time == 100.0 { 0 } else { 2 };
        assert_eq!(row.refant, expected_refant, "t={} refant", row.time);
        if !refant_absent {
            let reference = first
                .iter()
                .find(|candidate| candidate.antenna == row.antenna)
                .expect("t=100 row");
            for (gain, expected) in row.gains.iter().zip(&reference.gains) {
                assert_gain_close(
                    *gain,
                    *expected,
                    &format!("t={} antenna {}", row.time, row.antenna),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FLAG_ROW exclusion across gain types, modes, intervals and priors (#900)

/// Solve around an outlier on MAIN row 0 (baseline 0-1 at t=100) flagged
/// only by `FLAG_ROW`, optionally through a unit `calwt` prior table so the
/// solve reads its flags from the pre-applied rows, and check that every
/// other row corrects to the unit model. CASA flags every sample of a
/// `FLAG_ROW` row before solving (`VisEquation::divideCorrByModel`);
/// accumulated, the outlier would move the solutions by about 17 degrees.
fn assert_flag_row_outlier_is_excluded(
    kind: SyntheticGainFixtureKind,
    gain_type: GainType,
    solve_mode: GainSolveMode,
    solve_interval: GainSolveInterval,
    with_prior_table: bool,
) {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_gain_solve_fixture_ms(dir.path(), kind);
    common::make_row_flag_only_outlier(&ms_path, 0, Complex32::from_polar(3.0, 0.6));
    let prior_tables = if with_prior_table {
        vec![calwt_prior(
            &dir.path().join("prior.gcal"),
            &unit_prior_rows(100.0),
        )]
    } else {
        Vec::new()
    };
    let caltable = dir.path().join("row-flagged.gcal");
    let mut request = gain_request(caltable.clone(), gain_type, solve_mode, solve_interval);
    request.prior_calibration_tables = prior_tables.clone();

    let report = run_gain_solve(&ms_path, &request).expect("solve around a FLAG_ROW outlier");

    let intervals = match solve_interval {
        GainSolveInterval::Integration => 3,
        _ => 1,
    };
    assert_eq!(report.solution_row_count, 3 * intervals);
    let mut calibration_tables = prior_tables;
    calibration_tables.push(ApplyCalibrationTableSpec::new(&caltable));
    apply(&ms_path, calibration_tables);
    common::assert_corrected_rows_are_unit_model_except(&ms_path, &[0]);
}

#[test]
fn flag_row_outlier_is_excluded_from_g_phase() {
    use GainSolveInterval::{Infinite, Integration};
    for (interval, prior) in [(Infinite, false), (Infinite, true), (Integration, false)] {
        assert_flag_row_outlier_is_excluded(
            SyntheticGainFixtureKind::G,
            GainType::G,
            GainSolveMode::Phase,
            interval,
            prior,
        );
    }
}

#[test]
fn flag_row_outlier_is_excluded_from_g_amplitude_phase() {
    for prior in [false, true] {
        assert_flag_row_outlier_is_excluded(
            SyntheticGainFixtureKind::GAmplitudePhase,
            GainType::G,
            GainSolveMode::AmplitudePhase,
            GainSolveInterval::Infinite,
            prior,
        );
    }
}

#[test]
fn flag_row_outlier_is_excluded_from_t() {
    use GainSolveInterval::{Infinite, Integration};
    for (kind, mode, interval, prior) in [
        (
            SyntheticGainFixtureKind::T,
            GainSolveMode::Phase,
            Infinite,
            false,
        ),
        (
            SyntheticGainFixtureKind::T,
            GainSolveMode::Phase,
            Integration,
            true,
        ),
        (
            SyntheticGainFixtureKind::TAmplitudePhase,
            GainSolveMode::AmplitudePhase,
            Infinite,
            true,
        ),
    ] {
        assert_flag_row_outlier_is_excluded(kind, GainType::T, mode, interval, prior);
    }
}

#[test]
fn flag_row_outlier_is_excluded_from_bandpass_with_a_prior_table() {
    // Also pins B's TIME: row 0 leaves baseline 0-1 one sample at t=101
    // while the other baselines keep two centred on 100.5, so the weighted
    // centroid is (101 + 2*100.5 + 2*100.5) / 5 rather than a midpoint.
    let dir = TempDir::new().expect("tempdir");
    let prior_gains = [
        Complex32::new(1.0, 0.0),
        Complex32::from_polar(1.0, 0.3),
        Complex32::from_polar(1.0, -0.4),
    ];
    let bandpass_gains = [
        [[Complex32::new(1.0, 0.0); 2]; 2],
        [
            [Complex32::new(1.1, 0.15), Complex32::new(0.8, -0.2)],
            [Complex32::new(0.95, -0.1), Complex32::new(1.2, 0.25)],
        ],
        [
            [Complex32::new(0.9, -0.05), Complex32::new(1.15, 0.18)],
            [Complex32::new(1.05, 0.12), Complex32::new(0.88, -0.14)],
        ],
    ];
    let clusters = [100.0, 101.0].map(|time_seconds| common::SyntheticBandpassTimeCluster {
        field_id: 0,
        time_seconds,
        scan_number: 1,
        prior_gains: prior_gains.map(|gain| [gain; 2]),
        bandpass_gains,
    });
    let ms_path = common::create_bandpass_solve_fixture_ms(dir.path(), &clusters);
    common::make_row_flag_only_outlier(&ms_path, 0, Complex32::from_polar(3.0, 0.6));
    let prior_rows = (0..3)
        .map(|antenna| prior_row(100.0, antenna, [prior_gains[antenna as usize]; 2]))
        .collect::<Vec<_>>();
    let prior = calwt_prior(&dir.path().join("prior.gcal"), &prior_rows);
    let caltable = dir.path().join("row-flagged.bcal");

    let report = run_bandpass_solve(
        &ms_path,
        &bandpass_request(caltable.clone(), vec![prior.clone()]),
    )
    .expect("solve bandpass around a FLAG_ROW outlier");

    assert_eq!(report.solution_row_count, 3);
    for row in read_cal_rows(&caltable) {
        assert_close(row.time, (101.0 + 4.0 * 100.5) / 5.0, "B TIME");
        assert_close(row.interval, 0.0, "B INTERVAL");
    }
    apply(
        &ms_path,
        vec![prior, ApplyCalibrationTableSpec::new(&caltable)],
    );
    common::assert_corrected_rows_are_unit_model_except(&ms_path, &[0]);
}
