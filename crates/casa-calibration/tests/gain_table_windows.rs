// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]
//! A gain table's `SPECTRAL_WINDOW` against CASA's `gaincal` (#917): casa-rs
//! and CASA solve the same MeasurementSet, and every window row agrees,
//! except the two columns where CASA writes the solved window's frequency
//! (`docs/CASA (C++) bugs.md`).

mod common;

use std::path::{Path, PathBuf};

use casa_calibration::{
    CalibrationDataset, CalibrationSolveRequest, CalibrationSolveResult, GainSolveCombine,
    GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest, GainType,
    RefAntSelector, solve_calibration,
};
use casa_ms::selection::MsSelection;
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};
use tempfile::TempDir;

/// One window row: the columns a gain table collapses.
#[derive(Debug)]
struct WindowRow {
    channels: i32,
    frequency: Vec<f64>,
    width: Vec<f64>,
    effective_bw: Vec<f64>,
    resolution: Vec<f64>,
    total_bandwidth: f64,
    flagged: bool,
}

fn window_rows(caltable: &Path) -> Vec<WindowRow> {
    let table =
        Table::open(TableOptions::new(caltable.join("SPECTRAL_WINDOW"))).expect("SPECTRAL_WINDOW");
    (0..table.row_count())
        .map(|row| {
            let cell = |column: &str| {
                table
                    .cell_accessor(row, column)
                    .and_then(|cell| cell.value().map(|value| value.cloned()))
                    .expect("cell")
                    .expect("defined")
            };
            let array = |column: &str| match cell(column) {
                Value::Array(ArrayValue::Float64(values)) => values.iter().copied().collect(),
                other => panic!("{column}: {other:?}"),
            };
            let scalar = |column: &str| match cell(column) {
                Value::Scalar(scalar) => scalar,
                other => panic!("{column}: {other:?}"),
            };
            let (
                ScalarValue::Int32(channels),
                ScalarValue::Float64(total_bandwidth),
                ScalarValue::Bool(flagged),
            ) = (
                scalar("NUM_CHAN"),
                scalar("TOTAL_BANDWIDTH"),
                scalar("FLAG_ROW"),
            )
            else {
                panic!("NUM_CHAN, TOTAL_BANDWIDTH or FLAG_ROW of row {row}");
            };
            WindowRow {
                channels,
                frequency: array("CHAN_FREQ"),
                width: array("CHAN_WIDTH"),
                effective_bw: array("EFFECTIVE_BW"),
                resolution: array("RESOLUTION"),
                total_bandwidth,
                flagged,
            }
        })
        .collect()
}

/// Equal to a part in 10^12: CASA and casa-rs sum the channels in one
/// order, but the law should not hang on the last bit.
fn close(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(a, b)| (a - b).abs() <= 1e-12 * a.abs().max(b.abs()))
}

fn casa_rs_gaincal(ms: &Path, output: PathBuf, field: &str, spw: &str) {
    let mut selection = MsSelection::new().spw_selector(spw).expect("spw");
    if !field.is_empty() {
        selection.field = Some(field.to_string());
    }
    let request = GainSolveRequest {
        selection,
        output_table: output,
        gain_type: GainType::G,
        solve_mode: GainSolveMode::AmplitudePhase,
        solve_interval: GainSolveInterval::Infinite,
        combine: GainSolveCombine::default(),
        refant: RefAntSelector::AntennaId(0),
        prior_calibration_tables: Vec::new(),
        parang: false,
        model_source: GainSolveModelSource::PointSource,
        normalize_average_amplitude: false,
        min_snr: 0.0,
        min_baselines_per_antenna: 0,
        smodel: [1.0, 0.0, 0.0, 0.0],
    };
    match solve_calibration(
        CalibrationDataset::path(ms),
        CalibrationSolveRequest::Gain(request),
    )
    .expect("casa-rs gaincal")
    {
        CalibrationSolveResult::Gain(_) => {}
        CalibrationSolveResult::Bandpass(_) => unreachable!("gain request result"),
    }
}

/// Solve `source` with `spw` both ways and compare every window row.
fn compare(source: &Path, field: &str, spw: &str) {
    let root = TempDir::new().expect("tempdir");
    let ms = root.path().join("input.ms");
    common::copy_measurement_set(source, &ms).expect("copy");
    let ours = root.path().join("casa-rs.G");
    casa_rs_gaincal(&ms, ours.clone(), field, spw);
    let casa = match common::run_casa_gaincal(
        root.path(),
        &ms,
        common::CasaGaincalOptions {
            field,
            spw,
            refant: "0",
            gaintype: "G",
            calmode: "ap",
            solint: "inf",
            combine: "",
            prior_gaintables: Vec::new(),
            parang: false,
        },
    ) {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };
    let (ours, casa) = (window_rows(&ours), window_rows(&casa));
    assert_eq!(ours.len(), casa.len(), "{spw}: window rows");
    let mut solved = 0;
    for (row, (ours, casa)) in ours.iter().zip(&casa).enumerate() {
        let context = format!("{spw}: row {row}\n casa-rs {ours:?}\n CASA    {casa:?}");
        assert_eq!(
            (ours.channels, ours.flagged),
            (casa.channels, casa.flagged),
            "{context}"
        );
        assert!(close(&ours.frequency, &casa.frequency), "{context}");
        assert!(close(&ours.width, &casa.width), "{context}");
        assert!(close(&ours.effective_bw, &casa.effective_bw), "{context}");
        if casa.flagged {
            assert!(close(&ours.resolution, &casa.resolution), "{context}");
            assert_eq!(ours.total_bandwidth, casa.total_bandwidth, "{context}");
        } else {
            solved += 1;
            // CASA writes the frequency into both (setCTFrequencies); were
            // that fixed, this law would say so.
            assert!(close(&casa.resolution, &casa.frequency), "{context}");
            assert!(close(&[casa.total_bandwidth], &casa.frequency), "{context}");
            assert!(close(&ours.resolution, &ours.width), "{context}");
            assert_eq!(ours.total_bandwidth, ours.width[0], "{context}");
        }
    }
    assert_eq!(solved, 1, "{spw}: one window solved");
}

/// ngc5921's one window of 63 channels: whole, and two ranges of it.
#[test]
fn gain_table_windows_match_casa_on_ngc5921() {
    let Some(source) = common::ngc5921_ms_path() else {
        eprintln!("{}", common::casa_skip_reason());
        return;
    };
    for spw in ["0", "0:5~10;20~30"] {
        compare(&source, "0", spw);
    }
}

/// `alma/multiobs.ms`: two windows of four channels, one of increasing and
/// one of decreasing frequency, each solved while the other is not.
#[test]
fn gain_table_windows_match_casa_on_two_sidebands() {
    let Some(source) = casa_test_support::casatestdata_path_for_tier(
        casa_test_support::CasaTestDataTier::SlowParity,
        "measurementset/alma/multiobs.ms",
    )
    .filter(|path| path.exists()) else {
        eprintln!("gain table windows skipped: missing measurementset/alma/multiobs.ms");
        return;
    };
    for spw in ["1", "0:0~2"] {
        compare(&source, "", spw);
    }
}
