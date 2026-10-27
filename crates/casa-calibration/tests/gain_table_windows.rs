// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]
//! A gain table's `SPECTRAL_WINDOW` against CASA's `gaincal` (#917): casa-rs
//! and CASA solve the same MeasurementSet, and every column of every window
//! row agrees exactly, as CASA's own append requires
//! (`NewCalTable::mergeSpwMetaInfo`); and CASA appends to a casa-rs table.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

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
#[derive(Debug, PartialEq)]
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

/// casa-rs's `gaincal` of `ms` into `output`, with CASA's defaults
/// (`minblperant=4`).
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
        min_baselines_per_antenna: 4,
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

/// Solve `ms` with `spw` both ways and compare every window row exactly;
/// `solved` rows are unflagged. `None` when CASA is unavailable; a CASA
/// that runs and fails fails the law.
fn compare(ms: &Path, field: &str, spw: &str, solved: usize) -> Option<()> {
    if common::discover_casa_python().is_none() {
        eprintln!("{}", common::casa_skip_reason());
        return None;
    }
    let root = ms.parent().expect("a working directory");
    let ours = root.join(format!(
        "casa-rs-{}.G",
        spw.replace([':', '~', ';', ','], "_")
    ));
    casa_rs_gaincal(ms, ours.clone(), field, spw);
    let casa = match common::run_casa_gaincal(
        root,
        ms,
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
        Err(reason) => panic!("{spw}: {reason}"),
    };
    let (ours, theirs) = (window_rows(&ours), window_rows(&casa));
    assert_eq!(ours, theirs, "{spw}: window rows, casa-rs then CASA");
    assert_eq!(
        ours.iter().filter(|row| !row.flagged).count(),
        solved,
        "{spw}: windows with solutions"
    );
    std::fs::remove_dir_all(casa).expect("remove CASA's table");
    Some(())
}

/// A working copy of `source` under `root`.
fn working_copy(root: &TempDir, source: &Path) -> PathBuf {
    let ms = root.path().join("input.ms");
    common::copy_measurement_set(source, &ms).expect("copy");
    ms
}

fn multiobs() -> Option<PathBuf> {
    let source = casa_test_support::casatestdata_path_for_tier(
        casa_test_support::CasaTestDataTier::SlowParity,
        "measurementset/alma/multiobs.ms",
    )
    .filter(|path| path.exists());
    if source.is_none() {
        eprintln!("gain table windows skipped: missing measurementset/alma/multiobs.ms");
    }
    source
}

/// Run `script` with CASA's Python, `vis` and `caltable` in its
/// environment as `MS` and `TABLE`: its output, or why it failed. `None`
/// when no CASA Python is found.
fn casa(script: &str, vis: &Path, caltable: &Path) -> Option<Result<String, String>> {
    let Some(python) = common::discover_casa_python() else {
        eprintln!("{}", common::casa_skip_reason());
        return None;
    };
    let output = Command::new(python)
        .env("MS", vis)
        .env("TABLE", caltable)
        .arg("-c")
        .arg(script)
        .output()
        .expect("CASA python");
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Some(if output.status.success() {
        Ok(log)
    } else {
        Err(log)
    })
}

/// Flag, with CASA's table tool, every sample of data description 1
/// except baseline 0-1's, so that no antenna of its window has four
/// baselines. (casa-rs cannot yet rewrite this MeasurementSet: its SOURCE
/// holds string arrays casa-tables misreads, #923.)
const FLAG_ALL_BUT_ONE_BASELINE: &str = r#"
import os
from casatools import table
tb = table()
tb.open(os.environ["MS"], nomodify=False)
dd = tb.getcol("DATA_DESC_ID")
a1 = tb.getcol("ANTENNA1")
a2 = tb.getcol("ANTENNA2")
flags = tb.getcol("FLAG")
flags[:, :, (dd == 1) & ~((a1 == 0) & (a2 == 1))] = True
tb.putcol("FLAG", flags)
tb.close()
"#;

/// ngc5921's one window of 63 channels: whole, two ranges of it, and a
/// stride.
#[test]
fn gain_table_windows_match_casa_on_ngc5921() {
    let Some(source) = common::ngc5921_ms_path() else {
        eprintln!("{}", common::casa_skip_reason());
        return;
    };
    let root = TempDir::new().expect("tempdir");
    let ms = working_copy(&root, &source);
    for spw in ["0", "0:5~10;20~30", "0:41~50;3~17", "0:5~25^5"] {
        if compare(&ms, "0", spw, 1).is_none() {
            return;
        }
    }
}

/// `alma/multiobs.ms`: two windows of four channels, one of increasing and
/// one of decreasing frequency, each solved while the other is not; then
/// both selected, with the second's data too thin to solve (one baseline
/// left unflagged), so that its row is the solved collapse, flagged, as
/// CASA leaves a window it reached but could not solve.
#[test]
fn gain_table_windows_match_casa_on_two_sidebands() {
    let Some(source) = multiobs() else {
        return;
    };
    let root = TempDir::new().expect("tempdir");
    let ms = working_copy(&root, &source);
    for spw in ["1", "0:0~2"] {
        if compare(&ms, "", spw, 1).is_none() {
            return;
        }
    }
    match casa(FLAG_ALL_BUT_ONE_BASELINE, &ms, Path::new("")) {
        Some(Ok(_)) => {}
        Some(Err(log)) => panic!("flagging failed:\n{log}"),
        None => return,
    }
    compare(&ms, "", "0:0~2,1:1~2", 1);
}

/// CASA appends to a gain table casa-rs wrote: `gaincal(append=True)` of
/// ngc5921's second field onto casa-rs's table of its first merges the
/// window's metadata (`NewCalTable::mergeSpwMetaInfo`, which requires every
/// column to equal its own) and adds its rows.
#[test]
fn casa_appends_to_a_casa_rs_gain_table() {
    let Some(source) = common::ngc5921_ms_path() else {
        eprintln!("{}", common::casa_skip_reason());
        return;
    };
    let root = TempDir::new().expect("tempdir");
    let ms = working_copy(&root, &source);
    let table = root.path().join("casa-rs.G");
    casa_rs_gaincal(&ms, table.clone(), "0", "0");
    let rows = || {
        Table::open(TableOptions::new(&table))
            .expect("gain table")
            .row_count()
    };
    let before = (rows(), window_rows(&table));
    let script = r#"
import os
from casatasks import gaincal
gaincal(vis=os.environ["MS"], caltable=os.environ["TABLE"], field="1", spw="0",
        solint="inf", refant="0", gaintype="G", calmode="ap", minsnr=0.0,
        append=True)
"#;
    let log = match casa(script, &ms, &table) {
        Some(Ok(log)) => log,
        Some(Err(log)) => panic!("CASA gaincal(append=True):\n{log}"),
        None => return,
    };
    assert!(
        rows() > before.0,
        "CASA appended no rows ({} before):\n{log}",
        before.0
    );
    assert_eq!(window_rows(&table), before.1, "the window CASA merged");
}
