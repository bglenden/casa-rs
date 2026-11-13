// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent regressions for the shared-writer review findings.
use super::{EpochRef, MEpoch, PredictSpec, Simulator, SimulatorOptions, copy, support};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

fn inputs() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data")
}
fn opened(path: &Path) -> Simulator {
    Simulator::open_from_ms(
        path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: support::resources(1, 2 << 30),
            now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap()
}

#[test]
fn selecting_a_later_correlation_setup_keeps_sd_explicitly_unsupported() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sd.ms");
    copy(
        &inputs().join("sm_mixed_correlations/full_linear.ms"),
        &path,
    );
    let before = std::fs::read(path.join("table.dat")).unwrap();
    let mut sm = opened(&path);
    sm.set_data(&[1], &[], "").unwrap();
    sm.set_options(&casa_simulation::PredictionOptions {
        ftmachine: "sd".into(),
        grid_function: "BOX".into(),
        ..Default::default()
    })
    .unwrap();
    let error = sm
        .predict(&PredictSpec {
            images: vec![inputs().join("sm_image_layouts/canonical.image")],
            ..Default::default()
        })
        .expect_err(
            "mixed SD correlations remain unimplemented even when only the later setup is selected",
        );
    assert_eq!(
        error.outcome(),
        casa_simulation::CasaOutcome::NotYetImplemented
    );
    assert!(error.to_string().contains("#1299"));
    sm.close().unwrap();
    assert_eq!(std::fs::read(path.join("table.dat")).unwrap(), before);
}

struct Watch {
    scans: Arc<AtomicU64>,
    patch: Arc<dyn Fn() + Send + Sync>,
}
struct Count<'a>(&'a AtomicU64);
impl Visit for Count<'_> {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "simulation_prepared_scans" {
            self.0.fetch_add(value, Ordering::Relaxed);
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
impl Subscriber for Watch {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event()
            && (metadata
                .fields()
                .field("simulation_prepared_scans")
                .is_some()
                || metadata.fields().field("reads_data").is_some())
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        event.record(&mut Count(&self.scans));
        if event.metadata().fields().field("reads_data").is_some() {
            (self.patch)();
        }
    }
}

fn probe(path: &Path, blocked: bool) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "review::mixed_shape_close_holds_main_lock",
            "--nocapture",
        ])
        .env("CASA_RS_SIM_MIXED_LOCK_PROBE", path)
        .env(
            "CASA_RS_SIM_MIXED_LOCK_BLOCKED",
            if blocked { "1" } else { "0" },
        )
        .output()
        .unwrap();
    assert!(output.status.success(), "lock probe: {output:?}");
}

#[test]
fn mixed_shape_close_holds_main_lock() {
    if let Some(path) = std::env::var_os("CASA_RS_SIM_MIXED_LOCK_PROBE") {
        let blocked = std::env::var("CASA_RS_SIM_MIXED_LOCK_BLOCKED").unwrap() == "1";
        assert_eq!(
            casa_tables::TableWriteLock::acquire(Path::new(&path), 1).is_err(),
            blocked
        );
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("locked.ms");
    copy(
        &inputs().join("sm_mixed_correlations/full_linear.ms"),
        &path,
    );
    let mut sm = opened(&path);
    sm.predict(&PredictSpec {
        images: vec![inputs().join("sm_image_layouts/iquv.image")],
        ..Default::default()
    })
    .unwrap();
    let once = Arc::new(AtomicBool::new(false));
    let callback_once = once.clone();
    let other = path.clone();
    tracing::subscriber::with_default(
        Watch {
            scans: Arc::default(),
            patch: Arc::new(move || {
                if !callback_once.swap(true, Ordering::Relaxed) {
                    probe(&other, true);
                }
            }),
        },
        || sm.close().unwrap(),
    );
    assert!(once.load(Ordering::Relaxed));
    probe(&path, false);
}

/// Reorder a small actual CASA input, keeping the independent per-window
/// science reference and giving every alternating row a scan of its own.
fn alternating(path: &Path, rows: usize) -> serde_json::Value {
    use casa_tables::{ColumnOverrides, DataManagerKind, Table, TableOptions};
    use casa_types::{ScalarValue, Value};
    copy(&inputs().join("sm_mixed_correlations/full_linear.ms"), path);
    let source = Table::open(TableOptions::new(path)).unwrap();
    let mut table = Table::open_metadata_for_construction(TableOptions::new(path), 0).unwrap();
    let mapping = (0..rows)
        .map(|row| row / 2 + if row % 2 == 0 { 0 } else { 144 })
        .collect::<Vec<_>>();
    let mut columns = ColumnOverrides::for_row_count(rows);
    for column in source.schema().unwrap().columns() {
        let name = column.name();
        columns.insert_values(
            name,
            mapping
                .iter()
                .enumerate()
                .map(|(row, &original)| {
                    if name == "SCAN_NUMBER" {
                        Some(Value::Scalar(ScalarValue::Int32(row as i32)))
                    } else {
                        source
                            .cell_accessor(original, name)
                            .unwrap()
                            .value()
                            .unwrap()
                            .cloned()
                    }
                })
                .collect(),
        );
    }
    table
        .prepare_write()
        .save_with_column_overrides(
            TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan),
            &Default::default(),
            &columns,
        )
        .unwrap();
    let fixture: serde_json::Value = serde_json::from_reader(flate2::read::GzDecoder::new(
        &include_bytes!("../data/sm_mixed_correlations_casa.json.gz")[..],
    ))
    .unwrap();
    let mut case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "full_linear")
        .unwrap()
        .clone();
    case["after"]["rows"] = serde_json::Value::Array(
        mapping
            .iter()
            .map(|&row| case["polarization_map_corrected_rows"][row].clone())
            .collect(),
    );
    case.as_object_mut()
        .unwrap()
        .remove("polarization_map_corrected_rows");
    case
}

#[test]
fn alternating_shapes_prepare_a_linear_number_of_scans() {
    let directory = tempfile::tempdir().unwrap();
    for rows in [32, 128] {
        let path = directory.path().join(format!("alternating-{rows}.ms"));
        let case = alternating(&path, rows);
        let mut sm = opened(&path);
        sm.predict(&PredictSpec {
            images: vec![inputs().join("sm_image_layouts/iquv.image")],
            ..Default::default()
        })
        .unwrap();
        let count = Arc::new(AtomicU64::new(0));
        let start = std::time::Instant::now();
        tracing::subscriber::with_default(
            Watch {
                scans: count.clone(),
                patch: Arc::new(|| {}),
            },
            || sm.close().unwrap(),
        );
        let visits = count.load(Ordering::Relaxed);
        println!(
            "{rows} alternating scans: {visits} preparation visits, {:?}",
            start.elapsed()
        );
        assert!(
            visits > 0 && visits <= 2 * rows as u64,
            "preparation must be linear in scans: {visits}/{rows}"
        );
        assert!(super::existing_ms_calculations::compare(&path, &case) < 1e-3);
    }
}
