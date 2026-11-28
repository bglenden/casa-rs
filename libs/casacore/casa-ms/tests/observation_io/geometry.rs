// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_ms::observation_io::{Msv4Observation, ObservationError, OpenOptions, visit_stored_uvw};
use casa_tables::{TableError, TableWriteLock};
use zarrs::array::Array;
use zarrs::filesystem::FilesystemStore;

use super::fixture;

#[test]
fn stored_msv2_uvw_visits_flagged_rows_across_an_admitted_batch_boundary() {
    let root = tempfile::tempdir().unwrap();
    let path = fixture(root.path(), "ms_trial.ms");
    let mut ms = MeasurementSet::open(&path).unwrap();
    ms.main_table_mut()
        .cell_accessor_mut(0, "FLAG_ROW")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Bool(true)))
        .unwrap();
    ms.save().unwrap();

    let rows: Vec<_> = (0..ms.main_table().row_count()).collect();
    let expected: Vec<[f64; 3]> = ms
        .main_table()
        .column_accessor("UVW")
        .unwrap()
        .array_cells_owned(&rows)
        .unwrap()
        .into_iter()
        .map(|cell| match cell {
            Some(ArrayValue::Float64(values)) => values.as_slice().unwrap().try_into().unwrap(),
            _ => panic!("a defined UVW triple"),
        })
        .collect();
    drop(ms);

    let mut total = 0;
    visit_stored_uvw(
        &path,
        usize::MAX,
        &mut |bytes| {
            total = total.max(bytes);
            Ok(())
        },
        &mut |_| {},
    )
    .unwrap();

    let mut actual = Vec::new();
    assert_eq!(write_attempt(&path), Some(70));
    visit_stored_uvw(&path, total - 1, &mut |_| Ok(()), &mut |uvw| {
        if actual.is_empty() {
            assert_eq!(
                write_attempt(&path),
                Some(71),
                "a writer entered a live UVW survey"
            );
        }
        actual.push(uvw)
    })
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(write_attempt(&path), Some(70));
    assert!(matches!(
        visit_stored_uvw(
            &path,
            1,
            &mut |_| panic!("unadmitted grant"),
            &mut |_| panic!("unadmitted read")
        ),
        Err(ObservationError::OverBudget { budget: 1, .. })
    ));
}

const LOCK_PROBE: &str = "CASARS_TEST_UVW_WRITER_PATH";

fn write_attempt(path: &Path) -> Option<i32> {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "observation_io::geometry::independent_write_attempt",
        ])
        .env(LOCK_PROBE, path)
        .output()
        .unwrap()
        .status
        .code()
}

#[test]
fn independent_write_attempt() {
    if let Some(path) = std::env::var_os(LOCK_PROBE) {
        let code = match TableWriteLock::acquire(path, 1) {
            Ok(lock) => {
                lock.release().unwrap();
                70
            }
            Err(TableError::LockFailed { .. }) => 71,
            Err(error) => panic!("unexpected writer failure: {error}"),
        };
        std::process::exit(code);
    }
}

fn opening(path: &Path) -> (usize, usize) {
    let mut peak = 0;
    let reader = Msv4Observation::open_granted(path, &OpenOptions::default(), &mut |wanted| {
        peak = peak.max(wanted);
        Ok(())
    })
    .unwrap();
    (peak, reader.held_bytes())
}

#[test]
fn the_uvw_survey_releases_transient_opening_memory_before_traversing() {
    let root = tempfile::tempdir().unwrap();
    let path = fixture(root.path(), "ms_trial.ps.zarr");
    let (peak, retained) = opening(&path);
    assert!(peak > retained);
    let mut charges = Vec::new();
    let mut rows = 0;
    visit_stored_uvw(
        &path,
        peak,
        &mut |wanted| {
            charges.push(wanted);
            Ok(())
        },
        &mut |_| rows += 1,
    )
    .unwrap();
    assert_eq!(rows, 120);
    assert!(
        charges
            .windows(2)
            .any(|pair| pair[0] == peak && pair[1] == retained)
    );
}

#[test]
fn a_wide_compressed_uvw_chunk_is_refused_before_any_triples_are_decoded() {
    let root = tempfile::tempdir().unwrap();
    let path = fixture(root.path(), "ms_trial.ps.zarr");
    let uvw_path = path.join("ms_trial_0/UVW");
    let metadata = uvw_path.join("zarr.json");
    let mut document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&metadata).unwrap()).unwrap();
    let chunk_times: usize = 65_536;
    document["chunk_grid"]["configuration"]["chunk_shape"][0] = chunk_times.into();
    std::fs::write(metadata, serde_json::to_vec(&document).unwrap()).unwrap();
    std::fs::remove_dir_all(uvw_path.join("c")).unwrap();
    let storage = Arc::new(FilesystemStore::new(&path).unwrap());
    let uvw = Array::open(storage, "/ms_trial_0/UVW").unwrap();
    let numbers = vec![1.25_f64; chunk_times * 10 * 3];
    uvw.store_chunk(&[0, 0, 0], numbers.as_slice()).unwrap();
    drop(numbers);

    let (peak, _) = opening(&path);
    let budget = peak + (1 << 20);
    assert!(budget < chunk_times * 10 * 3 * size_of::<f64>());
    let error = visit_stored_uvw(&path, budget, &mut |_| Ok(()), &mut |_| {
        panic!("unadmitted chunk")
    })
    .unwrap_err();
    assert!(matches!(error, ObservationError::OverBudget { budget: found, .. } if found == budget));
}
