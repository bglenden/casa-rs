// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency matrices are admitted while preparing grids, then released.
use super::*;
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Value};

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn profile(path: &Path, count: usize) {
    copy_tree(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/sm_frequency_beams/frequency_power.vp"),
        path,
    );
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    let mut record = table
        .cell_accessor(0, "pbdescription")
        .unwrap()
        .record()
        .unwrap();
    let values = ArrayD::from_shape_fn(IxDyn(&[4, count]), |index| {
        [1., 5e-5, 1e-9, 1e-14][index[0]]
    });
    *record.get_mut("coeff").unwrap() = Value::Array(ArrayValue::Float64(values));
    let frequencies = ArrayD::from_shape_fn(IxDyn(&[count]), |i| {
        100e9 + i[0] as f64 * 200e9 / count as f64
    });
    *record.get_mut("fitfreqs").unwrap() = Value::Array(ArrayValue::Float64(frequencies));
    table
        .cell_accessor_mut(0, "pbdescription")
        .unwrap()
        .set_record(record)
        .unwrap();
    table.flush().unwrap();
}

fn measured(
    path: &Path,
    model: &Path,
    beam: &Path,
    measures: &Arc<dyn MeasuresProvider>,
) -> (Predicted, Closed) {
    let mut sm = configured(path, measures, BUDGET_320, 4);
    sm.observe(&scan(2)).unwrap();
    sm.set_vp(&VoltagePatternSpec {
        use_default_vp: false,
        vp_table: beam.to_string_lossy().into_owned(),
        do_squint: false,
        ..VoltagePatternSpec::default()
    })
    .unwrap();
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    let before = excess_now();
    EXCESS.store(before, Ordering::Relaxed);
    sm.predict(&PredictSpec {
        images: vec![model.to_path_buf()],
        ..PredictSpec::default()
    })
    .unwrap();
    let predicted = Predicted {
        during: Closed {
            heap: PEAK.load(Ordering::Relaxed) - baseline,
            admitted: window.close() - reserved,
        },
        beyond: EXCESS.load(Ordering::Relaxed) - before,
        kept: Closed {
            heap: LIVE.load(Ordering::Relaxed).saturating_sub(baseline),
            admitted: reserved_memory() - reserved,
        },
    };
    drop(window);
    let (closed, _) = measured_close(sm, path, false);
    (predicted, closed)
}

#[test]
fn frequency_matrix_storage_stays_admitted_as_fit_count_grows() {
    if !alone("frequency_curves::frequency_matrix_storage_stays_admitted_as_fit_count_grows") {
        return;
    }
    let policy = resources(BUDGET_320);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let model = directory.path().join("model.image");
    write_model_of(&model, 4, 64);
    let mut small: Option<Predicted> = None;
    for count in [64, 32768] {
        let beam = directory.path().join(format!("{count}.vp"));
        profile(&beam, count);
        let path = directory.path().join(format!("{count}.ms"));
        measured(&path, &model, &beam, &measures);
        for _ in 0..MEASURED_RUNS {
            let (predicted, closed) = measured(&path, &model, &beam, &measures);
            println!("{count} frequency fits: {predicted:?}; close {closed:?}");
            assert!(predicted.beyond <= ALLOWANCE_BYTES as i64);
            assert!(predicted.during.heap <= predicted.during.admitted + ALLOWANCE_BYTES);
            assert!(predicted.kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES);
            assert!(closed.heap <= closed.admitted + ALLOWANCE_BYTES);
            if count == 64 {
                small = Some(predicted);
            } else {
                let small = small.as_ref().unwrap();
                // Parsed records, coefficient copies and their owner grow
                // with supplied coefficients, never with 10,000 samples per fit.
                let input_bytes = (count * 5 * size_of::<f64>()) as u64;
                assert!(
                    predicted.during.heap <= small.during.heap + 4 * input_bytes + ALLOWANCE_BYTES
                );
                // The field grids bake in the beam and release its matrix.
                assert!(predicted.kept.heap <= small.kept.heap + ALLOWANCE_BYTES);
                assert!(predicted.kept.admitted <= small.kept.admitted + ALLOWANCE_BYTES);
            }
        }
    }
}
