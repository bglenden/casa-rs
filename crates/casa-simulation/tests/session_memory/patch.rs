// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory law of `openfromms` (#841 Phase 2): `close` patching
//! an opened MeasurementSet holds no more than it is admitted (the
//! mutation's plan and selected rows, then its batch reads, the new DATA,
//! the cells it writes and the tiled managers' I/O), and nothing beyond
//! that grows with the rows. It shares the binary's counting heap and runs
//! alone as its other laws do ([`super::alone`]).
//!
//! The process-wide table cache is off, as in casa-ms's write-memory laws:
//! the patch charges it with the process caches, and what it fills is that
//! charge's, which the law's caches charge before measuring. The tiles a
//! read or the write holds are then the patch's own, and charged as its
//! tiled I/O.
//!
//! A MeasurementSet's scans may be as many as its rows, so a second law
//! holds the patch of a prediction and a corruption to as little beyond
//! its admission with each row a scan of its own as with one scan (#886
//! review): what it keeps a scan, and a batch's runs of one scan and their
//! jobs, are admitted.

use std::path::Path;
use std::sync::Arc;

use casa_ms::{
    MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession,
};
use casa_types::ScalarValue;

use super::{
    ALLOWANCE_BYTES, BUDGET_32, Closed, EpochRef, MEASURED_RUNS, MEpoch, MeasuresProvider,
    PredictSpec, REFERENCE_MJD, ROWS_ALLOWANCE_BYTES, SCANS_ALLOWANCE_BYTES, Simulator,
    SimulatorOptions, alone, charge_process_caches, configured, measured_close, noise, resources,
    scan, write_model,
};

/// Integrations of the patch law's cases: ten times the rows.
const PATCH_INTEGRATIONS: [usize; 2] = [60, 600];

/// What a patch of `integrations` integrations of [`configured`]'s array
/// takes: a prediction of `model` when given, and with each MAIN row a scan
/// of its own when `row_scans`.
#[derive(Debug, Clone, Copy)]
struct PatchCase<'m> {
    integrations: usize,
    model: Option<&'m Path>,
    row_scans: bool,
}

/// A MeasurementSet of `case`'s integrations written to `ms`, its rows made
/// scans of their own when the case asks, reopened with `openfromms`,
/// predicted when the case asks, corrupted (so the patch reads DATA back)
/// and closed: what that `close` held.
fn patch(case: PatchCase<'_>, ms: &Path, measures: &Arc<dyn MeasuresProvider>) -> Closed {
    let mut sm = configured(ms, measures, BUDGET_32, 32);
    sm.observe(&scan(case.integrations)).expect("observe");
    sm.close().expect("close");
    if case.row_scans {
        number_each_row(ms);
    }
    let mut sm = Simulator::open_from_ms(
        ms,
        SimulatorOptions {
            measures: Arc::clone(measures),
            resources: resources(BUDGET_32),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .expect("openfromms");
    if let Some(model) = case.model {
        sm.predict(&PredictSpec {
            images: vec![model.to_path_buf()],
            component_list: None,
            incremental: true,
        })
        .expect("predict");
    }
    sm.set_noise(&noise()).expect("setnoise");
    sm.corrupt().expect("corrupt");
    measured_close(sm, ms, false).0
}

/// Give each MAIN row of `ms` a SCAN_NUMBER of its own: each is a scan
/// after `openfromms`.
fn number_each_row(ms: &Path) {
    let mut ms = MeasurementSet::open(ms).expect("open");
    let rows = ms.row_count();
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        (0..rows).collect(),
        vec![MeasurementSetWriteColumnPlan {
            name: "SCAN_NUMBER".to_string(),
            bytes_per_row: size_of::<ScalarValue>(),
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        }],
        MeasurementSetWriteResources {
            available_bytes: 1 << 20,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("plan");
    let mut writer =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut ms, plan).expect("start");
    loop {
        let rows = writer.next_mutation_rows().expect("rows").to_vec();
        if rows.is_empty() {
            break;
        }
        let numbers = rows
            .iter()
            .map(|&row| ScalarValue::Int32(row as i32 + 1))
            .collect();
        writer
            .write_mutation_batch(
                &mut ms,
                MeasurementSetMutationBatch {
                    row_indices: rows,
                    columns: vec![MeasurementSetMutationColumnBatch {
                        name: "SCAN_NUMBER".to_string(),
                        values: MeasurementSetMutationColumnValues::Scalars(numbers),
                    }],
                },
            )
            .expect("number the rows");
    }
    writer.finish_mutation().expect("finish");
}

/// The patches of `case` [`MEASURED_RUNS`] times, `what` they are, each
/// holding at most its admission and the allowance: the least heap and the
/// most admitted.
fn measure(
    what: &str,
    case: PatchCase<'_>,
    ms: &Path,
    measures: &Arc<dyn MeasuresProvider>,
) -> Closed {
    let mut held = Closed {
        heap: u64::MAX,
        admitted: 0,
    };
    for _ in 0..MEASURED_RUNS {
        let run = patch(case, ms, measures);
        println!(
            "{what}: the patch held {} bytes, admitted {}",
            run.heap, run.admitted
        );
        assert!(
            run.heap <= run.admitted + ALLOWANCE_BYTES,
            "{what}: the patch held {} bytes, beyond the {} admitted and the allowance",
            run.heap,
            run.admitted
        );
        held.heap = held.heap.min(run.heap);
        held.admitted = held.admitted.max(run.admitted);
    }
    held
}

/// Patching an opened MeasurementSet holds at most its admission and the
/// allowance at `close`, and ten times the rows hold no more beyond their
/// admission than a little: the patch's selected rows are admitted, and
/// what it reads and writes is a batch's.
#[test]
fn patching_an_opened_measurement_set_holds_its_admission_and_no_more_for_more_rows() {
    if !alone(
        "patch::patching_an_opened_measurement_set_holds_its_admission_and_no_more_for_more_rows",
    ) {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let ms = root.path().join("patched.ms");
    let case = |integrations| PatchCase {
        integrations,
        model: None,
        row_scans: false,
    };
    patch(case(PATCH_INTEGRATIONS[0]), &ms, &measures);
    let [few, many] = PATCH_INTEGRATIONS.map(|integrations| {
        let what = format!("{integrations} integrations");
        measure(&what, case(integrations), &ms, &measures)
    });
    assert!(
        many.admitted > few.admitted,
        "the patch admits its selected rows"
    );
    assert!(
        many.unadmitted() <= few.unadmitted() + ROWS_ALLOWANCE_BYTES as i64,
        "{} integrations: the patch held {} bytes beyond its admission, against {} for {}",
        PATCH_INTEGRATIONS[1],
        many.unadmitted(),
        few.unadmitted(),
        PATCH_INTEGRATIONS[0]
    );
}

/// Patching a prediction and a corruption holds at most its admission and
/// the allowance at `close`, and with each of 12,600 rows a scan of its own
/// no more beyond its admission than with the rows one scan, but a little
/// (#886 review): each scan's prediction segment, and a batch's runs and
/// jobs, of which there may be one a row, are admitted.
#[test]
fn patching_holds_its_admission_and_no_more_for_more_scans() {
    if !alone("patch::patching_holds_its_admission_and_no_more_for_more_scans") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let model = root.path().join("model.image");
    write_model(&model, 32);
    let ms = root.path().join("scans.ms");
    let case = |row_scans| PatchCase {
        integrations: PATCH_INTEGRATIONS[1],
        model: Some(&model),
        row_scans,
    };
    patch(case(true), &ms, &measures);
    let [one, many] = [false, true].map(|row_scans| {
        let what = if row_scans {
            "a scan a row"
        } else {
            "one scan"
        };
        measure(what, case(row_scans), &ms, &measures)
    });
    assert!(
        many.admitted > one.admitted,
        "the patch admits what it keeps a scan"
    );
    assert!(
        many.unadmitted() <= one.unadmitted() + SCANS_ALLOWANCE_BYTES as i64,
        "a scan a row: the patch held {} bytes beyond its admission, against {} for one scan",
        many.unadmitted(),
        one.unadmitted()
    );
}
