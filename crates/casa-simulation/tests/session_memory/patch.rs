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

use std::path::Path;
use std::sync::Arc;

use super::{
    ALLOWANCE_BYTES, BUDGET_32, Closed, EpochRef, MEASURED_RUNS, MEpoch, MeasuresProvider,
    REFERENCE_MJD, ROWS_ALLOWANCE_BYTES, Simulator, SimulatorOptions, alone, charge_process_caches,
    configured, measured_close, noise, resources, scan,
};

/// Integrations of the patch law's cases: ten times the rows.
const PATCH_INTEGRATIONS: [usize; 2] = [60, 600];

/// A MeasurementSet of `integrations` integrations of [`configured`]'s
/// array written to `ms`, reopened with `openfromms`, corrupted (so the
/// patch reads DATA back) and closed: what that `close` held.
fn patch(integrations: usize, ms: &Path, measures: &Arc<dyn MeasuresProvider>) -> Closed {
    let mut sm = configured(ms, measures, BUDGET_32, 32);
    sm.observe(&scan(integrations)).expect("observe");
    sm.close().expect("close");
    let mut sm = Simulator::open_from_ms(
        ms,
        SimulatorOptions {
            measures: Arc::clone(measures),
            resources: resources(BUDGET_32),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .expect("openfromms");
    sm.set_noise(&noise()).expect("setnoise");
    sm.corrupt().expect("corrupt");
    measured_close(sm, ms, false).0
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
    patch(PATCH_INTEGRATIONS[0], &ms, &measures);
    let [few, many] = PATCH_INTEGRATIONS.map(|integrations| {
        let mut held = Closed {
            heap: u64::MAX,
            admitted: 0,
        };
        for _ in 0..MEASURED_RUNS {
            let run = patch(integrations, &ms, &measures);
            println!(
                "{integrations} integrations: the patch held {} bytes, admitted {}",
                run.heap, run.admitted
            );
            assert!(
                run.heap <= run.admitted + ALLOWANCE_BYTES,
                "{integrations} integrations: the patch held {} bytes, beyond the {} admitted \
                 and the allowance",
                run.heap,
                run.admitted
            );
            held.heap = held.heap.min(run.heap);
            held.admitted = held.admitted.max(run.admitted);
        }
        held
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
