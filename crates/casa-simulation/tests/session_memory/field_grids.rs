// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory law of a predict under the primary beam over several
//! fields (#841): `predict` keeps the model's planes, not a set of grids
//! per field, and `close` prepares each field's grids within its admission,
//! evicting a set before it prepares another when its budget holds one. It
//! shares the binary's counting heap and runs alone as the other laws do
//! ([`super::alone`]).

use std::path::Path;
use std::sync::Arc;

use super::{
    ALLOWANCE_BYTES, BUDGET_32, CENTRE_RAD, Closed, DirectionRef, MDirection, MEASURED_RUNS,
    MeasuresProvider, ObserveSpec, PassSummary, PredictSpec, Predicted, PredictionOptions,
    Simulator, VoltagePatternSpec, alone, charge_process_caches, configured, measured_close,
    predict_after, resources, scan_from, write_model_of,
};

/// Pixels a side of the law's model: a set of its grids, about 40 MB, is
/// more than the least budget's `close` leaves beside its writer, so that
/// budget holds one set and evicts it at every field change.
const PIXELS: usize = 1_700;

/// A budget that holds every field's grids.
const ROOMY: u64 = 1 << 30;

/// The law's fields, each observed for one integration in each of two
/// passes.
const FIELDS: [&str; 2] = ["target", "offset"];
const PASSES: usize = 2;

/// Set the field `offset`, three and two cells from the model's centre,
/// and observe [`FIELDS`] in [`PASSES`] passes under the default voltage
/// pattern.
fn observe_fields(sm: &mut Simulator) {
    let direction = MDirection::from_angles(
        CENTRE_RAD[0] + 6.0e-6 / CENTRE_RAD[1].cos(),
        CENTRE_RAD[1] - 4.0e-6,
        DirectionRef::J2000,
    );
    sm.set_field("offset", &direction, "", 0.0)
        .expect("setfield");
    for pass in 0..PASSES {
        for (index, field) in FIELDS.iter().enumerate() {
            sm.observe(&ObserveSpec {
                source: (*field).to_string(),
                ..scan_from(pass * FIELDS.len() + index, 1)
            })
            .expect("observe");
        }
    }
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
}

/// A session of [`FIELDS`] within `budget`, predicted from `model` and
/// ready to close, or the admission's refusal of `predict`.
fn predicted(
    ms: &Path,
    model: &Path,
    measures: &Arc<dyn MeasuresProvider>,
    budget: u64,
) -> Option<Simulator> {
    let mut sm = configured(ms, measures, budget, 32);
    observe_fields(&mut sm);
    match sm.predict(&PredictSpec {
        images: vec![model.to_path_buf()],
        component_list: None,
        incremental: false,
    }) {
        Ok(()) => Some(sm),
        Err(error) => {
            assert!(error.admission_refused().is_some(), "{error}");
            sm.close().expect("close");
            let _ = std::fs::remove_dir_all(ms);
            None
        }
    }
}

/// Whether the session closes within `budget`.
fn closes(
    ms: &Path,
    model: &Path,
    measures: &Arc<dyn MeasuresProvider>,
    budget: u64,
) -> bool {
    let Some(sm) = predicted(ms, model, measures, budget) else {
        return false;
    };
    let closed = sm.close();
    let _ = std::fs::remove_dir_all(ms);
    match closed {
        Ok(()) => true,
        Err(error) => {
            assert!(error.admission_refused().is_some(), "{error}");
            false
        }
    }
}

/// `close` within `budget`, measured [`MEASURED_RUNS`] times, each held to
/// its admission and the allowance: the least heap, the most admitted, and
/// the last run's summary.
fn measure_close(
    ms: &Path,
    model: &Path,
    measures: &Arc<dyn MeasuresProvider>,
    budget: u64,
) -> (Closed, PassSummary) {
    let mut held = Closed {
        heap: u64::MAX,
        admitted: 0,
    };
    let mut last = None;
    for _ in 0..MEASURED_RUNS {
        let sm = predicted(ms, model, measures, budget).expect("predict fits");
        let (run, summary) = measured_close(sm, ms, true);
        let summary = summary.expect("the pass's summary");
        println!(
            "budget {budget}: close held {} bytes, admitted {}; {summary:?}",
            run.heap, run.admitted
        );
        assert!(
            run.heap <= run.admitted + ALLOWANCE_BYTES,
            "budget {budget}: close held {} bytes, beyond the {} admitted and the allowance",
            run.heap,
            run.admitted
        );
        held.heap = held.heap.min(run.heap);
        held.admitted = held.admitted.max(run.admitted);
        last = Some(summary);
    }
    (held, last.expect("measured"))
}

/// Under the beam, `predict` over two fields keeps the model's planes and
/// none of the fields' grids, and what it keeps is what it charges; `close`
/// holds no more than its admission and the allowance with a budget for
/// every field's grids, preparing each once, and with the least budget that
/// closes, which holds one set and prepares one at every visit: a set
/// evicted is freed before the next is prepared. Two sets of about 40 MB
/// held where one is admitted would be far beyond the allowance.
#[test]
fn a_per_field_predict_keeps_its_planes_and_close_holds_its_admission() {
    if !alone("field_grids::a_per_field_predict_keeps_its_planes_and_close_holds_its_admission") {
        return;
    }
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let model = root.path().join("model.image");
    write_model_of(&model, 32, PIXELS);
    let ms = root.path().join("fields.ms");
    // Once unmeasured: state a process keeps once it has predicted.
    assert!(closes(&ms, &model, &measures, ROOMY), "a roomy budget closes");
    let (_, roomy) = measure_close(&ms, &model, &measures, ROOMY);
    assert_eq!(roomy.grid_preparations, FIELDS.len() as u64);
    let set = roomy.grid_bytes / FIELDS.len() as u64;
    // One field under the beam, whose grids `predict` prepares. Before
    // `predict` dropped the model when it had read it, both held the image's
    // tile cache, a plane of 11.6 MB, beyond their reservations once the
    // read's was released.
    let one = predict_after(
        &PredictionOptions::default(),
        (&ms, &model, &measures),
        ROOMY,
        &[12.0],
        |sm| {
            sm.observe(&scan_from(0, 1)).expect("observe");
            sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
        },
    );
    println!(
        "one-field predict: held {} bytes against {} admitted, {} at most beyond its \
         reservations",
        one.during.heap, one.during.admitted, one.beyond
    );
    for _ in 0..MEASURED_RUNS {
        let Predicted {
            during,
            beyond,
            kept,
        } = predict_after(
            &PredictionOptions::default(),
            (&ms, &model, &measures),
            ROOMY,
            &[12.0],
            observe_fields,
        );
        println!(
            "per-field predict: held {} bytes against {} admitted, {beyond} at most beyond its \
             reservations; kept {} against {} charged; a set of grids {set}",
            during.heap, during.admitted, kept.heap, kept.admitted
        );
        assert!(
            during.heap <= during.admitted + ALLOWANCE_BYTES,
            "predict held {} bytes, beyond the {} admitted and the allowance",
            during.heap,
            during.admitted
        );
        for (label, beyond) in [("one field", one.beyond), ("per field", beyond)] {
            assert!(
                beyond <= ALLOWANCE_BYTES as i64,
                "{label}: predict held {beyond} bytes beyond its reservations at one moment"
            );
        }
        assert!(
            kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES,
            "predict keeps {} bytes against the {} its reservations keep",
            kept.heap,
            kept.admitted
        );
        assert!(
            kept.admitted < set,
            "predict keeps {} bytes, not the planes alone",
            kept.admitted
        );
    }
    // The least budget that closes, to a tenth of a set.
    let (mut refused, mut closed) = (0, ROOMY);
    while closed - refused > set / 10 {
        let middle = refused + (closed - refused) / 2;
        if closes(&ms, &model, &measures, middle) {
            closed = middle;
        } else {
            refused = middle;
        }
    }
    let (_, tight) = measure_close(&ms, &model, &measures, closed);
    assert!(
        tight.grid_bytes < 2 * set,
        "the least budget holds one set: {tight:?}"
    );
    assert_eq!(
        tight.grid_preparations,
        (FIELDS.len() * PASSES) as u64,
        "one set at a time prepares every visit"
    );
}
