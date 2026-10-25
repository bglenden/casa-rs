// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory laws of the simulator's mosaic machine (#841 Phase
//! 2): what `predict` holds while it builds every dish pair's beams on
//! their screens, and what it keeps per pointing. They share the binary's
//! counting heap and run alone as its other laws do ([`super::alone`]).

use super::{
    ALLOWANCE_BYTES, ANTENNAS, BUDGET_32, CELL_RAD, CENTRE_RAD, DirectionRef, MDirection,
    MEASURED_RUNS, ObserveSpec, Predicted, PredictionOptions, SCANS_ALLOWANCE_BYTES, alone,
    charge_process_caches, predict_after, predict_through, resources, scan_from, write_model,
};

/// The mosaic law's session budget: the 32-channel cases' and room for the
/// screens beside `close`'s writer.
const BUDGET_MOSAIC: u64 = BUDGET_32 + (32 << 20);

/// The mosaic law's dishes: ALMA's 12 m and ACA's 7 m in turn, so the set
/// has three dish pairs.
const MOSAIC_DISHES: [f64; ANTENNAS] = [12.0, 7.0, 12.0, 7.0, 12.0, 7.0];

fn mosaic() -> PredictionOptions {
    PredictionOptions {
        ftmachine: "mosaic".to_string(),
        ..PredictionOptions::default()
    }
}

/// `predict` through the simulator's mosaic machine transforms every dish
/// pair's beams on a screen and keeps the cells cut from them (#841 Phase
/// 2): while it runs, the heap stays within the most its reservations held
/// (the screens, then the cells, then the model's one set of grids and its
/// preparation) and the allowance, and at no moment more than the
/// allowance beyond them; after it, what the session keeps (the cells, the
/// corrections and the grids) is what its reservations keep.
#[test]
fn a_mosaic_predict_holds_its_admission_and_keeps_what_it_charges() {
    if !alone("mosaic::a_mosaic_predict_holds_its_admission_and_keeps_what_it_charges") {
        return;
    }
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let model = root.path().join("model.image");
    write_model(&model, 32);
    let ms = root.path().join("mosaic.ms");
    let options = mosaic();
    let run = || {
        predict_through(
            &options,
            (&ms, &model, &measures),
            BUDGET_MOSAIC,
            &MOSAIC_DISHES,
        )
    };
    // Once unmeasured: state a process keeps once it has predicted.
    run();
    for _ in 0..MEASURED_RUNS {
        let Predicted {
            during,
            beyond,
            kept,
        } = run();
        println!(
            "mosaic predict: held {} bytes against {} admitted, {beyond} at most beyond its \
             reservations; kept {} against {} charged",
            during.heap, during.admitted, kept.heap, kept.admitted
        );
        // Six cells of about 160 KB (three dish pairs, imaging and weight)
        // and the grids: about 1 MB.
        assert!(
            kept.admitted > ALLOWANCE_BYTES,
            "the cells and grids are charged: {kept:?}"
        );
        assert!(
            beyond <= ALLOWANCE_BYTES as i64,
            "predict held {beyond} bytes beyond its reservations at one moment"
        );
        assert!(
            during.heap <= during.admitted + ALLOWANCE_BYTES,
            "predict held {} bytes, beyond the {} admitted and the allowance",
            during.heap,
            during.admitted
        );
        assert!(
            kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES,
            "predict keeps {} bytes against the {} its reservations keep",
            kept.heap,
            kept.admitted
        );
    }
}

/// One-integration scans of the mosaic pointing law's cases.
const MOSAIC_POINTING_SCANS: usize = 4_096;

/// The mosaic machine keeps a pointing for each distinct phase centre and a
/// dish class for each antenna, charged with its kernels (#879 review,
/// Astra: 16,385 distinct pointings kept 1,048,448 bytes more than shared
/// ones, with nothing more charged). One scan, then
/// [`MOSAIC_POINTING_SCANS`] scans on one field and on as many fields inside
/// the model: `predict` keeps what its reservations keep in each, and the
/// many scans keep no more beyond them than the one by
/// [`SCANS_ALLOWANCE_BYTES`], where 32 bytes a pointing uncharged would be
/// 131 KB.
#[test]
fn a_mosaic_predict_charges_what_it_keeps_per_pointing() {
    if !alone("mosaic::a_mosaic_predict_charges_what_it_keeps_per_pointing") {
        return;
    }
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let model = root.path().join("model.image");
    write_model(&model, 32);
    let ms = root.path().join("pointings.ms");
    let options = mosaic();
    let run = |scans: usize, distinct: bool| {
        predict_after(
            &options,
            (&ms, &model, &measures),
            BUDGET_MOSAIC,
            &MOSAIC_DISHES,
            |sm| {
                for index in 0..scans {
                    let source = if distinct {
                        // A grid of pointings a tenth of a cell apart, within
                        // 3.2 cells of the centre.
                        let name = format!("p{index}");
                        let [column, row] = [index % 64, index / 64].map(|at| at as f64 - 32.0);
                        let direction = MDirection::from_angles(
                            CENTRE_RAD[0] + column * 0.1 * CELL_RAD / CENTRE_RAD[1].cos(),
                            CENTRE_RAD[1] + row * 0.1 * CELL_RAD,
                            DirectionRef::J2000,
                        );
                        sm.set_field(&name, &direction, "", 0.0).expect("setfield");
                        name
                    } else {
                        "target".to_string()
                    };
                    sm.observe(&ObserveSpec {
                        source,
                        ..scan_from(index, 1)
                    })
                    .expect("observe");
                }
            },
        )
    };
    // Once unmeasured: state a process keeps once it has predicted.
    run(MOSAIC_POINTING_SCANS, false);
    let one = run(1, false);
    let [shared, distinct] = [false, true].map(|distinct| run(MOSAIC_POINTING_SCANS, distinct));
    let cases = [
        ("one scan", one),
        ("shared pointings", shared),
        ("distinct pointings", distinct),
    ];
    for (label, Predicted { beyond, kept, .. }) in cases {
        println!(
            "{label}: predict kept {} against {} charged, {beyond} at most beyond its \
             reservations",
            kept.heap, kept.admitted
        );
        assert!(
            beyond <= ALLOWANCE_BYTES as i64,
            "{label}: predict held {beyond} bytes beyond its reservations at one moment"
        );
        assert!(
            kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES,
            "{label}: predict keeps {} bytes against the {} its reservations keep",
            kept.heap,
            kept.admitted
        );
    }
    for (label, many) in [("shared", shared), ("distinct", distinct)] {
        assert!(
            many.kept.unadmitted() - one.kept.unadmitted() <= SCANS_ALLOWANCE_BYTES as i64,
            "{MOSAIC_POINTING_SCANS} scans, {label} pointings, keep {} bytes beyond the \
             charge, one scan {}",
            many.kept.unadmitted(),
            one.kept.unadmitted()
        );
    }
}
