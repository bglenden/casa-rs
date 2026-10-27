// SPDX-License-Identifier: LGPL-3.0-or-later
//! A predict under the primary beam over several fields (#841, the
//! 106-pointing mosaic that needed 5.3 GB of grids): `predict` holds the
//! model's planes, and `close` prepares each field's grids as its write
//! reaches the field, keeping as many as its budget allows. What it writes
//! does not depend on how many it keeps: each field's rows are those a
//! session observing that field alone writes, whose one set of grids
//! `predict` prepares, bit for bit, and a budget that holds one set writes
//! the same MeasurementSet as one that holds every field's.

mod pass_summary;
mod predict_support;

use std::path::Path;

use casa_simulation::{Simulator, VoltagePatternSpec};
use pass_summary::{PassSummary, capture};
use predict_support::{
    bits, extended_model, predict_spec, read_main, resources, scan, session, write_model_of,
};

/// The fields observed, in two passes of one scan each.
const FIELDS: [&str; 2] = ["target", "offset"];
const PASSES: usize = 2;
/// Seconds of each scan.
const SCAN_S: f64 = 60.0;

/// The scans' (field, start) in observing order: the passes in turn.
fn visits() -> Vec<(&'static str, f64)> {
    (0..PASSES)
        .flat_map(|pass| {
            FIELDS
                .iter()
                .enumerate()
                .map(move |(index, field)| (*field, (pass * FIELDS.len() + index) as f64 * SCAN_S))
        })
        .collect()
}

/// A session at `path` within `memory` bytes observing `visits`, under the
/// default voltage pattern, predicting `model`; the pass's summary once it
/// closes, or the refusal of `predict` or `close`.
fn simulate(
    path: &Path,
    model: &Path,
    memory: u64,
    visits: &[(&str, f64)],
) -> Result<PassSummary, casa_simulation::SimulatorError> {
    let mut sm: Simulator = session(path, "ALMA", "XX YY", resources(2, memory));
    for (field, start) in visits {
        sm.observe(&scan(field, *start, start + SCAN_S))
            .expect("observe");
    }
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    sm.predict(&predict_spec(model, false))?;
    let (closed, summary) = capture(|| sm.close());
    closed?;
    Ok(summary.expect("the pass's summary"))
}

/// Each field's rows of a two-pass mosaic under the beam are, bit for bit,
/// the rows a session observing that field alone at the same times writes,
/// whose grids `predict` prepares: the per-field grids `close` prepares are
/// the same grids.
#[test]
fn each_fields_rows_are_those_of_a_session_of_that_field_alone() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let mosaic = directory.path().join("mosaic.ms");
    let summary = simulate(&mosaic, &model, 2 << 30, &visits()).expect("the mosaic closes");
    assert_eq!(
        summary.grid_preparations,
        FIELDS.len() as u64,
        "a budget for every field prepares each once"
    );
    let (rows, _) = read_main(&mosaic);
    for (index, field) in FIELDS.iter().enumerate() {
        let alone = directory.path().join(format!("{field}.ms"));
        let visits: Vec<_> = visits()
            .into_iter()
            .filter(|(visited, _)| visited == field)
            .collect();
        let summary = simulate(&alone, &model, 2 << 30, &visits).expect("one field closes");
        assert_eq!(summary.grid_preparations, 0, "predict prepared one field's");
        let (expected, _) = read_main(&alone);
        let observed: Vec<_> = rows.iter().filter(|row| row.field == index).collect();
        assert_eq!(observed.len(), expected.len(), "{field}: rows");
        assert!(
            bits(observed.iter().map(|row| &row.data))
                == bits(expected.iter().map(|row| &row.data)),
            "{field}: the mosaic's rows are the field's alone"
        );
    }
}

/// Pixels a side of the eviction test's model: a set of its grids, about
/// 40 MB, is more than `close`'s writer leaves beside it at the least
/// budget, where the writer takes half of what is free.
const LARGE: usize = 1_700;

/// The least budget that closes the mosaic of a [`LARGE`] model holds one
/// field's grids, so every visit prepares its field's again, as CASA's
/// `GridFT` transforms the model at every field change; the
/// MeasurementSet is bit for bit the one a budget for every field writes.
#[test]
fn a_budget_for_one_fields_grids_prepares_each_visit_and_writes_the_same_data() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let centre = LARGE / 2;
    let model = write_model_of(
        directory.path(),
        "large.image",
        LARGE,
        &[vec![
            (centre, centre, 1.5),
            (centre + 9, centre - 4, 0.75),
            (centre - 30, centre + 20, 0.5),
        ]],
    );
    let roomy = directory.path().join("roomy.ms");
    let summary = simulate(&roomy, &model, 2 << 30, &visits()).expect("the mosaic closes");
    assert_eq!(summary.grid_preparations, FIELDS.len() as u64);
    // A set's bytes, from what the roomy budget's cache held: every field's.
    let set = summary.grid_bytes / FIELDS.len() as u64;
    // The least budget that closes, to a tenth of a set.
    let tight = directory.path().join("tight.ms");
    let closes = |memory: u64| {
        let closed = simulate(&tight, &model, memory, &visits());
        if let Err(error) = &closed {
            assert!(
                error.admission_refused().is_some(),
                "only the budget refuses: {error}"
            );
        }
        let _ = std::fs::remove_dir_all(&tight);
        closed.ok()
    };
    let (mut refused, mut closed) = (0_u64, 2_u64 << 30);
    while closed - refused > set / 10 {
        let middle = refused + (closed - refused) / 2;
        if closes(middle).is_some() {
            closed = middle;
        } else {
            refused = middle;
        }
    }
    let summary = simulate(&tight, &model, closed, &visits()).expect("the least budget closes");
    println!("the least budget, {closed} bytes: {summary:?}");
    assert_eq!(
        summary.grid_preparations,
        visits().len() as u64,
        "one field's grids at a time prepares every visit"
    );
    let (roomy, _) = read_main(&roomy);
    let (tight, _) = read_main(&tight);
    assert!(
        bits(roomy.iter().map(|row| &row.data)) == bits(tight.iter().map(|row| &row.data)),
        "the budget changes nothing written"
    );
}
