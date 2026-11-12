// SPDX-License-Identifier: LGPL-3.0-or-later
//! Wide applied tables charge their values as well as their row metadata (#1053).
use super::apply::{check, measured};
use super::{ANTENNAS, charge_process_caches, configured, resources, scan};
use casa_calibration::{
    SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
};
use casa_simulation::ApplySpec;
use num_complex::Complex32;

#[test]
fn wide_applied_tables_and_terms_hold_their_admission() {
    if !super::alone("apply_frequency::wide_applied_tables_and_terms_hold_their_admission") {
        return;
    }
    const BUDGET: u64 = 256 << 20;
    const CHANNELS: usize = 4096;
    let resources = resources(BUDGET);
    charge_process_caches(resources.host(), resources.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    let reference_path = root.path().join("reference.ms");
    let mut reference = configured(&reference_path, &measures, BUDGET, CHANNELS);
    reference.observe(&scan(10)).unwrap();
    reference.close().unwrap();
    let table = root.path().join("wide.cal");
    let rows = (0..16).flat_map(|index| {
        (0..ANTENNAS as i32).map(move |antenna| SimulatedSolution {
            time_s: 5.0e9 + 10.0 * index as f64,
            field_id: 0,
            spectral_window_id: 0,
            antenna,
            values: SolutionValues::Channels(
                (0..CHANNELS)
                    .map(|channel| Complex32::new(1.0, 0.0001 * channel as f32))
                    .collect(),
            ),
        })
    });
    write_simulated_jones_table(
        &casa_ms::MeasurementSet::open(&reference_path).unwrap(),
        &table,
        SimulatedJones::Troposphere,
        rows,
    )
    .unwrap();
    let apply = ApplySpec {
        table: table.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.0,
        interp: "linear".to_string(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.0,
    };
    let session = |name: &str| {
        let mut sm = configured(&root.path().join(name), &measures, BUDGET, CHANNELS);
        sm.observe(&scan(48)).unwrap();
        sm
    };
    let mut warm = session("warm.ms");
    if warm.set_apply(&apply).is_ok() {
        warm.corrupt().unwrap();
    }
    drop(warm);
    let mut sm = session("measured.ms");
    let mut result = None;
    let loaded = measured(|| result = Some(sm.set_apply(&apply)));
    // Inspect peak admission before requiring success so the original
    // loader's under-charge is independently observable even though Tf
    // application was not implemented at that revision.
    check("wide setapply", &loaded);
    result.unwrap().unwrap();
    let corrupted = measured(|| sm.corrupt().unwrap());
    check("wide corrupt", &corrupted);
}
