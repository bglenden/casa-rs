// SPDX-License-Identifier: LGPL-3.0-or-later
//! Coverage masks and preserved scratch cells stay bounded by the write batch.
#[path = "../selection/input.rs"]
mod input;
use super::{
    ALLOWANCE_BYTES, BUDGET_32, CHANNEL_HZ, FREQUENCY_HZ, REFERENCE_MJD, ROWS_ALLOWANCE_BYTES,
    alone, charge_process_caches, configured, measured_close, resources, scan,
};
use casa_simulation::{ApplySpec, NoiseSpec, Simulator, SimulatorOptions, SpectralWindowSpec};
use casa_types::measures::{EpochRef, MEpoch, frequency::FrequencyRef};

#[test]
fn partial_window_coverage_admits_batch_masks_and_preserved_scratch() {
    if !alone("coverage::partial_window_coverage_admits_batch_masks_and_preserved_scratch") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(BUDGET_32);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    let run = |integrations: usize, opened: bool| {
        let reference = root.path().join("reference.ms");
        let mut reference_sm = configured(&reference, &measures, BUDGET_32, 32);
        reference_sm.observe(&scan(1)).unwrap();
        reference_sm.close().unwrap();
        let table = root.path().join("g.cal");
        input::fixed_gains(&reference, &table, 2.0);
        let path = root.path().join("data.ms");
        let mut sm = configured(&path, &measures, BUDGET_32, 32);
        sm.observe(&scan(integrations)).unwrap();
        let apply = ApplySpec {
            table: table.to_string_lossy().into_owned(),
            kind: String::new(),
            interval_s: 0.0,
            interp: "nearest".into(),
            calwt: false,
            spwmap: vec![-1],
            opacity: 0.0,
        };
        if !opened {
            sm.set_apply(&apply).unwrap();
            sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
        }
        sm.set_spectral_window(&SpectralWindowSpec {
            name: "later".into(),
            start_frequency_hz: FREQUENCY_HZ,
            channel_width_hz: CHANNEL_HZ,
            resolution_hz: CHANNEL_HZ,
            frame: FrequencyRef::TOPO,
            channel_count: 32,
            stokes: "XX YY".into(),
        })
        .unwrap();
        let mut later = scan(integrations);
        later.spectral_window = "later".into();
        sm.observe(&later).unwrap();
        if opened {
            sm.close().unwrap();
            input::prepare(&path);
            sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: measures.clone(),
                    resources: resources(BUDGET_32),
                    now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_apply(&apply).unwrap();
        }
        if opened {
            sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
        }
        sm.corrupt().unwrap();
        let (closed, _) = measured_close(sm, &path, false);
        assert!(
            closed.heap <= closed.admitted + ALLOWANCE_BYTES,
            "opened={opened}: {closed:?}"
        );
        std::fs::remove_dir_all(reference).unwrap();
        std::fs::remove_dir_all(table).unwrap();
        closed
    };
    for opened in [false, true] {
        run(60, opened);
        let few = run(60, opened);
        let many = run(600, opened);
        let admitted_growth = many.admitted.saturating_sub(few.admitted);
        assert!(
            many.heap <= few.heap + admitted_growth + ROWS_ALLOWANCE_BYTES,
            "coverage row growth: {few:?} -> {many:?}"
        );
        println!("coverage opened={opened}: {few:?} -> {many:?}");
    }
}

#[test]
fn uncovered_frequency_windows_reserve_no_sample_grids() {
    use super::apply::{check, measured};
    use casa_calibration::{
        SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
    };
    use casa_types::Complex32;
    if !alone("coverage::uncovered_frequency_windows_reserve_no_sample_grids") {
        return;
    }
    const BUDGET: u64 = 256 << 20;
    let policy = resources(BUDGET);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    let reference = root.path().join("reference.ms");
    let mut reference_sm = configured(&reference, &measures, BUDGET, 4);
    reference_sm.observe(&scan(1)).unwrap();
    reference_sm.close().unwrap();
    let table = root.path().join("tf.cal");
    let ms = casa_ms::MeasurementSet::open(&reference).unwrap();
    write_simulated_jones_table(
        &ms,
        &table,
        SimulatedJones::Troposphere,
        (0..super::ANTENNAS).map(|antenna| SimulatedSolution {
            time_s: 5e9,
            field_id: 0,
            spectral_window_id: 0,
            antenna: antenna as i32,
            values: SolutionValues::Channels(vec![Complex32::new(2.0, 0.0); 4]),
        }),
    )
    .unwrap();
    let mut sm = configured(&root.path().join("huge-uncovered.ms"), &measures, BUDGET, 4);
    sm.observe(&scan(4)).unwrap();
    sm.set_apply(&ApplySpec {
        table: table.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.0,
        interp: "nearest,linear".into(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.0,
    })
    .unwrap();
    sm.set_spectral_window(&SpectralWindowSpec {
        name: "later".into(),
        start_frequency_hz: FREQUENCY_HZ,
        channel_width_hz: CHANNEL_HZ,
        resolution_hz: CHANNEL_HZ,
        frame: FrequencyRef::TOPO,
        channel_count: 131073,
        stokes: "XX YY".into(),
    })
    .unwrap();
    let mut later = scan(1024);
    later.spectral_window = "later".into();
    sm.observe(&later).unwrap();
    // Reserving all cells for this uncovered window would request12GiB;
    // forming the covered four-channel term needs only a few KiB.
    let formed = measured(|| sm.corrupt().unwrap());
    check("uncovered frequency window", &formed);
    drop(sm); // No need to publish the deliberately huge uncorrupted MS.
}
