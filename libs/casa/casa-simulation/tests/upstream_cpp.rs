// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scenarios from casacore's ms/MSOper/test/tNewMSSimulator.cc, as used by
//! CASA 6.7.6.14. Native ownership replaces getMs's shared pointer; the
//! independent input arrays also check persisted antenna metadata.
use std::{num::NonZeroUsize, path::Path};

use casa_core::measures::{EpochRef, MEpoch};
use casa_ms::MeasurementSet;
use casa_simulation::{
    ArrayConfig, CoordinateSystem, ExecutionControls, SessionResources, Simulator, SimulatorOptions,
};

fn opened(path: &Path) -> Simulator {
    Simulator::open(
        path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: SessionResources::from_controls(&ExecutionControls {
                memory_budget: Some(256 << 20),
                workers: NonZeroUsize::new(1),
                ..ExecutionControls::default()
            })
            .unwrap(),
            now: MEpoch::from_mjd(60_000., EpochRef::UTC),
        },
    )
}

#[test]
fn upstream_new_ms_simulator_constructors_have_independent_lifetimes() {
    let root = tempfile::tempdir().unwrap();
    let first = root.path().join("newsim");
    let second = root.path().join("newsim2");
    let simulator = opened(&first);
    let other = opened(&second);
    simulator.close().unwrap();
    let retained = MeasurementSet::open(&first).unwrap();
    other.close().unwrap();
    assert_eq!(retained.row_count(), 0);
    assert_eq!(MeasurementSet::open(&second).unwrap().row_count(), 0);
    drop(retained);
    assert!(first.join("table.dat").is_file());
    assert!(second.join("table.dat").is_file());
}

#[test]
fn upstream_new_ms_simulator_initializes_two_through_ten_antennas() {
    let root = tempfile::tempdir().unwrap();
    let mut seed = 5489u64;
    let mut sample = || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        (seed >> 11) as f64 * (100. / (1u64 << 53) as f64)
    };
    for count in 2..=10 {
        let path = root.path().join(format!("newsim_{count}"));
        let mut sm = opened(&path);
        let input = ArrayConfig {
            telescope: "Simulated".into(),
            x: (0..count).map(|_| sample()).collect(),
            y: (0..count).map(|_| sample()).collect(),
            z: (0..count).map(|_| sample()).collect(),
            dish_diameter: (0..count).map(|_| sample()).collect(),
            offset: (0..count).map(|_| sample()).collect(),
            mount: vec!["ALT-AZ".into(); count],
            antenna_names: vec!["NAME".into(); count],
            pad_names: vec!["PAD".into(); count],
            coordinate_system: CoordinateSystem::Global,
        };
        sm.set_config(&input).unwrap();
        sm.close().unwrap();
        let ms = MeasurementSet::open(&path).unwrap();
        let antennas = ms.antenna().unwrap();
        assert_eq!(antennas.row_count(), count);
        for row in 0..count {
            assert_eq!(
                antennas.position(row).unwrap(),
                [input.x[row], input.y[row], input.z[row]]
            );
            assert_eq!(
                antennas.dish_diameter(row).unwrap(),
                input.dish_diameter[row]
            );
            assert_eq!(antennas.name(row).unwrap(), "NAME");
            assert_eq!(antennas.station(row).unwrap(), "PAD");
            assert_eq!(antennas.mount(row).unwrap(), "ALT-AZ");
        }
    }
}
