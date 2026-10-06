// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
};

use super::*;

#[test]
fn t52_native_generation_and_fresh_execution_reuse_share_the_reader() {
    let root = TempDir::new().unwrap();
    let profile =
        ProductionStorageProfile::new(root.path(), 1 << 30, 1 << 30, 100 << 20, 100 << 20, 2, 4)
            .unwrap()
            .with_measured_operations_rate(root.path())
            .unwrap();
    let problem = problem();
    let mut cold_runtime = runtime(root.path(), &profile);
    let surface = EvlaDishSurface::new(
        (0..=125)
            .map(|i| {
                let radius = i as f64 / 10.0;
                [radius, radius * radius / 36.0, radius / 18.0]
            })
            .collect(),
    )
    .unwrap();
    let input = NativeAwRequestInput {
        surface,
        antenna_diameter_m: 25.0,
        frequencies: vec![NativeAwFrequencyGroup {
            spectral_window: 0,
            channel_frequencies_hz: vec![3e9],
            cf_frequency_hz: 3e9,
        }],
        w_values: vec![0.0, 100.0],
        w_increment: 0.01,
        pa_values: vec![0.31],
        mueller_elements: vec![0, 15],
        reference_frequency_hz: 3e9,
        grid: NativeAwGrid {
            size: 128,
            sky_increment_rad: [-0.0003, 0.0003],
            oversampling: 4,
        },
        terms: NativeAwTerms {
            aperture: true,
            w_term: true,
            prolate_spheroidal: false,
            wideband: true,
            conjugate_beams: true,
        },
        maximum_cells: 4,
    };
    let deployment = ApplicationAwPreparation {
        source: crate::ApplicationAwSource::NativeEvla {
            input: Box::new(input),
            policy: crate::NativeAwCachePolicy::GenerateMissing,
            cache_bytes: 16 << 20,
        },
        private_root: root.path().join("native-cache"),
        storage_domain: profile.storage_domain(),
        resident_bytes: 1 << 20,
        conjugate_beams: true,
    };
    let cold = prepare_aw_projection(&problem, deployment.clone(), &cold_runtime).unwrap();
    assert_eq!(cold.prepared.len(), 4);
    assert_eq!(cold.receipts.len(), 2);
    let generated = &cold.receipts[1];
    assert_eq!(
        generated.status(),
        casa_imaging_runtime::ReceiptStatus::Completed
    );
    assert_eq!(
        generated
            .artifact_identities()
            .into_iter()
            .filter(|identity| generated.artifact_disposition(*identity)
                == Some(ArtifactDisposition::Built))
            .count(),
        4
    );
    let identities = cold
        .prepared
        .iter()
        .map(|cell| cell.descriptor().identity())
        .collect::<Vec<_>>();
    cold.bind_plan().unwrap();
    drop(cold);
    cold_runtime.attempts[0] = ExecutionAttemptId::from_sha256([21; 32]);
    let warm = prepare_aw_projection(&problem, deployment, &cold_runtime).unwrap();
    assert_eq!(
        warm.receipts.len(),
        1,
        "warm operation must not even admit a generation phase"
    );
    assert_eq!(
        warm.prepared
            .iter()
            .map(|cell| cell.descriptor().identity())
            .collect::<Vec<_>>(),
        identities
    );
    let receipt = &warm.receipts[0];
    assert_eq!(
        receipt
            .artifact_identities()
            .into_iter()
            .filter(|identity| receipt.artifact_disposition(*identity)
                == Some(ArtifactDisposition::Reused))
            .count(),
        4
    );
    warm.bind_plan().unwrap();
}
