// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded production-path phase counts with a fixed retained-history seed.

use super::*;

#[test]
#[ignore = "T51 release-only catalog scale control; external 120s / 2GiB guard required"]
#[allow(clippy::assertions_on_constants)]
fn t51_cold_catalog_receipt_boundaries() {
    assert!(!cfg!(debug_assertions));
    assert!(std::env::var_os("CASA_RS_T51_RECEIPT_BOUNDARY_PROBE").is_some());
    for count in [1, 32, 1024] {
        let root = TempDir::new().expect("catalog scale root");
        let profile = ProductionStorageProfile::new(
            root.path(),
            1 << 30,
            1 << 30,
            100 << 20,
            100 << 20,
            2,
            4,
        )
        .expect("scale storage")
        .with_measured_operations_rate(root.path())
        .expect("measured operations");
        let mut runtime = runtime(root.path(), &profile);
        let seed_casa = root.path().join("seed-casa");
        std::fs::create_dir(&seed_casa).expect("seed source");
        crate::aw_cache::tests::write_test_cache(&seed_casa);
        let seed = prepare_aw_projection(
            &problem(),
            ApplicationAwPreparation {
                casa_cache: seed_casa,
                private_root: root.path().join("seed-prepared"),
                storage_domain: profile.storage_domain(),
                resident_bytes: 1 << 20,
                conjugate_beams: true,
            },
            &runtime,
        )
        .expect("two terminal history seed receipts");
        assert_eq!(seed.receipts.len(), 2);
        let history = seed
            .receipts
            .iter()
            .map(|receipt| {
                let path = runtime
                    .receipts
                    .root_path()
                    .join(format!("{}.receipt.json", receipt.attempt_id()));
                (
                    path.clone(),
                    std::fs::read(path).expect("seed receipt bytes"),
                )
            })
            .collect::<Vec<_>>();
        drop(seed);
        runtime.attempts = [
            ExecutionAttemptId::from_sha256([10; 32]),
            ExecutionAttemptId::from_sha256([11; 32]),
            ExecutionAttemptId::from_sha256([12; 32]),
        ];
        let casa = root.path().join("casa");
        std::fs::create_dir(&casa).expect("source root");
        crate::aw_cache::tests::write_catalog_test_cache(&casa, count);
        eprintln!("t51_catalog_scale_begin cells={count}");
        let started = std::time::Instant::now();
        let phase = prepare_aw_projection(
            &problem(),
            ApplicationAwPreparation {
                casa_cache: casa,
                private_root: root.path().join("prepared"),
                storage_domain: profile.storage_domain(),
                resident_bytes: 1 << 20,
                conjugate_beams: true,
            },
            &runtime,
        )
        .expect("cold catalog");
        assert_eq!(phase.receipts.len(), 2);
        let loaded = &phase.receipts[1];
        assert_eq!(
            loaded.status(),
            casa_imaging_runtime::ReceiptStatus::Completed
        );
        assert_eq!(loaded.plan_node_identities().len(), 6);
        assert_eq!(
            loaded
                .artifact_identities()
                .into_iter()
                .filter(|identity| loaded.artifact_role(*identity)
                    == Some(casa_imaging_runtime::ArtifactRole::Prepared)
                    && loaded.artifact_disposition(*identity)
                        == Some(casa_imaging_runtime::ArtifactDisposition::Loaded))
                .count(),
            count
        );
        let receipt_bytes = phase
            .receipts
            .iter()
            .map(|receipt| {
                std::fs::metadata(
                    runtime
                        .receipts
                        .root_path()
                        .join(format!("{}.receipt.json", receipt.attempt_id())),
                )
                .expect("receipt bytes")
                .len()
            })
            .sum::<u64>();
        for (path, bytes) in history {
            assert_eq!(std::fs::read(path).expect("retained history"), bytes);
        }
        eprintln!(
            "t51_catalog_scale_complete cells={count} receipts=2 nodes=6 receipt_bytes={receipt_bytes} seconds={:.9}",
            started.elapsed().as_secs_f64()
        );
        drop(phase.bind_plan().expect("completed catalog reader"));
    }
}
