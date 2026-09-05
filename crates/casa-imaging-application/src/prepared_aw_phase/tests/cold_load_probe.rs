// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded real-payload cold Load evidence, using the ordinary planner and receipts.

use super::*;
use casa_images::PagedImage;
use casa_imaging_runtime::PreparedArtifactBudget;
use num_complex::Complex32;
use sha2::{Digest, Sha256};
use std::{fs, os::unix::fs::MetadataExt, path::PathBuf, process::Command, time::Instant};

#[test]
#[ignore = "requires retained full CF cache and fresh external store; release only with a 55s external timeout"]
fn t51_paired_cf_cold_load_discriminator() {
    assert!(
        !cfg!(debug_assertions),
        "timing requires a release test binary"
    );
    let started = Instant::now();
    let source = PathBuf::from(std::env::var_os("CASA_RS_VLASS_CF_CACHE").unwrap())
        .canonicalize()
        .unwrap();
    let external = PathBuf::from(std::env::var_os("CASA_RS_T51_CF_PROBE_VOLUME").unwrap())
        .canonicalize()
        .unwrap();
    let parent = std::env::var("CASA_RS_T51_CF_ENCODING_PARENT").unwrap();
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(revision.status.success());
    assert_eq!(String::from_utf8(revision.stdout).unwrap().trim(), parent);
    assert_eq!(
        fs::metadata(&source).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    assert_ne!(
        fs::metadata(external.parent().unwrap()).unwrap().dev(),
        fs::metadata(&external).unwrap().dev(),
        "scratch parent must be the external volume mount, not a retained cache/store"
    );
    assert!(source.starts_with(&external));
    assert!(
        !external.starts_with(&source),
        "scratch parent must be outside the source cache"
    );
    let root = tempfile::Builder::new()
        .prefix("t51-paired-cf-cold-load-")
        .tempdir_in(&external)
        .unwrap()
        .keep();
    let capacity: u64 = std::env::var("CASA_RS_T51_CF_VOLUME_CAPACITY_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    let available: u64 = std::env::var("CASA_RS_T51_CF_VOLUME_AVAILABLE_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    let profile = ProductionStorageProfile::new(
        &external,
        capacity,
        available,
        3_000_000_000,
        3_000_000_000,
        4,
        2,
    )
    .unwrap()
    .with_measured_operations_rate(&root)
    .unwrap();
    let mut runtime = runtime(&root, &profile);
    let executable = std::env::current_exe().unwrap();
    let executable_sha256: [u8; 32] = Sha256::digest(fs::read(&executable).unwrap()).into();
    runtime.build = BuildIdentity::from_sha256(executable_sha256);
    let problem = problem();
    let source_domain = profile.storage_domain();
    let setup_started = Instant::now();
    let cache = Arc::new(CasaAwCache::open(&source).unwrap());
    let inventory = cache.inventory();
    assert_eq!(
        (
            inventory.paired_cells,
            inventory.frequencies_hz.len(),
            inventory.w_values_lambda.len(),
            inventory.mueller_elements.as_slice(),
            inventory.parallactic_angles_deg.len()
        ),
        (1024, 16, 32, &[0, 15][..], 1)
    );
    let cache_budget_bytes = cache.prepared_cache_bytes().unwrap();
    let cache_entries = inventory.paired_cells;
    let store = Arc::new(
        PreparedArtifactStore::open(
            root.join("prepared"),
            &source_domain,
            PreparedArtifactBudget::new(cache_budget_bytes, cache_entries, 8 << 20).unwrap(),
        )
        .unwrap(),
    );
    let owner =
        crate::PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), &problem);
    let cells = cache
        .prepared_cells(&store, &owner, &runtime.implementation, &problem)
        .unwrap();
    // The source adapter orders the complete nonnegative frequency/W/Mueller/PA product.
    // Exact source-byte checks below independently bind these retained fixture ordinals.
    let selected = [
        ("small", 0, "0_0", [200, 200]),
        ("middle", 15 * 64 + 18 * 2, "15_18", [560, 560]),
        ("large", 31 * 2, "0_31", [2048, 2048]),
    ];
    eprintln!(
        "t51_paired_cf_load_header {}",
        serde_json::json!({
            "parent_revision": parent,
            "compiled_phase_sha256": format!("{:x}", Sha256::digest(include_bytes!("../../prepared_aw_phase.rs"))),
            "compiled_probe_sha256": format!("{:x}", Sha256::digest(include_bytes!("cold_load_probe.rs"))),
            "executable": executable, "executable_sha256": format!("{:x}", Sha256::digest(fs::read(&executable).unwrap())),
            "cache_root": source, "retained_probe_root": root, "receipt_root": runtime.receipts.root_path(),
            "metadata_store_setup_seconds": setup_started.elapsed().as_secs_f64(),
            "budget_streaming_bytes": 8 << 20, "effective_streaming_bytes": 64 << 10,
            "private_cache_budget_bytes": cache_budget_bytes, "private_cache_entries": cache_entries,
            "volume_capacity_bytes": capacity, "volume_available_bytes": available,
            "read_bytes_per_second": 3_000_000_000_u64, "write_bytes_per_second": 3_000_000_000_u64,
            "queue_slots": 4, "table_lock_slots": 2, "resource_policy": "Exclusive",
            "test_problem_stage_nanos": runtime.stage_nanos,
            "test_receipt_retention_entries": 32, "test_receipt_retention_bytes": 8 << 20,
            "production_receipt_retention_entries": 512,
            "source_cells_indexed_metadata_only": cells.len(),
            "payload_order": "imaging then weight; x*shape[1]+y; complex-f32 real/imag little-endian",
            "timing_class": "cold private store; OS cache uncontrolled; not storage throughput",
            "problem": "existing synthetic scalar test problem; real retained full-resolution CF payloads; no imaging",
            "native_matched_timing": null,
        })
    );
    let mut total_bytes = 0;
    for (ordinal, (label, index, stem, shape)) in selected.into_iter().enumerate() {
        assert!(
            started.elapsed().as_secs_f64() < 50.0,
            "bounded probe deadline"
        );
        let cell = cells[index].clone();
        assert_eq!(cell.descriptor().imaging_plane().unwrap().shape(), shape);
        assert_eq!(
            cell.descriptor().weight_plane().unwrap().shape(),
            [320, 320]
        );
        let node = cell
            .descriptor()
            .work_node_id(PreparedArtifactOperation::Load);
        let identity = cell.descriptor().identity();
        let entry = store.root().join("objects-v3").join(identity.to_string());
        assert!(!entry.exists(), "must be an actual cold private Load");
        let load_started = Instant::now();
        let ((_, result), receipt) = run_operation(
            &problem,
            &runtime,
            OperationInput {
                cache: Arc::clone(&cache),
                store: Arc::clone(&store),
                cell,
                source_domain: &source_domain,
                operation: PreparedArtifactOperation::Load,
                phase: 1_000_000 + ordinal as u64,
            },
        )
        .unwrap();
        let load_seconds = load_started.elapsed().as_secs_f64();
        let OperationResult::Artifact(artifact) = result else {
            panic!("cold Load returned no artifact")
        };
        assert_eq!(artifact.identity(), identity);
        let producer_nanos = receipt.stage_actual_elapsed_nanos(&node).unwrap();
        let io = IoBufferKind::ALL.into_iter().filter_map(|kind| {
            receipt.stage_actual_io(&node, kind).map(|actual| serde_json::json!({
                "kind": format!("{kind:?}"), "actual_bytes": actual.0, "actual_operations": actual.1,
                "predicted": receipt.stage_predicted_io(&node, kind),
            }))
        }).collect::<Vec<_>>();
        eprintln!(
            "t51_paired_cf_load_timing {}",
            serde_json::json!({
                "label": label, "artifact_identity": identity.to_string(), "run_operation_seconds": load_seconds,
                "receipt_producer_seconds": producer_nanos as f64 / 1e9, "io": io,
            })
        );
        let validation_started = Instant::now();
        let payload = fs::read(entry.join("payload.bin")).unwrap();
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(entry.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(manifest["identity"], identity.to_string());
        assert_eq!(manifest["payload_bytes"], payload.len() as u64);
        assert_eq!(
            manifest["payload_sha256"],
            format!("{:x}", Sha256::digest(&payload))
        );
        let mut offset = 0;
        for (segment_index, (prefix, plane_shape)) in [("CFS", shape), ("WTCFS", [320, 320])]
            .into_iter()
            .enumerate()
        {
            let plane_path = source.join(format!("{prefix}_0_0_CF_{stem}_0.im"));
            let image = PagedImage::<Complex32>::open(&plane_path).unwrap();
            let pixels = image
                .get_slice(
                    &[0, 0, 0, 0],
                    &[plane_shape[0] as usize, plane_shape[1] as usize, 1, 1],
                )
                .unwrap();
            let mut reference = Vec::with_capacity(pixels.len() * 8);
            for value in &pixels {
                assert!(value.re.is_finite() && value.im.is_finite());
                reference.extend_from_slice(&value.re.to_le_bytes());
                reference.extend_from_slice(&value.im.to_le_bytes());
            }
            let actual = &payload[offset..offset + reference.len()];
            assert!(
                actual == reference,
                "every-byte mismatch: {}",
                plane_path.display()
            );
            let digest = format!("{:x}", Sha256::digest(actual));
            assert_eq!(manifest["segments"][segment_index]["sha256"], digest);
            assert_eq!(manifest["segments"][segment_index]["offset"], offset as u64);
            eprintln!(
                "t51_paired_cf_load_payload {}",
                serde_json::json!({
                    "label": label, "path": plane_path, "shape": plane_shape, "bytes": actual.len(),
                    "sha256": digest, "every_byte_equal": true,
                })
            );
            offset += reference.len();
        }
        assert_eq!(offset, payload.len());
        assert_eq!(
            receipt.artifact_actual_bytes(identity),
            Some(payload.len() as u64)
        );
        assert_eq!(
            receipt.artifact_observed_identity(identity),
            Some(artifact.integrity_identity().as_bytes())
        );
        assert_eq!(
            receipt.stage_actual_io(&node, IoBufferKind::SourceReadAhead),
            None
        );
        let aggregate = receipt
            .stage_actual_io(&node, IoBufferKind::StorageManager)
            .unwrap();
        let predicted = receipt
            .stage_predicted_io(&node, IoBufferKind::StorageManager)
            .unwrap();
        assert!(aggregate.0 >= 2 * payload.len() as u64 && aggregate.0 <= predicted.0);
        assert!(aggregate.1 >= payload.len() as u64 / 8 + 2 && aggregate.1 <= predicted.1);
        total_bytes += payload.len();
        eprintln!(
            "t51_paired_cf_load_verified {}",
            serde_json::json!({
                "label": label, "validation_seconds_outside_load": validation_started.elapsed().as_secs_f64(),
                "payload_bytes": payload.len(), "expected_source_complex_pixel_reads": payload.len() / 8,
                "expected_source_plane_open_operations": 2, "retained_entry": entry,
            })
        );
    }
    assert_eq!(total_bytes, 38_840_832);
    assert!(
        started.elapsed().as_secs_f64() < 55.0,
        "bounded probe deadline"
    );
    eprintln!(
        "t51_paired_cf_load_complete {}",
        serde_json::json!({
            "verified_bytes": total_bytes, "wall_seconds": started.elapsed().as_secs_f64(), "retained_probe_root": root,
        })
    );
}
