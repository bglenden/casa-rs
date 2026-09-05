// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in, read-only timing discriminator for the existing cold CF serializer.

use super::{
    Complex32, KernelMetadata, LoadedCasaPlane, PagedImage, encode_complex32_range, read_metadata,
};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, process::Command, time::Instant};

const CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MAX_COHORT_BYTES: usize = 64 * 1024 * 1024;

fn bulk_reference(metadata: &KernelMetadata) -> (Vec<u8>, f64, f64, f64) {
    let started = Instant::now();
    let image = PagedImage::<Complex32>::open(&metadata.path).unwrap();
    let open_seconds = started.elapsed().as_secs_f64();
    let started = Instant::now();
    let pixels = image
        .get_slice(&[0, 0, 0, 0], &[metadata.shape[0], metadata.shape[1], 1, 1])
        .unwrap();
    let read_seconds = started.elapsed().as_secs_f64();
    drop(image);
    let mut bytes = Vec::with_capacity(pixels.len() * 8);
    let started = Instant::now();
    for value in &pixels {
        assert!(value.re.is_finite() && value.im.is_finite());
        bytes.extend_from_slice(&value.re.to_le_bytes());
        bytes.extend_from_slice(&value.im.to_le_bytes());
    }
    let encode_seconds = started.elapsed().as_secs_f64();
    assert!(
        pixels
            .iter()
            .any(|value| value.re != 0.0 || value.im != 0.0)
    );
    (bytes, open_seconds, read_seconds, encode_seconds)
}

fn production_payload(metadata: &KernelMetadata) -> (Vec<u8>, f64, f64, u64) {
    let started = Instant::now();
    let mut loaded = LoadedCasaPlane {
        name: "encoding-probe",
        image: PagedImage::<Complex32>::open(&metadata.path).unwrap(),
        last: None,
    };
    let open_seconds = started.elapsed().as_secs_f64();
    let mut bytes = vec![0; metadata.shape[0] * metadata.shape[1] * 8];
    let mut reads = 0;
    let started = Instant::now();
    for (index, chunk) in bytes.chunks_mut(CHUNK_BYTES).enumerate() {
        reads += encode_complex32_range(
            &mut loaded,
            metadata.shape,
            (index * CHUNK_BYTES) as u64,
            chunk,
        )
        .unwrap();
    }
    (bytes, open_seconds, started.elapsed().as_secs_f64(), reads)
}

#[test]
#[ignore = "requires explicit full-resolution CF planes; run the release binary with a 55s external timeout"]
fn t51_cf_encoding_discriminator() {
    let started = Instant::now();
    let root = PathBuf::from(std::env::var_os("CASA_RS_VLASS_CF_CACHE").unwrap());
    let names = std::env::var("CASA_RS_T51_CF_ENCODING_PLANES").unwrap();
    let parent = std::env::var("CASA_RS_T51_CF_ENCODING_PARENT").unwrap();
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(revision.status.success());
    assert_eq!(String::from_utf8(revision.stdout).unwrap().trim(), parent);
    let metadata_started = Instant::now();
    let planes = names
        .split(',')
        .map(|name| {
            assert!(name.ends_with(".im") && !name.contains(['/', '\\']));
            let (_, metadata) = read_metadata(&root.join(name)).unwrap();
            metadata
        })
        .collect::<Vec<_>>();
    assert!(!planes.is_empty() && planes.len() <= 6);
    let plane_bytes = |metadata: &KernelMetadata| metadata.shape[0] * metadata.shape[1] * 8;
    let cohort_bytes = planes
        .iter()
        .map(|metadata| {
            let bytes = plane_bytes(metadata);
            bytes * if bytes <= CHUNK_BYTES { 2 } else { 1 }
        })
        .sum::<usize>();
    assert!(cohort_bytes <= MAX_COHORT_BYTES);
    eprintln!(
        "t51_cf_encoding_header {}",
        serde_json::json!({
            "parent_revision": parent,
            "compiled_aw_cache_sha256": format!("{:x}", Sha256::digest(include_bytes!("../aw_cache.rs"))),
            "executable": std::env::current_exe().unwrap(),
            "cache_root": root,
            "plane_names": names,
            "metadata_seconds": metadata_started.elapsed().as_secs_f64(),
            "unique_payload_bytes": planes.iter().map(plane_bytes).sum::<usize>(),
            "cohort_payload_bytes": cohort_bytes,
            "payload_order": "last-axis-contiguous-complex-f32-little-endian",
            "chunk_bytes": CHUNK_BYTES,
            "timing_class": "cache-warm diagnostic; not storage throughput",
            "native_bulk_timing": null,
            "buffer_allocation_in_encoding_timers": false,
            "production_encoding_includes_scalar_source_reads": true,
            "bulk_encoding_includes_finite_validation": true,
        })
    );
    let mut verified_bytes = 0;
    for metadata in &planes {
        let trials = if plane_bytes(metadata) <= CHUNK_BYTES {
            2
        } else {
            1
        };
        for trial in 0..trials {
            assert!(
                started.elapsed().as_secs_f64() < 50.0,
                "bounded probe deadline"
            );
            let (bulk, production) = if trial == 0 {
                (bulk_reference(metadata), production_payload(metadata))
            } else {
                let production = production_payload(metadata);
                (bulk_reference(metadata), production)
            };
            assert_eq!(production.0.len(), bulk.0.len());
            let mismatch = production
                .0
                .iter()
                .zip(&bulk.0)
                .position(|(left, right)| left != right);
            assert_eq!(
                mismatch,
                None,
                "canonical byte mismatch in {}",
                metadata.path.display()
            );
            let production_hash = Sha256::digest(&production.0);
            let reference_hash = Sha256::digest(&bulk.0);
            assert_eq!(production_hash, reference_hash);
            verified_bytes += production.0.len();
            eprintln!(
                "t51_cf_encoding_plane {}",
                serde_json::json!({
                    "path": metadata.path,
                    "shape": metadata.shape,
                    "support": metadata.support,
                    "sampling": metadata.sampling,
                    "trial": trial,
                    "trial_order": if trial == 0 { "bulk-then-production" } else { "production-then-bulk" },
                    "bytes": production.0.len(),
                    "production_open_seconds": production.1,
                    "production_read_encode_seconds": production.2,
                    "production_source_pixel_reads": production.3,
                    "bulk_open_seconds": bulk.1,
                    "bulk_source_read_calls": 1,
                    "bulk_read_seconds": bulk.2,
                    "bulk_encode_seconds": bulk.3,
                    "payload_sha256": format!("{production_hash:x}"),
                    "every_byte_equal": true,
                })
            );
        }
    }
    assert_eq!(verified_bytes, cohort_bytes);
    assert!(
        started.elapsed().as_secs_f64() < 55.0,
        "bounded probe deadline"
    );
    eprintln!(
        "t51_cf_encoding_complete {}",
        serde_json::json!({"verified_bytes": verified_bytes, "wall_seconds": started.elapsed().as_secs_f64()})
    );
}
