// SPDX-License-Identifier: LGPL-3.0-or-later

//! Test-only reference and controls for the T51 array ownership transfer.

use super::*;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};

static PROCESS_COPY_CONTROL: AtomicBool = AtomicBool::new(false);

thread_local! {
    static COPY_CONTROL: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn copy_control_enabled() -> bool {
    COPY_CONTROL.get() || PROCESS_COPY_CONTROL.load(Ordering::Relaxed)
}

fn copying_control<T>(operation: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            COPY_CONTROL.set(self.0);
        }
    }
    let _reset = Reset(COPY_CONTROL.replace(true));
    operation()
}

fn metadata() -> (tempfile::TempDir, KernelMetadata) {
    let root = tempfile::TempDir::new().unwrap();
    super::tests::write_test_cache(root.path());
    let (_, metadata) = read_metadata(&root.path().join("CFS_one.im")).unwrap();
    (root, metadata)
}

#[test]
fn ownership_transfer_retains_allocation_and_exact_component_bits() {
    let (_root, metadata) = metadata();
    let finite = [
        0.0_f32,
        -0.0,
        f32::MAX,
        f32::MIN,
        f32::MIN_POSITIVE,
        f32::from_bits(1),
        -1.25,
    ];
    let values = (0..metadata.shape[0] * metadata.shape[1])
        .map(|index| {
            Complex32::new(
                finite[index % finite.len()],
                finite[(index + 1) % finite.len()],
            )
        })
        .collect::<Vec<_>>();
    let expected = values
        .iter()
        .map(|value| (value.re.to_bits(), value.im.to_bits()))
        .collect::<Vec<_>>();
    let pointer = values.as_ptr();
    let plane = Array2::from_shape_vec(metadata.shape, values).unwrap();
    let taps = take_plane_storage(kernel_layout(&metadata).unwrap(), plane).unwrap();
    assert_eq!(
        pointer,
        taps.as_ptr(),
        "ownership extraction must not allocate/copy"
    );
    assert_eq!(
        expected,
        taps.iter()
            .map(|value| (value.re.to_bits(), value.im.to_bits()))
            .collect::<Vec<_>>()
    );
    let plane = Array2::from_shape_vec(metadata.shape, taps).unwrap();
    let reference = copying_control(|| {
        adapt_kernel_from_plane(kernel_layout(&metadata).unwrap(), plane.clone())
    })
    .unwrap();
    let candidate = adapt_kernel_from_plane(kernel_layout(&metadata).unwrap(), plane).unwrap();
    assert_eq!(candidate, reference);
    assert_eq!(format!("{candidate:?}"), format!("{reference:?}"));
}

#[test]
fn ownership_transfer_rejects_unexpected_dimensions_layout_offset_and_backing() {
    let (_root, metadata) = metadata();
    let [nx, ny] = metadata.shape;
    let value = Complex32::new(1.0, -0.0);
    let cases = [
        Array2::from_elem((nx / 2, ny * 2), value),
        Array2::from_elem((nx, ny), value).reversed_axes(),
        Array2::from_elem((nx + 1, ny), value).slice_move(ndarray::s![1.., ..]),
        Array2::from_elem((nx + 1, ny), value).slice_move(ndarray::s![..nx, ..]),
    ];
    for plane in cases {
        assert!(adapt_kernel_from_plane(kernel_layout(&metadata).unwrap(), plane).is_err());
    }
}

#[test]
fn ownership_transfer_preserves_nonfinite_rejection() {
    let (_root, metadata) = metadata();
    for invalid in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        for value in [Complex32::new(invalid, 0.0), Complex32::new(0.0, invalid)] {
            let plane = Array2::from_elem(metadata.shape, value);
            assert!(
                copying_control(|| adapt_kernel_from_plane(
                    kernel_layout(&metadata).unwrap(),
                    plane.clone()
                ))
                .is_err()
            );
            assert!(adapt_kernel_from_plane(kernel_layout(&metadata).unwrap(), plane).is_err());
        }
    }
}

#[allow(dead_code, unused_imports)]
mod application_fixture {
    use crate as casa_imaging_application;
    use std::{
        path::{Path, PathBuf},
        sync::Mutex,
    };

    use casa_coordinates::{
        CoordinateModel, CoordinateSystem, DirectionCoordinate, LinearCoordinate, Projection,
        ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
    };
    use casa_images::PagedImage;
    use casa_imaging_application::{
        ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumAwProjection, ContinuumBeamPolicy,
        ContinuumImagingRequest, ContinuumMask, ContinuumMaskBox, ContinuumStopReason,
        ContinuumWeighting, SpectralImagingMode, TaskRequirement, VisibilityContinuumSubtraction,
        execute_continuum, resource_policy_for_task_requirements,
    };
    use casa_imaging_model::{
        ImageDomainRole, ProductBeamRule, ProductRole, ProductTerm, ProductUnit,
        ProductValidityRule,
    };
    use casa_imaging_runtime::{
        ArtifactDisposition, ArtifactRole, ClaimLifetime, FenceId, FenceKind, IoBufferKind,
        LeaseResource, ReceiptStatus, StorageUseKind,
    };
    use casa_ms::{
        CubeAxisConfig, CubeAxisValue, MeasurementSet, MeasurementSetBuilder, OptionalMainColumn,
        SubtableId, VisibilityDataColumn,
        column_def::{ColumnDef, ColumnKind},
        initialize_measurement_set_owner_manifest, schema,
    };
    use casa_types::{
        ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
        measures::frequency::FrequencyRef,
    };
    use ndarray::ArrayD;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/common/continuum_fixture.rs"
    ));
}

#[test]
fn ownership_transfer_fixture_preserves_paired_uv_coverage_for_unequal_sizes() {
    let root = tempfile::TempDir::new().unwrap();
    let cache = root.path().join("unequal-cache");
    write_control_cache(&cache, 320, 200);
    let catalog =
        CasaAwCache::open(&cache).expect("resized control must pass real cache validation");
    assert_eq!(catalog.entries.len(), 4);
    for entry in catalog.entries.values() {
        assert!(same_world_window(&entry.imaging, &entry.weight));
        let mut broken_weight = entry.weight.clone();
        broken_weight.uv.increment = [(-1.0_f64).to_bits(), 1.0_f64.to_bits()];
        assert!(
            !same_world_window(&entry.imaging, &broken_weight),
            "retained fixed-spacing defect must be detected"
        );
    }
}

fn write_control_cache(cache: &Path, imaging_extent: usize, weight_extent: usize) {
    fs::create_dir(cache).unwrap();
    for (frequency_suffix, frequency) in [("40ghz", 40.0e9), ("48ghz", 48.0e9)] {
        for (polarization_suffix, mueller) in [("rr", 0), ("ll", 15)] {
            for (weight, extent, value) in [
                (false, imaging_extent, Complex32::new(3.0, -1.0)),
                (true, weight_extent, Complex32::new(7.0, 2.0)),
            ] {
                // The original 16/32 fixture spans 32 UV units in both planes.
                let spacing = 32.0 / extent as f64;
                application_fixture::write_aw_sized_test_cell(
                    cache,
                    &format!(
                        "{}_{polarization_suffix}_{frequency_suffix}.im",
                        if weight { "WTCFS" } else { "CFS" }
                    ),
                    weight,
                    [-spacing, spacing],
                    mueller,
                    frequency,
                    value,
                    extent,
                );
            }
        }
    }
}

fn normal_fingerprint(normal: &casa_imaging_reconstruction::FinalNormalState) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for values in [normal.residual(), normal.normal_approximation()] {
        digest.update((values.len() as u64).to_le_bytes());
        for value in values {
            digest.update(value.re.to_bits().to_le_bytes());
            digest.update(value.im.to_bits().to_le_bytes());
        }
    }
    for values in [normal.sensitivity(), normal.sum_weights()] {
        digest.update((values.len() as u64).to_le_bytes());
        for value in values {
            digest.update(value.to_bits().to_le_bytes());
        }
    }
    format!("{:x}", digest.finalize())
}

#[test]
fn ownership_transfer_dirty_fingerprint_uses_complete_normal_arrays() {
    if std::env::var_os("CASA_RS_T51_DIRTY_FINGERPRINT_CHILD").is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "aw_cache::ownership_transfer_probe::ownership_transfer_dirty_fingerprint_uses_complete_normal_arrays",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("CASA_RS_T51_DIRTY_FINGERPRINT_CHILD", "1")
            .env("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "3000000000")
            .env("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "3000000000")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    let root = tempfile::TempDir::new().unwrap();
    let ms = application_fixture::vla_aw_measurement_set(root.path());
    let cache = root.path().join("aw-cache");
    write_control_cache(&cache, 16, 32);
    let mut request = application_fixture::request(
        ms,
        root.path().join("dirty"),
        crate::ContinuumAlgorithm::Dirty,
    );
    request.aw_projection = Some(application_fixture::aw_projection(cache, false));
    let result = crate::execute_continuum(request).unwrap();
    let normal = result.outcome.output.scientific.normal_state();
    assert!(normal.coefficient_term_count() > 0);
    assert!(normal.normal_moment_count() > 0);
    assert!(normal.coefficient_term(0).is_none());
    assert!(normal.normal_moment(0).is_none());
    assert!(!normal.residual().is_empty());
    assert!(!normal.normal_approximation().is_empty());
    assert!(!normal.sensitivity().is_empty());
    assert!(!normal.sum_weights().is_empty());
    assert_eq!(normal_fingerprint(normal).len(), 64);
}

#[test]
#[ignore = "T51 ownership-transfer controls require the authorized outer 900-second guard"]
fn t51_ownership_transfer_provider_controls() {
    let raw = PathBuf::from(std::env::var_os("CASA_RS_VLASS_CF_CACHE").unwrap());
    let root = PathBuf::from(std::env::var_os("CASA_RS_T51_TRANSFER_CONTROL_ROOT").unwrap());
    fs::create_dir(&root).unwrap();
    let catalog = CasaAwCache::open(raw).unwrap();
    for payload_bytes in [1_139_200_usize, 5_939_200, 19_302_400] {
        let entry = catalog
            .entries
            .values()
            .find(|entry| {
                (entry.imaging.shape[0] * entry.imaging.shape[1]
                    + entry.weight.shape[0] * entry.weight.shape[1])
                    * 8
                    == payload_bytes
            })
            .expect("predeclared size must exist in the observed source catalog");
        assert_eq!(entry.imaging.shape[0], entry.imaging.shape[1]);
        assert_eq!(entry.weight.shape[0], entry.weight.shape[1]);
        let fixture_root = root.join(payload_bytes.to_string());
        fs::create_dir(&fixture_root).unwrap();
        let ms = application_fixture::vla_aw_measurement_set(&fixture_root);
        let cache = fixture_root.join("aw-cache");
        write_control_cache(&cache, entry.imaging.shape[0], entry.weight.shape[0]);
        CasaAwCache::open(&cache).expect("validate controlled catalog before measured cohorts");
        let mut expected = None;
        for (trial, copying) in [true, false, false, true, true, false]
            .into_iter()
            .enumerate()
        {
            let mut request = application_fixture::request(
                ms.clone(),
                fixture_root.join(format!("trial-{trial}")),
                crate::ContinuumAlgorithm::Dirty,
            );
            let mut aw = application_fixture::aw_projection(cache.clone(), false);
            aw.resident_bytes = 402_653_184;
            request.aw_projection = Some(aw);
            request.task_requirements = vec![
                crate::TaskRequirement::AwProjection,
                crate::TaskRequirement::SerialCpu,
            ];
            request.resource_policy =
                crate::resource_policy_for_task_requirements(&request.task_requirements);
            eprintln!(
                "t51_transfer_begin payload_bytes={payload_bytes} trial={trial} copying={copying}"
            );
            PROCESS_COPY_CONTROL.store(copying, Ordering::Relaxed);
            let result = crate::execute_continuum(request);
            PROCESS_COPY_CONTROL.store(false, Ordering::Relaxed);
            let result = result.expect("canonical controlled AW application execution");
            let normal = result.outcome.output.scientific.normal_state();
            let digest = normal_fingerprint(normal);
            if let Some(expected) = &expected {
                assert_eq!(&digest, expected);
            } else {
                expected = Some(digest.clone());
            }
            eprintln!(
                "t51_transfer_complete payload_bytes={payload_bytes} trial={trial} copying={copying} normal_sha256={digest}"
            );
        }
    }
}
