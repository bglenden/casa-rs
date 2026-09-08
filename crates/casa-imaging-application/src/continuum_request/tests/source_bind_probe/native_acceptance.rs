// SPDX-License-Identifier: LGPL-3.0-or-later

//! T52 native-source acceptance using the frozen four-SPW, nine-field request.

use super::*;
use casa_imaging_runtime::{ArtifactDisposition, ArtifactRole, ReceiptFailureKind, ReceiptStatus};

#[test]
fn t52_native_w_plane_preflight_precedes_source_io() {
    for (planes, expected) in [
        (None, "at least two W planes"),
        (Some(0), "at least two W planes"),
        (Some(1), "at least two W planes"),
        (Some(usize::MAX), "explicit cell-count bound"),
    ] {
        let mut request = full_aw_request(
            PathBuf::from("absent.ms"),
            PathBuf::new(),
            PathBuf::from("unused-image"),
        );
        let aw = request.aw_projection.as_mut().unwrap();
        aw.w_plane_count = planes;
        aw.source = ContinuumAwCfSource::NativeEvla(NativeEvlaAwCache {
            root: PathBuf::from("unused-cache"),
            surface: PathBuf::from("absent.surface"),
            policy: NativeAwCachePolicy::ReuseOnly,
            working_size: 64,
            oversampling: 4,
            cache_bytes: 1 << 20,
            maximum_cells: 16,
        });
        let error = prepare(request).err().expect("invalid W count");
        assert!(error.to_string().contains(expected));
    }
}

fn required_path(name: &str) -> PathBuf {
    let path = PathBuf::from(std::env::var_os(name).expect(name));
    assert!(path.is_absolute());
    path
}

fn request() -> ContinuumImagingRequest {
    let role = std::env::var("CASA_RS_T52_ACCEPTANCE_ROLE").unwrap();
    assert!(matches!(role.as_str(), "dirty" | "clean"));
    let mut request = full_aw_request(
        required_path("CASA_RS_T52_MS"),
        PathBuf::new(),
        required_path("CASA_RS_T52_OUTPUT_PREFIX"),
    );
    request.image_size = 512;
    request.field_ids = Some((1516..=1524).collect());
    request.phase_center_field = Some(1520);
    request.spectral_window = Some("2,7,12,17".to_string());
    request.cycle_iterations = 2000;
    if role == "clean" {
        request.iterations = 30;
        request.mask = ContinuumMask::Boxes(vec![ContinuumMaskBox {
            blc: [272, 332],
            trc: [335, 395],
        }]);
    }
    request.aw_projection.as_mut().unwrap().source =
        ContinuumAwCfSource::NativeEvla(NativeEvlaAwCache {
            root: required_path("CASA_RS_T52_NATIVE_CACHE"),
            surface: required_path("CASA_RS_T52_SURFACE"),
            policy: if role == "dirty" {
                NativeAwCachePolicy::GenerateMissing
            } else {
                NativeAwCachePolicy::ReuseOnly
            },
            working_size: 2048,
            oversampling: 20,
            cache_bytes: 2 << 30,
            maximum_cells: 256,
        });
    request
}

#[test]
#[ignore = "requires the frozen VLASS MS and explicit surface; metadata-only preflight"]
fn t52_native_request_matches_frozen_casa_axes() {
    let prepared = prepare(request()).unwrap();
    let selection = prepared.observation.selection();
    let samples = selection.rows().selected_row_count() * 64 * 4;
    assert!(
        samples >= 1_000_000,
        "representative sample floor: {samples}"
    );
    assert_eq!(selection.spectral_windows().len(), 4);
    let native = prepared.native.unwrap();
    let crate::ApplicationAwSource::NativeEvla { input: request, .. } =
        native.aw_preparation.unwrap().source
    else {
        panic!("native source")
    };
    let frequencies = request
        .frequencies
        .iter()
        .map(|group| group.cf_frequency_hz)
        .collect::<Vec<_>>();
    eprintln!(
        "t52_native_request_preflight {}",
        serde_json::json!({
            "selected_samples": samples, "frequencies_hz": frequencies,
            "pa_rad": request.pa_values, "w_increment": request.w_increment,
            "w_values": request.w_values, "reference_frequency_hz": request.reference_frequency_hz,
            "working_sky_increment_rad": request.grid.sky_increment_rad,
        })
    );
    assert_eq!(frequencies, [2.091e9, 2.731e9, 3.371e9, 4.011e9]);
    assert_eq!(request.mueller_elements, [0, 15]);
    assert_eq!(request.pa_values.len(), 1);
    assert!((request.pa_values[0] - 0.9967240691184998).abs() < 1e-7);
    assert!((request.w_values[1] - 89.43149767910874).abs() < 1e-9);
    assert_eq!(request.grid.size, 2048);
    assert_eq!(request.grid.oversampling, 20);
    assert!((request.grid.sky_increment_rad[1] - 0.000014544410433286079).abs() < 1e-18);
}

#[test]
#[ignore = "requires frozen VLASS inputs and an external resource/time supervisor"]
fn t52_native_evla_representative_full_products() {
    let started = Instant::now();
    let role = std::env::var("CASA_RS_T52_ACCEPTANCE_ROLE").unwrap();
    let request = request();
    let prefix = request.image_name.clone();
    assert!(!prefix.exists());
    let mut prepared = prepare(request).unwrap();
    let selection = prepared.observation.selection();
    let selected_rows = selection.rows().selected_row_count();
    let selected_samples = selected_rows * 64 * 4;
    assert!(selected_samples >= 1_000_000);
    assert_eq!(selection.spectral_windows().len(), 4);
    assert!(
        selection
            .spectral_windows()
            .iter()
            .all(|spw| spw.channel_indices().len() == 64)
    );
    assert!(
        selection
            .correlations()
            .iter()
            .all(|pol| pol.products().len() == 4)
    );
    let native = prepared.native.as_mut().unwrap();
    native.runtime.resource_policy = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: native
            .runtime
            .authority
            .topology()
            .memory_domains
            .iter()
            .map(|domain| (domain.id.clone(), 16 << 30))
            .collect(),
        workers: Some(1),
        ..ResourceOverride::default()
    });
    eprintln!(
        "t52_native_full_products_start {}",
        serde_json::json!({
            "role": role, "output_prefix": prefix, "selected_rows": selected_rows,
            "selected_correlation_channel_samples": selected_samples,
            "image_size": 512, "fields": 9, "spectral_windows": 4,
            "channels_per_spw": 64, "workers": 1, "admission_bytes": 16_u64 << 30,
        })
    );
    let output = crate::execute(prepared).unwrap().output;
    for receipt in [&output.initial_receipt, &output.publication_receipt]
        .into_iter()
        .chain(output.final_major_receipt.iter())
    {
        assert_eq!(receipt.status(), ReceiptStatus::Completed);
        assert_eq!(receipt.initial_execution_knobs().workers, 1);
    }
    let expected_cold = match std::env::var("CASA_RS_T52_EXPECT_COLD").unwrap().as_str() {
        "1" => role == "dirty",
        "0" => false,
        _ => panic!("CASA_RS_T52_EXPECT_COLD must be 0 or 1"),
    };
    assert_eq!(
        output.aw_preparation_receipts.len(),
        if expected_cold { 2 } else { 1 }
    );
    for (index, receipt) in output.aw_preparation_receipts.iter().enumerate() {
        if expected_cold && index == 0 {
            assert_eq!(receipt.status(), ReceiptStatus::Failed);
            assert_eq!(
                receipt.failure_kind(),
                Some(ReceiptFailureKind::EvidenceContract)
            );
            let cells = receipt
                .artifact_identities()
                .into_iter()
                .filter(|artifact| receipt.artifact_role(*artifact) == Some(ArtifactRole::Cache))
                .collect::<Vec<_>>();
            assert_eq!(cells.len(), 256);
            assert!(
                cells
                    .into_iter()
                    .all(|cell| receipt.artifact_disposition(cell)
                        == Some(ArtifactDisposition::RejectedStale))
            );
        } else {
            assert_eq!(receipt.status(), ReceiptStatus::Completed);
        }
    }
    assert_eq!(
        output.scientific.normal_state().sample_count(),
        selected_samples
    );
    if role == "dirty" {
        assert_eq!(output.total_actual_minor_iterations, 0);
        assert!(output.final_major_receipt.is_none());
    } else {
        assert!(output.total_actual_minor_iterations > 1);
        assert!(output.final_major_receipt.is_some());
        assert!(
            output
                .scientific
                .final_model()
                .samples()
                .iter()
                .any(|sample| sample.value().value() != 0.0)
        );
    }
    let products = output
        .products
        .members()
        .iter()
        .map(|member| {
            let path = PathBuf::from(format!("{}{}", prefix.display(), member.name()));
            assert!(path.is_dir());
            serde_json::json!({"name": member.name(), "path": path})
        })
        .collect::<Vec<_>>();
    assert_eq!(products.len(), if role == "dirty" { 18 } else { 19 });
    eprintln!(
        "t52_native_full_products_complete {}",
        serde_json::json!({
            "role": role, "output_prefix": prefix, "selected_rows": selected_rows,
            "selected_correlation_channel_samples": selected_samples,
            "workers": 1, "execution_seconds": started.elapsed().as_secs_f64(),
            "major_cycles": output.major_cycle_count,
            "minor_iterations": output.total_minor_iterations,
            "actual_minor_iterations": output.total_actual_minor_iterations,
            "products": products,
        })
    );
}
