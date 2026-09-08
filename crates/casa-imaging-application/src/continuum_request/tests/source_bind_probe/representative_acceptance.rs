// SPDX-License-Identifier: LGPL-3.0-or-later

//! Supervised representative T51 acceptance through the production application.

use super::*;
use casa_imaging_runtime::{
    ArtifactDisposition, ArtifactRole, ExecutionReceipt, ReceiptFailureKind, ReceiptStatus,
};

#[test]
fn t51_acceptance_role_controls_are_frozen() {
    for (name, iterations) in [("dirty", 0), ("clean", 2000)] {
        let role = AcceptanceRole::parse(name).unwrap();
        let request = role.request(
            "source".into(),
            "cache".into(),
            "output".into(),
            "mask".into(),
        );
        assert_eq!(request.iterations, iterations);
        assert_eq!(request.cycle_iterations, 2000);
        assert_eq!(request.maximum_major_cycles, None);
        assert_eq!(request.image_size, 4096);
        assert_eq!(
            request.aw_projection.as_ref().unwrap().resident_bytes,
            384 << 20
        );
        assert!(matches!(
            (&role, &request.mask),
            (AcceptanceRole::Dirty, ContinuumMask::FullPlane)
                | (AcceptanceRole::Clean, ContinuumMask::Image(_))
        ));
    }
    for invalid in ["", "CLEAN", "clean1", "dirty "] {
        assert!(AcceptanceRole::parse(invalid).is_err());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptanceRole {
    Dirty,
    Clean,
}

impl AcceptanceRole {
    fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "dirty" => Ok(Self::Dirty),
            "clean" => Ok(Self::Clean),
            _ => Err("CASA_RS_T51_ACCEPTANCE_ROLE must be dirty or clean"),
        }
    }

    fn request(
        self,
        ms: PathBuf,
        cache: PathBuf,
        prefix: PathBuf,
        mask: PathBuf,
    ) -> ContinuumImagingRequest {
        let mut request = full_aw_request(ms, cache, prefix);
        request.cycle_iterations = 2000;
        if self == Self::Clean {
            request.iterations = 2000;
            request.mask = ContinuumMask::Image(mask);
        }
        request
    }
}

fn receipt_summary(receipt: &ExecutionReceipt, cold_catalog_miss: bool) -> serde_json::Value {
    if cold_catalog_miss {
        assert_eq!(receipt.status(), ReceiptStatus::Failed);
        assert_eq!(
            receipt.failure_kind(),
            Some(ReceiptFailureKind::EvidenceContract)
        );
        let artifacts = receipt
            .artifact_identities()
            .into_iter()
            .filter(|artifact| receipt.artifact_role(*artifact) == Some(ArtifactRole::Cache))
            .collect::<Vec<_>>();
        assert_eq!(artifacts.len(), 1024);
        assert!(artifacts.into_iter().all(|artifact| {
            receipt.artifact_disposition(artifact) == Some(ArtifactDisposition::RejectedStale)
        }));
    } else {
        assert_eq!(receipt.status(), ReceiptStatus::Completed);
    }
    assert_eq!(receipt.initial_execution_knobs().workers, 1);
    serde_json::json!({
        "attempt_id": format!("{:?}", receipt.attempt_id()),
        "status": receipt.status(),
        "expected_cold_catalog_miss": cold_catalog_miss,
        "plan_identity": receipt.plan_identity(),
        "problem_identity": receipt.problem_identity(),
        "workers": receipt.initial_execution_knobs().workers,
        "resource_policy": format!("{:?}", receipt.projected_resource_policy()),
    })
}

#[test]
#[ignore = "requires frozen T51 subset inputs and an external resource/time supervisor"]
fn t51_aw_subset_full_products() {
    run_full_products(true);
}

#[test]
#[ignore = "warm DIRTY timing diagnostic; requires frozen inputs and an external supervisor"]
fn t51_aw_subset_warm_dirty_products() {
    assert_eq!(
        std::env::var("CASA_RS_T51_ACCEPTANCE_ROLE").unwrap(),
        "dirty"
    );
    run_full_products(false);
}

fn run_full_products(require_cold_dirty: bool) {
    let started = Instant::now();
    let required = |name| PathBuf::from(std::env::var_os(name).expect(name));
    let role_name = std::env::var("CASA_RS_T51_ACCEPTANCE_ROLE").unwrap();
    let role = AcceptanceRole::parse(&role_name).unwrap();
    let prefix = required("CASA_RS_T51_ACCEPTANCE_OUTPUT_PREFIX");
    assert!(prefix.is_absolute());
    assert!(!prefix.exists());
    let parent = prefix.parent().unwrap();
    assert!(parent.is_dir());
    let stem = prefix.file_name().unwrap().to_str().unwrap();
    assert!(!stem.is_empty());
    assert!(
        fs::read_dir(parent).unwrap().all(|entry| {
            let name = entry.unwrap().file_name();
            !name.to_str().unwrap().starts_with(&format!("{stem}."))
        }),
        "output prefix already has products"
    );
    let ms = required("CASA_RS_T51_SOURCE_BIND_MS")
        .canonicalize()
        .unwrap();
    let cache = required("CASA_RS_T51_SOURCE_BIND_CF_CACHE")
        .canonicalize()
        .unwrap();
    let cache_parent = required("CASA_RS_T51_ACCEPTANCE_CACHE_PARENT")
        .canonicalize()
        .unwrap();
    let private_root = cache_parent.join(".casa-rs-aw-prepared");
    let objects = private_root.join("objects-v3");
    match role {
        AcceptanceRole::Dirty if require_cold_dirty => assert!(
            !private_root.exists(),
            "dirty requires a cold private store"
        ),
        _ => assert_eq!(fs::read_dir(&objects).unwrap().count(), 1024),
    }
    let mask = (role == AcceptanceRole::Clean)
        .then(|| required("CASA_RS_T51_SUBSET_MASK").canonicalize().unwrap());
    let request = role.request(
        ms.clone(),
        cache.clone(),
        prefix.clone(),
        mask.clone().unwrap_or_default(),
    );
    let mut prepared = subset_aw_request(request).unwrap();
    assert!(!prepared.write_model_column && !prepared.write_corrected_data);
    let selected_rows = prepared.observation.selection().rows().selected_row_count();
    assert_eq!(selected_rows, 10_080);
    let selection = prepared.observation.selection();
    assert_eq!(selection.spectral_windows().len(), 16);
    assert!(
        selection
            .spectral_windows()
            .iter()
            .all(|spw| spw.channel_indices().len() == 64)
    );
    assert!(!selection.correlations().is_empty());
    assert!(
        selection
            .correlations()
            .iter()
            .all(|polarization| polarization.products().len() == 4)
    );
    let selected_samples = selected_rows * 64 * 4;
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
    native.aw_preparation.as_mut().unwrap().private_root = private_root.clone();
    eprintln!(
        "t51_subset_full_products_start {}",
        serde_json::json!({
            "role": role_name, "output_prefix": prefix, "measurement_set": ms,
            "cold_private_store": role == AcceptanceRole::Dirty && require_cold_dirty,
            "casa_cache": cache, "prepared_store": private_root, "mask": mask,
            "selected_rows": selected_rows, "selected_correlation_channel_samples": selected_samples,
            "fields": 63, "spectral_windows": 16, "channels_per_spw": 64,
            "baselines": [[5,22], [4,5], [13,14], [12,13], [12,23]],
            "workers": 1, "admission_bytes": 16_u64 << 30, "aw_resident_bytes": 384_u64 << 20,
            "iterations": if role == AcceptanceRole::Clean { 2000 } else { 0 },
            "cycle_iterations": 2000,
        })
    );
    let outcome = crate::execute(prepared).unwrap().output;
    let execution_seconds = started.elapsed().as_secs_f64();
    let initial_receipt = receipt_summary(&outcome.initial_receipt, false);
    let publication_receipt = receipt_summary(&outcome.publication_receipt, false);
    let final_major_receipt = outcome
        .final_major_receipt
        .as_ref()
        .map(|receipt| receipt_summary(receipt, false));
    let aw_receipts = outcome
        .aw_preparation_receipts
        .iter()
        .enumerate()
        .map(|(index, receipt)| {
            receipt_summary(
                receipt,
                index == 0 && role == AcceptanceRole::Dirty && require_cold_dirty,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        aw_receipts.len(),
        if role == AcceptanceRole::Dirty && require_cold_dirty {
            2
        } else {
            1
        }
    );
    let stop_condition = match role {
        AcceptanceRole::Dirty => {
            assert_eq!(outcome.total_actual_minor_iterations, 0);
            assert_eq!(outcome.total_minor_iterations, 0);
            assert_eq!(outcome.major_cycle_count, 1);
            assert!(outcome.minor_cycles.is_empty());
            assert!(final_major_receipt.is_none());
            "dirty"
        }
        AcceptanceRole::Clean => {
            assert!(outcome.total_actual_minor_iterations > 1);
            assert!(outcome.total_minor_iterations <= 2000);
            assert!(final_major_receipt.is_some());
            assert_eq!(outcome.major_cycle_count, outcome.minor_cycles.len() + 1);
            assert!(outcome.minor_cycles.iter().all(|cycle| {
                cycle.stop_reason != crate::NativeMinorCycleStopReason::MultiscaleDivergence
                    && cycle.initial_peak_flux.is_finite()
                    && cycle.final_peak_flux.is_finite()
                    && cycle.total_flux.is_finite()
            }));
            let last = outcome.minor_cycles.last().unwrap();
            if outcome.total_minor_iterations == 2000 {
                "iteration_budget"
            } else {
                assert_eq!(
                    last.stop_reason,
                    crate::NativeMinorCycleStopReason::ThresholdReached
                );
                assert_eq!(
                    last.actual_iterations, 0,
                    "early terminal solve must observe a refreshed converged state"
                );
                assert!(
                    last.final_peak_flux <= last.global_threshold
                        || (last.global_threshold > 0.0
                            && (last.final_peak_flux - last.global_threshold).abs()
                                / last.global_threshold
                                < 0.01),
                    "fresh terminal residual must satisfy CASA's global convergence check"
                );
                "threshold_reached"
            }
        }
    };
    let mut model_digest = Sha256::new();
    let mut nonzero_model_samples = 0_usize;
    for sample in outcome.scientific.final_model().samples() {
        let value = sample.value().value();
        assert!(value.is_finite());
        nonzero_model_samples += usize::from(value != 0.0);
        model_digest.update(value.to_bits().to_le_bytes());
    }
    if role == AcceptanceRole::Clean {
        assert!(nonzero_model_samples > 0);
    }
    let normal = outcome.scientific.normal_state();
    assert_eq!(normal.sample_count(), selected_samples);
    assert_eq!(normal.coefficient_term_count(), 2);
    assert_eq!(normal.normal_moment_count(), 3);
    let mut normal_digest = Sha256::new();
    let mut nonzero_normal_samples = 0_usize;
    for values in [normal.residual(), normal.normal_approximation()] {
        for value in values {
            assert!(value.re.is_finite() && value.im.is_finite());
            nonzero_normal_samples += usize::from(value.re != 0.0 || value.im != 0.0);
            normal_digest.update(value.re.to_bits().to_le_bytes());
            normal_digest.update(value.im.to_bits().to_le_bytes());
        }
    }
    for value in normal.sensitivity().iter().chain(normal.sum_weights()) {
        assert!(value.is_finite());
        normal_digest.update(value.to_bits().to_le_bytes());
    }
    assert!(nonzero_normal_samples > 0);
    let products = outcome
        .products
        .members()
        .iter()
        .map(|member| {
            let path = PathBuf::from(format!("{}{name}", prefix.display(), name = member.name()));
            assert!(
                path.is_dir(),
                "published product is absent: {}",
                path.display()
            );
            serde_json::json!({"name": member.name(), "path": path,
            "content_identity": format!("{:?}", member.content_identity())})
        })
        .collect::<Vec<_>>();
    assert_eq!(
        products.len(),
        if role == AcceptanceRole::Clean {
            19
        } else {
            18
        }
    );
    let minor_cycles = outcome.minor_cycles.iter().map(|cycle| serde_json::json!({
        "cycle": cycle.cycle, "actual_iterations": cycle.actual_iterations,
        "total_actual_iterations": cycle.total_actual_iterations,
        "initial_peak_flux": cycle.initial_peak_flux, "final_peak_flux": cycle.final_peak_flux,
        "global_threshold": cycle.global_threshold, "effective_threshold": cycle.effective_threshold,
        "cycle_threshold": cycle.cycle_threshold, "noise_rms": cycle.noise_rms,
        "stop_reason": format!("{:?}", cycle.stop_reason), "total_flux": cycle.total_flux,
        "associated_replay_ordinal": cycle.associated_replay_ordinal,
        "recorded_components": cycle.recorded_components.iter().map(|component| serde_json::json!({
            "domain": component.cell().domain(), "coefficient": component.cell().coefficient(),
            "polarization": component.cell().polarization(), "pixel": component.cell().pixel(),
            "flux": component.flux(), "scale_px": component.scale_px(),
        })).collect::<Vec<_>>(),
    })).collect::<Vec<_>>();
    eprintln!(
        "t51_subset_full_products_complete {}",
        serde_json::json!({
            "role": role_name, "output_prefix": prefix, "prepared_store": private_root,
            "selected_rows": selected_rows, "selected_correlation_channel_samples": normal.sample_count(),
            "workers": 1, "admission_bytes": 16_u64 << 30,
            "execution_seconds": execution_seconds, "total_seconds": started.elapsed().as_secs_f64(),
            "model_sha256": format!("{:x}", model_digest.finalize()),
            "normal_sha256": format!("{:x}", normal_digest.finalize()),
            "nonzero_model_samples": nonzero_model_samples, "nonzero_normal_samples": nonzero_normal_samples,
            "major_cycles": outcome.major_cycle_count, "minor_iterations": outcome.total_minor_iterations,
            "actual_minor_iterations": outcome.total_actual_minor_iterations,
            "stop_condition": stop_condition, "minor_cycles": minor_cycles, "products": products,
            "initial_receipt": initial_receipt, "final_major_receipt": final_major_receipt,
            "publication_receipt": publication_receipt, "aw_preparation_receipts": aw_receipts,
        })
    );
}
