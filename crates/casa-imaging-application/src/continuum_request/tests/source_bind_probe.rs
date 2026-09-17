// SPDX-License-Identifier: LGPL-3.0-or-later

//! T51 metadata selection and canonical subset-cache preflight diagnostics.

use super::super::*;
use std::{fs, os::unix::fs::MetadataExt, process::Command, time::Instant};

mod model_export;
mod native_acceptance;
mod representative_acceptance;

fn subset_aw_request(
    request: ContinuumImagingRequest,
) -> Result<ApplicationRequest<CasaImageProductSink>, crate::ApplicationError> {
    use casa_imaging_model::{
        AntennaBaseline, AntennaSelection, RowSelection, compile_observation,
    };

    let mut prepared = prepare(request.clone())?;
    let (input, access) =
        casa_ms::resolve_selected_observation(prepared.observation.clone())?.into_parts();
    let full = compile_observation(input)?;
    let source = &full.sources()[0];
    assert_eq!(source.selection().rows().selected_row_count(), 655_200);
    assert_eq!(full.model(), ModelStateIdentity::Empty);
    assert!(
        !full
            .reference_data()
            .iter()
            .any(|(kind, _)| *kind == ReferenceDataKind::Ephemeris)
    );

    // T51 metadata census: W-extent quartiles among baselines with no flagged rows.
    let mut baselines = [(5, 22), (4, 5), (13, 14), (12, 13), (12, 23)]
        .map(|(first, second)| AntennaBaseline::new(first, second))
        .to_vec();
    baselines.sort_unstable();
    let ms = MeasurementSet::open(&request.measurement_set)?;
    let ddids = selected_data_descriptions(&request, &ms.data_description()?)?;
    let full_rows = ms.selected_observation_row_selection(
        &ddids,
        request.field_ids.as_deref(),
        request.uv_range.as_deref(),
        request.intent.as_deref(),
    )?;
    let budget = access.source_binding().content_budget();
    let mut rows = SelectedRowsBuilder::with_data_description_capacity(
        u64::try_from(ms.row_count())?,
        ddids.len(),
    );
    let mut coverage = BTreeMap::<(i32, i32), usize>::new();
    ms.visit_selected_observation_rows(
        &full_rows,
        MsSelectionIoBudget {
            available_bytes: 1 << 20,
            maximum_live_blocks: 2,
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| {
            let pair = AntennaBaseline::new(
                u32::try_from(row.antenna1()).unwrap(),
                u32::try_from(row.antenna2()).unwrap(),
            );
            if baselines.binary_search(&pair).is_ok() {
                assert!(
                    !row.flag_row(),
                    "census-selected baseline gained a row flag"
                );
                rows.push(SelectedMainRow::new(
                    u64::try_from(row.physical_row()).unwrap(),
                    u32::try_from(row.data_description_id()).unwrap(),
                ))
                .unwrap();
                *coverage
                    .entry((row.field_id(), row.data_description_id()))
                    .or_default() += 1;
            }
        },
    )?;
    let rows = rows.finish();
    assert_eq!(rows.selected_row_count(), 10_080);
    assert_eq!(coverage.len(), 63 * 16);
    assert!(coverage.values().all(|&count| count == 10));
    let selection = source.selection();
    let base = selection.rows_filter();
    let subset = ObservationSelection::new(
        rows,
        RowSelection::new(
            base.fields().clone(),
            base.times().clone(),
            base.uv_distances().clone(),
            AntennaSelection::Only(baselines.clone()),
            base.scans().clone(),
            base.observations().clone(),
            base.intents().clone(),
            base.arrays().clone(),
        ),
        selection.data_descriptions().to_vec(),
        selection.spectral_windows().to_vec(),
        selection.correlations().to_vec(),
    );
    let columns = source.generations().columns();
    prepared.observation = SelectedObservationResolutionRequest::new(
        prepared.observation.locator(),
        LogicalIdentity::from_sha256(hash(
            format!(
                "t51-baseline-subset-v1:{}:{baselines:?}",
                full.snapshot_id()
            )
            .as_bytes(),
        )),
        subset,
        columns.visibility(),
        columns.weights(),
        full.reference_data()
            .iter()
            .copied()
            .filter(|(kind, _)| *kind != ReferenceDataKind::Measures)
            .collect(),
        full.model(),
        budget,
        casa_ms::open_measures_runtime()?,
    );
    Ok(prepared)
}

#[test]
#[ignore = "requires T51 MS, unchanged full CF source and external scratch; binding only under a 60s timeout"]
fn t51_aw_subset_binding_only() {
    let started = Instant::now();
    let required = |name| {
        PathBuf::from(std::env::var_os(name).unwrap())
            .canonicalize()
            .unwrap()
    };
    let ms = required("CASA_RS_T51_SOURCE_BIND_MS");
    let cache = required("CASA_RS_T51_SOURCE_BIND_CF_CACHE");
    let external = required("CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT");
    assert_eq!(
        fs::metadata(&ms).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    assert_eq!(
        fs::metadata(&cache).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    let scratch = tempfile::Builder::new()
        .prefix("t51-subset-bind-")
        .tempdir_in(external)
        .unwrap();
    let prepared =
        subset_aw_request(full_aw_request(ms, cache, scratch.path().join("probe"))).unwrap();
    let (input, _) = casa_ms::resolve_selected_observation(prepared.observation)
        .unwrap()
        .into_parts();
    let observation = casa_imaging_model::compile_observation(input).unwrap();
    let problem = casa_imaging_model::compile(casa_imaging_model::ImagingRequest::new(
        prepared.specification,
        prepared.geometry,
        casa_imaging_model::ProblemInputIdentities::new(observation),
        prepared.model_lifecycle,
    ))
    .unwrap();
    let selection = problem.inputs().observation_snapshot().sources()[0].selection();
    assert_eq!(selection.rows().selected_row_count(), 10_080);
    assert_eq!(selection.spectral_windows().len(), 16);
    assert!(
        selection
            .spectral_windows()
            .iter()
            .all(|spw| spw.channel_indices().len() == 64)
    );
    assert!(!scratch.path().join(".casa-rs-aw-prepared").exists());
    eprintln!(
        "t51_subset_binding {}",
        serde_json::json!({
            "problem": problem.problem_id().to_string(),
            "observation": problem.inputs().observation().to_string(),
            "selected_rows": selection.rows().selected_row_count(),
            "field_ddid_groups": 1008, "channels_per_spw": 64,
            "seconds": started.elapsed().as_secs_f64(),
            "scope": "real predicate-validated subset; no CF import, weighting, model replay or imaging",
        })
    );
}

#[test]
#[ignore = "requires T51 MS/full CF source/external scratch and an explicitly bounded supervisor"]
#[allow(clippy::assertions_on_constants)]
fn t51_aw_subset_source_open_only() {
    assert!(!cfg!(debug_assertions), "use a release test binary");
    let started = Instant::now();
    let required = |name| {
        PathBuf::from(std::env::var_os(name).unwrap())
            .canonicalize()
            .unwrap()
    };
    let ms = required("CASA_RS_T51_SOURCE_BIND_MS");
    let cache = required("CASA_RS_T51_SOURCE_BIND_CF_CACHE");
    let external = required("CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT");
    assert_eq!(
        fs::metadata(&ms).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    assert_eq!(
        fs::metadata(&cache).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    let scratch = tempfile::Builder::new()
        .prefix("t51-subset-source-open-")
        .tempdir_in(external)
        .unwrap();
    let prepared =
        subset_aw_request(full_aw_request(ms, cache, scratch.path().join("probe"))).unwrap();
    eprintln!(
        "t51_source_open_probe stage=prepare seconds={:.9}",
        started.elapsed().as_secs_f64()
    );
    let stage = Instant::now();
    let (input, initial_access) = casa_ms::resolve_selected_observation(prepared.observation)
        .unwrap()
        .into_parts();
    let observation = casa_imaging_model::compile_observation(input).unwrap();
    let problem = casa_imaging_model::compile(casa_imaging_model::ImagingRequest::new(
        prepared.specification,
        prepared.geometry,
        casa_imaging_model::ProblemInputIdentities::new(observation),
        prepared.model_lifecycle,
    ))
    .unwrap();
    assert_eq!(
        problem.inputs().observation_snapshot().sources()[0]
            .selection()
            .rows()
            .selected_row_count(),
        10_080
    );
    eprintln!(
        "t51_source_open_probe stage=resolve_compile seconds={:.9}",
        stage.elapsed().as_secs_f64()
    );
    let runtime = prepared.native.unwrap().runtime;
    let policy = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: runtime
            .authority
            .topology()
            .memory_domains
            .iter()
            .map(|domain| (domain.id.clone(), 16 << 30))
            .collect(),
        workers: Some(1),
        ..ResourceOverride::default()
    });
    let stage = Instant::now();
    let initial_access = SelectedObservationSourceResources::finalize_access(
        &problem,
        initial_access,
        &runtime.authority,
        &policy,
    )
    .unwrap();
    eprintln!(
        "t51_source_open_probe stage=finalize seconds={:.9}",
        stage.elapsed().as_secs_f64()
    );
    let stage = Instant::now();
    let selected = initial_access.open(&problem).unwrap();
    eprintln!(
        "t51_source_open_probe stage=open seconds={:.9} total_seconds={:.9}",
        stage.elapsed().as_secs_f64(),
        started.elapsed().as_secs_f64()
    );
    drop(selected);
    assert!(!scratch.path().join(".casa-rs-aw-prepared").exists());
    assert!(started.elapsed().as_secs_f64() < 60.0);
}

#[test]
#[ignore = "requires T51 MS/full CF source/external scratch and an explicitly bounded supervisor"]
fn t51_aw_subset_cache_preparation() {
    let started = Instant::now();
    let required = |name| {
        PathBuf::from(std::env::var_os(name).unwrap())
            .canonicalize()
            .unwrap()
    };
    let ms = required("CASA_RS_T51_SOURCE_BIND_MS");
    let cache_path = required("CASA_RS_T51_SOURCE_BIND_CF_CACHE");
    let external = required("CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT");
    assert_eq!(
        fs::metadata(&ms).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    assert_eq!(
        fs::metadata(&cache_path).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    let cache = crate::CasaAwCache::open(&cache_path).unwrap();
    let inventory = cache.inventory();
    assert_eq!(inventory.paired_cells, 1024);
    assert_eq!(inventory.frequencies_hz.len(), 16);
    assert_eq!(inventory.w_values_lambda.len(), 32);
    assert_eq!(inventory.mueller_elements, [0, 15]);
    assert_eq!(inventory.parallactic_angles_deg.len(), 1);
    let resume = std::env::var_os("CASA_RS_T51_SUBSET_CACHE_RESUME").map(PathBuf::from);
    let expected_reused = if resume.is_some() {
        std::env::var("CASA_RS_T51_EXPECT_REUSED_CELLS")
            .unwrap()
            .parse::<usize>()
            .unwrap()
    } else {
        0
    };
    assert!(expected_reused <= 1024);
    let scratch = if let Some(root) = &resume {
        let root = root.canonicalize().unwrap();
        assert_eq!(root.parent(), Some(external.as_path()));
        assert!(root.join(".casa-rs-aw-prepared").is_dir());
        assert_ne!(root, ms);
        assert_ne!(root, cache_path);
        root
    } else {
        tempfile::Builder::new()
            .prefix("t51-subset-cache-")
            .tempdir_in(external)
            .unwrap()
            .keep()
    };
    eprintln!(
        "t51_subset_cache_start root={} source={} prepared_budget_bytes={}",
        scratch.display(),
        cache_path.display(),
        cache.prepared_cache_bytes().unwrap()
    );
    let prepared =
        subset_aw_request(full_aw_request(ms, cache_path, scratch.join("probe"))).unwrap();
    let (input, _) = casa_ms::resolve_selected_observation(prepared.observation)
        .unwrap()
        .into_parts();
    let observation = casa_imaging_model::compile_observation(input).unwrap();
    let problem = casa_imaging_model::compile(casa_imaging_model::ImagingRequest::new(
        prepared.specification,
        prepared.geometry,
        casa_imaging_model::ProblemInputIdentities::new(observation),
        prepared.model_lifecycle,
    ))
    .unwrap();
    let mut native = prepared.native.unwrap();
    if resume.is_some() {
        let receipt_root = tempfile::Builder::new()
            .prefix("continuation-receipts-")
            .tempdir_in(&scratch)
            .unwrap()
            .keep();
        let digest = hash(receipt_root.as_os_str().as_encoded_bytes());
        native.runtime.attempts =
            [0, 1, 2].map(|ordinal| ExecutionAttemptId::from_sha256(scoped(digest, ordinal)));
        native.runtime.receipts = ExecutionReceiptStore::new(
            &receipt_root,
            ReceiptRetention::new(2048, 512 << 20).unwrap(),
        )
        .unwrap();
        eprintln!(
            "t51_subset_continuation_receipts={}",
            receipt_root.display()
        );
    }
    let phase = crate::prepared_aw_phase::prepare_aw_projection(
        &problem,
        native.aw_preparation.unwrap(),
        &native.runtime,
    )
    .unwrap();
    assert_eq!(
        phase.receipts.len(),
        1025 - expected_reused,
        "canonical catalog validation plus missing imports must reconcile with retained completed objects"
    );
    let binding = phase.bind_plan().unwrap();
    assert_eq!(binding.projection.resident_byte_ceiling(), 384 << 20);
    eprintln!(
        "t51_subset_cache_complete {}",
        serde_json::json!({
            "root": scratch, "problem": problem.problem_id().to_string(),
            "receipts": phase.receipts.len(), "catalog_cells": 1024,
            "maximum_imaging_support": binding.projection.maximum_imaging_support(),
            "catalog_resident_bytes": binding.projection.catalog_resident_bytes(),
            "decoded_resident_ceiling": binding.projection.resident_byte_ceiling(),
            "seconds": started.elapsed().as_secs_f64(),
            "scope": "subset-bound canonical import and fresh reader binding; no CASA execution or image numerics",
        })
    );
}

#[test]
#[ignore = "requires complete subset CF store and verified T51 mask; supervised scientific preflight only"]
fn t51_aw_subset_clean1() {
    let started = Instant::now();
    let required = |name| {
        PathBuf::from(std::env::var_os(name).unwrap())
            .canonicalize()
            .unwrap()
    };
    let ms = required("CASA_RS_T51_SOURCE_BIND_MS");
    let cache = required("CASA_RS_T51_SOURCE_BIND_CF_CACHE");
    let external = required("CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT");
    let retained = required("CASA_RS_T51_SUBSET_CACHE_RESUME");
    let mask = required("CASA_RS_T51_SUBSET_MASK");
    let objects = retained.join(".casa-rs-aw-prepared/objects-v3");
    let entries = fs::read_dir(&objects)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        entries.len(),
        1024,
        "complete catalog required before numerical preflight"
    );
    assert!(entries.iter().all(|entry| {
        let name = entry.file_name();
        let name = name.to_str().unwrap();
        name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
    }));
    let scratch = tempfile::Builder::new()
        .prefix("t51-subset-clean1-")
        .tempdir_in(&external)
        .unwrap()
        .keep();
    let mut request = full_aw_request(ms, cache, scratch.join("probe"));
    request.iterations = 1;
    request.mask = ContinuumMask::Image(mask.clone());
    let mut prepared = subset_aw_request(request).unwrap();
    assert!(!prepared.write_model_column && !prepared.write_corrected_data);
    if let Some(bytes) = std::env::var_os("CASA_RS_T51_SUBSET_MEMORY_BYTES") {
        let bytes = bytes.to_str().unwrap().parse::<u64>().unwrap();
        assert!((12 << 30..32 << 30).contains(&bytes));
        let native = prepared.native.as_mut().unwrap();
        native.runtime.resource_policy = ResourcePolicy::Explicit(ResourceOverride {
            memory_bytes: native
                .runtime
                .authority
                .topology()
                .memory_domains
                .iter()
                .map(|domain| (domain.id.clone(), bytes))
                .collect(),
            workers: Some(1),
            ..ResourceOverride::default()
        });
    }
    prepared
        .native
        .as_mut()
        .unwrap()
        .aw_preparation
        .as_mut()
        .unwrap()
        .private_root = retained.join(".casa-rs-aw-prepared");
    eprintln!(
        "t51_subset_clean1_start {}",
        serde_json::json!({
            "root": scratch, "mask": mask, "prepared_store": objects,
            "selected_rows": prepared.observation.selection().rows().selected_row_count(),
            "memory_bytes_override": std::env::var("CASA_RS_T51_SUBSET_MEMORY_BYTES").ok(),
            "scope": "own subset initial state and canonical nonzero continuation; not full-selection parity or performance",
        })
    );
    let outcome = crate::execute(prepared).unwrap().output;
    assert_eq!(
        outcome.aw_preparation_receipts.len(),
        1,
        "validated warm catalog only"
    );
    assert_eq!(outcome.major_cycle_count, 2);
    assert_eq!(outcome.total_actual_minor_iterations, 1);
    assert!(outcome.final_major_receipt.is_some());
    assert_eq!(outcome.minor_cycles.len(), 1);
    let minor = &outcome.minor_cycles[0];
    assert!(minor.total_flux.is_finite() && minor.total_flux > 0.0);
    assert!(!minor.recorded_components.is_empty());
    let execution_seconds = started.elapsed().as_secs_f64();
    if std::env::var_os("CASA_RS_T51_EXPORT_MODEL").is_some() {
        assert_eq!(std::env::var("CASA_RS_T51_EXPORT_MODEL").unwrap(), "1");
        let export_started = Instant::now();
        let manifest = model_export::export(&outcome.scientific, &scratch).unwrap();
        eprintln!(
            "t51_authoritative_model_export {}",
            serde_json::json!({
                "manifest": manifest,
                "seconds": export_started.elapsed().as_secs_f64(),
                "scope": "read-only physical model export; not CASA input-equivalence acceptance",
            })
        );
    }
    let digest_started = Instant::now();
    let model = outcome.scientific.final_model();
    let model_samples = model
        .read_samples(0..model.sample_count())
        .expect("read final model samples");
    let mut model_digest = Sha256::new();
    let mut nonzero_model_samples = 0_usize;
    for sample in &model_samples {
        let value = sample.value().value();
        assert!(value.is_finite());
        nonzero_model_samples += usize::from(value != 0.0);
        model_digest.update(value.to_bits().to_le_bytes());
    }
    assert!(nonzero_model_samples > 0);
    let normal = outcome.scientific.normal_state();
    assert_eq!(normal.coefficient_term_count(), 2);
    assert_eq!(normal.normal_moment_count(), 3);
    let normal_window = normal
        .read_window(normal.slab().core_range())
        .expect("complete final normal window");
    let mut normal_digest = Sha256::new();
    for term in 0..normal.coefficient_term_count() {
        for value in normal_window.coefficient_term(term).unwrap().residual() {
            assert!(value.re.is_finite() && value.im.is_finite());
            normal_digest.update(value.re.to_bits().to_le_bytes());
            normal_digest.update(value.im.to_bits().to_le_bytes());
        }
    }
    for moment in 0..normal.normal_moment_count() {
        let moment = normal_window.normal_moment(moment).unwrap();
        for value in moment.normal_approximation() {
            assert!(value.re.is_finite() && value.im.is_finite());
            normal_digest.update(value.re.to_bits().to_le_bytes());
            normal_digest.update(value.im.to_bits().to_le_bytes());
        }
        for value in moment.sensitivity() {
            assert!(value.is_finite());
            normal_digest.update(value.to_bits().to_le_bytes());
        }
        assert!(moment.sum_weight().is_finite());
        normal_digest.update(moment.sum_weight().to_bits().to_le_bytes());
    }
    eprintln!(
        "t51_subset_clean1_complete {}",
        serde_json::json!({
            "root": scratch, "execution_seconds": execution_seconds,
            "digest_seconds": digest_started.elapsed().as_secs_f64(),
            "model_sha256": format!("{:x}", model_digest.finalize()),
            "normal_sha256": format!("{:x}", normal_digest.finalize()),
            "nonzero_model_samples": nonzero_model_samples,
            "major_passes": outcome.major_cycle_count,
            "actual_components": outcome.total_actual_minor_iterations,
            "absolute_component_flux": minor.total_flux,
            "initial_peak_flux": minor.initial_peak_flux,
            "final_peak_flux": minor.final_peak_flux,
            "scope": "canonical subset CLEAN1 ownership and nonzero replay only; no matched CASA component claim",
        })
    );
}

#[test]
#[ignore = "requires T51 subset source and verified mask; canonical sample census under an external timeout"]
fn t51_aw_subset_source_eligibility() {
    use casa_imaging_reconstruction::{SpectralStencilValidity, compile_spectral_stencil};
    let started = Instant::now();
    let required = |name| {
        PathBuf::from(std::env::var_os(name).unwrap())
            .canonicalize()
            .unwrap()
    };
    let scratch = tempfile::Builder::new()
        .prefix("t51-subset-source-census-")
        .tempdir_in(required("CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT"))
        .unwrap()
        .keep();
    let mut request = full_aw_request(
        required("CASA_RS_T51_SOURCE_BIND_MS"),
        required("CASA_RS_T51_SOURCE_BIND_CF_CACHE"),
        scratch.join("probe"),
    );
    request.iterations = 1;
    request.mask = ContinuumMask::Image(required("CASA_RS_T51_SUBSET_MASK"));
    let prepared = subset_aw_request(request).unwrap();
    let (input, access) = casa_ms::resolve_selected_observation(prepared.observation)
        .unwrap()
        .into_parts();
    let observation = casa_imaging_model::compile_observation(input).unwrap();
    let problem = casa_imaging_model::compile(casa_imaging_model::ImagingRequest::new(
        prepared.specification,
        prepared.geometry,
        casa_imaging_model::ProblemInputIdentities::new(observation),
        prepared.model_lifecycle,
    ))
    .unwrap();
    let runtime = prepared.native.unwrap().runtime;
    let access = SelectedObservationSourceResources::finalize_access(
        &problem,
        access,
        &runtime.authority,
        &runtime.resource_policy,
    )
    .unwrap();
    let mut source = access.open(&problem).unwrap();
    let mut counts = BTreeMap::<(i32, i32, u32, u32), [u64; 4]>::new();
    let mut w_range = [f64::INFINITY, f64::NEG_INFINITY];
    let mut pointing_pixels = std::collections::BTreeSet::new();
    let completion = source
        .traverse(&problem, |sample| {
            let selected = sample.selected();
            let address = selected.address();
            let evaluation = sample.spectral_evaluation();
            let stencil = compile_spectral_stencil(&problem, selected, evaluation).unwrap();
            let mapped = stencil.validity() == SpectralStencilValidity::Mapped;
            let parallel = matches!(
                address.correlation_type,
                casa_imaging_model::CorrelationType::CircularRr
                    | casa_imaging_model::CorrelationType::CircularLl
            );
            let entry = counts
                .entry((
                    selected.metadata().field_id,
                    address.data_description_id,
                    address.spectral_window_id,
                    address.channel_index,
                ))
                .or_default();
            entry[0] += 1;
            entry[1] += u64::from(evaluation.is_valid());
            entry[2] += u64::from(mapped);
            entry[3] += u64::from(mapped && parallel);
            if mapped && parallel {
                for projection in selected.domain_projections().iter() {
                    let w = projection.model().transformed_uvw_m()[2]
                        * evaluation.output_frame().centre_hz()
                        / 299_792_458.0;
                    w_range[0] = w_range[0].min(w);
                    w_range[1] = w_range[1].max(w);
                    if let Some(pixel) = projection.aw_pointing_pixel() {
                        pointing_pixels.insert(pixel.map(f64::to_bits));
                    }
                }
            }
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    assert_eq!(completion.sample_count(), 2_580_480);
    assert_eq!(counts.len(), 63 * 16 * 64);
    let mut groups = BTreeMap::<(i32, i32, u32), [u64; 4]>::new();
    for (&(field, ddid, spw, _), count) in &counts {
        let group = groups.entry((field, ddid, spw)).or_default();
        for (total, value) in group.iter_mut().zip(count) {
            *total += value;
        }
    }
    assert_eq!(groups.len(), 1008);
    let uncovered_groups = groups
        .iter()
        .filter(|(_, count)| count[3] == 0)
        .map(|(key, _)| *key)
        .collect::<Vec<_>>();
    let rows = counts.iter().map(|(&(field, ddid, spw, channel), count)|
        serde_json::json!({"field":field,"ddid":ddid,"spw":spw,"channel":channel,
            "selected":count[0],"source_valid":count[1],"spectral_mapped":count[2],"mapped_parallel_hand":count[3]}))
        .collect::<Vec<_>>();
    fs::write(
        scratch.join("source-census.json"),
        serde_json::to_vec(&rows).unwrap(),
    )
    .unwrap();
    eprintln!(
        "t51_subset_source_eligibility {}",
        serde_json::json!({
            "root":scratch,"problem":problem.problem_id().to_string(),"generation":completion.generation_id().to_string(),
            "samples":completion.sample_count(),"field_ddid_spw_groups":groups.len(),"channel_groups":counts.len(),
            "groups_without_mapped_parallel_hands":uncovered_groups,
            "channel_groups_without_mapped_parallel_hands":counts.values().filter(|count|count[3]==0).count(),
            "mapped_parallel_hand_samples":counts.values().map(|count|count[3]).sum::<u64>(),
            "transformed_w_lambda_range":w_range,"distinct_pointing_pixels":pointing_pixels.len(),
            "seconds":started.elapsed().as_secs_f64(),
            "scope":"canonical source eligibility and spectral mapping, not final Briggs weights or CF support selection"
        })
    );
    assert!(
        uncovered_groups.is_empty(),
        "every field/SPW must retain eligible parallel-hand samples"
    );
    assert!(!scratch.join(".casa-rs-aw-prepared").exists());
}

pub(super) fn full_aw_request(
    measurement_set: PathBuf,
    casa_cache: PathBuf,
    image_name: PathBuf,
) -> ContinuumImagingRequest {
    let requirements = vec![
        TaskRequirement::SerialCpu,
        TaskRequirement::AwProjection,
        TaskRequirement::PerChannelWeightDensity,
        TaskRequirement::WProjectionPlanes,
    ];
    // Values follow the pinned full dirty workload and native_application::application_request.
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: 4096,
        facets: 1,
        cell_arcsec: 0.6,
        phase_center_field: Some(1525),
        phase_center: None,
        outlier_file: None,
        field_ids: Some(
            (1107..=1127)
                .chain(1512..=1532)
                .chain(1542..=1562)
                .collect(),
        ),
        uv_range: Some("<12km".to_string()),
        intent: Some("OBSERVE_TARGET#UNSPECIFIED".to_string()),
        data_description: None,
        spectral_window: Some("2~17".to_string()),
        channel_start: Some(0),
        channel_count: Some(64),
        spectral_mode: SpectralImagingMode::Continuum,
        continuum_subtraction: None,
        data_column: Some("data".to_string()),
        polarizations: vec![PolarizationCoordinate::StokesI],
        algorithm: ContinuumAlgorithm::Mtmfs {
            terms: 2,
            scales_px: vec![0.0, 5.0, 12.0],
            small_scale_bias: 0.0,
        },
        weighting: ContinuumWeighting::Briggs(1.0),
        iterations: 0,
        cycle_iterations: 1,
        hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        maximum_major_cycles: None,
        noise_sigma: Some(5.0),
        cycle_factor: 3.0,
        minimum_psf_fraction: f64::from(0.05_f32),
        maximum_psf_fraction: f64::from(0.8_f32),
        gain: f64::from(0.1_f32),
        threshold_jy: 0.0,
        psf_cutoff: 0.35,
        primary_beam_limit: 0.0001,
        normalization: ProductNormalization::FlatNoise,
        beam_policy: ContinuumBeamPolicy::Common,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: true,
        pbcor: false,
        w_projection_planes: Some(32),
        aw_projection: Some(ContinuumAwProjection {
            source: ContinuumAwCfSource::CasaImport(casa_cache),
            resident_bytes: 384 << 20,
            w_plane_count: Some(32),
            psf_phase_center_direction_rad: None,
            vp_table: None,
            a_term: true,
            ps_term: false,
            wideband: true,
            conjugate_beams: true,
            use_pointing: true,
            pointing_offset_sigdev: vec![0.0],
            mosaic_weighting: false,
            compute_pa_step_deg: 360.0,
            rotate_pa_step_deg: 360.0,
        }),
        resource_policy: resource_policy_for_task_requirements(&requirements),
        task_requirements: requirements,
    }
}

#[test]
#[ignore = "requires the owner-initialized T51 MS; metadata only under a 60s external timeout"]
fn t51_full_aw_row_census() {
    let started = Instant::now();
    let path = PathBuf::from(std::env::var_os("CASA_RS_T51_SOURCE_BIND_MS").unwrap())
        .canonicalize()
        .unwrap();
    let ms = MeasurementSet::open(&path).unwrap();
    let descriptions = ms.data_description().unwrap();
    let ddids = (0..descriptions.row_count())
        .filter(|&row| (2..=17).contains(&descriptions.spectral_window_id(row).unwrap()))
        .map(|row| i32::try_from(row).unwrap())
        .collect::<Vec<_>>();
    let fields = (1107..=1127)
        .chain(1512..=1532)
        .chain(1542..=1562)
        .collect::<Vec<_>>();
    let selection = ms
        .selected_observation_row_selection(
            &ddids,
            Some(&fields),
            Some("<12km"),
            Some("OBSERVE_TARGET#UNSPECIFIED"),
        )
        .unwrap();
    let mut integrations = BTreeMap::<(i32, i32, u64), u64>::new();
    let mut baselines = BTreeMap::<(i32, i32), (u64, u64, f64, f64, BTreeSet<(i32, i32)>)>::new();
    let mut rows = 0_u64;
    ms.visit_selected_observation_rows(
        &selection,
        MsSelectionIoBudget {
            available_bytes: 1 << 20,
            maximum_live_blocks: 2,
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| {
            rows += 1;
            if rows.is_multiple_of(1024) {
                assert!(
                    started.elapsed().as_secs_f64() < 50.0,
                    "metadata census deadline"
                );
            }
            assert!(row.time_mjd_seconds().is_finite());
            let group = (row.field_id(), row.data_description_id());
            *integrations
                .entry((group.0, group.1, row.time_mjd_seconds().to_bits()))
                .or_default() += 1;
            let baseline = baselines
                .entry((row.antenna1(), row.antenna2()))
                .or_insert_with(|| (0, 0, f64::INFINITY, 0.0, BTreeSet::new()));
            baseline.0 += 1;
            baseline.1 += u64::from(row.flag_row());
            let w = row.uvw_m()[2].abs();
            assert!(w.is_finite());
            baseline.2 = baseline.2.min(w);
            baseline.3 = baseline.3.max(w);
            baseline.4.insert(group);
            assert!(integrations.len() <= 65_536 && baselines.len() <= 1024);
        },
    )
    .unwrap();
    assert_eq!(rows, 655_200, "full-selection provenance changed");
    let mut groups = BTreeMap::<(i32, i32), Vec<(f64, u64)>>::new();
    for ((field, ddid, time), count) in integrations {
        groups
            .entry((field, ddid))
            .or_default()
            .push((f64::from_bits(time), count));
    }
    assert_eq!(groups.len(), 63 * 16);
    eprintln!(
        "t51_row_census_header {}",
        serde_json::json!({
            "source": path, "selected_rows": rows, "field_ddid_groups": groups.len(),
            "baseline_count": baselines.len(), "wall_seconds": started.elapsed().as_secs_f64(),
            "scope": "scalar metadata only; raw stored W metres, not AW support or channel-weight eligibility",
        })
    );
    for ((field, ddid), times) in groups {
        eprintln!(
            "t51_row_census_group {}",
            serde_json::json!({
                "field": field, "ddid": ddid, "integrations": times,
            })
        );
    }
    for ((antenna1, antenna2), (count, flagged, minimum_w, maximum_w, coverage)) in baselines {
        eprintln!(
            "t51_row_census_baseline {}",
            serde_json::json!({
                "antennas": [antenna1, antenna2], "rows": count, "flagged_rows": flagged,
                "absolute_stored_w_metres": [minimum_w, maximum_w],
                "field_ddid_groups": coverage.len(),
            })
        );
    }
}

#[test]
#[ignore = "requires the owner-initialized T51 MS, full CF path, and external scratch; release only under a 60s external timeout"]
#[allow(clippy::assertions_on_constants)]
fn t51_full_aw_source_bind_only() {
    assert!(!cfg!(debug_assertions), "use a release test binary");
    let started = Instant::now();
    let required_path = |name| {
        PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("{name} is required")))
            .canonicalize()
            .unwrap()
    };
    let measurement_set = required_path("CASA_RS_T51_SOURCE_BIND_MS");
    let casa_cache = required_path("CASA_RS_T51_SOURCE_BIND_CF_CACHE");
    let external = required_path("CASA_RS_T51_SOURCE_BIND_SCRATCH_PARENT");
    assert_eq!(
        fs::metadata(&measurement_set).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    assert_eq!(
        fs::metadata(&casa_cache).unwrap().dev(),
        fs::metadata(&external).unwrap().dev()
    );
    assert_ne!(
        fs::metadata(external.parent().expect("external volume mount"))
            .unwrap()
            .dev(),
        fs::metadata(&external).unwrap().dev(),
        "scratch parent must be the external volume mount, not a retained data/store directory"
    );
    assert!(!external.starts_with(&measurement_set) && !external.starts_with(&casa_cache));
    let scratch = tempfile::Builder::new()
        .prefix("t51-source-bind-only-")
        .tempdir_in(&external)
        .unwrap();
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(revision.status.success());
    let request = full_aw_request(
        measurement_set,
        casa_cache,
        scratch.path().join("diagnostic"),
    );
    eprintln!(
        "t51_source_bind_header {}",
        serde_json::json!({
            "parent_revision": String::from_utf8(revision.stdout).unwrap().trim(),
            "compiled_prepare_sha256": format!("{:x}", Sha256::digest(include_bytes!("../../continuum_request.rs"))),
            "compiled_probe_sha256": format!("{:x}", Sha256::digest(include_bytes!("source_bind_probe.rs"))),
            "executable": std::env::current_exe().unwrap(),
            "request": format!("{request:?}"),
            "scope": "production metadata prepare + resolve + compile + certify + open; no CF import, visibility traversal, gridding, FFT, or product publication",
            "external_timeout_seconds": 60,
        })
    );

    let stage = Instant::now();
    let prepared = prepare(request).expect("prepare full T51 request");
    eprintln!(
        "t51_source_bind_stage name=prepare seconds={:.9}",
        stage.elapsed().as_secs_f64()
    );
    let mut native = prepared.native.expect("production runtime preparation");
    let runtime = native.runtime.clone();
    assert!(!scratch.path().join(".casa-rs-aw-prepared").exists());
    let stage = Instant::now();
    let resolved = casa_ms::resolve_selected_observation(prepared.observation.clone())
        .expect("resolve full T51 selection");
    eprintln!(
        "t51_source_bind_stage name=resolve seconds={:.9}",
        stage.elapsed().as_secs_f64()
    );
    let (snapshot, initial_access) = resolved.into_parts();
    let stage = Instant::now();
    let observation = casa_imaging_model::compile_observation(snapshot).unwrap();
    let problem = casa_imaging_model::compile(casa_imaging_model::ImagingRequest::new(
        prepared.specification,
        prepared.geometry,
        casa_imaging_model::ProblemInputIdentities::new(observation),
        prepared.model_lifecycle,
    ))
    .expect("compile full T51 problem");
    crate::validate_installed_implementation(&problem, prepared.task_requirements).unwrap();
    eprintln!(
        "t51_source_bind_stage name=compile seconds={:.9}",
        stage.elapsed().as_secs_f64()
    );
    let source = &problem.inputs().observation_snapshot().sources()[0];
    assert_eq!(source.selection().rows().selected_row_count(), 655_200);
    let budget = initial_access.source_binding().content_budget();
    eprintln!(
        "t51_source_bind_input {}",
        serde_json::json!({
            "selected_rows": source.selection().rows().selected_row_count(),
            "source_identity": source.identity().to_string(),
            "content_available_bytes": budget.available_bytes(),
            "maximum_live_blocks": budget.maximum_live_blocks(),
            "maximum_pointing_polynomial_terms": budget.maximum_pointing_polynomial_terms(),
        })
    );
    let requirements = initial_access
        .content_requirements(&problem)
        .expect("derive full source requirements without opening traversal");
    assert!(
        requirements.plan(budget).is_err(),
        "bootstrap remains insufficient for full execution"
    );
    let (_, small_access) = casa_ms::resolve_selected_observation(prepared.observation.clone())
        .expect("resolve small-cap control")
        .into_parts();
    let small_policy = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: native
            .runtime
            .authority
            .topology()
            .memory_domains
            .iter()
            .map(|domain| (domain.id.clone(), 64 << 20))
            .collect(),
        workers: Some(1),
        ..ResourceOverride::default()
    });
    native.runtime.resource_policy = small_policy;
    let failure = match crate::run_native(
        &problem,
        crate::NativeInput {
            observation: prepared.observation.clone(),
            initial_access: small_access,
            write_model_column: prepared.write_model_column,
            write_corrected_data: prepared.write_corrected_data,
            masks: prepared.masks,
            minor_cycle_image_response: prepared.minor_cycle_image_response,
            native: Ok(native),
        },
    ) {
        Ok(_) => panic!("an insufficient runtime cap must reject before CF preparation"),
        Err(error) => error,
    };
    assert!(
        failure
            .to_string()
            .contains("selected-observation host memory")
    );
    assert!(!scratch.path().join(".casa-rs-aw-prepared").exists());
    let stage = Instant::now();
    let initial_access = SelectedObservationSourceResources::finalize_access(
        &problem,
        initial_access,
        &runtime.authority,
        &runtime.resource_policy,
    )
    .expect("select the production source envelope before CF preparation");
    eprintln!(
        "t51_source_bind_stage name=finalize seconds={:.9} budget={:?}",
        stage.elapsed().as_secs_f64(),
        initial_access.source_binding().content_budget(),
    );
    let stage = Instant::now();
    let residency = initial_access.certify_residency(&problem);
    eprintln!(
        "t51_source_bind_stage name=certify seconds={:.9} result={residency:?}",
        stage.elapsed().as_secs_f64()
    );
    let residency = residency.expect("certify full T51 source residency before CF preparation");
    let stage = Instant::now();
    let selected = initial_access.open(&problem);
    eprintln!(
        "t51_source_bind_stage name=open seconds={:.9} total_seconds={:.9} error={:?}",
        stage.elapsed().as_secs_f64(),
        started.elapsed().as_secs_f64(),
        selected.as_ref().err()
    );
    let selected = selected.expect("open full T51 source before CF preparation");
    assert_eq!(selected.residency_certificate(), &residency);
    assert!(!scratch.path().join(".casa-rs-aw-prepared").exists());
    assert!(
        started.elapsed().as_secs_f64() < 60.0,
        "external deadline must also bound the probe"
    );
}
