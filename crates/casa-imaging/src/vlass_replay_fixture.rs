// SPDX-License-Identifier: LGPL-3.0-or-later

//! Experimental, external-only fixture for the full-band VLASS replay campaign.
//!
//! This is deliberately not a persisted production cache contract. Capture is
//! enabled only by an explicit environment variable, and the fixture records
//! the private Rust layout required by the exact release test executable.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::time::Instant;

use ndarray::Array2;
use num_complex::{Complex32, Complex64};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::{
    AwProjectCompactReplayStats, AwProjectMetalResidentMetadata, AwProjectMetalResidualScalePlan,
    AwProjectMetalSpillStore, DirtyProductFftPolicy, MosaicDirectMetalMode,
    MosaicMtmfsStreamGridAccumulation, MosaicMtmfsStreamGridConfig,
    MosaicPointingWeightAccumulator, replay_awproject_metal_global_program,
    replay_awproject_metal_prefetched_sequence,
};
use super::{
    AwProjectMetalPredictionSample, AwProjectMetalSample, AwProjectMetalSpillSection,
    AwProjectMetalSpilledProgram, AwProjectSampleStats, ImagingError, MosaicMtmfsStreamGridStorage,
    WProjectMetalComplex, copy_awproject_metal_centered_f64_plane,
};

const CAPTURE_PROVENANCE_ENV: &str = "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_FIXTURE_PROVENANCE";
#[cfg(test)]
const FULL16_BENCHMARK_PREFIX_ENV: &str =
    "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_FIXTURE_BENCHMARK_PREFIX";
#[cfg(test)]
const FOUR_SPW_BENCHMARK_PREFIX_ENV: &str =
    "CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_FIXTURE_FOUR_SPW_PREFIX";
const SCHEMA: &str = "casa-rs-vlass-full16-aw-replay-private-v1";

#[derive(Clone)]
pub(super) struct FixturePaths {
    pub(super) partial_payload: PathBuf,
    payload: PathBuf,
    manifest: PathBuf,
}

impl FixturePaths {
    pub(super) fn new(prefix: &Path) -> Result<Self, ImagingError> {
        let partial_payload = PathBuf::from(format!("{}.payload.partial", prefix.display()));
        let payload = PathBuf::from(format!("{}.payload", prefix.display()));
        let manifest = PathBuf::from(format!("{}.json", prefix.display()));
        if let Some(parent) = prefix.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                ImagingError::InvalidRequest(format!(
                    "create AWProject replay fixture directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        for path in [&partial_payload, &payload, &manifest] {
            if path.exists() {
                return Err(ImagingError::InvalidRequest(format!(
                    "AWProject replay fixture path already exists: {}",
                    path.display()
                )));
            }
        }
        Ok(Self {
            partial_payload,
            payload,
            manifest,
        })
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
fn decode_hex(value: &str) -> Result<[u8; 32], ImagingError> {
    if value.len() != 64 {
        return Err(ImagingError::InvalidRequest(
            "VLASS replay fixture section SHA-256 must contain 64 hexadecimal characters"
                .to_string(),
        ));
    }
    let mut output = [0u8; 32];
    for (index, slot) in output.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "parse VLASS replay fixture section SHA-256: {error}"
            ))
        })?;
    }
    Ok(output)
}

fn section_json(section: AwProjectMetalSpillSection) -> Value {
    json!({
        "offset": section.offset,
        "len": section.len,
        "byte_len": section.byte_len,
        "sha256": hex(&section.sha256),
    })
}

#[cfg(test)]
fn required_u64(value: &Value, key: &str) -> Result<u64, ImagingError> {
    value.get(key).and_then(Value::as_u64).ok_or_else(|| {
        ImagingError::InvalidRequest(format!("VLASS replay fixture is missing integer {key}"))
    })
}

#[cfg(test)]
fn required_f64(value: &Value, key: &str) -> Result<f64, ImagingError> {
    value.get(key).and_then(Value::as_f64).ok_or_else(|| {
        ImagingError::InvalidRequest(format!("VLASS replay fixture is missing number {key}"))
    })
}

#[cfg(test)]
fn to_usize(value: u64, key: &str) -> Result<usize, ImagingError> {
    usize::try_from(value).map_err(|_| {
        ImagingError::InvalidRequest(format!("VLASS replay fixture {key} exceeds usize"))
    })
}

#[cfg(test)]
fn section_from_json(
    value: &Value,
    label: &str,
) -> Result<AwProjectMetalSpillSection, ImagingError> {
    Ok(AwProjectMetalSpillSection {
        offset: required_u64(value, "offset")?,
        len: to_usize(required_u64(value, "len")?, &format!("{label}.len"))?,
        byte_len: to_usize(
            required_u64(value, "byte_len")?,
            &format!("{label}.byte_len"),
        )?,
        sha256: decode_hex(value.get("sha256").and_then(Value::as_str).ok_or_else(|| {
            ImagingError::InvalidRequest(format!("VLASS replay fixture is missing {label}.sha256"))
        })?)?,
    })
}

fn stats_json(stats: &AwProjectSampleStats) -> Value {
    json!({
        "attempted_samples": stats.attempted_samples,
        "accepted_samples": stats.accepted_samples,
        "rejected_not_gridable": stats.rejected_not_gridable,
        "rejected_invalid_input": stats.rejected_invalid_input,
        "rejected_rr_imaging_plan": stats.rejected_rr_imaging_plan,
        "rejected_ll_imaging_plan": stats.rejected_ll_imaging_plan,
        "rejected_rr_psf_plan": stats.rejected_rr_psf_plan,
        "rejected_ll_psf_plan": stats.rejected_ll_psf_plan,
        "rejected_nonfinite_coordinate": stats.rejected_nonfinite_coordinate,
        "rejected_outside_grid": stats.rejected_outside_grid,
        "rejected_kernel_index": stats.rejected_kernel_index,
        "rejected_invalid_normalization": stats.rejected_invalid_normalization,
    })
}

#[cfg(test)]
fn stats_from_json(value: &Value) -> Result<AwProjectSampleStats, ImagingError> {
    let get = |key| to_usize(required_u64(value, key)?, key);
    Ok(AwProjectSampleStats {
        attempted_samples: get("attempted_samples")?,
        accepted_samples: get("accepted_samples")?,
        rejected_not_gridable: get("rejected_not_gridable")?,
        rejected_invalid_input: get("rejected_invalid_input")?,
        rejected_rr_imaging_plan: get("rejected_rr_imaging_plan")?,
        rejected_ll_imaging_plan: get("rejected_ll_imaging_plan")?,
        rejected_rr_psf_plan: get("rejected_rr_psf_plan")?,
        rejected_ll_psf_plan: get("rejected_ll_psf_plan")?,
        rejected_nonfinite_coordinate: get("rejected_nonfinite_coordinate")?,
        rejected_outside_grid: get("rejected_outside_grid")?,
        rejected_kernel_index: get("rejected_kernel_index")?,
        rejected_invalid_normalization: get("rejected_invalid_normalization")?,
    })
}

fn program_json(program: &AwProjectMetalSpilledProgram) -> Value {
    json!({
        "prediction_samples": section_json(program.prediction_samples),
        "source_sample_indices": section_json(program.source_sample_indices),
        "kernels": section_json(program.kernels),
        "prediction_phases": section_json(program.prediction_phases),
        "tile_samples": section_json(program.tile_samples),
        "tile_phases": section_json(program.tile_phases),
        "term_weights": section_json(program.term_weights),
        "active_tile_ids": section_json(program.active_tile_ids),
        "tile_fragment_offsets": section_json(program.tile_fragment_offsets),
        "fragments": section_json(program.fragments),
        "tile_side": program.tile_side,
        "tiles_y": program.tiles_y,
        "residual_scale": {
            "max_kernel_norm": program.residual_scale_plan.max_kernel_norm,
            "residual_overlap": program.residual_scale_plan.residual_overlap,
        },
        "metadata": {
            "normalization_sumwt": program.metadata.normalization_sumwt,
            "gridded_samples": program.metadata.gridded_samples,
            "skipped_samples": program.metadata.skipped_samples,
            "sample_census": stats_json(&program.metadata.aw_sample_census),
        },
        "source_programs": program.source_programs,
        "payload_bytes": program.payload_bytes,
    })
}

#[cfg(test)]
fn program_from_json(value: &Value) -> Result<AwProjectMetalSpilledProgram, ImagingError> {
    let section = |key: &str| {
        section_from_json(
            value.get(key).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "VLASS replay fixture program is missing {key}"
                ))
            })?,
            key,
        )
    };
    let scale = value.get("residual_scale").ok_or_else(|| {
        ImagingError::InvalidRequest(
            "VLASS replay fixture program is missing residual_scale".to_string(),
        )
    })?;
    let metadata = value.get("metadata").ok_or_else(|| {
        ImagingError::InvalidRequest("VLASS replay fixture program is missing metadata".to_string())
    })?;
    Ok(AwProjectMetalSpilledProgram {
        prediction_samples: section("prediction_samples")?,
        source_sample_indices: section("source_sample_indices")?,
        kernels: section("kernels")?,
        prediction_phases: section("prediction_phases")?,
        tile_samples: section("tile_samples")?,
        tile_phases: section("tile_phases")?,
        term_weights: section("term_weights")?,
        active_tile_ids: section("active_tile_ids")?,
        tile_fragment_offsets: section("tile_fragment_offsets")?,
        fragments: section("fragments")?,
        tile_side: to_usize(required_u64(value, "tile_side")?, "tile_side")?,
        tiles_y: to_usize(required_u64(value, "tiles_y")?, "tiles_y")?,
        residual_scale_plan: AwProjectMetalResidualScalePlan {
            max_kernel_norm: required_f64(scale, "max_kernel_norm")?,
            residual_overlap: to_usize(
                required_u64(scale, "residual_overlap")?,
                "residual_overlap",
            )?,
        },
        metadata: AwProjectMetalResidentMetadata {
            normalization_sumwt: required_f64(metadata, "normalization_sumwt")?,
            gridded_samples: to_usize(
                required_u64(metadata, "gridded_samples")?,
                "gridded_samples",
            )?,
            skipped_samples: to_usize(
                required_u64(metadata, "skipped_samples")?,
                "skipped_samples",
            )?,
            aw_sample_census: stats_from_json(metadata.get("sample_census").ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "VLASS replay fixture metadata is missing sample_census".to_string(),
                )
            })?)?,
        },
        source_programs: to_usize(required_u64(value, "source_programs")?, "source_programs")?,
        payload_bytes: to_usize(required_u64(value, "payload_bytes")?, "payload_bytes")?,
    })
}

fn program_sections(
    program: &AwProjectMetalSpilledProgram,
) -> [(&'static str, AwProjectMetalSpillSection); 10] {
    [
        ("prediction_samples", program.prediction_samples),
        ("source_sample_indices", program.source_sample_indices),
        ("kernels", program.kernels),
        ("prediction_phases", program.prediction_phases),
        ("tile_samples", program.tile_samples),
        ("tile_phases", program.tile_phases),
        ("term_weights", program.term_weights),
        ("active_tile_ids", program.active_tile_ids),
        ("tile_fragment_offsets", program.tile_fragment_offsets),
        ("fragments", program.fragments),
    ]
}

fn byte_ledger(programs: &[AwProjectMetalSpilledProgram]) -> Value {
    let mut by_section = BTreeMap::<&'static str, usize>::new();
    let mut kernel_hashes = BTreeSet::<[u8; 32]>::new();
    let mut unique_kernel_bytes = 0usize;
    for program in programs {
        for (label, section) in program_sections(program) {
            let entry = by_section.entry(label).or_default();
            *entry = entry.saturating_add(section.byte_len);
            if label == "kernels" && kernel_hashes.insert(section.sha256) {
                unique_kernel_bytes = unique_kernel_bytes.saturating_add(section.byte_len);
            }
        }
    }
    let payload_bytes = by_section.values().copied().sum::<usize>();
    let kernel_payload_bytes = by_section.get("kernels").copied().unwrap_or(0);
    json!({
        "by_section": by_section,
        "payload_bytes": payload_bytes,
        "kernel_payload_bytes": kernel_payload_bytes,
        "unique_kernel_bytes": unique_kernel_bytes,
        "duplicated_kernel_bytes": kernel_payload_bytes.saturating_sub(unique_kernel_bytes),
        "segment_local_non_kernel_bytes": payload_bytes.saturating_sub(kernel_payload_bytes),
    })
}

fn contiguous_slice<'a, T>(array: &'a Array2<T>, label: &str) -> Result<&'a [T], ImagingError> {
    array.as_slice_memory_order().ok_or_else(|| {
        ImagingError::InvalidRequest(format!("VLASS replay fixture {label} is not contiguous"))
    })
}

pub(super) fn capture_if_requested(
    cache: &mut super::AwProjectCompactReplayCache,
    model_grids: &[Array2<Complex32>],
    storage: &MosaicMtmfsStreamGridStorage,
) -> Result<(), ImagingError> {
    let programs = cache.spilled_metal_global_programs.clone();
    let store = match cache.metal_global_spill_store.as_mut() {
        Some(store) if store.fixture_paths.is_some() => store,
        _ => return Ok(()),
    };
    let paths = store
        .fixture_paths
        .clone()
        .expect("fixture path checked above");
    let provenance_path = std::env::var_os(CAPTURE_PROVENANCE_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| {
            ImagingError::InvalidRequest(format!(
                "VLASS replay fixture capture requires {CAPTURE_PROVENANCE_ENV}"
            ))
        })?;
    let provenance_bytes = std::fs::read(&provenance_path).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "read VLASS replay fixture provenance {}: {error}",
            provenance_path.display()
        ))
    })?;
    let provenance: Value = serde_json::from_slice(&provenance_bytes).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "parse VLASS replay fixture provenance {}: {error}",
            provenance_path.display()
        ))
    })?;
    if model_grids.len() != 2 {
        return Err(ImagingError::Normalization(format!(
            "VLASS replay fixture requires two model terms, got {}",
            model_grids.len()
        )));
    }
    let MosaicMtmfsStreamGridStorage::MetalSharedF32 {
        grid,
        aw_compensation: Some(compensation),
        psf_term_count,
        residual_term_count,
        ..
    } = storage
    else {
        return Err(ImagingError::Normalization(
            "VLASS replay fixture requires compensated Metal residual storage".to_string(),
        ));
    };
    if *psf_term_count != 0 || *residual_term_count != 2 {
        return Err(ImagingError::Normalization(
            "VLASS replay fixture requires exactly two residual-only Metal planes".to_string(),
        ));
    }
    let grid_shape = grid.shape();
    let model_sections = model_grids
        .iter()
        .enumerate()
        .map(|(term, model)| {
            if model.shape() != grid_shape {
                return Err(ImagingError::Normalization(format!(
                    "VLASS replay fixture model term {term} shape {:?} differs from {grid_shape:?}",
                    model.shape()
                )));
            }
            let values = contiguous_slice(model, "model grid")?;
            store.write_slice(values, "fixture model grid")
        })
        .collect::<Result<Vec<_>, ImagingError>>()?;
    let mut baseline_sections = Vec::with_capacity(2);
    for term in 0..2 {
        let baseline = copy_awproject_metal_centered_f64_plane(grid, compensation, term)?;
        baseline_sections.push(store.write_slice(
            contiguous_slice(&baseline, "baseline residual grid")?,
            "fixture baseline residual grid",
        )?);
    }
    store.file.sync_all().map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "sync VLASS replay fixture payload {}: {error}",
            paths.partial_payload.display()
        ))
    })?;
    std::fs::rename(&paths.partial_payload, &paths.payload).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "publish VLASS replay fixture payload {}: {error}",
            paths.payload.display()
        ))
    })?;
    let manifest = json!({
        "schema": SCHEMA,
        "role": "external_experimental_replay_fixture_not_production_cache",
        "payload": {
            "path": paths.payload,
            "bytes": store.bytes_written,
        },
        "private_layout": {
            "pointer_bytes": size_of::<usize>(),
            "endianness": if cfg!(target_endian = "little") { "little" } else { "big" },
            "prediction_sample_bytes": size_of::<AwProjectMetalPredictionSample>(),
            "tile_sample_bytes": size_of::<AwProjectMetalSample>(),
            "complex32_bytes": size_of::<Complex32>(),
            "complex64_bytes": size_of::<Complex64>(),
            "kernel_value_bytes": size_of::<WProjectMetalComplex>(),
            "prediction_sample_alignment": align_of::<AwProjectMetalPredictionSample>(),
            "tile_sample_alignment": align_of::<AwProjectMetalSample>(),
        },
        "grid_shape": grid_shape,
        "nterms": 2,
        "programs": programs.iter().map(program_json).collect::<Vec<_>>(),
        "model_grids": model_sections.into_iter().map(section_json).collect::<Vec<_>>(),
        "baseline_residual_grids": baseline_sections.into_iter().map(section_json).collect::<Vec<_>>(),
        "byte_ledger": byte_ledger(&programs),
        "provenance": provenance,
        "provenance_sha256": hex(&Sha256::digest(&provenance_bytes)),
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
        ImagingError::InvalidRequest(format!("serialize VLASS replay fixture manifest: {error}"))
    })?;
    let mut manifest_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths.manifest)
        .map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "create VLASS replay fixture manifest {}: {error}",
                paths.manifest.display()
            ))
        })?;
    manifest_file.write_all(&manifest_bytes).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "write VLASS replay fixture manifest {}: {error}",
            paths.manifest.display()
        ))
    })?;
    manifest_file.sync_all().map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "sync VLASS replay fixture manifest {}: {error}",
            paths.manifest.display()
        ))
    })?;
    eprintln!(
        "awproject_vlass_replay_fixture status=frozen segments={} samples={} payload_bytes={} \
         file_bytes={} manifest_sha256={} manifest={}",
        programs.len(),
        programs
            .iter()
            .map(|program| program.prediction_samples.len)
            .sum::<usize>(),
        programs
            .iter()
            .map(|program| program.payload_bytes)
            .sum::<usize>(),
        store.bytes_written,
        hex(&Sha256::digest(&manifest_bytes)),
        paths.manifest.display(),
    );
    Err(ImagingError::InvalidRequest(
        "VLASS AWProject replay fixture capture completed before remaining imaging stages"
            .to_string(),
    ))
}

#[cfg(test)]
fn load_manifest(prefix: &Path) -> Result<(Value, PathBuf), ImagingError> {
    let manifest_path = PathBuf::from(format!("{}.json", prefix.display()));
    let bytes = std::fs::read(&manifest_path).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "read VLASS replay fixture manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    let manifest: Value = serde_json::from_slice(&bytes).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "parse VLASS replay fixture manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    if manifest.get("schema").and_then(Value::as_str) != Some(SCHEMA) {
        return Err(ImagingError::InvalidRequest(
            "unexpected VLASS replay fixture schema".to_string(),
        ));
    }
    let layout = manifest.get("private_layout").ok_or_else(|| {
        ImagingError::InvalidRequest("VLASS replay fixture is missing private_layout".to_string())
    })?;
    for (key, actual) in [
        ("pointer_bytes", size_of::<usize>()),
        (
            "prediction_sample_bytes",
            size_of::<AwProjectMetalPredictionSample>(),
        ),
        ("tile_sample_bytes", size_of::<AwProjectMetalSample>()),
        ("complex32_bytes", size_of::<Complex32>()),
        ("complex64_bytes", size_of::<Complex64>()),
        ("kernel_value_bytes", size_of::<WProjectMetalComplex>()),
    ] {
        if to_usize(required_u64(layout, key)?, key)? != actual {
            return Err(ImagingError::InvalidRequest(format!(
                "VLASS replay fixture private layout {key} changed"
            )));
        }
    }
    let payload = manifest
        .get("payload")
        .and_then(|value| value.get("path"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| {
            ImagingError::InvalidRequest("VLASS replay fixture is missing payload.path".to_string())
        })?;
    Ok((manifest, payload))
}

#[cfg(test)]
fn normalized_rms(actual: &[Complex64], reference: &[Complex64]) -> f64 {
    let (error, signal) =
        actual
            .iter()
            .zip(reference)
            .fold((0.0, 0.0), |(error, signal), (actual, reference)| {
                let delta = *actual - *reference;
                (error + delta.norm_sqr(), signal + reference.norm_sqr())
            });
    (error / signal.max(f64::MIN_POSITIVE)).sqrt()
}

#[cfg(test)]
fn model_grid_from_fixture(
    rows: usize,
    columns: usize,
    values: Vec<Complex32>,
) -> Result<Array2<Complex32>, ImagingError> {
    Array2::from_shape_vec((columns, rows), values)
        .map(Array2::reversed_axes)
        .map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "shape first-axis-contiguous VLASS replay fixture model grid: {error}"
            ))
        })
}

#[cfg(test)]
fn effective_support_stats_json(stats: &super::AwProjectMetalEffectiveSupportStats) -> Value {
    let role = |role: &super::AwProjectMetalEffectiveSupportRoleStats| {
        json!({
            "plan_count": role.plan_count,
            "unique_stencils": role.unique_stencils,
            "original_tap_visits": role.original_tap_visits,
            "retained_tap_visits": role.retained_tap_visits,
            "cropped_plans": role.cropped_plans,
        })
    };
    json!({
        "omitted_energy_fraction": stats.omitted_energy_fraction,
        "unique_stencils": stats.unique_stencils,
        "stencil_lookups": stats.stencil_lookups,
        "crop_evaluations": stats.crop_evaluations,
        "index_peak_entries": stats.index_peak_entries,
        "index_estimated_bytes": stats.index_estimated_bytes,
        "prefix_scratch_peak_bytes": stats.prefix_scratch_peak_bytes,
        "prediction": role(&stats.prediction),
        "tile": role(&stats.tile),
        "max_omitted_energy_fraction": stats.max_omitted_energy_fraction,
        "fallback_counts": stats.fallback_counts,
        "compile_seconds": stats.compile_elapsed.as_secs_f64(),
        "resident_kernel_bytes_before": stats.resident_kernel_bytes_before,
        "resident_kernel_bytes_after": stats.resident_kernel_bytes_after,
    })
}

#[cfg(test)]
fn benchmark(prefix: &Path) -> Result<Value, ImagingError> {
    let (manifest, payload_path) = load_manifest(prefix)?;
    let programs = manifest
        .get("programs")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ImagingError::InvalidRequest("VLASS replay fixture is missing programs".to_string())
        })?
        .iter()
        .map(program_from_json)
        .collect::<Result<Vec<_>, ImagingError>>()?;
    let shape = manifest
        .get("grid_shape")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ImagingError::InvalidRequest("VLASS replay fixture is missing grid_shape".to_string())
        })?;
    if shape.len() != 2 {
        return Err(ImagingError::InvalidRequest(
            "VLASS replay fixture grid_shape must contain two values".to_string(),
        ));
    }
    let rows = to_usize(
        shape[0].as_u64().ok_or_else(|| {
            ImagingError::InvalidRequest("invalid fixture grid row count".to_string())
        })?,
        "grid rows",
    )?;
    let columns = to_usize(
        shape[1].as_u64().ok_or_else(|| {
            ImagingError::InvalidRequest("invalid fixture grid column count".to_string())
        })?,
        "grid columns",
    )?;
    let file = File::open(&payload_path).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "open VLASS replay fixture payload {}: {error}",
            payload_path.display()
        ))
    })?;
    let file_bytes = file
        .metadata()
        .map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "stat VLASS replay fixture payload {}: {error}",
                payload_path.display()
            ))
        })?
        .len();
    let expected_file_bytes = manifest
        .get("payload")
        .and_then(|value| value.get("bytes"))
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ImagingError::InvalidRequest(
                "VLASS replay fixture is missing payload.bytes".to_string(),
            )
        })?;
    if file_bytes != expected_file_bytes {
        return Err(ImagingError::InvalidRequest(format!(
            "VLASS replay fixture payload size changed: {file_bytes} != {expected_file_bytes}"
        )));
    }
    let mut store = AwProjectMetalSpillStore {
        file,
        bytes_written: file_bytes,
        bytes_read: 0,
        fixture_paths: None,
    };
    let model_sections = manifest
        .get("model_grids")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ImagingError::InvalidRequest("VLASS replay fixture is missing model_grids".to_string())
        })?;
    let model_grids = model_sections
        .iter()
        .enumerate()
        .map(|(term, value)| {
            let section = section_from_json(value, &format!("model term {term}"))?;
            let values = store.read_vec::<Complex32>(section, "fixture model grid")?;
            model_grid_from_fixture(rows, columns, values)
        })
        .collect::<Result<Vec<_>, ImagingError>>()?;
    let mut accumulation = MosaicMtmfsStreamGridAccumulation {
        storage: MosaicMtmfsStreamGridStorage::new(MosaicMtmfsStreamGridConfig {
            rows,
            columns,
            psf_term_count: 3,
            residual_term_count: 2,
            residual_only: true,
            dirty_product_fft_policy: DirtyProductFftPolicy::correctness_first(),
            direct_metal_mode: MosaicDirectMetalMode::AwProjectGlobalReplay,
            awproject_grid: true,
            direct_metal_scratch_bytes: None,
        })?,
        pointing_weights: MosaicPointingWeightAccumulator::default(),
        reported_sumwt_terms: Vec::new(),
        aw_psf_sumwt_terms: Vec::new(),
        normalization_sumwt: 0.0,
        gridded_samples: 0,
        skipped_samples: 0,
        max_abs_w_lambda: 0.0,
        aw_sample_census: AwProjectSampleStats::default(),
        aw_metal_stats: Default::default(),
    };
    let mut replay_stats = AwProjectCompactReplayStats::default();
    store.bytes_read = 0;
    let started = Instant::now();
    let mut segment_receipts = Vec::with_capacity(programs.len());
    let mut effective_support_telemetry_markers = 0usize;
    let prefetch_stats = replay_awproject_metal_prefetched_sequence(
        &store,
        &programs,
        super::AwProjectMetalEffectiveSupportConfig::experiment_from_environment()?,
        |segment, descriptor, loaded| {
            let reload_seconds = loaded.reload_elapsed.as_secs_f64();
            let reload_bytes = loaded.bytes_read;
            let effective_support_receipt = loaded
                .effective_support
                .as_ref()
                .map(effective_support_stats_json);
            if let Some(effective_support) = loaded.effective_support.as_ref() {
                if effective_support.log(segment, descriptor.source_programs) {
                    effective_support_telemetry_markers =
                        effective_support_telemetry_markers.saturating_add(1);
                }
            }
            let mut program = loaded.program;
            let replay_started = Instant::now();
            replay_awproject_metal_global_program(
                2,
                &model_grids,
                &mut accumulation,
                &mut replay_stats,
                &mut program,
            )?;
            let metal_replay_seconds = replay_started.elapsed().as_secs_f64();
            segment_receipts.push(json!({
                "segment": segment,
                "samples": descriptor.prediction_samples.len,
                "source_programs": descriptor.source_programs,
                "payload_bytes": descriptor.payload_bytes,
                "reload_bytes": reload_bytes,
                "reload_seconds": reload_seconds,
                "metal_replay_seconds": metal_replay_seconds,
                "total_seconds": reload_seconds + metal_replay_seconds,
                "effective_support": effective_support_receipt,
            }));
            Ok(())
        },
    )?;
    store.bytes_read = store.bytes_read.saturating_add(prefetch_stats.bytes_read);
    let replay_seconds = started.elapsed().as_secs_f64();
    let replay_read_bytes = store.bytes_read;
    let MosaicMtmfsStreamGridStorage::MetalSharedF32 {
        grid,
        aw_compensation: Some(compensation),
        ..
    } = &accumulation.storage
    else {
        return Err(ImagingError::Normalization(
            "VLASS replay benchmark lost compensated Metal storage".to_string(),
        ));
    };
    let baseline_sections = manifest
        .get("baseline_residual_grids")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ImagingError::InvalidRequest(
                "VLASS replay fixture is missing baseline_residual_grids".to_string(),
            )
        })?;
    let mut nrmse = Vec::with_capacity(2);
    for (term, baseline_section) in baseline_sections.iter().enumerate() {
        let actual = copy_awproject_metal_centered_f64_plane(grid, compensation, term)?;
        let reference = store.read_vec::<Complex64>(
            section_from_json(baseline_section, "baseline residual grid")?,
            "fixture baseline residual grid",
        )?;
        nrmse.push(normalized_rms(
            contiguous_slice(&actual, "benchmark residual grid")?,
            &reference,
        ));
    }
    let samples = programs
        .iter()
        .map(|program| program.prediction_samples.len)
        .sum::<usize>();
    let source_programs = programs
        .iter()
        .map(|program| program.source_programs)
        .sum::<usize>();
    let rejected_samples = programs
        .iter()
        .map(|program| {
            let stats = &program.metadata.aw_sample_census;
            stats
                .attempted_samples
                .saturating_sub(stats.accepted_samples)
        })
        .sum::<usize>();
    Ok(json!({
        "schema": "casa-rs-vlass-full16-aw-replay-benchmark-v1",
        "seconds": replay_seconds,
        "seconds_per_accepted_sample": replay_seconds / samples.max(1) as f64,
        "samples": samples,
        "source_programs": source_programs,
        "segments": programs.len(),
        "rejected_samples": rejected_samples,
        "payload_bytes": programs.iter().map(|program| program.payload_bytes).sum::<usize>(),
        "reload_bytes": replay_read_bytes,
        "metal_dispatch_ms": accumulation.aw_metal_stats.dispatch_wait.as_secs_f64() * 1_000.0,
        "metal_total_ms": accumulation.aw_metal_stats.total.as_secs_f64() * 1_000.0,
        "nrmse": nrmse,
        "effective_support_telemetry_markers": effective_support_telemetry_markers,
        "effective_support": {
            "requested": prefetch_stats.requested,
            "decision": prefetch_stats.decision.label(),
            "reason": prefetch_stats.reason,
            "segment_count": prefetch_stats.segment_count,
            "compiled_segment_count": prefetch_stats.compiled_segment_count,
            "total_compile_seconds": prefetch_stats.total_compile.as_secs_f64(),
            "initial_prepare_seconds": prefetch_stats.initial_prepare.as_secs_f64(),
            "prefetch_wait_seconds": prefetch_stats.prefetch_wait.as_secs_f64(),
        },
        "segment_receipts": segment_receipts,
        "byte_ledger": manifest.get("byte_ledger").cloned().unwrap_or(Value::Null),
        "provenance": manifest.get("provenance").cloned().unwrap_or(Value::Null),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn effective_support_requested() -> bool {
        super::super::AwProjectMetalEffectiveSupportConfig::experiment_from_environment()
            .expect("parse effective-support experiment")
            .is_some()
    }

    fn assert_effective_support_segment_receipts(result: &Value, compiled: bool) {
        let segment_receipts = result["segment_receipts"]
            .as_array()
            .expect("per-segment receipts");
        for (segment, segment_receipt) in segment_receipts.iter().enumerate() {
            let support = &segment_receipt["effective_support"];
            if !compiled {
                assert!(
                    support.is_null(),
                    "segment {segment} unexpectedly compiled effective support"
                );
                continue;
            }

            let support = support
                .as_object()
                .unwrap_or_else(|| panic!("segment {segment} effective-support receipt"));
            let integer = |key: &str| {
                support[key]
                    .as_u64()
                    .unwrap_or_else(|| panic!("segment {segment} effective-support {key}"))
            };
            let number = |key: &str| {
                support[key]
                    .as_f64()
                    .unwrap_or_else(|| panic!("segment {segment} effective-support {key}"))
            };
            let threshold = number("omitted_energy_fraction");
            let max_omitted = number("max_omitted_energy_fraction");
            assert!(threshold.is_finite() && threshold > 0.0 && threshold <= 1.0e-4);
            assert!(max_omitted.is_finite() && max_omitted >= 0.0);
            assert!(max_omitted <= threshold);

            let unique_stencils = integer("unique_stencils");
            let crop_evaluations = integer("crop_evaluations");
            let index_peak_entries = integer("index_peak_entries");
            assert!(unique_stencils > 0);
            assert_eq!(crop_evaluations, unique_stencils);
            assert_eq!(index_peak_entries, unique_stencils);
            assert!(integer("index_estimated_bytes") > 0);
            assert!(integer("prefix_scratch_peak_bytes") > 0);

            let role = |key: &str| {
                support[key]
                    .as_object()
                    .unwrap_or_else(|| panic!("segment {segment} effective-support {key}"))
            };
            let prediction = role("prediction");
            let tile = role("tile");
            let role_integer = |role: &serde_json::Map<String, Value>, key: &str| {
                role[key]
                    .as_u64()
                    .unwrap_or_else(|| panic!("segment {segment} effective-support role {key}"))
            };
            for role in [prediction, tile] {
                let plan_count = role_integer(role, "plan_count");
                let role_unique = role_integer(role, "unique_stencils");
                let original_taps = role_integer(role, "original_tap_visits");
                let retained_taps = role_integer(role, "retained_tap_visits");
                let cropped_plans = role_integer(role, "cropped_plans");
                assert!(role_unique <= unique_stencils);
                assert!(retained_taps <= original_taps);
                assert!(cropped_plans <= plan_count);
            }
            assert_eq!(
                integer("stencil_lookups"),
                role_integer(prediction, "plan_count") + role_integer(tile, "plan_count")
            );

            let fallback_counts = support["fallback_counts"]
                .as_object()
                .unwrap_or_else(|| panic!("segment {segment} effective-support fallback counts"));
            let fallback_plans = fallback_counts.values().try_fold(0u64, |total, value| {
                value.as_u64().and_then(|count| total.checked_add(count))
            });
            assert!(
                fallback_plans.is_some_and(|count| {
                    count
                        <= role_integer(prediction, "plan_count") + role_integer(tile, "plan_count")
                }),
                "segment {segment} effective-support fallback count"
            );
            assert!(number("compile_seconds").is_finite() && number("compile_seconds") >= 0.0);
            assert_eq!(
                integer("resident_kernel_bytes_before"),
                integer("resident_kernel_bytes_after")
            );
        }
    }

    fn assert_effective_support_admission(
        result: &Value,
        requested: bool,
        decision: &str,
        reason: Option<&str>,
        compiled_segment_count: usize,
        telemetry_markers: usize,
    ) {
        let support = &result["effective_support"];
        assert_eq!(support["requested"], requested);
        assert_eq!(support["decision"], decision);
        assert_eq!(support["reason"], json!(reason));
        assert_eq!(support["segment_count"], result["segments"]);
        assert_eq!(support["compiled_segment_count"], compiled_segment_count);
        let total_compile_seconds = support["total_compile_seconds"]
            .as_f64()
            .expect("effective-support total compile seconds");
        if compiled_segment_count == 0 {
            assert_eq!(total_compile_seconds, 0.0);
        } else {
            assert!(total_compile_seconds >= 0.0);
        }
        assert!(
            support["initial_prepare_seconds"]
                .as_f64()
                .expect("initial prepare seconds")
                >= 0.0
        );
        assert!(
            support["prefetch_wait_seconds"]
                .as_f64()
                .expect("prefetch wait seconds")
                >= 0.0
        );
        assert_eq!(
            result["effective_support_telemetry_markers"],
            telemetry_markers
        );
        assert!(
            compiled_segment_count == 0
                || compiled_segment_count
                    == result["segments"]
                        .as_u64()
                        .expect("effective-support segment count") as usize
        );
        assert_effective_support_segment_receipts(result, compiled_segment_count > 0);
    }

    #[test]
    fn fixture_model_grid_preserves_casacore_first_axis_contiguous_storage() {
        let values = (0..6)
            .map(|value| Complex32::new(value as f32, 0.0))
            .collect();

        let grid = model_grid_from_fixture(2, 3, values).unwrap();

        assert_eq!(grid.shape(), &[2, 3]);
        assert_eq!(grid.strides(), &[1, 2]);
        assert_eq!(grid[(0, 0)].re, 0.0);
        assert_eq!(grid[(1, 0)].re, 1.0);
        assert_eq!(grid[(0, 1)].re, 2.0);
        assert_eq!(grid[(1, 2)].re, 5.0);
    }

    #[test]
    #[ignore = "requires the frozen external four-SPW VLASS replay fixture and Metal"]
    fn four_spw_exact_replay_benchmark() {
        let four_spw_prefix = std::env::var_os(FOUR_SPW_BENCHMARK_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the frozen four-SPW VLASS replay fixture prefix");

        let four_spw = benchmark(&four_spw_prefix).expect("run frozen four-SPW replay fixture");
        assert_eq!(four_spw["segments"], 1);
        assert_eq!(four_spw["samples"], 6_416_526);
        assert_eq!(four_spw["rejected_samples"], 0);
        if effective_support_requested() {
            assert_effective_support_admission(
                &four_spw,
                true,
                "rejected",
                Some("single_segment_no_prefetch_overlap"),
                0,
                0,
            );
        } else {
            assert_effective_support_admission(&four_spw, false, "not_requested", None, 0, 0);
        }
        for value in four_spw["nrmse"].as_array().expect("four-SPW NRMSE array") {
            assert!(
                value.as_f64().expect("finite benchmark NRMSE") <= 1e-3,
                "four-SPW benchmark NRMSE exceeds 1e-3: {value}"
            );
        }
    }

    #[test]
    #[ignore = "requires the frozen external 62 GiB VLASS replay fixture and Metal"]
    fn full16_exact_replay_benchmark() {
        let full16_prefix = std::env::var_os(FULL16_BENCHMARK_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the frozen full-16-SPW VLASS replay fixture prefix");
        let four_spw_prefix = std::env::var_os(FOUR_SPW_BENCHMARK_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the frozen four-SPW VLASS replay fixture prefix");

        let full16 = benchmark(&full16_prefix).expect("run frozen full-16-SPW replay fixture");
        assert_eq!(full16["segments"], 10);
        assert_eq!(full16["samples"], 25_030_848);
        assert_eq!(full16["rejected_samples"], 0);
        let effective_support_enabled = effective_support_requested();
        if effective_support_enabled {
            assert_effective_support_admission(&full16, true, "enabled", None, 10, 10);
        } else {
            assert_effective_support_admission(&full16, false, "not_requested", None, 0, 0);
        }
        let provenance = &full16["provenance"];
        assert_eq!(provenance["field_ids"].as_array().map(Vec::len), Some(63));
        assert_eq!(
            provenance["spw_ids"],
            json!([2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17])
        );
        assert_eq!(provenance["use_pointing"], true);
        for value in full16["nrmse"].as_array().expect("full-16-SPW NRMSE array") {
            assert!(
                value.as_f64().expect("finite benchmark NRMSE") <= 1e-3,
                "benchmark NRMSE exceeds 1e-3: {value}"
            );
        }

        let four_spw = benchmark(&four_spw_prefix).expect("run frozen four-SPW replay fixture");
        assert_eq!(four_spw["segments"], 1);
        assert_eq!(four_spw["samples"], 6_416_526);
        assert_eq!(four_spw["rejected_samples"], 0);
        if effective_support_enabled {
            assert_effective_support_admission(
                &four_spw,
                true,
                "rejected",
                Some("single_segment_no_prefetch_overlap"),
                0,
                0,
            );
        } else {
            assert_effective_support_admission(&four_spw, false, "not_requested", None, 0, 0);
        }
        let provenance = &four_spw["provenance"];
        assert_eq!(provenance["field_ids"].as_array().map(Vec::len), Some(63));
        assert_eq!(provenance["spw_ids"], json!([2, 7, 12, 17]));
        assert_eq!(provenance["use_pointing"], true);
        for value in four_spw["nrmse"].as_array().expect("four-SPW NRMSE array") {
            assert!(
                value.as_f64().expect("finite benchmark NRMSE") <= 1e-3,
                "four-SPW benchmark NRMSE exceeds 1e-3: {value}"
            );
        }

        let result = json!({
            "schema": "casa-rs-vlass-full16-aw-replay-campaign-v1",
            "seconds": full16["seconds"],
            "full16": full16,
            "four_spw": four_spw,
        });
        println!(
            "VLASS_REPLAY_BENCHMARK_JSON {}",
            serde_json::to_string(&result).expect("serialize benchmark result")
        );
    }
}
