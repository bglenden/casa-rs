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
#[cfg(test)]
use std::io::Read;
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
    AWPROJECT_METAL_EFFECTIVE_SUPPORT_FOCUSED_OMITTED_ENERGY_FRACTION, AwProjectCompactReplayStats,
    AwProjectMetalAotGroupedTileProgram, AwProjectMetalBatch, AwProjectMetalEffectiveSupportConfig,
    AwProjectMetalGroupedTilePlan, AwProjectMetalGroupedTileProgram,
    AwProjectMetalPrefetchSequenceStats, AwProjectMetalPrefetchedProgram,
    AwProjectMetalResidentMetadata, AwProjectMetalResidentProgram, AwProjectMetalResidualScalePlan,
    AwProjectMetalSampleRoleGroups, AwProjectMetalSpillStore, AwProjectMetalTilePlan,
    DirtyProductFftPolicy, MosaicDirectMetalMode, MosaicMtmfsStreamGridAccumulation,
    MosaicMtmfsStreamGridConfig, MosaicPointingWeightAccumulator,
    awproject_metal_runtime_build_counter_snapshot, compile_awproject_metal_aot_grouped_tile,
    hash_awproject_copy_slice, replay_awproject_metal_global_program,
};
use super::{
    AwProjectMetalAotGroupedTileLedger, AwProjectMetalAotGroupedTileReceipt,
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
#[cfg(test)]
const AOT_GROUPED_SIDECAR_FULL16_PREFIX_ENV: &str =
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_SIDECAR_FULL16_PREFIX";
#[cfg(test)]
const AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX_ENV: &str =
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX";
#[cfg(test)]
const AOT_GROUPED_RAW_PAYLOAD_SHA256_ENV: &str =
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_RAW_PAYLOAD_SHA256";
#[cfg(test)]
const AOT_GROUPED_COMPILE_RAW_PREFIX_ENV: &str =
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_COMPILE_RAW_PREFIX";
#[cfg(test)]
const AOT_GROUPED_COMPILE_SIDECAR_PREFIX_ENV: &str =
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_COMPILE_SIDECAR_PREFIX";
#[cfg(test)]
const AOT_GROUPED_COMPILER_BINARY_SHA256_ENV: &str =
    "CASA_RS_EXPERIMENTAL_AWPROJECT_AOT_GROUPED_COMPILER_BINARY_SHA256";
const SCHEMA: &str = "casa-rs-vlass-full16-aw-replay-private-v1";
#[cfg(test)]
const AOT_GROUPED_SIDECAR_SCHEMA: &str = "casa-rs-vlass-aw-replay-aot-grouped-tile-sidecar-v3";
#[cfg(test)]
const AOT_GROUPED_COMPILER_CONTRACT: &str =
    "effective-support-1e-6-incumbent-groups-source-role-map-v1";

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

fn aot_receipt_json(receipt: &AwProjectMetalAotGroupedTileReceipt) -> Value {
    json!({
        "omitted_energy_fraction_bits": receipt.omitted_energy_fraction_bits,
        "sample_count": receipt.sample_count,
        "group_count": receipt.group_count,
        "crop_decisions_sha256": hex(&receipt.crop_decisions_sha256),
        "grouped_plans_sha256": hex(&receipt.grouped_plans_sha256),
        "sample_role_groups_sha256": hex(&receipt.sample_role_groups_sha256),
        "grouped_route_sha256": hex(&receipt.grouped_route_sha256),
        "legacy_grouped_plans_sha256": hex(&receipt.legacy_grouped_plans_sha256),
        "legacy_grouped_route_sha256": hex(&receipt.legacy_grouped_route_sha256),
        "ledger": {
            "raw_resident_bytes_before_compile": receipt.ledger.raw_resident_bytes_before_compile,
            "raw_prediction_sample_bytes_replaced":
                receipt.ledger.raw_prediction_sample_bytes_replaced,
            "cropped_prediction_sample_bytes":
                receipt.ledger.cropped_prediction_sample_bytes,
            "raw_tile_sample_bytes_released": receipt.ledger.raw_tile_sample_bytes_released,
            "raw_route_bytes_released": receipt.ledger.raw_route_bytes_released,
            "grouped_plan_bytes": receipt.ledger.grouped_plan_bytes,
            "sample_role_group_bytes": receipt.ledger.sample_role_group_bytes,
            "grouped_route_bytes": receipt.ledger.grouped_route_bytes,
            "canonical_group_plan_capacity_bytes":
                receipt.ledger.canonical_group_plan_capacity_bytes,
            "canonical_group_sum_capacity_bytes":
                receipt.ledger.canonical_group_sum_capacity_bytes,
            "canonical_hashmap_estimated_bytes":
                receipt.ledger.canonical_hashmap_estimated_bytes,
            "tile_planner_known_peak_bytes": receipt.ledger.tile_planner_known_peak_bytes,
            "sample_role_group_capacity_bytes":
                receipt.ledger.sample_role_group_capacity_bytes,
            "final_hashmap_estimated_bytes": receipt.ledger.final_hashmap_estimated_bytes,
            "aot_group_sum_bytes": receipt.ledger.aot_group_sum_bytes,
            "fixed_scale_bytes": receipt.ledger.fixed_scale_bytes,
            "effective_support_hashmap_estimated_bytes":
                receipt.ledger.effective_support_hashmap_estimated_bytes,
            "effective_support_prefix_scratch_bytes":
                receipt.ledger.effective_support_prefix_scratch_bytes,
            "effective_support_scratch_estimated_bytes":
                receipt.ledger.effective_support_scratch_estimated_bytes,
            "compile_transient_bytes_peak_estimated":
                receipt.ledger.compile_transient_bytes_peak_estimated,
            "hashmap_uncertainty_reserve_bytes":
                receipt.ledger.hashmap_uncertainty_reserve_bytes,
            "compile_admission_bytes": receipt.ledger.compile_admission_bytes,
            "compile_admission_limit_bytes": receipt.ledger.compile_admission_limit_bytes,
            "persisted_tile_bytes": receipt.ledger.persisted_tile_bytes,
        },
    })
}

#[cfg(test)]
fn aot_receipt_from_json(
    value: &Value,
) -> Result<AwProjectMetalAotGroupedTileReceipt, ImagingError> {
    let hash = |key: &str| {
        decode_hex(value.get(key).and_then(Value::as_str).ok_or_else(|| {
            ImagingError::InvalidRequest(format!("VLASS AOT grouped-tile receipt is missing {key}"))
        })?)
    };
    let ledger = value.get("ledger").ok_or_else(|| {
        ImagingError::InvalidRequest(
            "VLASS AOT grouped-tile receipt is missing its byte ledger".to_string(),
        )
    })?;
    Ok(AwProjectMetalAotGroupedTileReceipt {
        omitted_energy_fraction_bits: required_u64(value, "omitted_energy_fraction_bits")?,
        sample_count: to_usize(required_u64(value, "sample_count")?, "sample_count")?,
        group_count: to_usize(required_u64(value, "group_count")?, "group_count")?,
        crop_decisions_sha256: hash("crop_decisions_sha256")?,
        grouped_plans_sha256: hash("grouped_plans_sha256")?,
        sample_role_groups_sha256: hash("sample_role_groups_sha256")?,
        grouped_route_sha256: hash("grouped_route_sha256")?,
        legacy_grouped_plans_sha256: hash("legacy_grouped_plans_sha256")?,
        legacy_grouped_route_sha256: hash("legacy_grouped_route_sha256")?,
        ledger: AwProjectMetalAotGroupedTileLedger {
            raw_resident_bytes_before_compile: to_usize(
                required_u64(ledger, "raw_resident_bytes_before_compile")?,
                "raw_resident_bytes_before_compile",
            )?,
            raw_prediction_sample_bytes_replaced: to_usize(
                required_u64(ledger, "raw_prediction_sample_bytes_replaced")?,
                "raw_prediction_sample_bytes_replaced",
            )?,
            cropped_prediction_sample_bytes: to_usize(
                required_u64(ledger, "cropped_prediction_sample_bytes")?,
                "cropped_prediction_sample_bytes",
            )?,
            raw_tile_sample_bytes_released: to_usize(
                required_u64(ledger, "raw_tile_sample_bytes_released")?,
                "raw_tile_sample_bytes_released",
            )?,
            raw_route_bytes_released: to_usize(
                required_u64(ledger, "raw_route_bytes_released")?,
                "raw_route_bytes_released",
            )?,
            grouped_plan_bytes: to_usize(
                required_u64(ledger, "grouped_plan_bytes")?,
                "grouped_plan_bytes",
            )?,
            sample_role_group_bytes: to_usize(
                required_u64(ledger, "sample_role_group_bytes")?,
                "sample_role_group_bytes",
            )?,
            grouped_route_bytes: to_usize(
                required_u64(ledger, "grouped_route_bytes")?,
                "grouped_route_bytes",
            )?,
            canonical_group_plan_capacity_bytes: to_usize(
                required_u64(ledger, "canonical_group_plan_capacity_bytes")?,
                "canonical_group_plan_capacity_bytes",
            )?,
            canonical_group_sum_capacity_bytes: to_usize(
                required_u64(ledger, "canonical_group_sum_capacity_bytes")?,
                "canonical_group_sum_capacity_bytes",
            )?,
            canonical_hashmap_estimated_bytes: to_usize(
                required_u64(ledger, "canonical_hashmap_estimated_bytes")?,
                "canonical_hashmap_estimated_bytes",
            )?,
            tile_planner_known_peak_bytes: to_usize(
                required_u64(ledger, "tile_planner_known_peak_bytes")?,
                "tile_planner_known_peak_bytes",
            )?,
            sample_role_group_capacity_bytes: to_usize(
                required_u64(ledger, "sample_role_group_capacity_bytes")?,
                "sample_role_group_capacity_bytes",
            )?,
            final_hashmap_estimated_bytes: to_usize(
                required_u64(ledger, "final_hashmap_estimated_bytes")?,
                "final_hashmap_estimated_bytes",
            )?,
            aot_group_sum_bytes: to_usize(
                required_u64(ledger, "aot_group_sum_bytes")?,
                "aot_group_sum_bytes",
            )?,
            fixed_scale_bytes: to_usize(
                required_u64(ledger, "fixed_scale_bytes")?,
                "fixed_scale_bytes",
            )?,
            effective_support_hashmap_estimated_bytes: to_usize(
                required_u64(ledger, "effective_support_hashmap_estimated_bytes")?,
                "effective_support_hashmap_estimated_bytes",
            )?,
            effective_support_prefix_scratch_bytes: to_usize(
                required_u64(ledger, "effective_support_prefix_scratch_bytes")?,
                "effective_support_prefix_scratch_bytes",
            )?,
            effective_support_scratch_estimated_bytes: to_usize(
                required_u64(ledger, "effective_support_scratch_estimated_bytes")?,
                "effective_support_scratch_estimated_bytes",
            )?,
            compile_transient_bytes_peak_estimated: to_usize(
                required_u64(ledger, "compile_transient_bytes_peak_estimated")?,
                "compile_transient_bytes_peak_estimated",
            )?,
            hashmap_uncertainty_reserve_bytes: to_usize(
                required_u64(ledger, "hashmap_uncertainty_reserve_bytes")?,
                "hashmap_uncertainty_reserve_bytes",
            )?,
            compile_admission_bytes: to_usize(
                required_u64(ledger, "compile_admission_bytes")?,
                "compile_admission_bytes",
            )?,
            compile_admission_limit_bytes: to_usize(
                required_u64(ledger, "compile_admission_limit_bytes")?,
                "compile_admission_limit_bytes",
            )?,
            persisted_tile_bytes: to_usize(
                required_u64(ledger, "persisted_tile_bytes")?,
                "persisted_tile_bytes",
            )?,
        },
    })
}

#[cfg(test)]
fn validate_aot_compile_ledger(
    receipt: &AwProjectMetalAotGroupedTileReceipt,
    effective_support: &Value,
) -> Result<(), ImagingError> {
    let ledger = &receipt.ledger;
    let persisted_tile_bytes = ledger
        .grouped_plan_bytes
        .saturating_add(ledger.sample_role_group_bytes)
        .saturating_add(ledger.grouped_route_bytes);
    let hashmap_estimated_bytes = ledger
        .canonical_hashmap_estimated_bytes
        .saturating_add(ledger.final_hashmap_estimated_bytes)
        .saturating_add(ledger.effective_support_hashmap_estimated_bytes);
    let estimated_peak = [
        ledger.raw_resident_bytes_before_compile,
        ledger.canonical_group_plan_capacity_bytes,
        ledger.canonical_group_sum_capacity_bytes,
        ledger.tile_planner_known_peak_bytes,
        ledger.sample_role_group_capacity_bytes,
        ledger.aot_group_sum_bytes,
        ledger.fixed_scale_bytes,
        ledger.effective_support_prefix_scratch_bytes,
        hashmap_estimated_bytes,
    ]
    .into_iter()
    .fold(0usize, usize::saturating_add);
    let hashmap_reserve = hashmap_estimated_bytes
        .saturating_mul(super::AWPROJECT_METAL_AOT_HASHMAP_RESERVE_MULTIPLIER)
        .max(super::AWPROJECT_METAL_AOT_HASHMAP_MINIMUM_RESERVE_BYTES);
    let support_scratch = to_usize(
        required_u64(effective_support, "index_estimated_bytes")?,
        "index_estimated_bytes",
    )?
    .saturating_add(to_usize(
        required_u64(effective_support, "prefix_scratch_peak_bytes")?,
        "prefix_scratch_peak_bytes",
    )?);
    if ledger.raw_resident_bytes_before_compile == 0
        || ledger.raw_prediction_sample_bytes_replaced == 0
        || ledger.raw_prediction_sample_bytes_replaced != ledger.cropped_prediction_sample_bytes
        || ledger.canonical_group_plan_capacity_bytes < ledger.grouped_plan_bytes
        || ledger.sample_role_group_capacity_bytes < ledger.sample_role_group_bytes
        || ledger.persisted_tile_bytes != persisted_tile_bytes
        || ledger.effective_support_hashmap_estimated_bytes
            != to_usize(
                required_u64(effective_support, "index_estimated_bytes")?,
                "index_estimated_bytes",
            )?
        || ledger.effective_support_prefix_scratch_bytes
            != to_usize(
                required_u64(effective_support, "prefix_scratch_peak_bytes")?,
                "prefix_scratch_peak_bytes",
            )?
        || ledger.effective_support_scratch_estimated_bytes != support_scratch
        || ledger.compile_transient_bytes_peak_estimated != estimated_peak
        || ledger.hashmap_uncertainty_reserve_bytes != hashmap_reserve
        || ledger.compile_admission_bytes != estimated_peak.saturating_add(hashmap_reserve)
        || ledger.compile_admission_limit_bytes
            != super::AWPROJECT_METAL_AOT_COMPILE_ADMISSION_LIMIT_BYTES
        || ledger.compile_admission_bytes > ledger.compile_admission_limit_bytes
    {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile compile admission ledger changed".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn validate_compact_source_sample_indices(
    section: AwProjectMetalSpillSection,
    sample_count: usize,
    raw_payload_bytes: u64,
) -> Result<(), ImagingError> {
    let empty_sha256: [u8; 32] = Sha256::digest(b"").into();
    let expected_bytes = section.len.checked_mul(size_of::<u32>()).ok_or_else(|| {
        ImagingError::InvalidRequest(
            "VLASS compact source-sample index byte count overflowed".to_string(),
        )
    })?;
    let section_end = u64::try_from(section.byte_len)
        .ok()
        .and_then(|byte_len| section.offset.checked_add(byte_len));
    let cardinality_is_canonical = section.len == 0 || section.len == sample_count;
    let hash_matches_cardinality = if section.len == 0 {
        section.sha256 == empty_sha256
    } else {
        section.sha256 != empty_sha256
    };
    if !cardinality_is_canonical
        || section.byte_len != expected_bytes
        || section_end.is_none_or(|end| end > raw_payload_bytes)
        || !hash_matches_cardinality
    {
        return Err(ImagingError::Normalization(format!(
            "VLASS compact source-sample index topology changed: indices={}, samples={}, \
             bytes={}, expected_bytes={expected_bytes}",
            section.len, sample_count, section.byte_len,
        )));
    }
    Ok(())
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
    let aot = match (
        program.aot_grouped_plans,
        program.aot_sample_role_groups,
        program.aot_active_tile_ids,
        program.aot_tile_fragment_offsets,
        program.aot_fragments,
        program.aot_receipt.as_ref(),
    ) {
        (
            Some(groups),
            Some(mappings),
            Some(active),
            Some(offsets),
            Some(fragments),
            Some(receipt),
        ) => json!({
            "grouped_plans": section_json(groups),
            "sample_role_groups": section_json(mappings),
            "active_tile_ids": section_json(active),
            "tile_fragment_offsets": section_json(offsets),
            "fragments": section_json(fragments),
            "receipt": aot_receipt_json(receipt),
        }),
        _ => Value::Null,
    };
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
        "aot_grouped_tile": aot,
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
    let aot = value
        .get("aot_grouped_tile")
        .filter(|value| !value.is_null());
    let aot_section = |key: &str| {
        aot.map(|aot| {
            section_from_json(
                aot.get(key).ok_or_else(|| {
                    ImagingError::InvalidRequest(format!(
                        "VLASS replay AOT grouped-tile program is missing {key}"
                    ))
                })?,
                key,
            )
        })
        .transpose()
    };
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
        aot_grouped_plans: aot_section("grouped_plans")?,
        aot_sample_role_groups: aot_section("sample_role_groups")?,
        aot_active_tile_ids: aot_section("active_tile_ids")?,
        aot_tile_fragment_offsets: aot_section("tile_fragment_offsets")?,
        aot_fragments: aot_section("fragments")?,
        aot_receipt: aot
            .map(|aot| {
                aot_receipt_from_json(aot.get("receipt").ok_or_else(|| {
                    ImagingError::InvalidRequest(
                        "VLASS replay AOT grouped-tile program is missing its receipt".to_string(),
                    )
                })?)
            })
            .transpose()?,
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

#[cfg(test)]
#[derive(Clone)]
struct AotGroupedSidecarProgram {
    prediction_samples: AwProjectMetalSpillSection,
    grouped_plans: AwProjectMetalSpillSection,
    sample_role_groups: AwProjectMetalSpillSection,
    active_tile_ids: AwProjectMetalSpillSection,
    tile_fragment_offsets: AwProjectMetalSpillSection,
    fragments: AwProjectMetalSpillSection,
    tile_side: usize,
    tiles_y: usize,
    source_programs: usize,
    payload_bytes: usize,
    receipt: AwProjectMetalAotGroupedTileReceipt,
    effective_support: Value,
}

#[cfg(test)]
fn aot_sidecar_program_json(program: &AotGroupedSidecarProgram) -> Value {
    json!({
        "prediction_samples": section_json(program.prediction_samples),
        "grouped_plans": section_json(program.grouped_plans),
        "sample_role_groups": section_json(program.sample_role_groups),
        "active_tile_ids": section_json(program.active_tile_ids),
        "tile_fragment_offsets": section_json(program.tile_fragment_offsets),
        "fragments": section_json(program.fragments),
        "tile_side": program.tile_side,
        "tiles_y": program.tiles_y,
        "source_programs": program.source_programs,
        "payload_bytes": program.payload_bytes,
        "receipt": aot_receipt_json(&program.receipt),
        "effective_support": program.effective_support,
    })
}

#[cfg(test)]
fn aot_sidecar_program_from_json(value: &Value) -> Result<AotGroupedSidecarProgram, ImagingError> {
    let section = |key: &str| {
        section_from_json(
            value.get(key).ok_or_else(|| {
                ImagingError::InvalidRequest(format!(
                    "VLASS AOT grouped-tile sidecar program is missing {key}"
                ))
            })?,
            key,
        )
    };
    Ok(AotGroupedSidecarProgram {
        prediction_samples: section("prediction_samples")?,
        grouped_plans: section("grouped_plans")?,
        sample_role_groups: section("sample_role_groups")?,
        active_tile_ids: section("active_tile_ids")?,
        tile_fragment_offsets: section("tile_fragment_offsets")?,
        fragments: section("fragments")?,
        tile_side: to_usize(required_u64(value, "tile_side")?, "tile_side")?,
        tiles_y: to_usize(required_u64(value, "tiles_y")?, "tiles_y")?,
        source_programs: to_usize(required_u64(value, "source_programs")?, "source_programs")?,
        payload_bytes: to_usize(required_u64(value, "payload_bytes")?, "payload_bytes")?,
        receipt: aot_receipt_from_json(value.get("receipt").ok_or_else(|| {
            ImagingError::InvalidRequest(
                "VLASS AOT grouped-tile sidecar program is missing receipt".to_string(),
            )
        })?)?,
        effective_support: value.get("effective_support").cloned().ok_or_else(|| {
            ImagingError::InvalidRequest(
                "VLASS AOT grouped-tile sidecar program is missing effective-support receipt"
                    .to_string(),
            )
        })?,
    })
}

fn program_sections(
    program: &AwProjectMetalSpilledProgram,
) -> Vec<(&'static str, AwProjectMetalSpillSection)> {
    let mut sections = vec![
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
    ];
    sections.extend(
        [
            ("aot_grouped_plans", program.aot_grouped_plans),
            ("aot_sample_role_groups", program.aot_sample_role_groups),
            ("aot_active_tile_ids", program.aot_active_tile_ids),
            (
                "aot_tile_fragment_offsets",
                program.aot_tile_fragment_offsets,
            ),
            ("aot_fragments", program.aot_fragments),
        ]
        .into_iter()
        .filter_map(|(label, section)| section.map(|section| (label, section))),
    );
    sections
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
fn sha256_path(path: &Path) -> Result<[u8; 32], ImagingError> {
    let mut file = File::open(path).map_err(|error| {
        ImagingError::InvalidRequest(format!("open {} for SHA-256: {error}", path.display()))
    })?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; 8 * 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            ImagingError::InvalidRequest(format!("hash {}: {error}", path.display()))
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}

#[cfg(test)]
fn validate_raw_program_hashes(
    descriptor: &AwProjectMetalSpilledProgram,
    program: &AwProjectMetalResidentProgram,
) -> Result<(), ImagingError> {
    let checks = [
        (
            "prediction_samples",
            descriptor.prediction_samples.sha256,
            hash_awproject_copy_slice(&program.prediction_batch.samples),
        ),
        (
            "source_sample_indices",
            descriptor.source_sample_indices.sha256,
            hash_awproject_copy_slice(&program.prediction_batch.source_sample_indices),
        ),
        (
            "kernels",
            descriptor.kernels.sha256,
            hash_awproject_copy_slice(&program.prediction_batch.kernels),
        ),
        (
            "prediction_phases",
            descriptor.prediction_phases.sha256,
            hash_awproject_copy_slice(&program.prediction_batch.phases),
        ),
        (
            "tile_samples",
            descriptor.tile_samples.sha256,
            hash_awproject_copy_slice(&program.tile_batch.samples),
        ),
        (
            "tile_phases",
            descriptor.tile_phases.sha256,
            hash_awproject_copy_slice(&program.tile_batch.phases),
        ),
        (
            "term_weights",
            descriptor.term_weights.sha256,
            hash_awproject_copy_slice(&program.tile_batch.term_weights),
        ),
        (
            "active_tile_ids",
            descriptor.active_tile_ids.sha256,
            hash_awproject_copy_slice(&program.tile_plan.active_tile_ids),
        ),
        (
            "tile_fragment_offsets",
            descriptor.tile_fragment_offsets.sha256,
            hash_awproject_copy_slice(&program.tile_plan.tile_fragment_offsets),
        ),
        (
            "fragments",
            descriptor.fragments.sha256,
            hash_awproject_copy_slice(&program.tile_plan.fragments),
        ),
    ];
    for (label, expected, actual) in checks {
        if expected == [0; 32] || actual != expected {
            return Err(ImagingError::Normalization(format!(
                "VLASS raw replay fixture {label} section hash changed"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
fn compile_aot_grouped_sidecar(
    raw_prefix: &Path,
    sidecar_prefix: &Path,
) -> Result<Value, ImagingError> {
    let raw_manifest_path = PathBuf::from(format!("{}.json", raw_prefix.display()));
    let raw_manifest_bytes = std::fs::read(&raw_manifest_path).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "read raw VLASS replay fixture manifest {}: {error}",
            raw_manifest_path.display()
        ))
    })?;
    let raw_manifest_sha256: [u8; 32] = Sha256::digest(&raw_manifest_bytes).into();
    let (raw_manifest, raw_payload_path) = load_manifest(raw_prefix)?;
    let raw_payload_sha256 = std::env::var(AOT_GROUPED_RAW_PAYLOAD_SHA256_ENV).map_err(|_| {
        ImagingError::InvalidRequest(format!(
            "VLASS AOT sidecar compilation requires {AOT_GROUPED_RAW_PAYLOAD_SHA256_ENV}"
        ))
    })?;
    decode_hex(&raw_payload_sha256)?;
    let compiler_binary_sha256 =
        std::env::var(AOT_GROUPED_COMPILER_BINARY_SHA256_ENV).map_err(|_| {
            ImagingError::InvalidRequest(format!(
                "VLASS AOT sidecar compilation requires \
                 {AOT_GROUPED_COMPILER_BINARY_SHA256_ENV}"
            ))
        })?;
    decode_hex(&compiler_binary_sha256)?;
    let raw_payload_bytes = std::fs::metadata(&raw_payload_path)
        .map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "stat raw VLASS replay fixture payload {}: {error}",
                raw_payload_path.display()
            ))
        })?
        .len();
    if raw_manifest
        .get("payload")
        .and_then(|payload| payload.get("bytes"))
        .and_then(Value::as_u64)
        != Some(raw_payload_bytes)
    {
        return Err(ImagingError::Normalization(
            "raw VLASS replay fixture payload size changed before AOT compilation".to_string(),
        ));
    }
    let raw_programs = raw_manifest
        .get("programs")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ImagingError::InvalidRequest(
                "raw VLASS replay fixture has no program inventory".to_string(),
            )
        })?
        .iter()
        .map(program_from_json)
        .collect::<Result<Vec<_>, _>>()?;
    let shape = raw_manifest
        .get("grid_shape")
        .and_then(Value::as_array)
        .filter(|shape| shape.len() == 2)
        .ok_or_else(|| {
            ImagingError::InvalidRequest(
                "raw VLASS replay fixture has invalid grid geometry".to_string(),
            )
        })?;
    let grid_width = to_usize(
        shape[0].as_u64().ok_or_else(|| {
            ImagingError::InvalidRequest("invalid raw VLASS grid width".to_string())
        })?,
        "grid_width",
    )?;
    let grid_height = to_usize(
        shape[1].as_u64().ok_or_else(|| {
            ImagingError::InvalidRequest("invalid raw VLASS grid height".to_string())
        })?,
        "grid_height",
    )?;
    let raw_file = File::open(&raw_payload_path).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "open raw VLASS replay fixture payload {}: {error}",
            raw_payload_path.display()
        ))
    })?;
    let mut raw_store = AwProjectMetalSpillStore {
        file: raw_file,
        bytes_written: raw_payload_bytes,
        bytes_read: 0,
        fixture_paths: None,
    };
    let mut sidecar_store = AwProjectMetalSpillStore::create_fixture(sidecar_prefix)?;
    let paths = sidecar_store
        .fixture_paths
        .clone()
        .expect("AOT sidecar store has fixture paths");
    let mut sidecar_programs = Vec::with_capacity(raw_programs.len());
    for descriptor in &raw_programs {
        let mut program = raw_store.reload(descriptor)?;
        validate_raw_program_hashes(descriptor, &program)?;
        let support = compile_awproject_metal_aot_grouped_tile(
            &mut program,
            AwProjectMetalEffectiveSupportConfig {
                omitted_energy_fraction:
                    AWPROJECT_METAL_EFFECTIVE_SUPPORT_FOCUSED_OMITTED_ENERGY_FRACTION,
            },
            grid_width,
            grid_height,
        )?;
        let aot = program.aot_grouped_tile.take().ok_or_else(|| {
            ImagingError::Normalization(
                "VLASS AOT grouped-tile compiler returned no artifact".to_string(),
            )
        })?;
        let prediction_samples = sidecar_store
            .write_slice(&program.prediction_batch.samples, "AOT prediction samples")?;
        if aot.receipt.ledger.raw_prediction_sample_bytes_replaced
            != descriptor.prediction_samples.byte_len
            || aot.receipt.ledger.cropped_prediction_sample_bytes != prediction_samples.byte_len
        {
            return Err(ImagingError::Normalization(
                "VLASS AOT grouped-tile prediction replacement byte equation changed".to_string(),
            ));
        }
        let grouped_plans = sidecar_store.write_slice(&aot.grouped.groups, "AOT grouped plans")?;
        let sample_role_groups =
            sidecar_store.write_slice(&aot.sample_role_groups, "AOT sample-role groups")?;
        let active_tile_ids = sidecar_store.write_slice(
            &aot.grouped.tile_plan.active_tile_ids,
            "AOT active tile IDs",
        )?;
        let tile_fragment_offsets = sidecar_store.write_slice(
            &aot.grouped.tile_plan.tile_fragment_offsets,
            "AOT tile fragment offsets",
        )?;
        let fragments =
            sidecar_store.write_slice(&aot.grouped.tile_plan.fragments, "AOT tile fragments")?;
        let payload_bytes = [
            prediction_samples,
            grouped_plans,
            sample_role_groups,
            active_tile_ids,
            tile_fragment_offsets,
            fragments,
        ]
        .into_iter()
        .map(|section| section.byte_len)
        .sum();
        sidecar_programs.push(AotGroupedSidecarProgram {
            prediction_samples,
            grouped_plans,
            sample_role_groups,
            active_tile_ids,
            tile_fragment_offsets,
            fragments,
            tile_side: aot.grouped.tile_plan.tile_side,
            tiles_y: aot.grouped.tile_plan.tiles_y,
            source_programs: descriptor.source_programs,
            payload_bytes,
            receipt: aot.receipt,
            effective_support: effective_support_stats_json(&support),
        });
    }
    sidecar_store.file.sync_all().map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "sync VLASS AOT sidecar payload {}: {error}",
            paths.partial_payload.display()
        ))
    })?;
    std::fs::rename(&paths.partial_payload, &paths.payload).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "publish VLASS AOT sidecar payload {}: {error}",
            paths.payload.display()
        ))
    })?;
    let sidecar_payload_sha256 = sha256_path(&paths.payload)?;
    let raw_tile_sample_bytes = raw_programs
        .iter()
        .map(|program| program.tile_samples.byte_len)
        .sum::<usize>();
    let raw_prediction_sample_bytes = raw_programs
        .iter()
        .map(|program| program.prediction_samples.byte_len)
        .sum::<usize>();
    let cropped_prediction_sample_bytes = sidecar_programs
        .iter()
        .map(|program| program.prediction_samples.byte_len)
        .sum::<usize>();
    let raw_route_bytes = raw_programs
        .iter()
        .map(|program| {
            program
                .active_tile_ids
                .byte_len
                .saturating_add(program.tile_fragment_offsets.byte_len)
                .saturating_add(program.fragments.byte_len)
        })
        .sum::<usize>();
    let specialized_payload_bytes = sidecar_programs
        .iter()
        .map(|program| program.payload_bytes)
        .sum::<usize>();
    if raw_prediction_sample_bytes != cropped_prediction_sample_bytes {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile aggregate prediction replacement bytes changed".to_string(),
        ));
    }
    let manifest = json!({
        "schema": AOT_GROUPED_SIDECAR_SCHEMA,
        "role": "external_experimental_aot_grouped_tile_sidecar_not_production_default",
        "key": {
            "raw_manifest_sha256": hex(&raw_manifest_sha256),
            "raw_payload_sha256": raw_payload_sha256,
            "raw_payload_bytes": raw_payload_bytes,
            "omitted_energy_fraction_bits":
                AWPROJECT_METAL_EFFECTIVE_SUPPORT_FOCUSED_OMITTED_ENERGY_FRACTION.to_bits(),
            "private_layout": raw_manifest.get("private_layout").cloned().unwrap_or(Value::Null),
            "aot_private_layout": {
                "grouped_plan_bytes": size_of::<AwProjectMetalGroupedTilePlan>(),
                "grouped_plan_alignment": align_of::<AwProjectMetalGroupedTilePlan>(),
                "sample_role_group_bytes": size_of::<AwProjectMetalSampleRoleGroups>(),
                "sample_role_group_alignment": align_of::<AwProjectMetalSampleRoleGroups>(),
            },
            "compiler_contract": AOT_GROUPED_COMPILER_CONTRACT,
            "compiler_binary_sha256": compiler_binary_sha256,
        },
        "payload": {
            "path": paths.payload,
            "bytes": sidecar_store.bytes_written,
            "sha256": hex(&sidecar_payload_sha256),
        },
        "grid_shape": [grid_width, grid_height],
        "programs": sidecar_programs.iter().map(aot_sidecar_program_json).collect::<Vec<_>>(),
        "byte_lifetime_ledger": {
            "raw_prediction_sample_bytes_replaced_at_compile": raw_prediction_sample_bytes,
            "cropped_prediction_sample_bytes_persisted": cropped_prediction_sample_bytes,
            "raw_prediction_sample_bytes_retained_for_replay": 0,
            "raw_prediction_sample_bytes_read_during_replay": 0,
            "prediction_replacement_equation":
                "raw_prediction_sample_bytes_replaced_at_compile == \
                 cropped_prediction_sample_bytes_persisted",
            "raw_tile_sample_bytes_released_at_compile": raw_tile_sample_bytes,
            "raw_ungrouped_route_bytes_released_at_compile": raw_route_bytes,
            "specialized_sidecar_section_bytes": specialized_payload_bytes,
            "specialized_sidecar_file_bytes": sidecar_store.bytes_written,
            "raw_sections_referenced_not_copied": [
                "source_sample_indices",
                "kernels",
                "prediction_phases",
                "tile_phases",
                "term_weights",
            ],
            "raw_sections_replaced_not_read": [
                "prediction_samples",
                "tile_samples",
                "active_tile_ids",
                "tile_fragment_offsets",
                "fragments",
            ],
            "runtime_grouping_builds": 0,
            "runtime_sort_builds": 0,
            "runtime_route_builds": 0,
        },
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "serialize VLASS AOT grouped-tile sidecar manifest: {error}"
        ))
    })?;
    let mut manifest_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths.manifest)
        .map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "create VLASS AOT sidecar manifest {}: {error}",
                paths.manifest.display()
            ))
        })?;
    manifest_file.write_all(&manifest_bytes).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "write VLASS AOT sidecar manifest {}: {error}",
            paths.manifest.display()
        ))
    })?;
    manifest_file.sync_all().map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "sync VLASS AOT sidecar manifest {}: {error}",
            paths.manifest.display()
        ))
    })?;
    Ok(json!({
        "raw_fixture": raw_prefix,
        "sidecar": sidecar_prefix,
        "segments": sidecar_programs.len(),
        "samples": sidecar_programs.iter().map(|program| program.receipt.sample_count).sum::<usize>(),
        "raw_payload_sha256": raw_payload_sha256,
        "compiler_binary_sha256": compiler_binary_sha256,
        "sidecar_payload_sha256": hex(&sidecar_payload_sha256),
        "sidecar_payload_bytes": sidecar_store.bytes_written,
        "manifest_sha256": hex(&Sha256::digest(&manifest_bytes)),
    }))
}

#[cfg(test)]
fn load_aot_grouped_sidecar(
    raw_prefix: &Path,
    sidecar_prefix: &Path,
) -> Result<(Value, PathBuf, Vec<AotGroupedSidecarProgram>, [u8; 32]), ImagingError> {
    let manifest_path = PathBuf::from(format!("{}.json", sidecar_prefix.display()));
    let manifest_bytes = std::fs::read(&manifest_path).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "read VLASS AOT grouped-tile sidecar manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "parse VLASS AOT grouped-tile sidecar manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    if manifest.get("schema").and_then(Value::as_str) != Some(AOT_GROUPED_SIDECAR_SCHEMA) {
        return Err(ImagingError::InvalidRequest(
            "unexpected VLASS AOT grouped-tile sidecar schema".to_string(),
        ));
    }
    let raw_manifest_path = PathBuf::from(format!("{}.json", raw_prefix.display()));
    let raw_manifest_sha256 = sha256_path(&raw_manifest_path)?;
    let key = manifest.get("key").ok_or_else(|| {
        ImagingError::InvalidRequest(
            "VLASS AOT grouped-tile sidecar is missing its immutable key".to_string(),
        )
    })?;
    let expected_manifest_sha = decode_hex(
        key.get("raw_manifest_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "VLASS AOT grouped-tile sidecar key is missing raw manifest SHA-256"
                        .to_string(),
                )
            })?,
    )?;
    if raw_manifest_sha256 != expected_manifest_sha {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile sidecar raw manifest key changed".to_string(),
        ));
    }
    decode_hex(
        key.get("raw_payload_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "VLASS AOT grouped-tile sidecar key is missing raw payload SHA-256".to_string(),
                )
            })?,
    )?;
    if key
        .get("omitted_energy_fraction_bits")
        .and_then(Value::as_u64)
        != Some(AWPROJECT_METAL_EFFECTIVE_SUPPORT_FOCUSED_OMITTED_ENERGY_FRACTION.to_bits())
    {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile sidecar threshold key changed".to_string(),
        ));
    }
    let (raw_manifest, raw_payload_path) = load_manifest(raw_prefix)?;
    if key.get("private_layout") != raw_manifest.get("private_layout") {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile sidecar private-layout key changed".to_string(),
        ));
    }
    let aot_layout = key.get("aot_private_layout").ok_or_else(|| {
        ImagingError::InvalidRequest(
            "VLASS AOT grouped-tile sidecar key is missing its private AOT layout".to_string(),
        )
    })?;
    for (name, expected) in [
        (
            "grouped_plan_bytes",
            size_of::<AwProjectMetalGroupedTilePlan>(),
        ),
        (
            "grouped_plan_alignment",
            align_of::<AwProjectMetalGroupedTilePlan>(),
        ),
        (
            "sample_role_group_bytes",
            size_of::<AwProjectMetalSampleRoleGroups>(),
        ),
        (
            "sample_role_group_alignment",
            align_of::<AwProjectMetalSampleRoleGroups>(),
        ),
    ] {
        if aot_layout.get(name).and_then(Value::as_u64) != Some(expected as u64) {
            return Err(ImagingError::Normalization(format!(
                "VLASS AOT grouped-tile sidecar private layout {name} changed"
            )));
        }
    }
    if key.get("compiler_contract").and_then(Value::as_str) != Some(AOT_GROUPED_COMPILER_CONTRACT) {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile compiler contract changed".to_string(),
        ));
    }
    let expected_compiler_binary_sha256 = std::env::var(AOT_GROUPED_COMPILER_BINARY_SHA256_ENV)
        .map_err(|_| {
            ImagingError::InvalidRequest(format!(
                "VLASS AOT grouped-tile replay requires \
                 {AOT_GROUPED_COMPILER_BINARY_SHA256_ENV}"
            ))
        })?;
    let expected_compiler_binary_sha256 = decode_hex(&expected_compiler_binary_sha256)?;
    let sealed_compiler_binary_sha256 = decode_hex(
        key.get("compiler_binary_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "VLASS AOT grouped-tile sidecar key is missing compiler executable SHA-256"
                        .to_string(),
                )
            })?,
    )?;
    if sealed_compiler_binary_sha256 != expected_compiler_binary_sha256 {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile compiler executable changed".to_string(),
        ));
    }
    let raw_payload_bytes = std::fs::metadata(&raw_payload_path)
        .map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "stat VLASS raw replay payload {}: {error}",
                raw_payload_path.display()
            ))
        })?
        .len();
    if key.get("raw_payload_bytes").and_then(Value::as_u64) != Some(raw_payload_bytes) {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile sidecar raw payload size key changed".to_string(),
        ));
    }
    let payload = manifest.get("payload").ok_or_else(|| {
        ImagingError::InvalidRequest(
            "VLASS AOT grouped-tile sidecar is missing payload metadata".to_string(),
        )
    })?;
    let payload_path = payload
        .get("path")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| {
            ImagingError::InvalidRequest(
                "VLASS AOT grouped-tile sidecar is missing payload.path".to_string(),
            )
        })?;
    let payload_bytes = std::fs::metadata(&payload_path)
        .map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "stat VLASS AOT sidecar payload {}: {error}",
                payload_path.display()
            ))
        })?
        .len();
    if payload.get("bytes").and_then(Value::as_u64) != Some(payload_bytes) {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile sidecar payload size changed".to_string(),
        ));
    }
    let expected_payload_sha256 = decode_hex(
        payload
            .get("sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ImagingError::InvalidRequest(
                    "VLASS AOT grouped-tile sidecar is missing payload SHA-256".to_string(),
                )
            })?,
    )?;
    let programs = manifest
        .get("programs")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ImagingError::InvalidRequest(
                "VLASS AOT grouped-tile sidecar has no program inventory".to_string(),
            )
        })?
        .iter()
        .map(aot_sidecar_program_from_json)
        .collect::<Result<Vec<_>, _>>()?;
    let raw_programs = raw_manifest
        .get("programs")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ImagingError::InvalidRequest(
                "raw VLASS replay fixture has no program inventory".to_string(),
            )
        })?
        .iter()
        .map(program_from_json)
        .collect::<Result<Vec<_>, _>>()?;
    if raw_programs.len() != programs.len() {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile raw/sidecar program inventories differ".to_string(),
        ));
    }
    for (program, raw) in programs.iter().zip(&raw_programs) {
        validate_compact_source_sample_indices(
            raw.source_sample_indices,
            program.receipt.sample_count,
            raw_payload_bytes,
        )?;
        let sections = [
            program.prediction_samples,
            program.grouped_plans,
            program.sample_role_groups,
            program.active_tile_ids,
            program.tile_fragment_offsets,
            program.fragments,
        ];
        let section_bytes = sections
            .iter()
            .map(|section| section.byte_len)
            .sum::<usize>();
        if section_bytes != program.payload_bytes
            || sections.iter().any(|section| {
                section.sha256 == [0; 32]
                    || section
                        .offset
                        .checked_add(section.byte_len as u64)
                        .is_none_or(|end| end > payload_bytes)
            })
        {
            return Err(ImagingError::Normalization(
                "VLASS AOT grouped-tile sidecar section ledger changed".to_string(),
            ));
        }
        if program.grouped_plans.sha256 != program.receipt.grouped_plans_sha256
            || program.sample_role_groups.sha256 != program.receipt.sample_role_groups_sha256
            || program.receipt.omitted_energy_fraction_bits
                != AWPROJECT_METAL_EFFECTIVE_SUPPORT_FOCUSED_OMITTED_ENERGY_FRACTION.to_bits()
            || program.prediction_samples.len != program.receipt.sample_count
            || program.sample_role_groups.len != program.receipt.sample_count
            || program.grouped_plans.len != program.receipt.group_count
            || program.receipt.grouped_plans_sha256 != program.receipt.legacy_grouped_plans_sha256
            || program.receipt.grouped_route_sha256 != program.receipt.legacy_grouped_route_sha256
            || program.receipt.crop_decisions_sha256 == [0; 32]
        {
            return Err(ImagingError::Normalization(
                "VLASS AOT grouped-tile sidecar differential hash contract changed".to_string(),
            ));
        }
        if program.receipt.ledger.raw_prediction_sample_bytes_replaced
            != raw.prediction_samples.byte_len
            || program.receipt.ledger.cropped_prediction_sample_bytes
                != program.prediction_samples.byte_len
            || raw.prediction_samples.byte_len != program.prediction_samples.byte_len
        {
            return Err(ImagingError::Normalization(
                "VLASS AOT grouped-tile prediction replacement equation changed".to_string(),
            ));
        }
        validate_aot_compile_ledger(&program.receipt, &program.effective_support)?;
    }
    let raw_prediction_sample_bytes = raw_programs
        .iter()
        .map(|program| program.prediction_samples.byte_len)
        .sum::<usize>();
    let cropped_prediction_sample_bytes = programs
        .iter()
        .map(|program| program.prediction_samples.byte_len)
        .sum::<usize>();
    let lifetime = manifest.get("byte_lifetime_ledger").ok_or_else(|| {
        ImagingError::InvalidRequest(
            "VLASS AOT grouped-tile sidecar is missing its lifetime ledger".to_string(),
        )
    })?;
    let expected_references = json!([
        "source_sample_indices",
        "kernels",
        "prediction_phases",
        "tile_phases",
        "term_weights",
    ]);
    let expected_replacements = json!([
        "prediction_samples",
        "tile_samples",
        "active_tile_ids",
        "tile_fragment_offsets",
        "fragments",
    ]);
    if to_usize(
        required_u64(lifetime, "raw_prediction_sample_bytes_replaced_at_compile")?,
        "raw_prediction_sample_bytes_replaced_at_compile",
    )? != raw_prediction_sample_bytes
        || to_usize(
            required_u64(lifetime, "cropped_prediction_sample_bytes_persisted")?,
            "cropped_prediction_sample_bytes_persisted",
        )? != cropped_prediction_sample_bytes
        || raw_prediction_sample_bytes != cropped_prediction_sample_bytes
        || required_u64(lifetime, "raw_prediction_sample_bytes_retained_for_replay")? != 0
        || required_u64(lifetime, "raw_prediction_sample_bytes_read_during_replay")? != 0
        || lifetime
            .get("prediction_replacement_equation")
            .and_then(Value::as_str)
            != Some(
                "raw_prediction_sample_bytes_replaced_at_compile == \
                 cropped_prediction_sample_bytes_persisted",
            )
        || lifetime.get("raw_sections_referenced_not_copied") != Some(&expected_references)
        || lifetime.get("raw_sections_replaced_not_read") != Some(&expected_replacements)
    {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile aggregate prediction lifetime changed".to_string(),
        ));
    }
    Ok((manifest, payload_path, programs, expected_payload_sha256))
}

#[cfg(test)]
fn reload_aot_grouped_prefetched(
    raw_store: &AwProjectMetalSpillStore,
    sidecar_store: &AwProjectMetalSpillStore,
    raw: &AwProjectMetalSpilledProgram,
    sidecar: &AotGroupedSidecarProgram,
) -> Result<AwProjectMetalPrefetchedProgram, ImagingError> {
    let started = Instant::now();
    validate_compact_source_sample_indices(
        raw.source_sample_indices,
        sidecar.receipt.sample_count,
        raw_store.bytes_written,
    )?;
    if raw.source_programs != sidecar.source_programs
        || raw.prediction_samples.len != sidecar.receipt.sample_count
        || raw.term_weights.len != sidecar.receipt.sample_count.saturating_mul(2)
        || sidecar.prediction_samples.len != sidecar.receipt.sample_count
        || sidecar.sample_role_groups.len != sidecar.receipt.sample_count
        || sidecar.grouped_plans.len != sidecar.receipt.group_count
    {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile sidecar segment topology changed".to_string(),
        ));
    }
    let prediction_samples =
        sidecar_store.read_vec_at(sidecar.prediction_samples, "AOT prediction samples")?;
    let groups = sidecar_store
        .read_vec_at::<AwProjectMetalGroupedTilePlan>(sidecar.grouped_plans, "AOT grouped plans")?;
    let sample_role_groups = sidecar_store.read_vec_at::<AwProjectMetalSampleRoleGroups>(
        sidecar.sample_role_groups,
        "AOT sample-role groups",
    )?;
    let grouped_tile_plan = AwProjectMetalTilePlan {
        tile_side: sidecar.tile_side,
        tiles_y: sidecar.tiles_y,
        active_tile_ids: sidecar_store
            .read_vec_at(sidecar.active_tile_ids, "AOT active tile IDs")?,
        tile_fragment_offsets: sidecar_store
            .read_vec_at(sidecar.tile_fragment_offsets, "AOT tile fragment offsets")?,
        fragments: sidecar_store.read_vec_at(sidecar.fragments, "AOT tile fragments")?,
        plan_elapsed: std::time::Duration::ZERO,
    };
    // The benchmark verifies the complete sidecar payload after starting its
    // replay timer. Section reloads do not rehash it a second time.
    let raw_bytes = [
        raw.source_sample_indices,
        raw.kernels,
        raw.prediction_phases,
        raw.tile_phases,
        raw.term_weights,
    ]
    .into_iter()
    .map(|section| section.byte_len)
    .sum::<usize>();
    let program = AwProjectMetalResidentProgram {
        prediction_batch: super::AwProjectMetalPredictionBatch {
            samples: prediction_samples,
            source_sample_indices: raw_store
                .read_vec_at(raw.source_sample_indices, "source sample indices")?,
            cf_metadata: Vec::new(),
            kernels: raw_store.read_vec_at(raw.kernels, "shared kernels")?,
            phases: raw_store.read_vec_at(raw.prediction_phases, "prediction phases")?,
        },
        tile_batch: AwProjectMetalBatch {
            samples: Vec::new(),
            kernels: Vec::new(),
            phases: raw_store.read_vec_at(raw.tile_phases, "tile phases")?,
            term_weights: raw_store.read_vec_at(raw.term_weights, "term weights")?,
            kernel_pack: std::time::Duration::ZERO,
        },
        tile_plan: AwProjectMetalTilePlan::default(),
        aot_grouped_tile: Some(AwProjectMetalAotGroupedTileProgram {
            group_sums: vec![[0.0; 4]; groups.len()],
            grouped: AwProjectMetalGroupedTileProgram {
                groups,
                tile_plan: grouped_tile_plan,
            },
            sample_role_groups,
            fixed_scales: Vec::with_capacity(2),
            inverse_fixed_scales: Vec::with_capacity(2),
            receipt: sidecar.receipt.clone(),
        }),
        residual_scale_plan: raw.residual_scale_plan,
        selected_model_plan: None,
        metadata: raw.metadata.clone(),
    };
    let sidecar_bytes = sidecar.payload_bytes;
    Ok(AwProjectMetalPrefetchedProgram {
        program,
        bytes_read: u64::try_from(raw_bytes.saturating_add(sidecar_bytes)).map_err(|_| {
            ImagingError::InvalidRequest(
                "VLASS AOT grouped-tile replay read-byte count exceeds u64".to_string(),
            )
        })?,
        reload_elapsed: started.elapsed(),
        effective_support: None,
    })
}

#[cfg(test)]
fn replay_aot_grouped_prefetched_sequence(
    raw_store: &AwProjectMetalSpillStore,
    sidecar_store: &AwProjectMetalSpillStore,
    raw_programs: &[AwProjectMetalSpilledProgram],
    sidecar_programs: &[AotGroupedSidecarProgram],
    mut consume: impl FnMut(
        usize,
        &AwProjectMetalSpilledProgram,
        &AotGroupedSidecarProgram,
        AwProjectMetalPrefetchedProgram,
    ) -> Result<(), ImagingError>,
) -> Result<AwProjectMetalPrefetchSequenceStats, ImagingError> {
    let runtime_builds_before = awproject_metal_runtime_build_counter_snapshot();
    if raw_programs.len() != sidecar_programs.len() {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile raw/sidecar segment inventories differ".to_string(),
        ));
    }
    let mut stats = AwProjectMetalPrefetchSequenceStats {
        requested: true,
        decision: super::AwProjectMetalEffectiveSupportAdmissionDecision::Enabled,
        segment_count: raw_programs.len(),
        ..AwProjectMetalPrefetchSequenceStats::default()
    };
    let Some((first_raw, first_sidecar)) = raw_programs.first().zip(sidecar_programs.first())
    else {
        stats.record_runtime_build_delta(runtime_builds_before);
        return Ok(stats);
    };
    let initial_started = Instant::now();
    let mut current = Some(reload_aot_grouped_prefetched(
        raw_store,
        sidecar_store,
        first_raw,
        first_sidecar,
    )?);
    stats.initial_prepare = initial_started.elapsed();
    for (segment, (raw, sidecar)) in raw_programs.iter().zip(sidecar_programs).enumerate() {
        let loaded = current
            .take()
            .expect("AOT grouped-tile replay retains its current segment");
        stats.bytes_read = stats.bytes_read.saturating_add(loaded.bytes_read);
        stats.aot_use_count = stats.aot_use_count.saturating_add(1);
        let (next, wait) = std::thread::scope(|scope| {
            let loader = raw_programs
                .get(segment + 1)
                .zip(sidecar_programs.get(segment + 1))
                .map(|(next_raw, next_sidecar)| {
                    scope.spawn(move || {
                        reload_aot_grouped_prefetched(
                            raw_store,
                            sidecar_store,
                            next_raw,
                            next_sidecar,
                        )
                    })
                });
            consume(segment, raw, sidecar, loaded)?;
            let Some(loader) = loader else {
                return Ok((None, std::time::Duration::ZERO));
            };
            let wait_started = Instant::now();
            let loaded = loader.join().map_err(|_| {
                ImagingError::InvalidRequest(
                    "VLASS AOT grouped-tile prefetch worker panicked".to_string(),
                )
            })??;
            Ok((Some(loaded), wait_started.elapsed()))
        })?;
        current = next;
        stats.prefetch_wait += wait;
    }
    stats.record_runtime_build_delta(runtime_builds_before);
    Ok(stats)
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
fn benchmark(prefix: &Path, sidecar_prefix: &Path) -> Result<Value, ImagingError> {
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
    let sidecar = load_aot_grouped_sidecar(prefix, sidecar_prefix)?;
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
    let sidecar_file = File::open(&sidecar.1).map_err(|error| {
        ImagingError::InvalidRequest(format!(
            "open VLASS AOT grouped-tile sidecar payload {}: {error}",
            sidecar.1.display()
        ))
    })?;
    let sidecar_bytes = sidecar_file
        .metadata()
        .map_err(|error| {
            ImagingError::InvalidRequest(format!(
                "stat VLASS AOT grouped-tile sidecar payload {}: {error}",
                sidecar.1.display()
            ))
        })?
        .len();
    let sidecar_store = AwProjectMetalSpillStore {
        file: sidecar_file,
        bytes_written: sidecar_bytes,
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
    let sidecar_verification_started = Instant::now();
    if sha256_path(&sidecar.1)? != sidecar.3 {
        return Err(ImagingError::Normalization(
            "VLASS AOT grouped-tile sidecar payload SHA-256 changed".to_string(),
        ));
    }
    let sidecar_payload_verification_seconds = sidecar_verification_started.elapsed().as_secs_f64();
    let mut segment_receipts = Vec::with_capacity(programs.len());
    let prefetch_stats = replay_aot_grouped_prefetched_sequence(
        &store,
        &sidecar_store,
        &programs,
        &sidecar.2,
        |segment, descriptor, sidecar, loaded| {
            let reload_seconds = loaded.reload_elapsed.as_secs_f64();
            let reload_bytes = loaded.bytes_read;
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
            let raw_reload_bytes = [
                descriptor.source_sample_indices,
                descriptor.kernels,
                descriptor.prediction_phases,
                descriptor.tile_phases,
                descriptor.term_weights,
            ]
            .into_iter()
            .map(|section| section.byte_len)
            .sum::<usize>();
            segment_receipts.push(json!({
                "segment": segment,
                "samples": sidecar.receipt.sample_count,
                "source_programs": descriptor.source_programs,
                "payload_bytes": raw_reload_bytes.saturating_add(sidecar.payload_bytes),
                "raw_reload_bytes": raw_reload_bytes,
                "sidecar_reload_bytes": sidecar.payload_bytes,
                "raw_prediction_sample_bytes_not_read": descriptor.prediction_samples.byte_len,
                "sidecar_cropped_prediction_sample_bytes_read":
                    sidecar.prediction_samples.byte_len,
                "raw_replaced_section_bytes_read": 0,
                "reload_bytes": reload_bytes,
                "reload_seconds": reload_seconds,
                "metal_replay_seconds": metal_replay_seconds,
                "total_seconds": reload_seconds + metal_replay_seconds,
                "effective_support": sidecar.effective_support,
                "aot_grouped_tile": aot_receipt_json(&sidecar.receipt),
            }));
            Ok(())
        },
    )?;
    store.bytes_read = store.bytes_read.saturating_add(prefetch_stats.bytes_read);
    let replay_seconds = started.elapsed().as_secs_f64();
    let replay_read_bytes = store.bytes_read;
    let raw_replay_read_bytes = segment_receipts
        .iter()
        .filter_map(|receipt| receipt.get("raw_reload_bytes").and_then(Value::as_u64))
        .sum::<u64>();
    let sidecar_replay_read_bytes = segment_receipts
        .iter()
        .filter_map(|receipt| receipt.get("sidecar_reload_bytes").and_then(Value::as_u64))
        .sum::<u64>();
    let raw_prediction_sample_bytes_not_read = segment_receipts
        .iter()
        .filter_map(|receipt| {
            receipt
                .get("raw_prediction_sample_bytes_not_read")
                .and_then(Value::as_u64)
        })
        .sum::<u64>();
    let sidecar_cropped_prediction_sample_bytes_read = segment_receipts
        .iter()
        .filter_map(|receipt| {
            receipt
                .get("sidecar_cropped_prediction_sample_bytes_read")
                .and_then(Value::as_u64)
        })
        .sum::<u64>();
    let selected_payload_bytes = raw_replay_read_bytes.saturating_add(sidecar_replay_read_bytes);
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
        "payload_bytes": selected_payload_bytes,
        "reload_bytes": replay_read_bytes,
        "raw_reload_bytes": raw_replay_read_bytes,
        "sidecar_reload_bytes": sidecar_replay_read_bytes,
        "raw_prediction_sample_bytes_not_read": raw_prediction_sample_bytes_not_read,
        "sidecar_cropped_prediction_sample_bytes_read":
            sidecar_cropped_prediction_sample_bytes_read,
        "raw_replaced_section_bytes_read": 0,
        "timed_io_bytes": replay_read_bytes.saturating_add(sidecar_bytes),
        "sidecar_payload_verification": {
            "bytes": sidecar_bytes,
            "seconds": sidecar_payload_verification_seconds,
            "included_in_seconds": true,
        },
        "metal_dispatch_ms": accumulation.aw_metal_stats.dispatch_wait.as_secs_f64() * 1_000.0,
        "metal_total_ms": accumulation.aw_metal_stats.total.as_secs_f64() * 1_000.0,
        "nrmse": nrmse,
        "effective_support_telemetry_markers": 0,
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
        "aot_grouped_tile": {
            "enabled": true,
            "use_count": prefetch_stats.aot_use_count,
            "runtime_grouping_builds": prefetch_stats.runtime_grouping_builds,
            "runtime_sort_builds": prefetch_stats.runtime_sort_builds,
            "runtime_route_builds": prefetch_stats.runtime_route_builds,
            "sidecar_artifact_bytes": sidecar.0
                .get("payload")
                .and_then(|payload| payload.get("bytes"))
                .cloned()
                .unwrap_or(Value::from(0)),
            "byte_lifetime_ledger": sidecar.0
                .get("byte_lifetime_ledger")
                .cloned()
                .unwrap_or(Value::Null),
        },
        "segment_receipts": segment_receipts,
        "byte_ledger": manifest.get("byte_ledger").cloned().unwrap_or(Value::Null),
        "provenance": manifest.get("provenance").cloned().unwrap_or(Value::Null),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn assert_aot_grouped_admission(result: &Value) {
        let segment_count = result["segments"].as_u64().expect("segment count") as usize;
        let support = &result["effective_support"];
        assert_eq!(support["requested"], true);
        assert_eq!(support["decision"], "enabled");
        assert_eq!(support["reason"], Value::Null);
        assert_eq!(support["segment_count"], segment_count);
        assert_eq!(support["compiled_segment_count"], 0);
        assert_eq!(support["total_compile_seconds"], 0.0);
        assert_eq!(result["effective_support_telemetry_markers"], 0);
        assert_effective_support_segment_receipts(result, true);
        let aot = &result["aot_grouped_tile"];
        assert_eq!(aot["enabled"], true);
        assert_eq!(aot["use_count"], segment_count);
        assert_eq!(aot["runtime_grouping_builds"], 0);
        assert_eq!(aot["runtime_sort_builds"], 0);
        assert_eq!(aot["runtime_route_builds"], 0);
        assert!(
            aot["sidecar_artifact_bytes"]
                .as_u64()
                .is_some_and(|bytes| bytes > 0)
        );
        let verification = &result["sidecar_payload_verification"];
        assert_eq!(verification["included_in_seconds"], true);
        assert_eq!(verification["bytes"], aot["sidecar_artifact_bytes"]);
        assert!(
            verification["seconds"]
                .as_f64()
                .is_some_and(|seconds| seconds.is_finite() && seconds >= 0.0)
        );
        assert_eq!(
            result["timed_io_bytes"].as_u64(),
            result["reload_bytes"]
                .as_u64()
                .zip(verification["bytes"].as_u64())
                .map(|(reload, verification)| reload.saturating_add(verification))
        );
        assert_eq!(result["raw_replaced_section_bytes_read"], 0);
        assert_eq!(
            result["raw_prediction_sample_bytes_not_read"],
            result["sidecar_cropped_prediction_sample_bytes_read"]
        );
    }

    #[test]
    fn compact_source_sample_index_topology_accepts_absent_or_complete_diagnostics() {
        let empty = super::AwProjectMetalSpillSection {
            offset: 32,
            len: 0,
            byte_len: 0,
            sha256: Sha256::digest(b"").into(),
        };
        super::validate_compact_source_sample_indices(empty, 8, 32).unwrap();

        let complete = super::AwProjectMetalSpillSection {
            offset: 0,
            len: 8,
            byte_len: size_of::<[u32; 8]>(),
            sha256: [1; 32],
        };
        super::validate_compact_source_sample_indices(complete, 8, 32).unwrap();

        let partial = super::AwProjectMetalSpillSection {
            len: 4,
            byte_len: size_of::<[u32; 4]>(),
            ..complete
        };
        let error = super::validate_compact_source_sample_indices(partial, 8, 32)
            .expect_err("partial diagnostic identities must fail closed");
        assert!(
            error
                .to_string()
                .contains("compact source-sample index topology changed")
        );

        let malformed_empty = super::AwProjectMetalSpillSection {
            sha256: [1; 32],
            ..empty
        };
        super::validate_compact_source_sample_indices(malformed_empty, 8, 32)
            .expect_err("an absent diagnostic section must retain its canonical empty hash");
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
    #[ignore = "requires one frozen external VLASS replay fixture"]
    fn compile_aot_grouped_tile_sidecar() {
        let raw_prefix = std::env::var_os(AOT_GROUPED_COMPILE_RAW_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the raw frozen VLASS replay fixture prefix");
        let sidecar_prefix = std::env::var_os(AOT_GROUPED_COMPILE_SIDECAR_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the VLASS AOT grouped-tile sidecar prefix");
        let receipt = compile_aot_grouped_sidecar(&raw_prefix, &sidecar_prefix)
            .expect("compile frozen VLASS AOT grouped-tile sidecar");
        println!(
            "VLASS_AOT_GROUPED_SIDECAR_JSON {}",
            serde_json::to_string(&receipt).expect("serialize AOT sidecar receipt")
        );
    }

    #[test]
    #[ignore = "requires the frozen external four-SPW VLASS replay fixture and Metal"]
    fn four_spw_exact_replay_benchmark() {
        let four_spw_prefix = std::env::var_os(FOUR_SPW_BENCHMARK_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the frozen four-SPW VLASS replay fixture prefix");
        let four_spw_sidecar = std::env::var_os(AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the frozen four-SPW AOT grouped-tile sidecar prefix");

        let four_spw = benchmark(&four_spw_prefix, &four_spw_sidecar)
            .expect("run frozen four-SPW replay fixture");
        assert_eq!(four_spw["segments"], 1);
        assert_eq!(four_spw["samples"], 6_416_526);
        assert_eq!(four_spw["rejected_samples"], 0);
        assert_aot_grouped_admission(&four_spw);
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
        let full16_sidecar = std::env::var_os(AOT_GROUPED_SIDECAR_FULL16_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the frozen full-16-SPW AOT grouped-tile sidecar prefix");
        let four_spw_sidecar = std::env::var_os(AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX_ENV)
            .map(PathBuf::from)
            .expect("set the frozen four-SPW AOT grouped-tile sidecar prefix");

        let full16 = benchmark(&full16_prefix, &full16_sidecar)
            .expect("run frozen full-16-SPW replay fixture");
        assert_eq!(full16["segments"], 10);
        assert_eq!(full16["samples"], 25_030_848);
        assert_eq!(full16["rejected_samples"], 0);
        assert_aot_grouped_admission(&full16);
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

        let four_spw = benchmark(&four_spw_prefix, &four_spw_sidecar)
            .expect("run frozen four-SPW replay fixture");
        assert_eq!(four_spw["segments"], 1);
        assert_eq!(four_spw["samples"], 6_416_526);
        assert_eq!(four_spw["rejected_samples"], 0);
        assert_aot_grouped_admission(&four_spw);
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
