// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sequential native applications used by simanalyze.
use super::{Parameters, failure, plan::Plan, preflight, total_power};
use crate::{SessionResources, simobserve::SimobserveError, simutil::analysis};
use casa_images::{ImageInterface, PagedImage};
use casa_imaging_application::{Cancel, ImagingRequest, RunContext};
use casa_imaging_runtime::{Demand, admit};
use casa_ms::{
    MeasurementSet, SubtableId,
    concat::{ConcatRequest, concat_with_memory},
};
use casa_types::ScalarValue;
use serde_json::json;
use std::path::PathBuf;

pub(crate) fn context(resources: SessionResources, cancel: &Cancel) -> RunContext {
    RunContext {
        host: *resources.host(),
        policy: *resources.policy(),
        cancel: cancel.clone(),
        summary: None,
    }
}

pub(super) fn execute(
    p: &Parameters<'_>,
    plan: &Plan,
    resources: SessionResources,
    cancel: &Cancel,
) -> Result<Vec<PathBuf>, SimobserveError> {
    preflight::check(p, plan)?;
    let mut products = Vec::new();
    let mut parameters = plan.imaging.clone();
    let request = if plan.inputs.synthesis.is_empty() {
        None
    } else {
        let mut request =
            ImagingRequest::from_tclean_with_defaults(&plan.imaging).map_err(failure)?;
        request.backend = match resources.backend() {
            crate::BackendRequest::Cpu => casa_imaging_application::BackendRequest::Cpu,
            crate::BackendRequest::Metal => casa_imaging_application::BackendRequest::Gpu,
            crate::BackendRequest::Auto => casa_imaging_application::BackendRequest::Auto,
        };
        Some(request)
    };
    if plan.inputs.synthesis.len() > 1 {
        if plan.ms.exists() {
            if !p.flag("overwrite") {
                return Err(failure("concat MS already exists"));
            }
            MeasurementSet::open(&plan.ms).map_err(failure)?;
        }
        let staging = tempfile::tempdir_in(&plan.directory).map_err(failure)?;
        let target = staging.path().join("concat.ms");
        let memory = resources
            .policy()
            .memory(resources.host())
            .saturating_sub(casa_imaging_runtime::reserved_memory());
        let _memory = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "simulation concatenation",
                memory,
            },
        )
        .map_err(failure)?;
        concat_with_memory(
            &ConcatRequest {
                vis: plan.inputs.synthesis.clone(),
                concatvis: target.clone(),
                freqtol: json!(""),
                dirtol: json!(""),
                respectname: false,
                timesort: false,
                copypointing: true,
                visweightscale: plan.concat_weights.clone(),
                forcesingleephemfield: json!(""),
            },
            usize::try_from(memory).map_err(failure)?,
        )
        .map_err(failure)?;
        casa_imaging_products::publish_staged_image(&target, &plan.ms).map_err(failure)?;
        products.push(plan.ms.clone());
        // Concat merges matched source rows. Select the gridder from its
        // actual catalogue, rather than counting duplicate input fields.
        let ms = MeasurementSet::open(&plan.ms).map_err(failure)?;
        let source = ms
            .subtable(SubtableId::Source)
            .ok_or_else(|| failure("no SOURCE table"))?;
        let codes = source.column_accessor("CODE").map_err(failure)?;
        let fields = (0..source.row_count())
            .filter(
                |&row| matches!(codes.scalar_cell(row), Ok(ScalarValue::String(v)) if v == "OBJ"),
            )
            .count();
        parameters.insert(
            "gridder".into(),
            json!(if fields == 1 { "standard" } else { "mosaic" }),
        );
    }
    if let Some(input) = &plan.inputs.total_power {
        products.extend(total_power::image(input, p, plan, resources, cancel)?);
    }
    if let Some(initial) = request {
        let _seed = start_model(p, plan, &mut parameters, resources)?;
        let mut request =
            ImagingRequest::from_tclean_with_defaults(&parameters).map_err(failure)?;
        request.backend = initial.backend;
        casa_imaging_application::execute(&request, context(resources, cancel)).map_err(failure)?;
    }
    Ok(products)
}

// simanalyze passes seed pixels to tclean with a Jy/pixel label. Preserve
// CASA's numerical seed convention in a private copy; never relabel the
// caller's Jy/beam image in place, as the old CASA task does.
fn start_model(
    p: &Parameters<'_>,
    plan: &Plan,
    parameters: &mut serde_json::Map<String, serde_json::Value>,
    resources: SessionResources,
) -> Result<Option<tempfile::TempDir>, SimobserveError> {
    let supplied = super::plan::expand_project(&plan.directory, p.text("modelimage"));
    let path = super::plan::locate(&plan.directory, &supplied);
    if supplied.is_empty() || !path.exists() {
        parameters.insert("startmodel".into(), json!(""));
        return Ok(None);
    }
    let image = PagedImage::<f32>::open(&path).map_err(failure)?;
    if image.units() == "Jy/pixel" {
        parameters.insert("startmodel".into(), json!(path));
        return Ok(None);
    }
    if image.units() != "Jy/beam" {
        return Err(failure(format!(
            "modelimage unit {:?} is not Jy/pixel or Jy/beam",
            image.units()
        )));
    }
    let memory = image
        .shape()
        .iter()
        .try_fold(16u64, |bytes, &axis| bytes.checked_mul(axis as u64))
        .and_then(|bytes| bytes.checked_add(32 << 20))
        .ok_or_else(|| failure("seed model memory overflows"))?;
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "simulation seed model",
            memory,
        },
    )
    .map_err(failure)?;
    let staging = tempfile::tempdir_in(&plan.directory).map_err(failure)?;
    let target = staging.path().join("model");
    analysis::write_product(
        &target,
        &image.get().map_err(failure)?,
        image.coordinates().clone(),
        "Jy/pixel",
        &image.image_info().map_err(failure)?,
        image.default_mask().map_err(failure)?.as_ref(),
    )
    .map_err(failure)?;
    parameters.insert("startmodel".into(), json!(target));
    Ok(Some(staging))
}
