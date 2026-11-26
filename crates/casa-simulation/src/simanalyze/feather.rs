// SPDX-License-Identifier: LGPL-3.0-or-later
//! The feather step following simanalyze's synthesis imaging.
use super::{
    Parameters, failure,
    plan::{Plan, suffixed},
};
use crate::{
    SessionResources,
    simobserve::SimobserveError,
    simutil::{self, analysis},
};
use casa_images::{
    ImageInterface, PagedImage,
    analysis::{FeatherRequest, feather},
};
use casa_imaging_runtime::{Demand, admit};

pub(super) fn combine(
    p: &Parameters<'_>,
    plan: &mut Plan,
    resources: SessionResources,
) -> Result<(), SimobserveError> {
    if plan.inputs.synthesis.is_empty() {
        return Ok(());
    }
    let supplied = p.text("featherimage");
    let low = if !supplied.is_empty() {
        let path = p.locate(&plan.directory, supplied);
        if !path.exists() {
            return Err(failure(format!(
                "Could not find featherimage {}",
                path.display()
            )));
        }
        path
    } else if plan.inputs.total_power.is_some() && p.text("modelimage").is_empty() {
        let project = plan.directory.file_name().unwrap().to_string_lossy();
        plan.directory.join(format!("{project}.sd.image"))
    } else {
        return Ok(());
    };
    let high = plan.file(".image");
    let target = plan.file(".feather.image");
    if target.exists() {
        if !p.flag("overwrite") {
            return Err(failure("feather image already exists"));
        }
        let existing = std::fs::canonicalize(&target).map_err(failure)?;
        if [&low, &plan.model]
            .iter()
            .any(|path| std::fs::canonicalize(path).is_ok_and(|path| path == existing))
        {
            return Err(failure("feather input would be overwritten"));
        }
        PagedImage::<f32>::open(&target).map_err(failure)?;
    }
    let memory = super::memory::feather(
        super::memory::ImageGrid::read(&high)?,
        super::memory::ImageGrid::read(&low)?,
    )?;
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "simulation feather",
            memory,
        },
    )
    .map_err(failure)?;
    let staging = tempfile::tempdir_in(&plan.directory).map_err(failure)?;
    simutil::skymodel::flat_image(&high, None).map_err(failure)?;
    let staged = staging.path().join("image");
    feather(&FeatherRequest {
        imagename: staged.clone(),
        highres: high,
        lowres: low,
        sdfactor: 1.,
        effdishdiam: -1.0,
        lowpassfiltersd: false,
        overwrite: false,
    })
    .map_err(failure)?;
    casa_imaging_products::publish_staged_image(&staged, &target).map_err(failure)?;
    let residual = plan.file(".residual");
    let residual = simutil::skymodel::flat_image(&residual, None).map_err(failure)?;
    let residual = PagedImage::<f32>::open(&residual).map_err(failure)?;
    analysis::write_product(
        &plan.file(".feather.residual.flat"),
        &residual.get().map_err(failure)?,
        residual.coordinates().clone(),
        residual.units(),
        &residual.image_info().map_err(failure)?,
        residual.default_mask().map_err(failure)?.as_ref(),
    )
    .map_err(failure)?;
    plan.root = suffixed(&plan.root, ".feather");
    plan.stem.push_str(".feather");
    Ok(())
}
