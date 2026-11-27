// SPDX-License-Identifier: LGPL-3.0-or-later
//! Regrid TP, multiply by the synthesis PB, feather, and divide by PB.
use super::{failure, plan::Plan};
use crate::{SessionResources, simobserve::SimobserveError, simutil::analysis};
use casa_imageanalysis::{
    ImpbcorRequest, RegridTemplate,
    analysis::{FeatherRequest, feather},
    impbcor, regrid,
};
use casa_images::{PagedImage, RegridMethod};
use casa_imaging_runtime::{Demand, admit};
use std::path::{Path, PathBuf};

pub(super) fn execute(
    plan: &Plan,
    high: &Path,
    resources: SessionResources,
) -> Result<Vec<PathBuf>, SimobserveError> {
    let low = plan.directory.join(format!("{}.sd.image", plan.project));
    let image = PagedImage::<f32>::open(high).map_err(failure)?;
    let tp = PagedImage::<f32>::open(&low).map_err(failure)?;
    let cells = |shape: &[usize]| {
        shape
            .iter()
            .try_fold(1u64, |count, &n| count.checked_mul(n as u64))
    };
    let memory = cells(image.shape())
        .and_then(|n| n.checked_mul(160))
        .and_then(|high| cells(tp.shape())?.checked_mul(48)?.checked_add(high))
        .and_then(|bytes| bytes.checked_add(128 << 20))
        .ok_or_else(|| failure("combined image dimensions overflow"))?;
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "ALMA PB-weighted feather",
            memory,
        },
    )
    .map_err(failure)?;
    let template = RegridTemplate::of(&image);
    let interpolated = regrid(&tp, &template, RegridMethod::Cubic).map_err(failure)?;
    let staging = tempfile::tempdir_in(&plan.directory).map_err(failure)?;
    let regridded = staging.path().join("tp.regrid");
    analysis::write_product(
        &regridded,
        &interpolated.data,
        interpolated.coordinates,
        tp.units(),
        &tp.image_info().map_err(failure)?,
        Some(&interpolated.mask),
    )
    .map_err(failure)?;
    drop(interpolated.data);
    let pb = high.with_extension("pb");
    let scaled = plan
        .directory
        .join(format!("{}.sd.image.pbscaled", plan.project));
    pbcor(&regridded, &pb, &scaled, "multiply")?;
    let raw = plan
        .directory
        .join(format!("{}.feather.image0", plan.project));
    feather(&FeatherRequest {
        imagename: raw.clone(),
        highres: high.to_path_buf(),
        lowres: scaled.clone(),
        sdfactor: 1.,
        effdishdiam: -1.0,
        lowpassfiltersd: false,
        overwrite: false,
    })
    .map_err(failure)?;
    let output = plan
        .directory
        .join(format!("{}.feather.image", plan.project));
    pbcor(&raw, &pb, &output, "divide")?;
    Ok(vec![scaled, raw, output])
}

fn pbcor(image: &Path, pb: &Path, output: &Path, mode: &str) -> Result<(), SimobserveError> {
    impbcor(&ImpbcorRequest {
        imagename: image.to_path_buf(),
        pbimage: pb.to_path_buf().into(),
        outfile: output.to_path_buf(),
        mode: mode.to_owned(),
        cutoff: -1.,
        box_selection: None,
        region: None,
        chans: None,
        stokes: None,
        mask: None,
        stretch: false,
        overwrite: false,
    })
    .map_err(failure)?;
    Ok(())
}
