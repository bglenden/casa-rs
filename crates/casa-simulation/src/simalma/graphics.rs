// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compose all child figures and the four-panel combination summary.
use super::{Parameters, failure, plan::Plan};
use crate::{
    SessionResources,
    simanalyze::plots::{identify, statim},
    simobserve::{SimobserveError, SimobserveOutcome},
    simutil::{analysis, skymodel::flat_image},
};
use casa_images::{ImageInterface, PagedImage};
use casa_imaging_runtime::{Demand, admit};
use casa_plot_data::plot_document::PlotFigure;
use std::{collections::BTreeSet, path::Path};

// Keep logical output names during screen-only preparation so repeated TP
// antennas replace the same figures. Strip them before returning to the host.
pub(super) fn mode(p: &Parameters) -> &str {
    if p.text("graphics") == "screen" {
        "both"
    } else {
        p.text("graphics")
    }
}

pub(super) fn finish(
    p: &Parameters,
    plan: &Plan,
    payload: &Path,
    resources: SessionResources,
    outcome: &mut SimobserveOutcome,
) -> Result<(), SimobserveError> {
    if p.flag("image") && !plan.tp_vis.is_empty() {
        outcome.plots.push(combination(plan, payload, resources)?);
    }
    let mut files = BTreeSet::new();
    outcome.plots.reverse();
    outcome.plots.retain(|figure| {
        figure
            .output_file
            .as_ref()
            .is_none_or(|path| files.insert(path.clone()))
    });
    outcome.plots.reverse();
    let bytes = std::fs::metadata(payload).map_err(failure)?.len();
    for figure in &mut outcome.plots {
        figure.document.validate_payload(bytes).map_err(failure)?;
        figure.show = matches!(p.text("graphics"), "screen" | "both");
        if p.text("graphics") == "screen" {
            figure.output_file = None;
        }
    }
    Ok(())
}

fn combination(
    plan: &Plan,
    path: &Path,
    resources: SessionResources,
) -> Result<PlotFigure, SimobserveError> {
    let best = plan
        .arrays
        .iter()
        .min_by(|a, b| a.resolution.total_cmp(&b.resolution))
        .unwrap();
    let suffix = if plan.noisy { ".noisy" } else { "" };
    let stem = if plan.arrays.len() > 1 {
        format!("{}.concat", plan.project)
    } else {
        format!("{}{suffix}", best.stem)
    };
    let high = plan.directory.join(format!("{stem}.image.flat"));
    let feather = plan
        .directory
        .join(format!("{}.feather.image", plan.project));
    let cube = PagedImage::<f32>::open(&feather).map_err(failure)?;
    let cube_bytes = cube
        .shape()
        .iter()
        .try_fold(16u64, |bytes, &axis| bytes.checked_mul(axis as u64))
        .ok_or_else(|| failure("combination cube memory overflows"))?;
    drop(cube);
    let info = PagedImage::<f32>::open(&high).map_err(failure)?;
    let cells = info
        .shape()
        .iter()
        .try_fold(1u64, |n, &axis| n.checked_mul(axis as u64))
        .ok_or_else(|| failure("combination figure dimensions overflow"))?;
    let memory = cells
        .checked_mul(128)
        .and_then(|bytes| bytes.checked_add(cube_bytes))
        .and_then(|bytes| bytes.checked_add(plan.model.preparation_bytes))
        .and_then(|bytes| bytes.checked_add(128 << 20))
        .ok_or_else(|| failure("combination plot memory overflows"))?;
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "ALMA combination figure",
            memory,
        },
    )
    .map_err(failure)?;
    let model = plan.directory.join(format!("{}.skymodel.flat", best.stem));
    let model = if model.exists() {
        model
    } else {
        plan.directory
            .join(format!("{}.compskymodel.flat", best.stem))
    };
    let staging = tempfile::tempdir_in(&plan.directory).map_err(failure)?;
    let private_model = staging.path().join("model.flat");
    copy_image(&model, &private_model)?;
    let convolved = analysis::convimage(&private_model, &high)
        .map_err(failure)?
        .convolved;
    let private_feather = staging.path().join("feather.image");
    copy_image(&feather, &private_feather)?;
    let feather_flat = flat_image(&private_feather, None).map_err(failure)?;
    let tp = plan
        .directory
        .join(format!("{}.sd.image.flat", plan.project));
    let paths = [convolved, tp, high, feather_flat];
    let titles = [
        format!("{}.skymodel.flat.regrid.conv", best.stem),
        format!("{}.sd.image.flat", plan.project),
        format!("{stem}.image.flat"),
        format!("{}.feather.image.flat", plan.project),
    ];
    let mut payload =
        crate::simobserve::graphics::payload::Payload::append(path).map_err(failure)?;
    let mut panels = Vec::new();
    for (index, (path, title)) in paths.iter().zip(titles).enumerate() {
        let mut panel = statim(path, true, &mut payload, resources)?;
        panel.title = title;
        identify(&mut panel, index);
        panels.push(panel);
    }
    payload.finish().map_err(failure)?;
    Ok(PlotFigure {
        output_file: Some(plan.directory.join(format!("{}.combine.png", plan.project))),
        show: false,
        document: crate::simobserve::graphics::document::document(
            "simulation-combination",
            panels,
            2,
        ),
    })
}

fn copy_image(input: &Path, output: &Path) -> Result<(), SimobserveError> {
    let image = PagedImage::<f32>::open(input).map_err(failure)?;
    analysis::write_product(
        output,
        &image.get().map_err(failure)?,
        image.coordinates().clone(),
        image.units(),
        &image.image_info().map_err(failure)?,
        image.default_mask().map_err(failure)?.as_ref(),
    )
    .map_err(failure)?;
    Ok(())
}
