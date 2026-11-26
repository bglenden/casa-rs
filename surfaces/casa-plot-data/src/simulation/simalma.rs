// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compose all child figures and the four-panel ALMA combination summary.

use std::collections::BTreeSet;
use std::path::Path;

use casa_images::{ImageInterface, PagedImage};
use casa_imaging_runtime::{Demand, admit};
use casa_simulation::{
    SimalmaCombinationInput, SimalmaFinishInput,
    simobserve::SimobserveError,
    simutil::{analysis, skymodel::flat_image},
};

use super::document::document;
use super::failure;
use super::simanalyze::{identify, statim};
use crate::plot_document::{PlotFigure, PlotPayloadWriter};

pub(super) fn finish(
    input: &SimalmaFinishInput<'_>,
    payload_path: &Path,
    open_writer: impl FnOnce() -> Result<PlotPayloadWriter, SimobserveError>,
    figures: &mut Vec<PlotFigure>,
) -> Result<(), SimobserveError> {
    if let Some(combo) = &input.combination {
        let mut writer = open_writer()?;
        let figure = combination(combo, &mut writer)?;
        writer.finish().map_err(failure)?;
        figures.push(figure);
    }
    let mut files = BTreeSet::new();
    figures.reverse();
    figures.retain(|figure| {
        figure
            .output_file
            .as_ref()
            .is_none_or(|path| files.insert(path.clone()))
    });
    figures.reverse();
    let bytes = if payload_path.exists() {
        std::fs::metadata(payload_path).map_err(failure)?.len()
    } else {
        0
    };
    for figure in figures {
        figure.document.validate_payload(bytes).map_err(failure)?;
        figure.show = matches!(input.graphics_mode, "screen" | "both");
        if input.graphics_mode == "screen" {
            figure.output_file = None;
        }
    }
    Ok(())
}

fn combination(
    input: &SimalmaCombinationInput<'_>,
    payload: &mut PlotPayloadWriter,
) -> Result<PlotFigure, SimobserveError> {
    let high = input
        .directory
        .join(format!("{}.image.flat", input.high_stem));
    let feather = input
        .directory
        .join(format!("{}.feather.image", input.project));
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
        .and_then(|bytes| bytes.checked_add(input.model_preparation_bytes))
        .and_then(|bytes| bytes.checked_add(128 << 20))
        .ok_or_else(|| failure("combination plot memory overflows"))?;
    let _memory = admit(
        input.resources.host(),
        input.resources.policy(),
        &Demand {
            phase: "ALMA combination figure",
            memory,
        },
    )
    .map_err(failure)?;
    let model = input
        .directory
        .join(format!("{}.skymodel.flat", input.best_stem));
    let model = if model.exists() {
        model
    } else {
        input
            .directory
            .join(format!("{}.compskymodel.flat", input.best_stem))
    };
    let staging = tempfile::tempdir_in(input.directory).map_err(failure)?;
    let private_model = staging.path().join("model.flat");
    copy_image(&model, &private_model)?;
    let convolved = analysis::convimage(&private_model, &high)
        .map_err(failure)?
        .convolved;
    let private_feather = staging.path().join("feather.image");
    copy_image(&feather, &private_feather)?;
    let feather_flat = flat_image(&private_feather, None).map_err(failure)?;
    let tp = input
        .directory
        .join(format!("{}.sd.image.flat", input.project));
    let paths = [convolved, tp, high, feather_flat];
    let titles = [
        format!("{}.skymodel.flat.regrid.conv", input.best_stem),
        format!("{}.sd.image.flat", input.project),
        format!("{}.image.flat", input.high_stem),
        format!("{}.feather.image.flat", input.project),
    ];
    let mut panels = Vec::new();
    for (index, (path, title)) in paths.iter().zip(titles).enumerate() {
        let mut panel = statim(path, true, payload, input.resources)?;
        panel.title = title;
        identify(&mut panel, index);
        panels.push(panel);
    }
    Ok(PlotFigure {
        output_file: Some(
            input
                .directory
                .join(format!("{}.combine.png", input.project)),
        ),
        show: false,
        document: document("simulation-combination", panels, 2),
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
