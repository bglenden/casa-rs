// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's image and analysis figures, with complete native array payloads.
use super::{
    Parameters, failure,
    plan::{Plan, suffixed},
};
use crate::simobserve::graphics::{document, image, observation, payload::Payload};
use crate::{SessionResources, simobserve::SimobserveError};
use casa_images::PagedImage;
use casa_plot_data::plot_document::{PlotDocument, PlotDocumentPanel, PlotFigure, PlotLayerData};
use std::path::Path;

pub(super) fn prepare(
    p: &Parameters<'_>,
    plan: &Plan,
    model_flat: &Path,
    output: &Path,
    resources: SessionResources,
    append: bool,
) -> Result<Vec<PlotFigure>, SimobserveError> {
    let mut payload = if append {
        Payload::append(output).map_err(failure)?
    } else {
        Payload::open(output).map_err(failure)?
    };
    let mut plots = Vec::new();
    if p.flag("image") && !plan.inputs.synthesis.is_empty() {
        plots.push(image_figure(p, plan, model_flat, &mut payload, resources)?);
    }
    if p.flag("analyze")
        && let Some(figure) = analysis_figure(p, plan, model_flat, &mut payload, resources)?
    {
        plots.push(figure);
    }
    let bytes = payload.finish().map_err(failure)?;
    for figure in &plots {
        figure.document.validate_payload(bytes).map_err(failure)?;
    }
    Ok(plots)
}
fn image_figure(
    p: &Parameters<'_>,
    plan: &Plan,
    model_flat: &Path,
    payload: &mut Payload,
    resources: SessionResources,
) -> Result<PlotFigure, SimobserveError> {
    let flat = plan.file(".image.flat");
    let residual = plan.file(".residual.flat");
    let regrid = suffixed(model_flat, ".regrid");
    let convolved = suffixed(model_flat, ".regrid.conv");
    let mut panels = Vec::new();
    for (index, (path, statistics)) in [
        (&regrid, false),
        (&convolved, true),
        (&flat, true),
        (&residual, true),
    ]
    .into_iter()
    .enumerate()
    {
        let mut panel = statim(path, statistics, payload, resources)?;
        // The image figure shares the clean image's display range with
        // its residual; the analysis figure recomputes each separately.
        if index == 3 {
            let range = range(&panels[2]);
            for layer in &mut panel.layers {
                if let Some(PlotLayerData::Image { range: target, .. }) = &mut layer.data {
                    *target = range;
                }
            }
        }
        identify(&mut panel, index);
        panels.push(panel);
    }
    Ok(figure(
        p,
        plan,
        ".image.png",
        document::document("simulation-image", panels, 2),
    ))
}

fn analysis_figure(
    p: &Parameters<'_>,
    plan: &Plan,
    model_flat: &Path,
    payload: &mut Payload,
    resources: SessionResources,
) -> Result<Option<PlotFigure>, SimobserveError> {
    let flat = plan.file(".image.flat");
    let residual = plan.file(".residual.flat");
    let regrid = suffixed(model_flat, ".regrid");
    let convolved = suffixed(model_flat, ".regrid.conv");
    let mut selected = Vec::new();
    if p.flag("showuv") {
        let model = PagedImage::<f32>::open(&plan.model).map_err(failure)?;
        let frequency = model.coordinates().coordinate(2).reference_value()[0] / 1e9;
        selected.push(observation::uv_panel(&plan.ms, frequency, payload)?);
    }
    if p.flag("showpsf") && !plan.inputs.synthesis.is_empty() {
        let path = if plan.stem.ends_with(".feather") {
            plan.root.with_extension("psf")
        } else {
            plan.file(".psf")
        };
        let mut psf = image::image_panel(&path, payload, resources, true)?;
        psf.title = path.display().to_string();
        selected.push(psf);
    }
    for (flag, path, statistics) in [
        ("showmodel", regrid, false),
        ("showconvolved", convolved, true),
        ("showclean", flat, true),
        ("showresidual", residual, true),
        ("showdifference", plan.file(".diff"), true),
        ("showfidelity", plan.file(".fidelity"), false),
    ] {
        if flag == "showresidual" && !path.exists() {
            continue;
        }
        if p.flag(flag) {
            selected.push(statim(&path, statistics, payload, resources)?);
        }
    }
    if !selected.is_empty() {
        let count = selected.len();
        let columns = match count {
            1..=3 => count as u32,
            4 => 2,
            _ => 3,
        };
        let mut panels: Vec<PlotDocumentPanel> = Vec::new();
        for (index, mut panel) in selected.into_iter().enumerate() {
            identify(&mut panel, index);
            if panels.len() < 6 {
                panels.push(panel);
            } else {
                // CASA nextfig stops advancing after six axes, while
                // subsequent selected images still draw on the last one.
                let last = panels.last_mut().expect("six panels");
                last.title = panel.title;
                last.axes = panel.axes;
                last.layers.extend(panel.layers);
                last.annotations.extend(panel.annotations);
            }
        }
        return Ok(Some(figure(
            p,
            plan,
            ".analysis.png",
            document::document("simulation-analysis", panels, columns),
        )));
    }
    Ok(None)
}

pub(crate) fn statim(
    path: &Path,
    statistics: bool,
    payload: &mut Payload,
    resources: SessionResources,
) -> Result<PlotDocumentPanel, SimobserveError> {
    let (mut panel, _) = image::image_panel_for_simutil(path, payload, resources)?;
    if !statistics {
        panel.annotations.clear();
    }
    panel.axes.clear();
    panel.title_font_size = Some("x-small".into());
    panel.title = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    Ok(panel)
}
fn range(panel: &PlotDocumentPanel) -> [f64; 2] {
    panel
        .layers
        .iter()
        .find_map(|l| match &l.data {
            Some(PlotLayerData::Image { range, .. }) => Some(*range),
            _ => None,
        })
        .expect("image range")
}
pub(crate) fn identify(panel: &mut PlotDocumentPanel, index: usize) {
    panel.id = format!("panel-{index}");
    for (i, layer) in panel.layers.iter_mut().enumerate() {
        layer.id = format!("panel-{index}-layer-{i}");
    }
    for (i, annotation) in panel.annotations.iter_mut().enumerate() {
        annotation.id = format!("panel-{index}-annotation-{i}");
    }
}
fn figure(p: &Parameters<'_>, plan: &Plan, suffix: &str, document: PlotDocument) -> PlotFigure {
    let stem = if !p.flag("image") || plan.inputs.synthesis.is_empty() {
        plan.directory
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    } else {
        plan.stem
            .strip_suffix(".feather")
            .unwrap_or(&plan.stem)
            .to_owned()
    };
    PlotFigure {
        output_file: matches!(p.text("graphics"), "file" | "both")
            .then(|| plan.directory.join(format!("{stem}{suffix}"))),
        show: matches!(p.text("graphics"), "screen" | "both"),
        document,
    }
}
