// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's image and analysis figures, with complete native array payloads.

use std::path::Path;

use casa_images::PagedImage;
use casa_simulation::{SessionResources, SimanalyzePlotInput, simobserve::SimobserveError};

use super::document::document;
use super::failure;
use super::{image, observation};
use crate::plot_document::{
    PlotDocument, PlotDocumentPanel, PlotFigure, PlotLayerData, PlotPayloadWriter,
};

pub(super) fn prepare(
    input: &SimanalyzePlotInput<'_>,
    payload: &mut PlotPayloadWriter,
) -> Result<Vec<PlotFigure>, SimobserveError> {
    let mut plots = Vec::new();
    if input.image && input.has_synthesis {
        plots.push(image_figure(input, payload)?);
    }
    if input.analyze
        && let Some(figure) = analysis_figure(input, payload)?
    {
        plots.push(figure);
    }
    Ok(plots)
}

fn suffixed(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    std::path::PathBuf::from(value)
}

fn image_figure(
    input: &SimanalyzePlotInput<'_>,
    payload: &mut PlotPayloadWriter,
) -> Result<PlotFigure, SimobserveError> {
    let flat = suffixed(input.root, ".image.flat");
    let residual = suffixed(input.root, ".residual.flat");
    let regrid = suffixed(input.model_flat, ".regrid");
    let convolved = suffixed(input.model_flat, ".regrid.conv");
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
        let mut panel = statim(path, statistics, payload, input.resources)?;
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
        input,
        ".image.png",
        document("simulation-image", panels, 2),
    ))
}

fn analysis_figure(
    input: &SimanalyzePlotInput<'_>,
    payload: &mut PlotPayloadWriter,
) -> Result<Option<PlotFigure>, SimobserveError> {
    let flat = suffixed(input.root, ".image.flat");
    let residual = suffixed(input.root, ".residual.flat");
    let regrid = suffixed(input.model_flat, ".regrid");
    let convolved = suffixed(input.model_flat, ".regrid.conv");
    let mut selected = Vec::new();
    if input.showuv {
        let model = PagedImage::<f32>::open(input.model).map_err(failure)?;
        let frequency = model.coordinates().coordinate(2).reference_value()[0] / 1e9;
        selected.push(observation::uv_panel(input.ms, frequency, payload)?);
    }
    if input.showpsf && input.has_synthesis {
        let path = if input.stem.ends_with(".feather") {
            input.root.with_extension("psf")
        } else {
            suffixed(input.root, ".psf")
        };
        let mut psf = image::image_panel(&path, payload, input.resources, true)?;
        psf.title = path.display().to_string();
        selected.push(psf);
    }
    for (flag, path, statistics) in [
        (input.showmodel, regrid, false),
        (input.showconvolved, convolved, true),
        (input.showclean, flat, true),
        (input.showresidual, residual, true),
        (input.showdifference, suffixed(input.root, ".diff"), true),
        (input.showfidelity, suffixed(input.root, ".fidelity"), false),
    ] {
        if !path.exists() && statistics && path == suffixed(input.root, ".residual.flat") {
            continue;
        }
        if flag {
            selected.push(statim(&path, statistics, payload, input.resources)?);
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
        let mut fig = figure(
            input,
            ".analysis.png",
            document("simulation-analysis", panels, columns),
        );
        if let Some(override_path) = &input.output_file_override
            && fig.output_file.is_some()
        {
            fig.output_file = Some(override_path.clone());
        }
        return Ok(Some(fig));
    }
    Ok(None)
}

pub(crate) fn statim(
    path: &Path,
    statistics: bool,
    payload: &mut PlotPayloadWriter,
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

fn figure(input: &SimanalyzePlotInput<'_>, suffix: &str, document: PlotDocument) -> PlotFigure {
    let stem = if !input.image || !input.has_synthesis {
        input
            .directory
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    } else {
        input
            .stem
            .strip_suffix(".feather")
            .unwrap_or(input.stem)
            .to_owned()
    };
    PlotFigure {
        output_file: matches!(input.graphics_mode, "file" | "both")
            .then(|| input.directory.join(format!("{stem}{suffix}"))),
        show: matches!(input.graphics_mode, "screen" | "both"),
        document,
    }
}
