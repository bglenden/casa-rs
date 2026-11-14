// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's displayed image order, histogram colour limits and PSF annotation.

use std::path::Path;

use casa_images::{ImageInterface, PagedImage};
use casa_imaging_runtime::{Demand, admit};
use casa_ms::plot_document::{PlotArrayDtype, PlotDocumentPanel, PlotLayerData, PlotLayerKind};
use ndarray::IxDyn;

use super::document::{annotation, layer, panel};
use super::payload::{Payload, view};
use super::{SimobserveError, failure};
use crate::SessionResources;
use crate::simutil::analysis;

pub(crate) fn image_panel(
    path: &Path,
    payload: &mut Payload,
    resources: SessionResources,
    psf: bool,
) -> Result<PlotDocumentPanel, SimobserveError> {
    prepare_image(path, payload, resources, psf, false).map(|(panel, _)| panel)
}

pub(crate) fn image_panel_for_simutil(
    path: &Path,
    payload: &mut Payload,
    resources: SessionResources,
) -> Result<(PlotDocumentPanel, analysis::ImageStatistics), SimobserveError> {
    prepare_image(path, payload, resources, false, true)
}

fn prepare_image(
    path: &Path,
    payload: &mut Payload,
    resources: SessionResources,
    psf: bool,
    public: bool,
) -> Result<(PlotDocumentPanel, analysis::ImageStatistics), SimobserveError> {
    let image = PagedImage::<f32>::open(path).map_err(failure)?;
    let shape = image.shape();
    let cells = shape
        .iter()
        .try_fold(1_u64, |n, &size| n.checked_mul(size as u64))
        .ok_or_else(|| failure("image size overflows"))?;
    let memory = cells
        .checked_mul(12)
        .and_then(|n| n.checked_add(8 << 20))
        .ok_or_else(|| failure("image memory overflows"))?;
    let _reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "simulation image plot",
            memory,
        },
    )
    .map_err(failure)?;
    let stats = analysis::statim(path).map_err(failure)?;
    let pixels = image.get().map_err(failure)?;
    let mask = image.default_mask().map_err(failure)?;
    let cell = analysis::cell_size(image.coordinates())
        .map_err(failure)?
        .map(|radians| radians.to_degrees() * 3600.0);
    let [width, height] = [shape[0], shape[1]];
    let extent = [
        width as f64 * cell[0] / 2.0,
        -(width as f64) * cell[0] / 2.0,
        -(height as f64) * cell[1] / 2.0,
        height as f64 * cell[1] / 2.0,
    ];
    let mut output = panel(
        "image",
        &path.file_name().unwrap_or_default().to_string_lossy(),
        ["", ""],
        [[extent[0], extent[1]], [extent[2], extent[3]]],
        true,
    );
    // imshow preserves fixed image extents; the other spatial panels use
    // CASA's axis('equal'), which instead adjusts the data limits on draw.
    output.aspect = Some(casa_ms::plot_document::PlotAspect::Equal);
    let range = if psf || stats.corner_samples == 0 {
        [stats.min, stats.max]
    } else {
        histogram_range(&pixels, mask.as_ref(), stats.min as f32, stats.max as f32)
    };
    let start = payload.position();
    let mut position = vec![0; shape.len()];
    if public {
        // CASA statim requests [-1, -1, 1, 1] and squeezes the result:
        // the second Stokes/channel when present, clamped on singleton axes.
        for axis in 2..shape.len().min(4) {
            position[axis] = 1.min(shape[axis] - 1);
        }
    }
    for y in (0..height).rev() {
        position[1] = y;
        for x in 0..width {
            position[0] = x;
            let valid = mask.as_ref().is_none_or(|mask| mask[IxDyn(&position)]);
            let value = if valid { pixels[IxDyn(&position)] } else { 0.0 };
            payload.f32(value)?;
        }
    }
    output.layers.push(layer(
        "pixels",
        PlotLayerKind::Image,
        PlotLayerData::Image {
            pixels: view(
                start,
                vec![height as u64, width as u64],
                vec![],
                PlotArrayDtype::F32Le,
            ),
            extent,
            range,
            color_map: "jet".into(),
            interpolation: "bilinear".into(),
            origin: if psf { "lower" } else { "upper" }.into(),
            color_bar: !psf,
            color_bar_font_size: Some("x-small".into()),
        },
        None,
        None,
    ));
    let text = if psf {
        let values: Vec<f32> = pixels.iter().copied().collect();
        let radians = std::f64::consts::PI / (180.0 * 3600.0);
        let beam = casa_imaging_products::fit_restoring_beam(
            &values,
            [width, height],
            [cell[0] * radians, cell[1] * radians],
            0.35,
        )
        .map_err(failure)?;
        let major = beam.major_fwhm_rad() / radians;
        let minor = beam.minor_fwhm_rad() / radians;
        for axis in &mut output.axes {
            axis.lower = -3.0 * major;
            axis.upper = 3.0 * major;
        }
        format!("bmaj={}\nbmin={}", scientific(major), scientific(minor))
    } else {
        format!(
            "min={}\nmax={}\nRMS={}\n{}",
            scientific(stats.min),
            scientific(stats.max),
            scientific(stats.rms),
            stats.unit
        )
    };
    output
        .annotations
        .push(annotation("statistics", [0.05, 0.95], text, true));
    Ok((output, stats))
}

fn scientific(value: f64) -> String {
    let text = format!("{value:.1e}");
    let (mantissa, exponent) = text.split_once('e').expect("scientific number");
    let exponent: i32 = exponent.parse().expect("integer exponent");
    format!("{mantissa}e{exponent:+03}")
}

/// LatticeHistograms<float> accumulates its bin-centre values by repeated
/// float additions. Preserve that rounding, plus simutil.statim's unusual
/// low-count guard; it keeps sparse sky models from clipping every source.
fn histogram_range(
    pixels: &ndarray::ArrayD<f32>,
    mask: Option<&ndarray::ArrayD<bool>>,
    min: f32,
    max: f32,
) -> [f64; 2] {
    let mut width = (max - min) / 200.0;
    if width == 0.0 {
        width = 0.001;
    }
    let mut counts = [0_u64; 200];
    for (position, &value) in pixels.indexed_iter() {
        if value.is_finite() && mask.is_none_or(|mask| mask[&position]) {
            let index = ((value - min) / width).max(0.0) as usize;
            counts[index.min(199)] += 1;
        }
    }
    let mut cumulative = [0.0_f32; 200];
    let mut centres = [0.0_f32; 200];
    let mut centre = min + width / 2.0;
    let mut count = 0.0_f32;
    for i in 0..200 {
        count += counts[i] as f32;
        cumulative[i] = count;
        centres[i] = centre;
        centre += width;
    }
    let mut high = 199;
    while cumulative[high] > 0.995 * cumulative[199]
        && high > 0
        && 0.995 * cumulative[199] > cumulative[0]
    {
        high -= 1;
    }
    [f64::from(centres[0]), f64::from(centres[high])]
}
