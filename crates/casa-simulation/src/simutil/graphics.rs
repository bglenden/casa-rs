// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public `simutil` panels using the same native builders as `simobserve`.
//!
//! Scientific preparation stays in Rust. These functions write raw arrays
//! into an existing caller-owned file and return descriptors whose bounds
//! have been validated against that complete payload.

use std::path::Path;
use std::sync::Arc;

use casa_ms::plot_document::{PlotDocument, PlotLayerData};
use casa_types::measures::{MPosition, MeasuresProvider, direction::MDirection};

use crate::SessionResources;
use crate::simobserve::SimobserveError;
use crate::simobserve::graphics::{document, image, observation, payload::Payload};

/// The physical frame and observation represented by an elevation plot.
pub struct EphemerisPlotRequest<'a> {
    /// Source direction, including its reference frame.
    pub source: MDirection,
    /// Reference date as a TAI-labelled MJD, following CASA's helper.
    pub date_mjd: f64,
    /// Observatory location; a supplied cofa is used for unknown telescopes.
    pub position: MPosition,
    /// Ephemerides, Earth orientation and observatory data.
    pub measures: Arc<dyn MeasuresProvider>,
    /// Centre the reference time on transit rather than on the supplied date.
    pub use_hour_angle: bool,
    /// Read the actual observation time range when supplied.
    pub measurement_set: Option<&'a Path>,
    /// Otherwise draw a duration bar about transit when positive, hours.
    pub duration_hours: f64,
}

/// Prepare CASA's 96-sample elevation curve, transit and observation bar.
///
/// # Errors
/// Invalid frames, a source never above the horizon, unreadable observation
/// metadata or an unwritable payload are refused before rendering.
pub fn ephemeris_plot(
    request: &EphemerisPlotRequest<'_>,
    payload_path: &Path,
) -> Result<PlotDocument, SimobserveError> {
    let mut payload = Payload::open(payload_path)?;
    let panel = crate::simobserve::graphics::ephemeris::panel_for(request, &mut payload)?;
    let document = document::document("ephemeris", vec![panel], 1);
    document
        .validate_payload(payload.finish()?)
        .map_err(|e| SimobserveError::raised(e.to_string()))?;
    Ok(document)
}

/// CASA's display-only controls for `statim`. `incell` is unused by CASA.
#[derive(Clone, Debug)]
pub struct ImagePlotOptions {
    /// None or empty for histogram limits, one value for the upper limit,
    /// two for lower and upper. The binding returns computed limits through
    /// an empty caller list, as CASA does.
    pub display_range: Option<Vec<f64>>,
    /// Draw the colour scale alongside the image.
    pub bar: bool,
    /// Annotate the minimum, maximum, corner RMS and brightness unit.
    pub show_statistics: bool,
}

impl Default for ImagePlotOptions {
    fn default() -> Self {
        Self {
            display_range: None,
            bar: true,
            show_statistics: true,
        }
    }
}

/// Prepare `statim`'s complete displayed plane and its histogram limits.
///
/// # Errors
/// An unreadable image, invalid display range, failed resource admission,
/// or an unwritable payload file is refused before rendering.
pub fn image_plot(
    path: &Path,
    payload_path: &Path,
    options: &ImagePlotOptions,
    resources: SessionResources,
) -> Result<(PlotDocument, [f64; 2], super::analysis::ImageStatistics), SimobserveError> {
    casa_imaging_runtime::charge_process_caches(resources.host(), resources.policy())
        .map_err(|error| SimobserveError::raised(error.to_string()))?;
    let mut payload = Payload::open(payload_path)?;
    let (mut panel, stats) = image::image_panel_for_simutil(path, &mut payload, resources)?;
    let mut limits = [0.0; 2];
    for layer in &mut panel.layers {
        if let Some(PlotLayerData::Image {
            range, color_bar, ..
        }) = &mut layer.data
        {
            match options.display_range.as_deref() {
                None | Some([]) => {}
                Some([upper]) => range[1] = *upper,
                Some([lower, upper]) => *range = [*lower, *upper],
                Some(_) => {
                    return Err(SimobserveError::raised(
                        "statim disprange has too many elements",
                    ));
                }
            }
            *color_bar = options.bar;
            limits = *range;
        }
    }
    if !options.show_statistics {
        panel.annotations.clear();
    }
    // imshow sets the image extents; statim leaves existing axis labels alone.
    panel.axes.clear();
    panel.title_font_size = Some("x-small".into());
    panel.title = path
        .to_string_lossy()
        .split('/')
        .nth(1)
        .unwrap_or_else(|| path.to_str().unwrap_or_default())
        .to_string();
    let mut document = document::document("statim", vec![panel], 1);
    document.layout.as_mut().expect("image layout").font_size = None;
    document
        .validate_payload(payload.finish()?)
        .map_err(|e| SimobserveError::raised(e.to_string()))?;
    Ok((document, limits, stats))
}

/// Prepare every antenna and dish, using CASA's local tangent-plane layout.
///
/// # Errors
/// Empty or mismatched vectors, invalid positions/diameters, or an
/// unwritable payload are refused. No antenna is dropped or thinned.
pub fn antenna_plot(
    positions: &[[f64; 3]],
    diameters: &[f64],
    names: &[String],
    payload_path: &Path,
) -> Result<PlotDocument, SimobserveError> {
    if positions.is_empty()
        || positions.len() != diameters.len()
        || positions.len() != names.len()
        || positions.iter().flatten().any(|v| !v.is_finite())
        || diameters.iter().any(|v| !v.is_finite() || *v < 0.0)
    {
        return Err(SimobserveError::raised(
            "plotants requires matching finite antenna positions, diameters and names",
        ));
    }
    let mut payload = Payload::open(payload_path)?;
    let mut panel = observation::antenna_panel(positions, diameters, names, &mut payload)?;
    panel.axes.clear();
    let mut document = document::document("plotants", vec![panel], 1);
    document.layout = None;
    document
        .validate_payload(payload.finish()?)
        .map_err(|e| SimobserveError::raised(e.to_string()))?;
    Ok(document)
}
