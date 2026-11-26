// SPDX-License-Identifier: LGPL-3.0-or-later

pub(super) mod components;
mod maxfit;
mod peaks;

pub use maxfit::{ImageMaxFit, MaxFitOptions, fit_image_maximum};

use casa_components::{ComponentList, SkyComponent};
use casa_coordinates::{CoordinateModel, CoordinateType};
use casa_core::{RecordValue, Value};

use crate::analysis::lel_mask::PreparedMask;
use crate::{ImageError, ImageInterface};

use self::components::{PixelEllipse, SkyPlane};
use self::peaks::{Peak, PeakRanking};
use super::decompose::{WindowFit, fit_window};
use super::read_window::PixelSections;
use super::{StridedSection, SubimageOptions, SubimagePlan, SubimageRegion};

/// Options for locating astronomical sources in a single sky plane.
pub struct FindSourcesOptions<'a> {
    /// Restrict the search to this region of the image.
    pub region: Option<SubimageRegion<'a>>,
    /// Optional Boolean image expression, in addition to the image mask.
    pub mask: Option<&'a str>,
    /// Maximum number of detections to return.
    pub maximum: usize,
    /// Minimum peak brightness as a fraction of the brightest detection.
    pub cutoff: f64,
    /// Report point sources instead of fitting Gaussian shapes.
    pub point: bool,
    /// Half-width of the local Gaussian fit in pixels.
    pub fit_width: i64,
    /// Look for negative as well as positive peaks.
    pub find_negative: bool,
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}

/// Find compact sources and return the CASA component-list record for the plane.
///
/// Searches the combined region and expression mask with a three-row window.
/// Each candidate is refined by a weighted quadratic. Extended components use a
/// Gaussian fitted to the original selected pixels around the candidate.
///
/// # Errors
/// An invalid region, mask, sky plane, failed image read or failed world
/// coordinate conversion returns an image error.
pub fn find_image_sources(
    image: &dyn ImageInterface<f32>,
    coordinate_record: RecordValue,
    options: FindSourcesOptions<'_>,
) -> Result<RecordValue, ImageError> {
    if image.image_info()?.beam_set.has_multi_beam() {
        return Err(invalid(
            "This application does not support images with multiple beams",
        ));
    }
    let plan = SubimagePlan::new(
        image,
        Some(coordinate_record),
        SubimageOptions {
            region: options.region,
            dropdeg: true,
            keepaxes: &[],
        },
    )?;
    let sky = sky_plane(&plan, image.units())?;
    let unit = vec![1; image.ndim()];
    let selection = &plan.selection;
    let section = StridedSection {
        start: &selection.start,
        shape: &selection.shape,
        stride: selection.stride.as_deref().unwrap_or(&unit),
    };
    let reader = PixelSections {
        image,
        selection,
        mask: PreparedMask::new(options.mask, image.shape(), image.coordinates(), false)?,
    };
    let candidates = peaks::search(
        &reader,
        &plan,
        &section,
        options.maximum,
        options.cutoff,
        if options.find_negative {
            PeakRanking::Signed
        } else {
            PeakRanking::Positive
        },
    )?;
    let mut components = ComponentList::new();
    let width = if options.point { 0 } else { options.fit_width };
    for peak in candidates {
        let (component, _) = fit_component(&reader, &section, &plan, &sky, peak, width)?;
        components.push(component);
    }
    components_record(components)
}

fn sky_plane<'a>(plan: &'a SubimagePlan, brightness: &'a str) -> Result<SkyPlane<'a>, ImageError> {
    let Some(index) = plan.coordinates.find_coordinate(CoordinateType::Direction) else {
        return Err(invalid("The image must contain a direction coordinate"));
    };
    let axes = plan
        .coordinates
        .coordinate_pixel_axes(index)
        .unwrap_or_default();
    let [Some(lon), Some(lat)] = axes.as_slice() else {
        return Err(invalid(
            "Source finding requires both nondegenerate sky axes",
        ));
    };
    if plan.shape.len() != 2 {
        return Err(invalid(
            "Source finding requires an image region with exactly two nondegenerate sky axes",
        ));
    }
    let CoordinateModel::Direction(direction) = plan.coordinates.coordinate(index) else {
        unreachable!("direction coordinate selected by its type");
    };
    Ok(SkyPlane {
        direction,
        sky_order: [*lon, *lat],
        brightness,
        beam: plan.image_info.beam_set.single_beam(),
    })
}

fn components_record(components: ComponentList) -> Result<RecordValue, ImageError> {
    let mut record = components
        .to_record()
        .map_err(|err| invalid(err.to_string()))?;
    for field in record.fields_mut() {
        if let Value::Record(component) = &mut field.value {
            components::format_shape_record(component);
        }
    }
    Ok(record)
}

fn fit_component(
    reader: &PixelSections<'_, f32>,
    section: &StridedSection<'_>,
    plan: &SubimagePlan,
    sky: &SkyPlane<'_>,
    peak: Peak,
    width: i64,
) -> Result<(SkyComponent, Peak), ImageError> {
    let shape = [plan.shape[0], plan.shape[1]];
    if width != 0
        && let Some((fitted, ellipse)) = fit_source(reader, section, plan, shape, peak, width)?
        && let Ok(component) = sky.component(fitted, Some(ellipse))
    {
        return Ok((component, fitted));
    }
    Ok((sky.component(peak, None)?, peak))
}

fn fit_source(
    reader: &PixelSections<'_, f32>,
    section: &StridedSection<'_>,
    plan: &SubimagePlan,
    shape: [usize; 2],
    peak: Peak,
    width: i64,
) -> Result<Option<(Peak, PixelEllipse)>, ImageError> {
    let default = PixelEllipse {
        major: width as f64,
        minor: width as f64,
        x_to_y: 0.,
    };
    if width < 0 {
        return Ok(Some((peak, default)));
    }
    let width = width as usize;
    let center = peak.position.map(|pixel| pixel as usize);
    if (0..2).any(|axis| width > center[axis] || width >= shape[axis] - center[axis]) {
        return Ok(Some((peak, default)));
    }
    let lower = center.map(|c| c - width);
    let [x_axis, y_axis] = [plan.kept_axes[0], plan.kept_axes[1]];
    let mut start = vec![0; section.shape.len()];
    let mut extents = vec![1; section.shape.len()];
    let size = 2 * width + 1;
    let block = (64 * 1024_usize).div_ceil(section.stride[x_axis]);
    let mut observations = Vec::new();
    for y in 0..size {
        start[y_axis] = lower[1] + y;
        for x in (0..size).step_by(block) {
            start[x_axis] = lower[0] + x;
            extents[x_axis] = block.min(size - x);
            let (pixels, valid) =
                reader.read(&section.source_start(&start), &extents, section.stride)?;
            for (offset, &value) in pixels.iter().enumerate() {
                if valid
                    .as_ref()
                    .is_none_or(|valid| valid.as_slice().expect("one-dimensional row")[offset])
                {
                    observations.push(([(x + offset) as f64, y as f64], value));
                }
            }
        }
    }
    let [height, x, y, major, ratio, pa, ..] = match fit_window(observations, peak.height) {
        WindowFit::Point => return Ok(None),
        WindowFit::DefaultWidth => return Ok(Some((peak, default))),
        WindowFit::Fitted(solution) => solution,
    };
    Ok(Some((
        Peak {
            position: [x + lower[0] as f64, y + lower[1] as f64],
            height,
        },
        PixelEllipse {
            major,
            minor: major * ratio,
            x_to_y: pa + std::f64::consts::FRAC_PI_2,
        },
    )))
}
