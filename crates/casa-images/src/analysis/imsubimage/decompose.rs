// SPDX-License-Identifier: LGPL-3.0-or-later

mod contours;
mod gaussian;
pub(super) use gaussian::{Parameters, SkyFit, WindowFit, evaluate_sky, fit_sky, fit_window};
mod points;
mod runs;

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_types::{ArrayD, RecordValue};
use ndarray::IxDyn;

use crate::analysis::lel_mask::PreparedMask;
use crate::{ImageError, ImageInterface};

use super::read_window::PixelSections;
use super::{SubimageOptions, SubimagePlan, SubimageRegion};

/// Selection and source extraction options for image decomposition.
pub struct DecompositionOptions<'a> {
    /// Pixel or world region of the source image.
    pub region: Option<SubimageRegion<'a>>,
    /// Optional Boolean image expression.
    pub mask: Option<&'a str>,
    /// Stretch an expression mask along compatible degenerate axes.
    pub stretch: bool,
    /// Find contiguous islands and local maxima without contour deblending.
    pub simple: bool,
    /// Inclusive minimum intensity for a detection.
    pub threshold: f64,
    /// Number of contour levels for deblending.
    pub contours: i64,
    /// Minimum range in contour levels before a source merges.
    pub min_range: i64,
    /// Maximum number of changed axes in a neighboring pixel.
    pub neighbor_axes: i64,
    /// Fit component parameters instead of returning moment estimates.
    pub fit: bool,
    /// Stop retrying a fit when its RMS falls below this value.
    pub maximum_rms: f64,
    /// Maximum number of retries, or negative to select CASA's default.
    pub max_retry: i64,
    /// Maximum number of iterations per fit.
    pub max_iterations: i64,
    /// Relative fit convergence criterion.
    pub criterion: f64,
}

/// Decomposed regions and their optional Gaussian component parameters.
pub struct ImageDecomposition {
    /// One row of parameters per component, with positions in world units.
    pub components: ArrayD<f64>,
    /// Inclusive lower corners in the selected pixel grid.
    pub blc: ArrayD<i64>,
    /// Exclusive upper corners in the selected pixel grid.
    pub trc: ArrayD<i64>,
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}

/// Identify islands above a contour and estimate or fit their Gaussian sources.
///
/// # Errors
/// The source region, mask, dimensions or selected options are invalid, or
/// pixel storage fails during a read.
pub fn decompose_image(
    image: &dyn ImageInterface<f32>,
    coordinate_record: RecordValue,
    options: DecompositionOptions<'_>,
) -> Result<ImageDecomposition, ImageError> {
    if options.threshold < 0. || options.threshold.is_nan() {
        return Err(invalid(format!(
            "Threshold = {}. You must specify a nonnegative threshold",
            options.threshold,
        )));
    }
    if !options.simple && (options.contours < 2 || options.min_range < 1) {
        return Err(invalid(
            "Decomposition requires at least two contours and a positive contour range",
        ));
    }
    if options.fit
        && (options.max_iterations <= 0 || options.criterion < 0. || !options.criterion.is_finite())
    {
        return Err(invalid(
            "A fit requires a positive iteration limit and a finite, nonnegative convergence criterion",
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
    plan.require_single_region()?;
    let rank = plan.shape.len();
    if !(2..=3).contains(&rank) && (options.fit || !options.simple) {
        return Err(invalid(
            "Component fitting and contour deblending require two or three nondegenerate axes",
        ));
    }
    let reader = PixelSections {
        image,
        selection: &plan.selection,
        mask: PreparedMask::new(
            options.mask,
            image.shape(),
            image.coordinates(),
            options.stretch,
        )?,
    };
    let threshold = options.threshold as f32;
    let connection = options.neighbor_axes.clamp(1, 3) as usize;
    if options.simple && !options.fit {
        let bounds = runs::locate(&reader, &plan, threshold, connection)?;
        let rows = bounds.len();
        let mut blc = Vec::with_capacity(rows * rank);
        let mut trc = Vec::with_capacity(rows * rank);
        for region in bounds {
            blc.extend(region.lower.into_iter().map(|i| i as i64));
            trc.extend(region.upper.into_iter().map(|i| i as i64));
        }
        return Ok(ImageDecomposition {
            components: matrix(Vec::new(), 0, 0),
            blc: matrix(blc, rows, rank),
            trc: matrix(trc, rows, rank),
        });
    }

    let pixels = points::Pixels::read(&reader, &plan, threshold)?;
    let maximum = pixels
        .samples
        .iter()
        .map(|sample| sample.value)
        .fold(threshold, f32::max);
    let offsets = points::neighborhood(rank, connection);
    let mut visited = vec![false; pixels.samples.len()];
    let islands = points::connected(
        &pixels,
        0..pixels.samples.len(),
        &offsets,
        |_| true,
        &mut visited,
    );
    let settings = gaussian::FitOptions {
        maximum_rms: options.maximum_rms,
        retries: options.max_retry,
        iterations: options.max_iterations.max(0) as usize,
        criterion: options.criterion,
    };
    let mut params = Vec::new();
    let mut blc = Vec::new();
    let mut trc = Vec::new();
    let mut regions = 0;
    if options.simple {
        for island in &islands {
            let (lower, upper) = pixels.bounds(island);
            blc.extend(lower.into_iter().map(|i| i as i64));
            trc.extend(upper.into_iter().map(|i| i as i64));
            regions += 1;
            if options.fit {
                let estimates = gaussian::peak_estimates(&pixels, island, &reader, &plan)?;
                params.extend(gaussian::fit(&pixels, island, estimates, &settings));
            }
        }
    } else if !islands.is_empty() {
        let levels = contours::levels(threshold, maximum, options.contours as usize);
        for island in islands {
            let peak = island
                .iter()
                .map(|&point| pixels.samples[point].value)
                .fold(threshold, f32::max);
            if levels.partition_point(|&level| level <= peak) < options.min_range as usize {
                continue;
            }
            let components = contours::deblend(
                &pixels,
                &island,
                &offsets,
                &levels,
                options.min_range as usize,
                &mut visited,
            );
            let mut estimates = Vec::with_capacity(components.len());
            for component in &components {
                let (lower, upper) = pixels.bounds(component);
                blc.extend(lower.into_iter().map(|i| i as i64));
                trc.extend(upper.into_iter().map(|i| i as i64));
                regions += 1;
                estimates.push(gaussian::estimate(&pixels, component));
            }
            if options.fit {
                let mut assigned: Vec<_> = components.into_iter().flatten().collect();
                assigned.sort_unstable();
                params.extend(gaussian::fit(&pixels, &assigned, estimates, &settings));
            } else {
                params.extend(estimates);
            }
        }
    }
    let count = params.len();
    let mut components = Vec::with_capacity(count * rank * 3);
    for component in params {
        components.extend(world_parameters(component, &plan.coordinates, rank)?);
    }
    Ok(ImageDecomposition {
        components: matrix(components, count, rank * 3),
        blc: matrix(blc, regions, rank),
        trc: matrix(trc, regions, rank),
    })
}

fn matrix<T>(data: Vec<T>, rows: usize, columns: usize) -> ArrayD<T> {
    let shape = if rows == 0 { [0, 0] } else { [rows, columns] };
    ArrayD::from_shape_vec(IxDyn(&shape), data).expect("decomposition matrix dimensions")
}

fn world_parameters(
    pixels: gaussian::Parameters,
    coordinates: &CoordinateSystem,
    rank: usize,
) -> Result<Vec<f64>, ImageError> {
    // CASA stores the pixel estimates in Float, then converts centers and
    // the width vector independently to world coordinates. The second shape
    // term in two dimensions is an axial ratio and remains dimensionless.
    let pixels = pixels.map(|value| f64::from(value as f32));
    let mut result = pixels[..rank * 3].to_vec();
    let mut center = coordinates.to_world(&pixels[1..1 + rank])?;
    let mut widths = coordinates.to_world(&pixels[1 + rank..1 + 2 * rank])?;
    coordinates.convert_world_offsets(&mut widths, false)?;
    for axis in 0..center.len() {
        if let Some((owner, local)) = coordinates.world_axis_owner(axis)
            && let CoordinateModel::Direction(direction) = coordinates.coordinate(owner)
        {
            center[axis] = direction.in_world_units([center[axis]; 2])[local];
            widths[axis] = direction.in_world_units([widths[axis]; 2])[local];
        }
    }
    for axis in 0..rank {
        result[1 + axis] = center.get(axis).copied().unwrap_or(pixels[1 + axis]);
        if rank != 2 || axis == 0 {
            result[1 + rank + axis] = widths.get(axis).copied().unwrap_or(pixels[1 + rank + axis]);
        }
    }
    result
        .iter_mut()
        .for_each(|value| *value = f64::from(*value as f32));
    Ok(result)
}
