// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::{ArrayD, RecordValue, Value, measures::doppler::DopplerRef};
use casa_lattices::{ScratchSpace, TempStoragePolicy};
use ndarray::{IxDyn, ShapeBuilder};

use crate::analysis::imsubimage::fitprofile::selection::ProfileSelection;
use crate::analysis::imsubimage::{
    SubimagePlan, SubimageRegion, reduce_subimage_coordinate_record,
};
use crate::{GaussianBeam, ImageError, ImageInterface, TempImage};

use super::axis::MomentAxis;
use super::collapse::{MomentSamples, PixelClip};
use super::output::SUFFIXES;
use super::smooth;
use super::window::Method;

/// Options for the public image tool's moment products.
pub struct ImageMomentOptions<'a> {
    /// Selected CASA moment numbers, in result and publication order.
    pub moments: &'a [i32],
    /// Zero-based pixel axis or a negative value to select the spectral axis.
    pub axis: i64,
    /// Region to select before smoothing and collapse.
    pub region: Option<SubimageRegion<'a>>,
    /// LEL mask to combine with the active input mask.
    pub mask: Option<&'a str>,
    /// Stretch compatible expression masks.
    pub stretch: bool,
    /// Remove the collapsed axis from the result.
    pub drop: bool,
    /// CASA window and fit method names.
    pub methods: &'a [String],
    /// Smoothing axes, kernel types and widths in pixels.
    pub smoothing: (&'a [i64], &'a [String], &'a [f64]),
    /// Accepted or rejected pixel ranges.
    pub inclusion: &'a [f64],
    /// Pixels strictly inside this range are rejected.
    pub exclusion: &'a [f64],
    /// Signal-to-noise threshold for window and fit modes.
    pub peak_snr: f32,
    /// Noise standard deviation, or 0 to estimate it.
    pub noise: f32,
    /// Velocity convention used when evaluating a spectral moment.
    pub doppler: DopplerRef,
    /// Final product paths protected from aliasing named mask inputs.
    pub destinations: &'a [&'a Path],
}

/// Completed moment images and optional auxiliary smoothed image.
pub struct ImageMomentProducts {
    /// Requested moment images in call order.
    pub moments: Vec<TempImage<f32>>,
    /// Image used to select samples when separable smoothing was requested.
    pub smoothed: Option<TempImage<f32>>,
}

/// Resolve the ordered public product names. An empty name denotes a temporary image.
///
/// # Errors
/// The moment list is empty, contains unsupported numbers, or names a product twice.
pub fn moment_output_names(
    moments: &[i32],
    requested: &str,
    input: Option<&str>,
) -> Result<Vec<String>, ImageError> {
    if moments.is_empty()
        || moments.len() > SUFFIXES.len()
        || moments.iter().any(|n| !(-1..=11).contains(n))
    {
        return Err(invalid(
            "image.moments accepts one to thirteen moment numbers from -1 through 11",
        ));
    }
    if moments.len() == 1 {
        return Ok(vec![requested.to_owned()]);
    }
    let base = if requested.is_empty() {
        input.unwrap_or("Temporary_Image")
    } else {
        requested
    };
    let mut names = Vec::with_capacity(moments.len());
    for &moment in moments {
        let path = format!("{base}{}", SUFFIXES[(moment + 1) as usize]);
        if names.contains(&path) {
            return Err(invalid(format!("Duplicate moment output name {path}")));
        }
        names.push(path);
    }
    Ok(names)
}

/// Collapse masked profiles from an attached Float image into moment products.
///
/// # Errors
/// Returns a selection or I/O error for incompatible axes, regions, masks,
/// methods or products.
pub fn make_image_moments(
    input: &dyn ImageInterface<f32>,
    coordinates: RecordValue,
    beams: Option<&RecordValue>,
    options: ImageMomentOptions<'_>,
) -> Result<ImageMomentProducts, ImageError> {
    moment_output_names(options.moments, "", None)?;
    let clip = PixelClip::from_tool(options.inclusion, options.exclusion)?;
    let plan = SubimagePlan::new_with_beams(
        input,
        Some(coordinates),
        crate::analysis::imsubimage::SubimageOptions {
            region: options.region,
            dropdeg: false,
            keepaxes: &[],
        },
        beams,
    )?;
    let axis = if options.axis < 0 {
        let axis = plan
            .coordinates
            .find_coordinate(CoordinateType::Spectral)
            .and_then(|index| plan.coordinates.coordinate_pixel_axes(index))
            .and_then(|axes| axes.first().copied().flatten())
            .ok_or_else(|| {
                invalid("There is no spectral axis in this image -- specify the axis")
            })?;
        if plan.shape[axis] == 1 {
            return Err(invalid("Illegal moment axis; it has only 1 pixel"));
        }
        axis
    } else {
        usize::try_from(options.axis)
            .ok()
            .filter(|&axis| axis < plan.shape.len())
            .ok_or_else(|| invalid("Illegal moment axis; out of range"))?
    };
    let stokes_axis = plan
        .coordinates
        .find_coordinate(CoordinateType::Stokes)
        .and_then(|coordinate| plan.coordinates.coordinate_pixel_axes(coordinate))
        .and_then(|axes| axes.first().copied().flatten());
    if stokes_axis == Some(axis) && plan.image_info.beam_set.n_stokes() > 1 {
        return Err(invalid(
            "Number of polarizations is not consistent with the restoring beam set",
        ));
    }
    let kernels = smooth::kernels(&plan.shape, options.smoothing)?;
    let method = Method::new(options.methods, clip, !kernels.is_empty(), options.moments)?;
    let smooth::Sources {
        replacement,
        plan,
        smoothed,
    } = smooth::prepare(input, plan, axis, &kernels, &options)?;
    let mut options = options;
    let source = replacement.as_ref().map_or(input, |image| image);
    if replacement.is_some() {
        options.region = None;
        options.mask = None;
    }
    let mut moments = reduce(
        source,
        plan,
        smoothed.as_ref(),
        axis,
        clip,
        method,
        &options,
    )?;
    if let Some(beams) = beams {
        retain_beam_units(&mut moments, beams)?;
    }
    Ok(ImageMomentProducts { moments, smoothed })
}

fn retain_beam_units(
    products: &mut [TempImage<f32>],
    input_info: &RecordValue,
) -> Result<(), ImageError> {
    let info = products[0].image_info()?;
    let Some(target) = info.beam_set.single_beam() else {
        return Ok(());
    };
    let as_beam = |value: &Value| match value {
        Value::Record(record) => GaussianBeam::from_record(record).is_ok_and(|beam| beam == target),
        _ => false,
    };
    let original = input_info
        .get("restoringbeam")
        .filter(|value| as_beam(value))
        .or_else(|| {
            let Value::Record(planes) = input_info.get("perplanebeams")? else {
                return None;
            };
            planes
                .fields()
                .iter()
                .map(|field| &field.value)
                .find(|value| as_beam(value))
        });
    if let Some(beam) = original {
        let mut record = info.to_record();
        record.upsert("restoringbeam", beam.clone());
        for product in products {
            product.set_image_info_record(record.clone())?;
        }
    }
    Ok(())
}

fn reduce(
    input: &dyn ImageInterface<f32>,
    plan: SubimagePlan,
    smoothed: Option<&TempImage<f32>>,
    axis: usize,
    clip: PixelClip,
    method: Method,
    options: &ImageMomentOptions<'_>,
) -> Result<Vec<TempImage<f32>>, ImageError> {
    let moment_selection = ProfileSelection::from_plan(plan, axis)?;
    let plan = &moment_selection.plan;
    let moment_axis = MomentAxis::with_doppler(&plan.coordinates, axis, options.doppler)?;
    let mut shape = plan.shape.clone();
    let mut coordinates = plan.coordinates.clone();
    if options.drop {
        shape.remove(axis);
        remove_axis(&mut coordinates, plan.coordinate_record.as_ref(), axis)?;
    } else {
        shape[axis] = 1;
    }
    if shape.is_empty() {
        return Err(invalid("Cannot remove the only image axis"));
    }
    let mut products = Vec::new();
    for &moment in options.moments {
        let mut product = temp_image(shape.clone(), coordinates.clone(), options.moments.len())?;
        product.set_units(super::moment_units(
            input.units(),
            &moment_axis.unit,
            moment,
        ))?;
        product.set_misc_info(input.misc_info())?;
        product.set_image_info(&plan.image_info)?;
        product.make_mask("mask0", true, false)?;
        products.push(product);
    }
    let weighted_world = options
        .moments
        .iter()
        .any(|moment| matches!(moment, 1 | 2 | 4));
    let extrema_world = options
        .moments
        .iter()
        .any(|moment| matches!(moment, 9 | 11));
    let mut separable_world = if weighted_world && moment_axis.profile_is_separable() {
        vec![None; plan.shape[axis]]
    } else {
        Vec::new()
    };
    let guide_geometry = smoothed
        .map(|image| {
            let plan = SubimagePlan::new(
                image,
                None,
                crate::analysis::imsubimage::SubimageOptions {
                    region: None,
                    dropdeg: false,
                    keepaxes: &[],
                },
            )?;
            ProfileSelection::from_plan(plan, axis)
        })
        .transpose()?;
    let mut guide = guide_geometry
        .as_ref()
        .zip(smoothed)
        .map(|(geometry, image)| geometry.reader(image, None, false, []))
        .transpose()?;
    let noise = if matches!(method, Method::Clip) || options.noise > 0. {
        options.noise
    } else {
        smooth::noise(
            input,
            smoothed,
            options.region,
            options.mask,
            options.stretch,
        )?
    };
    let peak_snr = if options.peak_snr <= 0. {
        3.
    } else {
        options.peak_snr
    };
    let scale = if options.moments.contains(&0) {
        moment_axis.integrated_scale()?
    } else {
        1.
    };
    let mut samples = MomentSamples::new(method, clip, peak_snr, noise, scale);
    let mut reader = moment_selection.reader(
        input,
        options.mask,
        options.stretch,
        options.destinations.iter().copied(),
    )?;
    let row_length = shape.first().copied().unwrap_or(1).min(16 * 1024);
    let mut first = 0;
    while first < moment_selection.lanes {
        let length = row_length.min(moment_selection.lanes - first);
        let mut positions = moment_selection.position(first);
        if options.drop {
            positions.remove(axis);
        }
        let length = length.min(shape[0] - positions[0]);
        let mut values = vec![vec![0.; length]; products.len()];
        let mut masks = vec![vec![false; length]; products.len()];
        for offset in 0..length {
            let position = moment_selection.position(first + offset);
            let lane = reader.read(&position)?;
            let guide = guide
                .as_mut()
                .map(|guide| guide.read(&position))
                .transpose()?;
            let mut local = if weighted_world || extrema_world {
                moment_axis.profile_pixel(&position)
            } else {
                Vec::new()
            };
            samples.extract(&lane, guide.as_ref(), |index| {
                if !weighted_world {
                    return Ok(index as f64);
                }
                if let Some(cached) = separable_world.get_mut(index) {
                    if let Some(world) = *cached {
                        return Ok(world);
                    }
                    let world = moment_axis.sample_world(&mut local, index as f64)?;
                    *cached = Some(world);
                    return Ok(world);
                }
                moment_axis.sample_world(&mut local, index as f64)
            })?;
            for (which, &moment) in options.moments.iter().enumerate() {
                if let Some(value) = samples.measure(moment) {
                    let world = if !weighted_world && matches!(moment, 9 | 11) {
                        moment_axis.sample_world(&mut local, value)?
                    } else {
                        value
                    };
                    values[which][offset] = world as f32;
                    masks[which][offset] = true;
                }
            }
        }
        let mut block = vec![1; shape.len()];
        block[0] = length;
        for ((product, values), mask) in products.iter_mut().zip(values).zip(masks) {
            let values =
                ArrayD::from_shape_vec(IxDyn(&block).f(), values).expect("moment row shape");
            let mask = ArrayD::from_shape_vec(IxDyn(&block).f(), mask).expect("moment row mask");
            product.put_slice(&values, &positions)?;
            product.put_mask_slice("mask0", &mask, &positions)?;
        }
        first += length;
    }
    Ok(products)
}

fn remove_axis(
    coordinates: &mut CoordinateSystem,
    native: Option<&RecordValue>,
    axis: usize,
) -> Result<(), ImageError> {
    let world = coordinates
        .pixel_axis_to_world_axis(axis)
        .ok_or_else(|| invalid("Moment pixel axis has no visible world axis"))?;
    let (owner, _) = coordinates
        .pixel_axis_owner(axis)
        .expect("mapped moment axis");
    if coordinates
        .coordinate_pixel_axes(owner)
        .is_some_and(|axes| axes.len() == 1)
        && coordinates
            .coordinate_world_axes(owner)
            .is_some_and(|axes| axes.len() == 1)
    {
        let ndim = coordinates.n_pixel_axes();
        let kept = (0..ndim)
            .filter(|&number| number != axis)
            .collect::<Vec<_>>();
        let record = native
            .filter(|record| {
                record
                    .fields()
                    .iter()
                    .any(|field| field.name.starts_with("pixelmap"))
            })
            .cloned()
            .unwrap_or_else(|| coordinates.to_record());
        let reduced = reduce_subimage_coordinate_record(&record, ndim, &kept)?;
        *coordinates = CoordinateSystem::from_record(&reduced)?;
        return Ok(());
    }
    let replacement = coordinates.reference_value()[world];
    coordinates.remove_world_axis(world, replacement)?;
    Ok(())
}

pub(super) fn temp_image(
    shape: Vec<usize>,
    coordinates: CoordinateSystem,
    products: usize,
) -> Result<TempImage<f32>, ImageError> {
    TempImage::new(
        shape,
        coordinates,
        TempStoragePolicy::Auto {
            memory_budget_bytes: 64 * 1024 * 1024 / products.max(1),
            scratch: ScratchSpace::SystemTemp,
        },
    )
}

pub(super) fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
