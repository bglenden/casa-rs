// SPDX-License-Identifier: LGPL-3.0-or-later

//! A CASA mask image, read onto an image domain's direction grid and
//! matched to its channels and Stokes planes as tclean matches it
//! (`SDMaskHandler::fillMask`, `expandMask`, `copyMask`).
//!
//! tclean expands a mask when it has one channel and the image several, or
//! else one Stokes plane and the image several (`expandMask`):
//!
//! * a continuum Stokes I mask to every channel and Stokes plane;
//! * a continuum mask of the image's Stokes planes to every channel, plane
//!   by plane, and a continuum mask of another number of Stokes planes is
//!   refused;
//! * a cube Stokes I mask to every Stokes plane, channel by channel, when
//!   its channels are the image's (their number and spectral coordinate);
//!   otherwise it is refused.
//!
//! Any other mask is regridded onto the image's direction and spectral
//! axes (`copyMask`): casa-rs reads one whose Stokes planes are the
//! image's and whose channels are the image's, or one continuum channel,
//! and does not regrid a mask along frequency or Stokes yet (#980).
//!
//! Each plane is reprojected onto the domain's direction grid
//! ([`casa_imaging_deconvolution::reproject_mask_support`]); a plane's
//! support is its finite, non-zero pixels valid in the image's default
//! pixel mask.

use std::path::Path;

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate};
use casa_images::AnyPagedImage;
use casa_imaging_deconvolution::{MaskPlanes, ReconstructionMaskPlan, reproject_mask_support};
use casa_imaging_model::DirectionCoordinateSpec;
use ndarray::{ArrayD, IxDyn};

use super::PrepareError;

/// The planes of the image a mask is read onto: its channels and Stokes
/// planes, and its spectral coordinate.
#[derive(Clone, Copy)]
pub(super) struct TargetPlanes<'a> {
    pub(super) channels: usize,
    pub(super) polarizations: usize,
    pub(super) spectral: Option<&'a SpectralCoordinate>,
}

/// Where a mask image's axes are, by pixel axis.
struct MaskAxes {
    shape: Vec<usize>,
    direction: [usize; 2],
    stokes: Option<usize>,
    spectral: Option<usize>,
}

impl MaskAxes {
    fn extent(&self, axis: Option<usize>) -> usize {
        axis.map_or(1, |axis| self.shape[axis])
    }

    /// The index of pixel `[x, y]` of plane `channel` and `stokes`.
    fn index(&self, [x, y]: [usize; 2], channel: usize, stokes: usize) -> IxDyn {
        let mut index = vec![0; self.shape.len()];
        index[self.direction[0]] = x;
        index[self.direction[1]] = y;
        if let Some(axis) = self.spectral {
            index[axis] = channel;
        }
        if let Some(axis) = self.stokes {
            index[axis] = stokes;
        }
        IxDyn(&index)
    }
}

/// The plan of the mask image at `path` on `target`'s direction grid of
/// `target_size` pixels, with the planes tclean gives it for an image of
/// `planes`.
pub(super) fn reproject_image_mask(
    path: &Path,
    target: DirectionCoordinateSpec,
    target_size: [usize; 2],
    planes: TargetPlanes<'_>,
) -> Result<ReconstructionMaskPlan, PrepareError> {
    let (coordinates, axes, supports) = match AnyPagedImage::open(path)? {
        AnyPagedImage::Float32(image) => {
            let axes = mask_axes(image.shape(), image.coordinates())?;
            let supports = plane_supports(&image.get()?, image.get_mask()?.as_ref(), &axes);
            (image.coordinates().clone(), axes, supports)
        }
        AnyPagedImage::Float64(image) => {
            let axes = mask_axes(image.shape(), image.coordinates())?;
            let supports = plane_supports(&image.get()?, image.get_mask()?.as_ref(), &axes);
            (image.coordinates().clone(), axes, supports)
        }
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            return Err(PrepareError::ComplexMaskImage);
        }
    };
    let held = expansion(path, &coordinates, &axes, planes)?;
    let direction = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or(PrepareError::MaskImageDirection)?;
    let source = super::domains::direction_model_spec(coordinates.coordinate(direction))?;
    let source_shape = axes.direction.map(|axis| axes.shape[axis]);
    let mut support = Vec::with_capacity(held.count() * target_size[0] * target_size[1]);
    for plane in &supports {
        support.extend_from_slice(&reproject_mask_support(
            source,
            source_shape,
            plane,
            target,
            target_size,
        )?);
    }
    Ok(ReconstructionMaskPlan::Reprojected {
        coordinate: target,
        planes: held,
        support: support.into_boxed_slice(),
    })
}

/// Where the direction, Stokes and spectral axes of an image of `shape`
/// are: each coordinate's pixel axes follow those of the coordinates
/// before it. Every other axis has one pixel.
fn mask_axes(shape: &[usize], coordinates: &CoordinateSystem) -> Result<MaskAxes, PrepareError> {
    let first_axis = |index: usize| {
        (0..index)
            .map(|k| coordinates.coordinate(k).n_pixel_axes())
            .sum::<usize>()
    };
    let refuse = || PrepareError::MaskImageShape {
        shape: shape.to_vec(),
    };
    let direction = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or(PrepareError::MaskImageDirection)?;
    let longitude = first_axis(direction);
    let axes = MaskAxes {
        shape: shape.to_vec(),
        direction: [longitude, longitude + 1],
        stokes: coordinates
            .find_coordinate(CoordinateType::Stokes)
            .map(first_axis),
        spectral: coordinates
            .find_coordinate(CoordinateType::Spectral)
            .map(first_axis),
    };
    let named = [
        Some(axes.direction[0]),
        Some(axes.direction[1]),
        axes.stokes,
        axes.spectral,
    ];
    if shape.len() < longitude + 2
        || named.iter().flatten().any(|axis| *axis >= shape.len())
        || (0..shape.len()).any(|axis| !named.contains(&Some(axis)) && shape[axis] != 1)
    {
        return Err(refuse());
    }
    Ok(axes)
}

/// Each plane's support, channel-major: its finite, non-zero pixels valid
/// in the default pixel `mask`, x-major.
fn plane_supports<T>(
    values: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axes: &MaskAxes,
) -> Vec<Vec<bool>>
where
    T: Into<f64> + Copy,
{
    let [nx, ny] = axes.direction.map(|axis| axes.shape[axis]);
    let mut planes = Vec::new();
    for channel in 0..axes.extent(axes.spectral) {
        for stokes in 0..axes.extent(axes.stokes) {
            let plane = (0..nx * ny)
                .map(|cell| {
                    let index = axes.index([cell / ny, cell % ny], channel, stokes);
                    let value: f64 = values[&index].into();
                    value.is_finite()
                        && value != 0.0
                        && mask.is_none_or(|mask| mask[&index])
                })
                .collect();
            planes.push(plane);
        }
    }
    planes
}

/// The planes tclean gives a mask of `axes` on an image of `target`
/// (`SDMaskHandler::fillMask`), or its refusal.
fn expansion(
    path: &Path,
    coordinates: &CoordinateSystem,
    axes: &MaskAxes,
    target: TargetPlanes<'_>,
) -> Result<MaskPlanes, PrepareError> {
    let channels = axes.extent(axes.spectral);
    let stokes = axes.extent(axes.stokes);
    let refuse = |reason: String| PrepareError::MaskImageExpansion {
        path: path.into(),
        reason,
    };
    let stokes_refusal = || {
        refuse(format!(
            "Cannot extend the input mask of {stokes} Stokes planes to the output mask of {} \
             Stokes planes. Please modify the input mask to make it a Stokes I mask or a mask \
             with the same number of Stokes planes as the output.",
            target.polarizations
        ))
    };
    let expands = (channels == 1 && target.channels > 1) || (stokes == 1 && target.polarizations > 1);
    if expands {
        // `expandMask`.
        return if channels == 1 {
            match stokes {
                1 => Ok(MaskPlanes::SHARED),
                stokes if stokes == target.polarizations => Ok(MaskPlanes::new(1, stokes)?),
                _ => Err(stokes_refusal()),
            }
        } else if stokes == 1 {
            same_channels(coordinates, axes, target).map_err(refuse)?;
            Ok(MaskPlanes::new(channels, 1)?)
        } else {
            Err(refuse(format!(
                "Input mask,{} does not conform with the number of channels in output mask",
                path.display()
            )))
        };
    }
    // `copyMask` regrids the mask's direction and spectral axes onto the
    // image's: a plane for each Stokes plane and channel the image has.
    let copies = stokes == target.polarizations
        && (channels == 1
            || (channels == target.channels && same_channels(coordinates, axes, target).is_ok()));
    if copies {
        Ok(MaskPlanes::new(channels, stokes)?)
    } else {
        Err(PrepareError::MaskImageRegrid {
            path: path.into(),
            shape: axes.shape.clone(),
        })
    }
}

/// Whether the mask's channels are the image's: as many, on a spectral
/// coordinate near the image's (`SDMaskHandler::compareSpectralCoordinate`,
/// casacore `SpectralCoordinate::near` at its tolerance of 1e-6 on the
/// frequency system, rest frequency and linear mapping); CASA's refusal
/// otherwise.
fn same_channels(
    coordinates: &CoordinateSystem,
    axes: &MaskAxes,
    target: TargetPlanes<'_>,
) -> Result<(), String> {
    let channels = axes.extent(axes.spectral);
    let spectral = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .map(|index| coordinates.coordinate(index));
    let near = match (spectral, target.spectral) {
        (Some(CoordinateModel::Spectral(mask)), Some(image)) => spectral_near(mask, image),
        _ => false,
    };
    if channels != target.channels {
        return Err(if near {
            "Cannot extend the input mask. The number of the channels in Input mask does not \
             match with that of the output mask. Use a single channel mask or a mask that \
             matches the spectral coordiante of the output."
        } else {
            "Cannot extend the input mask. Spectral coordiante and the number of channels of \
             the input mask does not match with those of the output mask. Use a single channel \
             mask or a mask that matches the spectral coordiante of the output."
        }
        .to_string());
    }
    if !near {
        return Err("Cannot extend the input mask. Spectral coordiante of Input mask does not \
                    match with that of the output mask. Use a single channel mask or a mask \
                    that matches the spectral coordiante of the output."
            .to_string());
    }
    Ok(())
}

/// casacore `SpectralCoordinate::near` at its default tolerance, on what
/// places the channels: the frequency system, the rest frequency and the
/// linear mapping.
fn spectral_near(mask: &SpectralCoordinate, image: &SpectralCoordinate) -> bool {
    let near = |left: f64, right: f64| {
        left == right || (left - right).abs() <= 1.0e-6 * left.abs().max(right.abs())
    };
    let pairs = |left: Vec<f64>, right: Vec<f64>| {
        left.len() == right.len()
            && left
                .iter()
                .zip(&right)
                .all(|(left, right)| near(*left, *right))
    };
    let (mask_model, image_model) = (
        CoordinateModel::Spectral(Box::new(mask.clone())),
        CoordinateModel::Spectral(Box::new(image.clone())),
    );
    mask.frequency_ref() == image.frequency_ref()
        && near(mask.rest_frequency(), image.rest_frequency())
        && pairs(mask_model.reference_value(), image_model.reference_value())
        && pairs(mask_model.increment(), image_model.increment())
        && pairs(mask_model.reference_pixel(), image_model.reference_pixel())
}
