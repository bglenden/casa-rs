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
use std::sync::Arc;

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate};
use casa_images::{AnyPagedImage, ImagePixel, PagedImage};
use casa_imaging_deconvolution::{MaskPlanes, ReconstructionMaskPlan, reproject_mask_support};
use casa_imaging_model::DirectionCoordinateSpec;

use super::PrepareError;

/// The bound on the tiles a mask image's read caches, as a restart's CLEAN
/// mask is read.
const CACHE_BYTES: usize = 64 * 1024;

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

    /// The start and shape of the direction plane of `channel` and
    /// `stokes`: one pixel along every other axis.
    fn plane(&self, channel: usize, stokes: usize) -> (Vec<usize>, Vec<usize>) {
        let mut start = vec![0; self.shape.len()];
        let mut shape = vec![1; self.shape.len()];
        for axis in self.direction {
            shape[axis] = self.shape[axis];
        }
        if let Some(axis) = self.spectral {
            start[axis] = channel;
        }
        if let Some(axis) = self.stokes {
            start[axis] = stokes;
        }
        (start, shape)
    }

    /// Pixels of one direction plane.
    fn plane_cells(&self) -> u64 {
        self.direction.iter().fold(1_u64, |cells, &axis| {
            cells.saturating_mul(self.shape[axis] as u64)
        })
    }
}

/// A mask image of real pixels.
enum RealImage {
    Float32(PagedImage<f32>),
    Float64(PagedImage<f64>),
}

/// A real mask image opened onto an image domain, read plane by plane once
/// its read is admitted ([`Self::reading_bytes`]), as a restart's CLEAN
/// mask is.
pub(crate) struct MaskImage {
    image: RealImage,
    axes: MaskAxes,
    /// The planes tclean gives it on the image (`SDMaskHandler::fillMask`).
    held: MaskPlanes,
    source: DirectionCoordinateSpec,
    target: DirectionCoordinateSpec,
    target_size: [usize; 2],
    /// Bytes of one pixel of the image.
    pixel_bytes: u64,
    tile_bytes: u64,
}

impl MaskImage {
    /// Open the mask image at `path` for `target`'s direction grid of
    /// `target_size` pixels on an image of `planes`, reading only its
    /// description: CASA's refusal, or the planes it gives the mask.
    pub(super) fn open(
        path: &Path,
        target: DirectionCoordinateSpec,
        target_size: [usize; 2],
        planes: TargetPlanes<'_>,
    ) -> Result<Self, PrepareError> {
        let (image, pixel_bytes) = match AnyPagedImage::open_with_cache(path, CACHE_BYTES)? {
            AnyPagedImage::Float32(image) => (RealImage::Float32(image), 4_u64),
            AnyPagedImage::Float64(image) => (RealImage::Float64(image), 8),
            AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
                return Err(PrepareError::ComplexMaskImage);
            }
        };
        let (shape, coordinates, tile) = match &image {
            RealImage::Float32(image) => (image.shape(), image.coordinates(), image.tile_shape()),
            RealImage::Float64(image) => (image.shape(), image.coordinates(), image.tile_shape()),
        };
        let axes = mask_axes(shape, coordinates)?;
        let held = expansion(path, coordinates, &axes, planes)?;
        let direction = coordinates
            .find_coordinate(CoordinateType::Direction)
            .ok_or(PrepareError::MaskImageDirection)?;
        let source = super::domains::direction_model_spec(coordinates.coordinate(direction))?;
        let tile_bytes = tile.iter().fold(pixel_bytes, |bytes, &side| {
            bytes.saturating_mul(side as u64)
        });
        Ok(Self {
            image,
            axes,
            held,
            source,
            target,
            target_size,
            pixel_bytes,
            tile_bytes,
        })
    }

    /// Target pixels of one plane.
    fn target_plane(&self) -> u64 {
        (self.target_size[0] as u64).saturating_mul(self.target_size[1] as u64)
    }

    /// Heap bytes [`Self::read`] holds at most: one source plane as it is
    /// read (its pixels, its default pixel mask and its support) and that
    /// plane reprojected; every plane's support as it is gathered and as
    /// the plan shares it; the bounded tile cache and two decoded/I/O tile
    /// buffers.
    pub(crate) fn reading_bytes(&self) -> u64 {
        self.axes
            .plane_cells()
            .saturating_mul(self.pixel_bytes + 2)
            .saturating_add(self.target_plane())
            .saturating_add(self.retained_bytes().saturating_mul(2))
            .saturating_add(self.tile_bytes.saturating_mul(2))
            .saturating_add(CACHE_BYTES as u64)
    }

    /// Heap bytes the plan keeps of [`Self::read`]: its shared support, a
    /// plane for each plane it holds.
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.target_plane().saturating_mul(self.held.count() as u64)
    }

    /// The plan of the mask on the target grid: each source plane read and
    /// its support reprojected, one plane at a time.
    pub(crate) fn read(self) -> Result<ReconstructionMaskPlan, PrepareError> {
        let source_shape = self.axes.direction.map(|axis| self.axes.shape[axis]);
        let held = self.held;
        let cells = self.target_size[0] * self.target_size[1];
        let mut support = Vec::with_capacity(cells * held.count());
        for channel in 0..held.channels() {
            for stokes in 0..held.polarizations() {
                let (start, shape) = self.axes.plane(channel, stokes);
                let plane = match &self.image {
                    RealImage::Float32(image) => plane_support(image, &start, &shape)?,
                    RealImage::Float64(image) => plane_support(image, &start, &shape)?,
                };
                support.extend_from_slice(&reproject_mask_support(
                    self.source,
                    source_shape,
                    &plane,
                    self.target,
                    self.target_size,
                )?);
            }
        }
        let support: Arc<[bool]> = support.into();
        Ok(ReconstructionMaskPlan::Reprojected {
            coordinate: self.target,
            planes: held,
            support,
        })
    }
}

/// The mask image at `path` opened onto its own direction grid, for an
/// image of `channels` channels on its own spectral coordinate and
/// `polarizations` Stokes planes: the counting law's view of the reader
/// compile uses ([`crate::mask_image_law`]).
pub(crate) fn open_onto_itself(
    path: &Path,
    channels: usize,
    polarizations: usize,
) -> Result<MaskImage, PrepareError> {
    let (coordinates, shape) = match AnyPagedImage::open(path)? {
        AnyPagedImage::Float32(image) => (image.coordinates().clone(), image.shape().to_vec()),
        AnyPagedImage::Float64(image) => (image.coordinates().clone(), image.shape().to_vec()),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            return Err(PrepareError::ComplexMaskImage);
        }
    };
    let axes = mask_axes(&shape, &coordinates)?;
    let direction = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or(PrepareError::MaskImageDirection)?;
    let target = super::domains::direction_model_spec(coordinates.coordinate(direction))?;
    let spectral = match coordinates
        .find_coordinate(CoordinateType::Spectral)
        .map(|index| coordinates.coordinate(index))
    {
        Some(CoordinateModel::Spectral(spectral)) => Some(&**spectral),
        _ => None,
    };
    MaskImage::open(
        path,
        target,
        axes.direction.map(|axis| shape[axis]),
        TargetPlanes {
            channels,
            polarizations,
            spectral,
        },
    )
}

/// The plan of the mask image at `path` on `target`'s direction grid of
/// `target_size` pixels, with the planes tclean gives it for an image of
/// `planes`, read without admission.
#[cfg(test)]
pub(super) fn reproject_image_mask(
    path: &Path,
    target: DirectionCoordinateSpec,
    target_size: [usize; 2],
    planes: TargetPlanes<'_>,
) -> Result<ReconstructionMaskPlan, PrepareError> {
    MaskImage::open(path, target, target_size, planes)?.read()
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

/// The support of the direction plane of `image` at `start` of `shape`
/// (one pixel along every other axis): its finite, non-zero pixels valid in
/// the default pixel mask, x-major. The plane's values, in index order, run
/// over x with y fastest, whatever axes lie between.
fn plane_support<T>(
    image: &PagedImage<T>,
    start: &[usize],
    shape: &[usize],
) -> Result<Vec<bool>, PrepareError>
where
    T: ImagePixel + Into<f64> + Copy,
{
    let values = image.get_slice(start, shape)?;
    let mask = image.get_mask_slice(start, shape, &vec![1; shape.len()])?;
    let set = |value: &T| {
        let value: f64 = (*value).into();
        value.is_finite() && value != 0.0
    };
    Ok(match mask {
        Some(mask) => values
            .iter()
            .zip(mask.iter())
            .map(|(value, valid)| *valid && set(value))
            .collect(),
        None => values.iter().map(set).collect(),
    })
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
    let expands =
        (channels == 1 && target.channels > 1) || (stokes == 1 && target.polarizations > 1);
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
        return Err(
            "Cannot extend the input mask. Spectral coordiante of Input mask does not \
                    match with that of the output mask. Use a single channel mask or a mask \
                    that matches the spectral coordiante of the output."
                .to_string(),
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use casa_coordinates::{
        DirectionCoordinate, Projection, ProjectionType, StokesCoordinate, StokesType,
    };
    use casa_types::measures::{direction::DirectionRef, frequency::FrequencyRef};

    fn spectral(increment_hz: f64) -> SpectralCoordinate {
        SpectralCoordinate::new(FrequencyRef::LSRK, 1.0e9, increment_hz, 0.0, 1.0e9)
    }

    /// A mask of `stokes` Stokes planes and `channels` channels on the
    /// spectral coordinate of `increment_hz` channels.
    fn mask(stokes: usize, channels: usize, increment_hz: f64) -> (CoordinateSystem, MaskAxes) {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.5],
            [-1e-5, 1e-5],
            [4.0, 4.0],
        ));
        let types = [StokesType::I, StokesType::Q, StokesType::U, StokesType::V];
        coordinates.add_coordinate(StokesCoordinate::new(types[..stokes].to_vec()));
        coordinates.add_coordinate(spectral(increment_hz));
        let axes = mask_axes(&[8, 8, stokes, channels], &coordinates).unwrap();
        (coordinates, axes)
    }

    /// tclean's planes for a mask of (Stokes, channels) on an image of
    /// (polarizations, channels), whose channels are 1 MHz wide.
    fn expand(
        (stokes, channels): (usize, usize),
        (polarizations, image_channels): (usize, usize),
        increment_hz: f64,
    ) -> Result<MaskPlanes, PrepareError> {
        let (coordinates, axes) = mask(stokes, channels, increment_hz);
        let image = spectral(1.0e6);
        expansion(
            Path::new("in.mask"),
            &coordinates,
            &axes,
            TargetPlanes {
                channels: image_channels,
                polarizations,
                spectral: Some(&image),
            },
        )
    }

    fn refused(result: Result<MaskPlanes, PrepareError>) -> String {
        match result {
            Err(PrepareError::MaskImageExpansion { reason, .. }) => reason,
            other => panic!("expected CASA's refusal, not {other:?}"),
        }
    }

    /// `SDMaskHandler::fillMask` and `expandMask`: what each mask becomes on
    /// each image, and what CASA refuses.
    #[test]
    fn masks_expand_to_the_image_planes_as_casa_expands_them() {
        let planes = |channels, stokes| MaskPlanes::new(channels, stokes).unwrap();
        let near = 1.0e6;
        // A continuum Stokes I mask to every plane.
        for image in [(1, 4), (4, 1), (4, 4), (1, 1)] {
            assert_eq!(expand((1, 1), image, near).unwrap(), MaskPlanes::SHARED);
        }
        // A continuum mask of the image's Stokes planes to every channel.
        assert_eq!(expand((4, 1), (4, 4), near).unwrap(), planes(1, 4));
        assert_eq!(expand((4, 1), (4, 1), near).unwrap(), planes(1, 4));
        // A cube Stokes I mask to every Stokes plane, on the image's
        // channels; a cube of the image's planes as it is.
        assert_eq!(expand((1, 3), (4, 3), near).unwrap(), planes(3, 1));
        assert_eq!(expand((1, 3), (1, 3), near).unwrap(), planes(3, 1));
        assert_eq!(expand((4, 3), (4, 3), near).unwrap(), planes(3, 4));
        // CASA refuses a continuum mask of another number of Stokes planes
        // on a cube, and a cube Stokes I mask of other channels.
        assert!(
            refused(expand((2, 1), (4, 4), near))
                .starts_with("Cannot extend the input mask of 2 Stokes planes")
        );
        assert!(refused(expand((1, 3), (4, 4), near)).contains("number of the channels"));
        assert!(refused(expand((1, 3), (4, 3), 2.0e6)).contains("Spectral coordiante of Input"));
        assert!(refused(expand((1, 3), (4, 4), 2.0e6)).contains("and the number of channels"));
        // tclean regrids any other mask along frequency or Stokes, which
        // casa-rs does not yet.
        for (mask, image, increment) in [
            ((2, 1), (4, 1), near),
            ((1, 3), (1, 3), 2.0e6),
            ((1, 3), (1, 1), near),
            ((2, 3), (4, 3), near),
        ] {
            assert!(
                matches!(
                    expand(mask, image, increment),
                    Err(PrepareError::MaskImageRegrid { .. })
                ),
                "{mask:?} on {image:?}"
            );
        }
    }
}
