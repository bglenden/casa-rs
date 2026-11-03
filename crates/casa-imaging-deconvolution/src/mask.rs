// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction masks.
//!
//! Reconstruction masks constrain where model updates may be placed. They are
//! intentionally distinct from product-validity masks. Static user masks and
//! auto-multithreshold masks are the same closed type, so solvers need no
//! mask-mode branch.
//!
//! A mask covers every plane of its image domain, as CASA's `.mask` image
//! does: one support shared by every channel or polarization, as CASA
//! expands a continuum or Stokes I mask, or one per plane, as an automatic
//! mask makes ([`MaskPlanes`]).

mod auto_multithresh;
mod auto_planes;
mod plans;

use std::sync::Arc;

use casa_imaging_model::DirectionCoordinateSpec;
use thiserror::Error;

pub use auto_multithresh::{
    AUTO_MULTITHRESH_BYTES_PER_CELL, AutoMaskBeam, AutoMaskCycle, AutoMaskThreshold,
    AutoMultithreshControls, AutoMultithreshEvidence, auto_multithresh, auto_multithresh_bytes,
};
pub use auto_planes::{
    AutoMaskEvidence, AutoMaskPlane, AutoMaskPsf, AutoMaskRead, AutoMaskSource, MaskPlaneKey,
};
pub use plans::{
    ImageDomainMaskMaterialization, ImageDomainReconstructionMaskPlans,
    ImageDomainReconstructionMasks, NoiseMaskExclusion, ReconstructionMaskPlan,
    ReconstructionMaskSet,
};

/// Reproject decoded source support onto the target direction grid.
///
/// Storage/application code supplies only decoded samples and coordinates;
/// reconstruction owns traversal, mapping, sampling, and uncovered support.
pub fn reproject_mask_support(
    source_coordinate: DirectionCoordinateSpec,
    source_shape: [usize; 2],
    source_support: &[bool],
    target_coordinate: DirectionCoordinateSpec,
    target_shape: [usize; 2],
) -> Result<Box<[bool]>, MaskError> {
    validate_shape(source_shape)?;
    validate_shape(target_shape)?;
    if source_support.len() != source_shape[0] * source_shape[1] {
        return Err(MaskError::ShapeMismatch);
    }
    let mut target_support = vec![false; target_shape[0] * target_shape[1]];
    for x in 0..target_shape[0] {
        for y in 0..target_shape[1] {
            if reprojected_pixel(source_coordinate, source_shape, target_coordinate, [x, y])?
                .is_some_and(|[sx, sy]| source_support[sx * source_shape[1] + sy])
            {
                target_support[x * target_shape[1] + y] = true;
            }
        }
    }
    Ok(target_support.into_boxed_slice())
}

pub(crate) fn reprojected_pixel(
    source_coordinate: DirectionCoordinateSpec,
    source_shape: [usize; 2],
    target_coordinate: DirectionCoordinateSpec,
    target_pixel: [usize; 2],
) -> Result<Option<[usize; 2]>, MaskError> {
    let world = direction_pixel_to_world(
        target_coordinate,
        [target_pixel[0] as f64, target_pixel[1] as f64],
    )?;
    let source = direction_world_to_pixel(source_coordinate, world)?;
    let x = source[0].round();
    let y = source[1].round();
    Ok(
        (x >= 0.0 && y >= 0.0 && x < source_shape[0] as f64 && y < source_shape[1] as f64)
            .then_some([x as usize, y as usize]),
    )
}

pub(crate) fn direction_pixel_to_world(
    coordinate: DirectionCoordinateSpec,
    pixel: [f64; 2],
) -> Result<[f64; 2], MaskError> {
    if coordinate.projection() != casa_imaging_model::Projection::Sin {
        return Err(MaskError::UnsupportedReprojection);
    }
    let offset = [
        pixel[0] - coordinate.reference_pixel()[0],
        pixel[1] - coordinate.reference_pixel()[1],
    ];
    let pc = coordinate.pc();
    let increment = coordinate.increment_rad();
    let x = increment[0] * (pc[0][0] * offset[0] + pc[0][1] * offset[1]);
    let y = increment[1] * (pc[1][0] * offset[0] + pc[1][1] * offset[1]);
    let radius_squared = x * x + y * y;
    if radius_squared > 1.0 + 1.0e-12 {
        return Err(MaskError::UnsupportedReprojection);
    }
    let phi = if radius_squared == 0.0 {
        0.0
    } else {
        x.atan2(-y)
    };
    let theta = (1.0 - radius_squared.min(1.0)).max(0.0).sqrt().asin();
    let reference = coordinate.reference_direction();
    let alpha_p = reference.longitude_rad();
    let delta_p = reference.latitude_rad();
    let phi_p = coordinate.pole_deg()[0].to_radians();
    let dphi = phi - phi_p;
    let sin_lat = theta.sin() * delta_p.sin() + theta.cos() * delta_p.cos() * dphi.cos();
    let latitude = sin_lat.clamp(-1.0, 1.0).asin();
    let longitude = alpha_p
        + (-theta.cos() * dphi.sin())
            .atan2(theta.sin() * delta_p.cos() - theta.cos() * delta_p.sin() * dphi.cos());
    Ok([longitude, latitude])
}

/// The pixel of `world` (longitude and latitude in radians, in the
/// coordinate's frame) under `coordinate`'s SIN projection: CASA
/// `DirectionCoordinate::toPixel` for the projections the imaging route
/// compiles.
///
/// # Errors
///
/// The projection is not SIN, the PC matrix is singular, or the pixel is
/// not finite.
pub fn direction_world_to_pixel(
    coordinate: DirectionCoordinateSpec,
    world: [f64; 2],
) -> Result<[f64; 2], MaskError> {
    if coordinate.projection() != casa_imaging_model::Projection::Sin {
        return Err(MaskError::UnsupportedReprojection);
    }
    let reference = coordinate.reference_direction();
    let alpha_p = reference.longitude_rad();
    let delta_p = reference.latitude_rad();
    let phi_p = coordinate.pole_deg()[0].to_radians();
    let delta_alpha = world[0] - alpha_p;
    let sin_theta =
        world[1].sin() * delta_p.sin() + world[1].cos() * delta_p.cos() * delta_alpha.cos();
    let theta = sin_theta.clamp(-1.0, 1.0).asin();
    let phi = phi_p
        + (-world[1].cos() * delta_alpha.sin()).atan2(
            world[1].sin() * delta_p.cos() - world[1].cos() * delta_p.sin() * delta_alpha.cos(),
        );
    let x = theta.cos() * phi.sin();
    let y = -theta.cos() * phi.cos();
    let increment = coordinate.increment_rad();
    let intermediate = [x / increment[0], y / increment[1]];
    let pc = coordinate.pc();
    let determinant = pc[0][0] * pc[1][1] - pc[0][1] * pc[1][0];
    if !determinant.is_finite() || determinant.abs() < 1.0e-15 {
        return Err(MaskError::UnsupportedReprojection);
    }
    let offset = [
        (pc[1][1] * intermediate[0] - pc[0][1] * intermediate[1]) / determinant,
        (-pc[1][0] * intermediate[0] + pc[0][0] * intermediate[1]) / determinant,
    ];
    let reference_pixel = coordinate.reference_pixel();
    let pixel = [
        reference_pixel[0] + offset[0],
        reference_pixel[1] + offset[1],
    ];
    if pixel.iter().all(|value| value.is_finite()) {
        Ok(pixel)
    } else {
        Err(MaskError::UnsupportedReprojection)
    }
}

/// Inclusive signed pixel box in the named mask coordinate grid.
/// Materialization intersects these bounds with the target image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaskBox {
    blc: [i64; 2],
    trc: [i64; 2],
}

impl MaskBox {
    /// Construct an inclusive non-inverted box.
    pub fn new(blc: [i64; 2], trc: [i64; 2]) -> Result<Self, MaskError> {
        if blc[0] > trc[0] || blc[1] > trc[1] {
            return Err(MaskError::InvertedBox);
        }
        Ok(Self { blc, trc })
    }

    /// Return the inclusive lower corner.
    #[must_use]
    pub const fn blc(self) -> [i64; 2] {
        self.blc
    }

    /// Return the inclusive upper corner.
    #[must_use]
    pub const fn trc(self) -> [i64; 2] {
        self.trc
    }
}

/// The planes a reconstruction mask holds for its image domain's channels
/// and polarizations. An axis of one plane is shared by every channel or
/// polarization of the image, as CASA expands a continuum mask to every
/// channel and a Stokes I mask to every Stokes plane
/// (`SDMaskHandler::expandMask`); otherwise there is a plane for each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaskPlanes {
    channels: usize,
    polarizations: usize,
}

impl MaskPlanes {
    /// One plane shared by every channel and polarization.
    pub const SHARED: Self = Self {
        channels: 1,
        polarizations: 1,
    };

    /// `channels` by `polarizations` planes; an axis of one is shared.
    ///
    /// # Errors
    ///
    /// Either count is zero.
    pub const fn new(channels: usize, polarizations: usize) -> Result<Self, MaskError> {
        if channels == 0 || polarizations == 0 {
            Err(MaskError::InvalidShape)
        } else {
            Ok(Self {
                channels,
                polarizations,
            })
        }
    }

    /// The channel planes; one is shared by every channel.
    #[must_use]
    pub const fn channels(self) -> usize {
        self.channels
    }

    /// The polarization planes; one is shared by every polarization.
    #[must_use]
    pub const fn polarizations(self) -> usize {
        self.polarizations
    }

    /// The planes held.
    #[must_use]
    pub const fn count(self) -> usize {
        self.channels * self.polarizations
    }

    /// Whether these planes cover an image of `channels` channels and
    /// `polarizations` polarizations: each axis is shared or matches.
    #[must_use]
    pub const fn covers(self, channels: usize, polarizations: usize) -> bool {
        (self.channels == 1 || self.channels == channels)
            && (self.polarizations == 1 || self.polarizations == polarizations)
    }

    /// The held plane of image plane `channel` and `polarization`, or
    /// `None` outside an axis with a plane per element.
    const fn ordinal(self, channel: usize, polarization: usize) -> Option<usize> {
        let channel = if self.channels == 1 { 0 } else { channel };
        let polarization = if self.polarizations == 1 {
            0
        } else {
            polarization
        };
        if channel < self.channels && polarization < self.polarizations {
            Some(channel * self.polarizations + polarization)
        } else {
            None
        }
    }
}

/// Immutable reconstruction-mask support on one direction grid, for every
/// plane of its image domain: one support per held plane ([`MaskPlanes`]),
/// each x-major.
///
/// The support is shared, not copied, by every clone: the plan an
/// automatic mask evolves from, the minor cycle that cleans within it and
/// the run's outcome hold one generation's support between them, so a run
/// holds at most the generation in use and the one being made.
#[derive(Debug, Clone)]
pub struct ReconstructionMask {
    coordinate: DirectionCoordinateSpec,
    shape: [usize; 2],
    planes: MaskPlanes,
    support: Arc<[bool]>,
}

impl ReconstructionMask {
    /// The all-valid default support for one exact model grid.
    pub fn full_plane(
        coordinate: DirectionCoordinateSpec,
        shape: [usize; 2],
    ) -> Result<Self, MaskError> {
        validate_shape(shape)?;
        Ok(Self::shared(
            coordinate,
            shape,
            vec![true; shape[0] * shape[1]],
        ))
    }

    /// Compile target-grid pixel boxes into one static mask, clipping each box
    /// to the target. An empty union is valid zero support (CASA stop code 7).
    pub fn from_boxes(
        coordinate: DirectionCoordinateSpec,
        shape: [usize; 2],
        boxes: impl IntoIterator<Item = MaskBox>,
    ) -> Result<Self, MaskError> {
        validate_shape(shape)?;
        let mut support = vec![false; shape[0] * shape[1]];
        let upper = shape.map(|length| i64::try_from(length - 1).unwrap_or(i64::MAX));
        for region in boxes {
            // LCBox clips each bound; SDMaskHandler skips an empty intersection
            // into its initially zero mask. This never expands to FullPlane.
            let blc = region.blc.map(|value| value.max(0));
            let trc = [region.trc[0].min(upper[0]), region.trc[1].min(upper[1])];
            if blc[0] > trc[0] || blc[1] > trc[1] {
                continue;
            }
            let [x0, y0] = blc.map(|value| usize::try_from(value).expect("clipped pixel index"));
            let [x1, y1] = trc.map(|value| usize::try_from(value).expect("clipped pixel index"));
            for x in x0..=x1 {
                for y in y0..=y1 {
                    support[x * shape[1] + y] = true;
                }
            }
        }
        Ok(Self::shared(coordinate, shape, support))
    }

    /// Exact target-grid support already reprojected by the geometry owner,
    /// one plane shared by every channel and polarization. Empty support is
    /// valid: CASA stops with zero-mask code7, preserving the residual and
    /// publishing the empty mask without applying components.
    pub fn from_reprojected_support(
        target_coordinate: DirectionCoordinateSpec,
        target_shape: [usize; 2],
        support: &[bool],
    ) -> Result<Self, MaskError> {
        Self::from_plane_support(
            target_coordinate,
            target_shape,
            MaskPlanes::SHARED,
            support.to_vec(),
        )
    }

    /// Exact target-grid support of `planes` planes, plane after plane
    /// (channel-major), each x-major. A `Vec` or boxed support is copied
    /// once into shared storage; an `Arc` is shared as it is.
    ///
    /// # Errors
    ///
    /// The shape is empty or the support is not one plane's per plane.
    pub fn from_plane_support(
        coordinate: DirectionCoordinateSpec,
        shape: [usize; 2],
        planes: MaskPlanes,
        support: impl Into<Arc<[bool]>>,
    ) -> Result<Self, MaskError> {
        validate_shape(shape)?;
        let support = support.into();
        if Some(support.len()) != (shape[0] * shape[1]).checked_mul(planes.count()) {
            return Err(MaskError::ShapeMismatch);
        }
        Ok(Self {
            coordinate,
            shape,
            planes,
            support,
        })
    }

    /// Return the exact target direction coordinate.
    #[must_use]
    pub const fn coordinate(&self) -> DirectionCoordinateSpec {
        self.coordinate
    }

    /// Return the `[x, y]` mask shape.
    #[must_use]
    pub const fn shape(&self) -> [usize; 2] {
        self.shape
    }

    /// Return the planes held.
    #[must_use]
    pub const fn planes(&self) -> MaskPlanes {
        self.planes
    }

    /// Every held plane's support, plane after plane (channel-major), each
    /// x-major.
    #[must_use]
    pub fn support(&self) -> &[bool] {
        &self.support
    }

    /// The x-major support of image plane `channel` and `polarization`: a
    /// shared axis serves every channel or polarization; `None` beyond an
    /// axis with a plane per element.
    #[must_use]
    pub fn plane(&self, channel: usize, polarization: usize) -> Option<&[bool]> {
        let cells = self.shape[0] * self.shape[1];
        self.planes
            .ordinal(channel, polarization)
            .map(|ordinal| &self.support[ordinal * cells..(ordinal + 1) * cells])
    }

    fn shared(coordinate: DirectionCoordinateSpec, shape: [usize; 2], support: Vec<bool>) -> Self {
        Self {
            coordinate,
            shape,
            planes: MaskPlanes::SHARED,
            support: support.into(),
        }
    }
}

/// Why a reconstruction mask could not be built.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MaskError {
    /// A required authoritative Normal State window could not be loaded.
    #[error(transparent)]
    NormalAccess(#[from] crate::NormalStateError),
    /// Authoritative model support could not be loaded.
    #[error(transparent)]
    ModelAccess(#[from] crate::ModelLifecycleError),
    /// Mask-plan cardinality does not equal the compiled image-domain count.
    #[error("reconstruction mask plans must name every compiled image domain exactly once")]
    DomainCardinalityMismatch,
    /// Source and target direction grids cannot be mapped by the accepted law.
    #[error("mask direction grids require a supported exact reprojection")]
    UnsupportedReprojection,
    /// The current PSF cannot define a finite positive beam area.
    #[error("normal-state PSF cannot define a finite positive beam area")]
    InvalidBeamArea,
    /// A pixel box is inverted.
    #[error("mask box requires blc <= trc")]
    InvertedBox,
    /// The support's shape or direction coordinate does not match the
    /// target grid.
    #[error("mask shape or direction coordinate differs from the target grid")]
    ShapeMismatch,
    /// Shape is empty or overflows addressable storage.
    #[error("mask shape must be finite and non-empty")]
    InvalidShape,
    /// Auto-mask residual state contained a non-finite sample.
    #[error("auto-mask residual contains a non-finite sample")]
    NonfiniteResidual,
    /// Auto-mask controls or beam area are invalid.
    #[error("auto-multithreshold controls are invalid")]
    InvalidAutoControls,
}

fn validate_shape(shape: [usize; 2]) -> Result<(), MaskError> {
    if shape[0] == 0 || shape[1] == 0 || shape[0].checked_mul(shape[1]).is_none() {
        Err(MaskError::InvalidShape)
    } else {
        Ok(())
    }
}

fn offset_mask_pixel(
    pixel: [usize; 2],
    offset: [isize; 2],
    shape: [usize; 2],
) -> Option<[usize; 2]> {
    let x = pixel[0].checked_add_signed(offset[0])?;
    let y = pixel[1].checked_add_signed(offset[1])?;
    (x < shape[0] && y < shape[1]).then_some([x, y])
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::{DirectionFrame, Projection, SkyDirection};

    fn coordinate(reference_pixel: [f64; 2]) -> DirectionCoordinateSpec {
        DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
            reference_pixel,
            [-1.0e-4, 1.0e-4],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 90.0],
        )
    }

    #[test]
    fn aligned_mask_reprojection_preserves_exact_support() {
        let support = [false, true, false, true, true, false, false, false, true];
        let projected = reproject_mask_support(
            coordinate([1.0, 1.0]),
            [3, 3],
            &support,
            coordinate([1.0, 1.0]),
            [3, 3],
        )
        .expect("aligned reprojection");
        assert_eq!(projected.as_ref(), support);
    }

    #[test]
    fn reprojection_uses_the_target_and_source_reference_pixels() {
        let mut support = [false; 9];
        support[4] = true;
        let projected = reproject_mask_support(
            coordinate([1.0, 1.0]),
            [3, 3],
            &support,
            coordinate([2.0, 1.0]),
            [4, 3],
        )
        .expect("shifted reference-pixel reprojection");
        assert!(projected[2 * 3 + 1]);
        assert_eq!(projected.iter().filter(|value| **value).count(), 1);
    }

    #[test]
    fn reprojected_zero_support_is_a_valid_empty_clean_mask() {
        let grid = coordinate([0.0, 0.0]);
        let mask = ReconstructionMask::from_reprojected_support(grid, [2, 3], &[false; 6])
            .expect("an explicit zero mask stops cleaning, rather than failing");
        assert_eq!(mask.shape(), [2, 3]);
        assert!(mask.support().iter().all(|cleanable| !cleanable));
        assert!(matches!(
            ReconstructionMask::from_reprojected_support(grid, [2, 3], &[false; 5]),
            Err(MaskError::ShapeMismatch)
        ));
    }
    #[test]
    fn boxes_clip_to_target_and_empty_intersections_remain_zero() {
        let grid = coordinate([0.0, 0.0]);
        let upper =
            ReconstructionMask::from_boxes(grid, [3, 2], [MaskBox::new([1, 0], [10, 10]).unwrap()])
                .unwrap();
        assert_eq!(upper.support(), &[false, false, true, true, true, true]);
        let outside =
            ReconstructionMask::from_boxes(grid, [3, 2], [MaskBox::new([3, 0], [10, 10]).unwrap()])
                .unwrap();
        assert_eq!(outside.support(), &[false; 6]);
        let empty = ReconstructionMask::from_boxes(grid, [3, 2], []).unwrap();
        assert_eq!(empty.support(), &[false; 6]);
    }

    /// A shared axis serves every channel or polarization, as CASA expands
    /// a continuum or Stokes I mask (`SDMaskHandler::expandMask`); an axis
    /// with a plane per element serves only its own.
    #[test]
    fn a_shared_axis_serves_every_plane_along_it() {
        let grid = coordinate([0.0, 0.0]);
        // Two polarizations shared by every channel: each plane's first
        // pixel is its polarization's.
        let planes = MaskPlanes::new(1, 2).unwrap();
        let mask = ReconstructionMask::from_plane_support(
            grid,
            [1, 2],
            planes,
            vec![true, false, false, true],
        )
        .unwrap();
        for channel in [0, 7] {
            assert_eq!(mask.plane(channel, 0), Some(&[true, false][..]));
            assert_eq!(mask.plane(channel, 1), Some(&[false, true][..]));
        }
        assert_eq!(mask.plane(0, 2), None);
        assert!(planes.covers(5, 2) && !planes.covers(5, 4));
        assert!(MaskPlanes::SHARED.covers(5, 4));
        // Three channels of one polarization.
        let cube = ReconstructionMask::from_plane_support(
            grid,
            [1, 1],
            MaskPlanes::new(3, 1).unwrap(),
            vec![false, true, false],
        )
        .unwrap();
        assert_eq!(cube.plane(1, 3), Some(&[true][..]));
        assert_eq!(cube.plane(3, 0), None);
        assert!(matches!(
            ReconstructionMask::from_plane_support(grid, [1, 1], planes, vec![true]),
            Err(MaskError::ShapeMismatch)
        ));
        assert!(MaskPlanes::new(0, 1).is_err());
    }
}
