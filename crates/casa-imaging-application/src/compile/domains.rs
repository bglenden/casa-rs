// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image domains: the main image and the outlier file's, each with its
//! direction coordinate, product coordinate system and clean mask.

use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, ObsInfo, ProjectionType,
};
use casa_images::AnyPagedImage;
use casa_imaging_model::{
    AxisOrder, DirectionCoordinateSpec, DirectionFrame, FacetLayout, ImageAxis, ImageDomainRole,
    ImageDomainSpec, ImageShape, PolarizationCoordinate, Projection, SkyDirection,
};
use casa_imaging_reconstruction::{
    AutoMultithreshControls, ImageDomainReconstructionMaskPlans, MaskBox, ReconstructionMaskPlan,
};
use casa_types::measures::direction::DirectionRef;

use super::boxed;
use super::direction::{
    Centre, ImageSpectralCoordinate, direction_spec, image_coordinates,
    parse_phase_center_direction,
};
use super::outliers::read_outlier_domains;
use crate::{ApplicationError, CasaImageDomainOutput, ImagingRequest, UseMask};

/// The pixels a domain's minor cycle may update.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum DomainMask {
    /// Every valid model pixel.
    FullPlane,
    /// The union of inclusive pixel boxes `[x0, y0, x1, y1]`.
    Boxes(Vec<[usize; 4]>),
    /// The non-zero pixels of a CASA image, reprojected onto the domain.
    Image(PathBuf),
    /// Row-major support over the domain's own grid.
    PixelSupport(Box<[bool]>),
    /// CASA auto-multithresh support from the current normal state.
    AutoMultithresh(AutoMultithreshControls),
}

/// One image domain of the run.
pub(super) struct PreparedImageDomain {
    pub(super) role: ImageDomainRole,
    pub(super) output: PathBuf,
    pub(super) image_size: usize,
    pub(super) direction: DirectionCoordinateSpec,
    pub(super) coordinates: CoordinateSystem,
    pub(super) mask: DomainMask,
}

impl PreparedImageDomain {
    /// The model's specification of this domain: one facet, axes
    /// direction, polarization, spectral.
    pub(super) fn spec(&self) -> ImageDomainSpec {
        ImageDomainSpec::new(
            self.role.clone(),
            ImageShape::new(self.image_size, self.image_size),
            self.direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )
    }

    /// Where the product sink writes this domain's images.
    pub(super) fn output(&self) -> CasaImageDomainOutput {
        CasaImageDomainOutput::new(
            self.role.clone(),
            self.output.clone(),
            self.coordinates.clone(),
        )
    }
}

/// The main domain and the outlier file's, ordered by role.
pub(super) fn prepare_domains(
    request: &ImagingRequest,
    centre: &Centre,
    spectral: ImageSpectralCoordinate,
    observation: &ObsInfo,
) -> Result<Vec<PreparedImageDomain>, ApplicationError> {
    let polarizations = &request.stokes;
    let mut domains = vec![PreparedImageDomain {
        role: ImageDomainRole::Main,
        output: request.imagename.clone(),
        image_size: request.imsize,
        direction: centre.direction,
        coordinates: image_coordinates(
            centre.direction,
            request.imsize,
            polarizations,
            spectral,
            observation,
        ),
        mask: request_mask(request),
    }];
    if let Some(path) = request.outlierfile.as_deref() {
        for outlier in read_outlier_domains(path, request.imsize, request.cell)? {
            let centre = parse_phase_center_direction(&outlier.phase_center)?;
            let direction = direction_spec(
                outlier.image_size,
                outlier.cell_arcsec,
                DirectionFrame::J2000,
                centre.longitude_rad(),
                centre.latitude_rad(),
            );
            domains.push(PreparedImageDomain {
                role: ImageDomainRole::Outlier(outlier.name),
                output: outlier.output,
                image_size: outlier.image_size,
                direction,
                coordinates: image_coordinates(
                    direction,
                    outlier.image_size,
                    polarizations,
                    spectral,
                    observation,
                ),
                mask: outlier.mask,
            });
        }
    }
    domains.sort_by(|left, right| left.role.cmp(&right.role));
    Ok(domains)
}

/// The main domain's mask: CASA auto-multithresh, the user mask image or
/// boxes, or every pixel. The auto-multithresh controls the catalog does
/// not carry take CASA's defaults (`smoothfactor = 1`, `cutthreshold =
/// 0.01`, `minpercentchange = -1`).
fn request_mask(request: &ImagingRequest) -> DomainMask {
    match (request.usemask, &request.mask_image) {
        (UseMask::AutoMultithresh, _) => DomainMask::AutoMultithresh(AutoMultithreshControls {
            sidelobe_factor: request.sidelobethreshold,
            noise_factor: request.noisethreshold,
            low_noise_factor: request.lownoisethreshold,
            negative_factor: request.negativethreshold,
            minimum_beam_fraction: request.minbeamfrac,
            smooth_factor: 1.0,
            cut_threshold: 0.01,
            grow_iterations: request.growiterations,
            minimum_percent_change: -1.0,
        }),
        (UseMask::User, Some(path)) => DomainMask::Image(path.clone()),
        (UseMask::User, None) if request.mask_box.is_empty() => DomainMask::FullPlane,
        (UseMask::User, None) => DomainMask::Boxes(request.mask_box.clone()),
    }
}

/// Each domain's mask plan, bound to its direction coordinate.
pub(super) fn mask_plans(
    domains: &[PreparedImageDomain],
) -> Result<ImageDomainReconstructionMaskPlans, ApplicationError> {
    Ok(ImageDomainReconstructionMaskPlans::new(
        domains
            .iter()
            .map(|domain| mask_plan(domain.mask.clone(), domain.direction, domain.image_size))
            .collect::<Result<Vec<_>, _>>()?,
    )?)
}

fn mask_plan(
    mask: DomainMask,
    coordinate: DirectionCoordinateSpec,
    image_size: usize,
) -> Result<ReconstructionMaskPlan, ApplicationError> {
    Ok(match mask {
        DomainMask::FullPlane => ReconstructionMaskPlan::FullPlane { coordinate },
        DomainMask::Boxes(boxes) => ReconstructionMaskPlan::Boxes {
            coordinate,
            boxes: boxes
                .into_iter()
                .map(|[x0, y0, x1, y1]| MaskBox::new([x0, y0], [x1, y1]))
                .collect::<Result<Vec<_>, _>>()?,
        },
        DomainMask::Image(path) => reproject_image_mask(&path, coordinate, image_size)?,
        DomainMask::PixelSupport(support) => {
            if image_size
                .checked_mul(image_size)
                .is_none_or(|expected| support.len() != expected)
            {
                return Err(boxed("pixel mask support does not match its image domain"));
            }
            ReconstructionMaskPlan::Reprojected {
                coordinate,
                source_coordinate: coordinate,
                source_shape: [image_size, image_size],
                support,
            }
        }
        DomainMask::AutoMultithresh(controls) => ReconstructionMaskPlan::AutoMultithresh {
            coordinate,
            controls,
            completed_major_cycles: 0,
            cycle_threshold_reached: false,
            previous: None,
            evolution_stopped: false,
        },
    })
}

/// The non-zero, unmasked, finite pixels of a one-plane CASA image,
/// reprojected onto `target_spec`.
fn reproject_image_mask(
    path: &Path,
    target_spec: DirectionCoordinateSpec,
    target_size: usize,
) -> Result<ReconstructionMaskPlan, ApplicationError> {
    let (source_shape, source_coordinates, source_support) = match AnyPagedImage::open(path)? {
        AnyPagedImage::Float32(image) => {
            let mask = image.get_mask()?;
            let support = image
                .get()?
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    value.is_finite()
                        && *value != 0.0
                        && mask.as_ref().is_none_or(|mask| mask[index])
                })
                .collect::<Vec<_>>();
            (image.shape().to_vec(), image.coordinates().clone(), support)
        }
        AnyPagedImage::Float64(image) => {
            let mask = image.get_mask()?;
            let support = image
                .get()?
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    value.is_finite()
                        && *value != 0.0
                        && mask.as_ref().is_none_or(|mask| mask[index])
                })
                .collect::<Vec<_>>();
            (image.shape().to_vec(), image.coordinates().clone(), support)
        }
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            return Err(boxed("reconstruction masks require a real CASA image"));
        }
    };
    if source_shape.len() < 2
        || source_shape[2..].iter().any(|extent| *extent != 1)
        || source_support.len() != source_shape[0] * source_shape[1]
    {
        return Err(boxed(
            "reconstruction mask must contain one two-dimensional direction plane",
        ));
    }
    let index = source_coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| boxed("mask image has no direction coordinate"))?;
    let source_spec = direction_model_spec(source_coordinates.coordinate(index))?;
    let support = casa_imaging_reconstruction::reproject_mask_support(
        source_spec,
        [source_shape[0], source_shape[1]],
        &source_support,
        target_spec,
        [target_size, target_size],
    )?;
    Ok(ReconstructionMaskPlan::Reprojected {
        coordinate: target_spec,
        source_coordinate: source_spec,
        source_shape: [source_shape[0], source_shape[1]],
        support,
    })
}

fn direction_model_spec(
    coordinate: &CoordinateModel,
) -> Result<DirectionCoordinateSpec, ApplicationError> {
    let CoordinateModel::Direction(direction) = coordinate else {
        return Err(boxed("mask direction-coordinate lookup was inconsistent"));
    };
    if direction.projection().projection_type() != ProjectionType::SIN {
        return Err(boxed(
            "native mask reprojection currently requires SIN coordinates",
        ));
    }
    let frame = match direction.direction_ref() {
        DirectionRef::J2000 => DirectionFrame::J2000,
        DirectionRef::B1950 => DirectionFrame::B1950,
        DirectionRef::GALACTIC => DirectionFrame::Galactic,
        DirectionRef::ICRS => DirectionFrame::Icrs,
        _ => {
            return Err(boxed(
                "mask direction frame is not supported by native imaging",
            ));
        }
    };
    let reference = coordinate.reference_value();
    let pixel = coordinate.reference_pixel();
    let increment = coordinate.increment();
    let pc = direction.pc_matrix();
    Ok(DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(frame, reference[0], reference[1]),
        [pixel[0], pixel[1]],
        [increment[0], increment[1]],
        [[pc[[0, 0]], pc[[0, 1]]], [pc[[1, 0]], pc[[1, 1]]]],
        [
            direction.longpole().to_degrees(),
            direction.latpole().to_degrees(),
        ],
    ))
}

/// Pixels a model plane holds across every domain, times `planes` and
/// `polarizations`.
pub(super) fn model_samples(
    domains: &[PreparedImageDomain],
    planes: usize,
    polarizations: &[PolarizationCoordinate],
) -> Result<usize, ApplicationError> {
    domains
        .iter()
        .try_fold(0_usize, |total, domain| {
            domain
                .image_size
                .saturating_mul(domain.image_size)
                .checked_add(total)
        })
        .and_then(|samples| samples.checked_mul(planes))
        .and_then(|samples| samples.checked_mul(polarizations.len()))
        .ok_or_else(|| boxed("reconstruction model sample count overflowed"))
}
