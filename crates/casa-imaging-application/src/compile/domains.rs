// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image domains: the main image and the outlier file's, each with its
//! direction coordinate, product coordinate system and clean mask.

use std::collections::BTreeSet;
use std::path::PathBuf;

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, ObsInfo, ProjectionType,
};
use casa_imaging_deconvolution::{
    AutoMultithreshControls, ImageDomainReconstructionMaskPlans, MaskBox, MaskPlanes,
    NoiseEstimate, ReconstructionMaskPlan,
};
use casa_imaging_model::{
    AxisOrder, DirectionCoordinateSpec, DirectionFrame, FacetLayout, ImageAxis, ImageDomainRole,
    ImageDomainSpec, ImageShape, PolarizationCoordinate, Projection, SkyDirection,
    optimum_image_size,
};
use casa_types::measures::direction::DirectionRef;

use super::PrepareError;
use super::direction::{
    Centre, ImageSpectralCoordinate, direction_spec, image_coordinates,
    parse_phase_center_direction,
};
use super::outliers::read_outlier_domains;
use casa_imaging_products::CasaImageDomainOutput;

use crate::{Gridder, ImagingRequest, UseMask};

/// The pixels a domain's minor cycle may update.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum DomainMask {
    /// Every valid model pixel.
    FullPlane,
    /// The union of inclusive pixel boxes `[x0, y0, x1, y1]`.
    Boxes(Vec<[i64; 4]>),
    /// The non-zero pixels of a CASA image, reprojected onto the domain and
    /// matched to its planes ([`super::mask_image`]).
    Image(PathBuf),
    /// Support over the domain's own grid: each plane x-major, plane after
    /// plane (channel-major), shared along an axis of one plane.
    PixelSupport {
        /// The planes held.
        planes: MaskPlanes,
        /// Their support.
        support: Box<[bool]>,
    },
    /// CASA auto-multithresh support from the current normal state.
    AutoMultithresh(AutoMultithreshControls),
}

/// One image domain of the run.
pub(super) struct PreparedImageDomain {
    pub(super) role: ImageDomainRole,
    pub(super) output: PathBuf,
    /// Pixels along x and y.
    pub(super) image_size: [usize; 2],
    pub(super) direction: DirectionCoordinateSpec,
    pub(super) coordinates: CoordinateSystem,
    pub(super) mask: DomainMask,
    /// CASA facets the first image field only (`SynthesisImager`'s
    /// `createFacetImageStoreList`); outliers are single.
    pub(super) facets: FacetLayout,
}

impl PreparedImageDomain {
    /// The model's specification of this domain: its facets, axes
    /// direction, polarization, spectral.
    pub(super) fn spec(&self) -> ImageDomainSpec {
        ImageDomainSpec::new(
            self.role.clone(),
            ImageShape::new(self.image_size[0], self.image_size[1]),
            self.direction,
            self.facets,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )
    }

    /// Where the image writer writes this domain's images.
    pub(super) fn output(&self) -> CasaImageDomainOutput {
        CasaImageDomainOutput::new(
            self.role.clone(),
            self.output.clone(),
            self.coordinates.clone(),
        )
    }
}

/// The main domain and the outlier file's, ordered by role, each writing
/// to its own output.
pub(super) fn prepare_domains(
    request: &ImagingRequest,
    centre: &Centre,
    spectral: &ImageSpectralCoordinate,
    observation: &ObsInfo,
) -> Result<Vec<PreparedImageDomain>, PrepareError> {
    let polarizations = &request.stokes;
    let facets = match request.gridder {
        Gridder::Standard { facets } if facets.get() > 1 => FacetLayout::Regular {
            columns: facets.get(),
            rows: facets.get(),
        },
        _ => FacetLayout::Single,
    };
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
        facets,
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
                facets: FacetLayout::Single,
            });
        }
    }
    // Distinct outputs give distinct roles: an outlier's output is its name
    // resolved beside the outlier file.
    let mut outputs = BTreeSet::new();
    for domain in &domains {
        if !outputs.insert(domain.output.as_path()) {
            return Err(PrepareError::DuplicateOutput {
                path: domain.output.clone(),
            });
        }
    }
    domains.sort_by(|left, right| left.role.cmp(&right.role));
    for domain in &domains {
        warn_inefficient_size(domain.image_size);
    }
    Ok(domains)
}

/// `tclean`'s warning for a side that is not CASA's efficient size
/// (`SynthesisParamsImage::verify`): the image is still made at the size
/// asked for. Like CASA, it names y only when it differs from x.
fn warn_inefficient_size(image_size: [usize; 2]) {
    let [x, y] = image_size;
    let [best_x, best_y] = image_size.map(optimum_image_size);
    if best_x != x {
        tracing::warn!(
            "imsize with {x} pixels is not an efficient imagesize. Try {best_x} instead."
        );
    }
    if x != y && best_y != y {
        tracing::warn!(
            "imsize with {y} pixels is not an efficient imagesize. Try {best_y} instead."
        );
    }
}

/// The main domain's mask: CASA auto-multithresh with tclean's controls,
/// the user mask image or boxes, or every pixel.
fn request_mask(request: &ImagingRequest) -> DomainMask {
    match (request.usemask, &request.mask_image) {
        (UseMask::AutoMultithresh, _) => DomainMask::AutoMultithresh(AutoMultithreshControls {
            sidelobe_factor: request.sidelobethreshold,
            noise_factor: request.noisethreshold,
            low_noise_factor: request.lownoisethreshold,
            negative_factor: request.negativethreshold,
            minimum_beam_fraction: request.minbeamfrac,
            smooth_factor: request.smoothfactor,
            cut_threshold: request.cutthreshold,
            grow_iterations: request.growiterations,
            grow_prune: request.dogrowprune,
            minimum_percent_change: request.minpercentchange,
            noise: noise_estimate(request),
            verbose: request.verbose,
        }),
        (UseMask::User, Some(path)) => DomainMask::Image(path.clone()),
        (UseMask::User, None) if request.mask_box.is_empty() && !request.explicit_empty_mask => {
            DomainMask::FullPlane
        }
        (UseMask::User, None) => DomainMask::Boxes(request.mask_box.clone()),
    }
}

/// tclean's noise estimate (`fastnoise`).
pub(super) const fn noise_estimate(request: &ImagingRequest) -> NoiseEstimate {
    if request.fastnoise {
        NoiseEstimate::Fast
    } else {
        NoiseEstimate::Robust
    }
}

/// Each domain's mask plan, bound to its direction coordinate, for images
/// of `channels` channels.
pub(super) fn mask_plans(
    domains: &[PreparedImageDomain],
    channels: usize,
) -> Result<ImageDomainReconstructionMaskPlans, PrepareError> {
    Ok(ImageDomainReconstructionMaskPlans::new(
        domains
            .iter()
            .map(|domain| mask_plan(domain, channels))
            .collect::<Result<Vec<_>, _>>()?,
    )?)
}

fn mask_plan(
    domain: &PreparedImageDomain,
    channels: usize,
) -> Result<ReconstructionMaskPlan, PrepareError> {
    let coordinate = domain.direction;
    let image_size = domain.image_size;
    Ok(match domain.mask.clone() {
        DomainMask::FullPlane => ReconstructionMaskPlan::FullPlane { coordinate },
        DomainMask::Boxes(boxes) => ReconstructionMaskPlan::Boxes {
            coordinate,
            boxes: boxes
                .into_iter()
                .map(|[x0, y0, x1, y1]| MaskBox::new([x0, y0], [x1, y1]))
                .collect::<Result<Vec<_>, _>>()?,
        },
        DomainMask::Image(path) => {
            let coordinate_of = |kind| {
                domain
                    .coordinates
                    .find_coordinate(kind)
                    .map(|index| domain.coordinates.coordinate(index))
            };
            let polarizations = match coordinate_of(CoordinateType::Stokes) {
                Some(CoordinateModel::Stokes(stokes)) => stokes.stokes().len(),
                _ => 1,
            };
            let spectral = match coordinate_of(CoordinateType::Spectral) {
                Some(CoordinateModel::Spectral(spectral)) => Some(&**spectral),
                _ => None,
            };
            super::mask_image::reproject_image_mask(
                &path,
                coordinate,
                image_size,
                super::mask_image::TargetPlanes {
                    channels,
                    polarizations,
                    spectral,
                },
            )?
        }
        DomainMask::PixelSupport { planes, support } => {
            assert_eq!(
                (image_size[0] * image_size[1]).checked_mul(planes.count()),
                Some(support.len()),
                "a pixel support covers exactly its image domain's planes"
            );
            ReconstructionMaskPlan::Reprojected {
                coordinate,
                planes,
                support,
            }
        }
        DomainMask::AutoMultithresh(controls) => ReconstructionMaskPlan::AutoMultithresh {
            coordinate,
            controls,
            completed_major_cycles: 0,
            cycle_threshold_reached: false,
            previous: None,
            stopped_channels: Box::new([]),
        },
    })
}

/// The model's direction coordinate of a mask image's direction
/// `coordinate`.
pub(super) fn direction_model_spec(
    coordinate: &CoordinateModel,
) -> Result<DirectionCoordinateSpec, PrepareError> {
    let CoordinateModel::Direction(direction) = coordinate else {
        unreachable!("find_coordinate(Direction) names a direction coordinate");
    };
    let projection = direction.projection().projection_type();
    if projection != ProjectionType::SIN {
        return Err(PrepareError::MaskImageProjection { projection });
    }
    let frame = match direction.direction_ref() {
        DirectionRef::J2000 => DirectionFrame::J2000,
        DirectionRef::B1950 => DirectionFrame::B1950,
        DirectionRef::GALACTIC => DirectionFrame::Galactic,
        DirectionRef::ICRS => DirectionFrame::Icrs,
        frame => return Err(PrepareError::MaskImageFrame { frame }),
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
) -> Result<usize, PrepareError> {
    domains
        .iter()
        .try_fold(0_usize, |total, domain| {
            domain.image_size[0]
                .saturating_mul(domain.image_size[1])
                .checked_add(total)
        })
        .and_then(|samples| samples.checked_mul(planes))
        .and_then(|samples| samples.checked_mul(polarizations.len()))
        .ok_or(PrepareError::ImageSize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_coordinates::{
        DirectionCoordinate, Projection as CoordinateProjection, SpectralCoordinate,
        StokesCoordinate, StokesType,
    };
    use casa_images::{ImagePixel, PagedImage};
    use casa_types::measures::frequency::FrequencyRef;
    use ndarray::{ArrayD, IxDyn};
    use std::path::Path;

    use crate::compile::mask_image::{TargetPlanes, reproject_image_mask};

    fn direction() -> DirectionCoordinate {
        DirectionCoordinate::new(
            DirectionRef::J2000,
            CoordinateProjection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-1e-5, 1e-5],
            [1.5, 0.0],
        )
    }

    /// A one-plane mask image of `T` pixels on a 3 by 2 direction plane:
    /// non-zero at (0, 0), (2, 0), (1, 1) and (2, 1), with (2, 1) invalid in
    /// its default pixel mask; the direction first, or (as a transposed CASA
    /// mask reads) after Stokes. Its support reprojected onto its own grid.
    fn support<T: ImagePixel + From<f32>>(dir: &Path, direction_first: bool) -> Box<[bool]> {
        let mut coordinates = CoordinateSystem::new();
        let stokes = StokesCoordinate::new(vec![StokesType::I]);
        let shape = if direction_first {
            coordinates.add_coordinate(direction());
            coordinates.add_coordinate(stokes);
            vec![3, 2, 1, 1]
        } else {
            coordinates.add_coordinate(stokes);
            coordinates.add_coordinate(direction());
            vec![1, 3, 2, 1]
        };
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.0e9,
            1.0e6,
            0.0,
            1.0e9,
        ));
        let plane = |index: &IxDyn| {
            if direction_first {
                (index[0], index[1])
            } else {
                (index[1], index[2])
            }
        };
        let values = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
            let set = matches!(plane(&index), (0, 0) | (2, 0) | (1, 1) | (2, 1));
            T::from(if set { 1.0 } else { 0.0 })
        });
        let valid = ArrayD::from_shape_fn(IxDyn(&shape), |index| plane(&index) != (2, 1));
        let path = dir.join(format!("{direction_first}-{}.mask", values.len()));
        let mut image = PagedImage::<T>::create(shape, coordinates, &path).unwrap();
        image.put_slice(&values, &[0, 0, 0, 0]).unwrap();
        image.put_mask("mask0", &valid).unwrap();
        image.set_default_mask("mask0").unwrap();
        image.save().unwrap();
        drop(image);
        let target = direction_model_spec(&CoordinateModel::Direction(direction())).unwrap();
        let plan = reproject_image_mask(
            &path,
            target,
            [3, 2],
            TargetPlanes {
                channels: 1,
                polarizations: 1,
                spectral: None,
            },
        )
        .unwrap();
        let ReconstructionMaskPlan::Reprojected { support, .. } = plan else {
            panic!("a mask image is reprojected");
        };
        support
    }

    #[test]
    fn a_mask_reads_its_direction_plane_wherever_its_axes_are() {
        // [Stokes, RA, Dec, Freq] of shape [1, 3, 2, 1] has the 3 by 2 plane
        // of a direction-first mask; taken from its first two axes it was 1
        // by 3, and a default pixel mask indexed by a running count panicked
        // on a many-axis image (#1005 and #1022 reviews, #1002). The support
        // runs over x, y fastest.
        let expected = [true, false, false, true, true, false];
        for direction_first in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                &*support::<f32>(dir.path(), direction_first),
                &expected,
                "f32, direction first {direction_first}"
            );
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                &*support::<f64>(dir.path(), direction_first),
                &expected,
                "f64, direction first {direction_first}"
            );
        }
    }
    #[test]
    fn an_empty_region_file_is_zero_support_but_an_absent_mask_is_full_plane() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("empty.crtf");
        std::fs::write(&path, "#CRTF\n# no regions\n").unwrap();
        let explicit = crate::request::tests::request(serde_json::json!({"mask": path}));
        assert!(matches!(request_mask(&explicit), DomainMask::Boxes(boxes) if boxes.is_empty()));
        let default = crate::request::tests::request(serde_json::json!({}));
        assert_eq!(request_mask(&default), DomainMask::FullPlane);
        let empty_argument = crate::request::tests::request(serde_json::json!({"mask": []}));
        assert_eq!(request_mask(&empty_argument), DomainMask::FullPlane);
        let automatic = crate::request::tests::request(
            serde_json::json!({"usemask": "auto-multithresh", "mask": path}),
        );
        assert!(matches!(
            request_mask(&automatic),
            DomainMask::AutoMultithresh(_)
        ));
        // A new explicit empty region must not silently reuse an old mask:
        // it stays the request's own empty mask, and a cleaning run over an
        // old mask is refused rather than merged.
        let mut continuation = explicit;
        continuation.imagename = directory.path().join("image");
        std::fs::write(directory.path().join("image.model"), "old model").unwrap();
        std::fs::write(directory.path().join("image.mask"), "old mask").unwrap();
        continuation.check_tclean_output_prefixes().unwrap();
        assert!(
            matches!(request_mask(&continuation), DomainMask::Boxes(boxes) if boxes.is_empty())
        );
        continuation.niter = 5;
        assert!(continuation.check_tclean_output_prefixes().is_err());
    }
}
