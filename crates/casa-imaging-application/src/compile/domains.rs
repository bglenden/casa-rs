// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image domains: the main image and the outlier file's, each with its
//! direction coordinate, product coordinate system and clean mask.

use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;

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
use crate::RunContext;
use casa_imaging_products::CasaImageDomainOutput;
use casa_imaging_runtime::{Demand, Reservation, admit};

use crate::{Gridder, ImagingRequest, SpectralMaskRange, UseMask, UserMaskBox};

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
        /// Their support, shared by the plan and every mask made from it.
        support: std::sync::Arc<[bool]>,
    },
    /// CASA auto-multithresh support from the current normal state.
    AutoMultithresh(AutoMultithreshControls),
    /// Inclusive pixel boxes `[x0, y0, x1, y1]`, each on its channels: a
    /// CRTF region limited to a spectral range over part of a cube.
    ChannelBoxes(Vec<([i64; 4], RangeInclusive<usize>)>),
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
    (spectral, channels): (&ImageSpectralCoordinate, usize),
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
        mask: request_mask(request, spectral, channels)?,
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

/// The main domain's mask, on an image of `channels` channels on
/// `spectral`: CASA auto-multithresh with tclean's controls, the user mask
/// image or boxes ([`user_boxes`]), or every pixel.
fn request_mask(
    request: &ImagingRequest,
    spectral: &ImageSpectralCoordinate,
    channels: usize,
) -> Result<DomainMask, PrepareError> {
    Ok(match (request.usemask, &request.mask_image) {
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
        (UseMask::User, None) => user_boxes(&request.mask_box, spectral, channels)?,
    })
}

/// The user mask's boxes on an image of `channels` channels on `spectral`:
/// a box limited to a spectral range masks the channels casacore's
/// `AnnRegion` extension gives it ([`range_channels`]), and a box whose
/// range misses every channel is left out, as CASA leaves its region out.
/// Boxes over every channel are plain boxes; a box over part of a cube
/// makes the mask one of a plane per channel ([`DomainMask::ChannelBoxes`]).
fn user_boxes(
    boxes: &[UserMaskBox],
    spectral: &ImageSpectralCoordinate,
    channels: usize,
) -> Result<DomainMask, PrepareError> {
    let every = 0..=channels.saturating_sub(1);
    let mut kept = Vec::with_capacity(boxes.len());
    for user in boxes {
        let span = match user.range {
            None => every.clone(),
            Some(range) => match range_channels(range, spectral, channels)? {
                Some((first, last)) => first..=last,
                None => {
                    tracing::warn!(
                        "the mask region of box {:?} lies outside the image's channels, so \
                         it is left out, as CASA leaves it out",
                        user.corners
                    );
                    continue;
                }
            },
        };
        kept.push((user.corners, span));
    }
    Ok(if kept.iter().all(|(_, span)| *span == every) {
        DomainMask::Boxes(kept.into_iter().map(|(corners, _)| corners).collect())
    } else {
        DomainMask::ChannelBoxes(kept)
    })
}

/// The support of `boxes`, each on its channels, over an image of
/// `[nx, ny]` pixels and `channels` channels: plane after plane, each
/// x-major (pixel `(x, y)` at `x · ny + y`). A box's corners are inclusive
/// and clipped to the image, as a [`MaskBox`]'s are.
fn channel_box_support(
    boxes: &[([i64; 4], RangeInclusive<usize>)],
    [nx, ny]: [usize; 2],
    channels: usize,
) -> Arc<[bool]> {
    let plane = nx * ny;
    let mut support = vec![false; plane * channels];
    let clip = |a: i64, b: i64, extent: usize| {
        let (low, high) = (a.min(b).max(0), a.max(b).min(extent as i64 - 1));
        (low <= high).then_some(low as usize..=high as usize)
    };
    for ([x0, y0, x1, y1], span) in boxes {
        let (Some(xs), Some(ys)) = (clip(*x0, *x1, nx), clip(*y0, *y1, ny)) else {
            continue;
        };
        for channel in span.clone().filter(|channel| *channel < channels) {
            for x in xs.clone() {
                let row = channel * plane + x * ny;
                support[row + ys.start()..=row + ys.end()].fill(true);
            }
        }
    }
    support.into()
}

/// The channels `first..=last` a spectral range masks on an image of
/// `channels` channels on `spectral`, or `None` when it masks none.
/// casacore's `AnnRegion::_extend` takes each end's pixel on the image's
/// spectral axis and orders them; `WCBox` hands them to `LCBox`, which
/// takes `Int(p + 0.5)` of each (truncating toward zero), clamps the low
/// end to 0 and the high end to the last channel, and refuses a box whose
/// ends cross, which leaves the region out.
///
/// # Errors
///
/// [`PrepareError::MaskRange`] when an end is not a finite pixel.
fn range_channels(
    range: SpectralMaskRange,
    spectral: &ImageSpectralCoordinate,
    channels: usize,
) -> Result<Option<(usize, usize)>, PrepareError> {
    let ends = match range {
        SpectralMaskRange::Frequency(hz) => hz
            .map(|frequency| (frequency - spectral.reference_frequency_hz) / spectral.increment_hz),
        SpectralMaskRange::Pixel(pixels) => pixels,
    };
    if !ends.iter().all(|end| end.is_finite()) {
        return Err(PrepareError::MaskRange { range });
    }
    let [low, high] = if ends[1] <= ends[0] {
        [ends[1], ends[0]]
    } else {
        ends
    };
    let last = i64::try_from(channels).unwrap_or(i64::MAX) - 1;
    let first = ((low + 0.5) as i64).max(0);
    let end = ((high + 0.5) as i64).min(last);
    Ok((first <= end).then_some((first as usize, end as usize)))
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
/// of `channels` channels, and what the plans keep of the mask images
/// read for them: each image's read is admitted under `context` before it
/// is read ([`super::mask_image::MaskImage::reading_bytes`]), and the
/// support its plan shares stays charged.
pub(super) fn mask_plans(
    domains: &[PreparedImageDomain],
    channels: usize,
    context: &RunContext,
) -> Result<(ImageDomainReconstructionMaskPlans, Reservation), PrepareError> {
    let mut resident = Reservation::none();
    let plans = domains
        .iter()
        .map(|domain| mask_plan(domain, channels, context, &mut resident))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((ImageDomainReconstructionMaskPlans::new(plans)?, resident))
}

fn mask_plan(
    domain: &PreparedImageDomain,
    channels: usize,
    context: &RunContext,
    resident: &mut Reservation,
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
            let image = super::mask_image::MaskImage::open(
                &path,
                coordinate,
                image_size,
                super::mask_image::TargetPlanes {
                    channels,
                    polarizations,
                    spectral,
                },
            )?;
            let mut charge = admit(
                &context.host,
                &context.policy,
                &Demand {
                    phase: "clean mask image",
                    memory: image.reading_bytes(),
                },
            )?;
            charge.retain(image.retained_bytes());
            let plan = image.read()?;
            resident.join(charge);
            plan
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
        DomainMask::ChannelBoxes(boxes) => {
            // The support is the plan's for the whole run, charged as a mask
            // image's is.
            let bytes = (image_size[0] * image_size[1] * channels) as u64;
            let mut charge = admit(
                &context.host,
                &context.policy,
                &Demand {
                    phase: "clean mask regions",
                    memory: bytes,
                },
            )?;
            charge.retain(bytes);
            let support = channel_box_support(&boxes, image_size, channels);
            resident.join(charge);
            ReconstructionMaskPlan::Reprojected {
                coordinate,
                planes: MaskPlanes::new(channels, 1)?,
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
    fn support<T: ImagePixel + From<f32>>(
        dir: &Path,
        direction_first: bool,
    ) -> std::sync::Arc<[bool]> {
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
    /// A spectral axis of `channels` channels from 999.98875… MHz in steps
    /// of 49.9994… MHz: the LSRK cube axis CASA 6.7.6.14's tclean lays on
    /// casatestdata's `refim_point.ms` (test_mask_2), as its `.mask` reads.
    fn refim_point_axis() -> ImageSpectralCoordinate {
        ImageSpectralCoordinate {
            frequency_reference: FrequencyRef::LSRK,
            reference_frequency_hz: 999_988_750.387_257_2,
            increment_hz: 1_049_988_187.906_619_9 - 999_988_750.387_257_2,
            rest_frequency_hz: 0.0,
            additional_rest_frequencies_hz: Vec::new(),
            undefined_frame: false,
        }
    }

    /// The main domain's mask of `request` on a one-channel image.
    fn mask_of(request: &ImagingRequest) -> DomainMask {
        request_mask(request, &refim_point_axis(), 1).unwrap()
    }

    /// The channels CASA 6.7.6.14 masks for `circle[[50pix,50pix],10pix],
    /// range=[...]` on `refim_point.ms`'s 20-channel cube, by `rg.fromtext`
    /// and tclean's `.mask` (2026-11-03): each end's pixel rounded by
    /// `Int(p + 0.5)`, clamped to the cube, the region left out when the
    /// ends cross.
    #[test]
    fn a_spectral_range_masks_the_channels_casa_masks() {
        let axis = refim_point_axis();
        let hz = |low: f64, high: f64| SpectralMaskRange::Frequency([low * 1e9, high * 1e9]);
        for (range, expected) in [
            (hz(1.1, 1.5), Some((2, 10))),
            (hz(1.5, 1.1), Some((2, 10))),
            (hz(1.12, 1.47), Some((2, 9))),
            (hz(1.124, 1.4749), Some((2, 9))),
            (hz(1.1251, 1.4751), Some((3, 10))),
            (hz(1.13, 1.48), Some((3, 10))),
            (hz(1.125, 1.475), Some((3, 10))),
            (hz(1.07, 1.08), Some((1, 2))),
            (hz(1.074, 1.076), Some((1, 2))),
            (hz(1.101, 1.119), Some((2, 2))),
            (hz(0.9, 0.95), Some((0, 0))),
            (hz(0.97, 0.98), Some((0, 0))),
            (hz(0.5, 1.12), Some((0, 2))),
            (hz(1.9, 3.0), Some((18, 19))),
            (hz(1.96, 1.99), Some((19, 19))),
            (hz(2.0, 2.1), None),
            (hz(2.1, 2.3), None),
            (hz(0.5, 0.9), None),
            (SpectralMaskRange::Pixel([1.4, 3.6]), Some((1, 4))),
        ] {
            assert_eq!(
                range_channels(range, &axis, 20).unwrap(),
                expected,
                "{range:?}"
            );
        }
    }

    /// A ranged box over every channel, or a one-channel image's only
    /// channel, is a plain box; one whose range misses the image is left
    /// out; one over part of a cube masks only its channels, a plane per
    /// channel. CASA's MFS image of `refim_point.ms` is one channel at about
    /// 1.5 GHz, 1 GHz wide: `range=[1.1GHz,1.2GHz]` and `[0.1GHz,0.2GHz]`
    /// mask it, `[3GHz,3.2GHz]` does not (stop code 7).
    #[test]
    fn ranged_boxes_resolve_on_the_image_channels() {
        let plain = UserMaskBox::new([1, 2, 3, 4]);
        let ranged = |low: f64, high: f64| UserMaskBox {
            corners: [5, 6, 7, 8],
            range: Some(SpectralMaskRange::Frequency([low * 1e9, high * 1e9])),
        };
        let mfs = ImageSpectralCoordinate {
            reference_frequency_hz: 1.5e9,
            increment_hz: 1.0e9,
            ..refim_point_axis()
        };
        for (low, high, kept) in [
            (1.1, 1.2, true),
            (0.1, 0.2, true),
            (1.4, 1.6, true),
            (3.0, 3.2, false),
        ] {
            let boxes = user_boxes(&[plain, ranged(low, high)], &mfs, 1).unwrap();
            let expected = if kept {
                vec![[1, 2, 3, 4], [5, 6, 7, 8]]
            } else {
                vec![[1, 2, 3, 4]]
            };
            assert_eq!(boxes, DomainMask::Boxes(expected), "{low}~{high} GHz");
        }
        let cube = refim_point_axis();
        assert_eq!(
            user_boxes(&[ranged(0.9, 2.1)], &cube, 20).unwrap(),
            DomainMask::Boxes(vec![[5, 6, 7, 8]])
        );
        assert_eq!(
            user_boxes(&[ranged(2.1, 2.3)], &cube, 20).unwrap(),
            DomainMask::Boxes(Vec::new())
        );
        // test_mask_2's region on the cube: its channels 2 to 10, and a plain
        // box beside it on every channel.
        assert_eq!(
            user_boxes(&[ranged(1.1, 1.5), plain], &cube, 20).unwrap(),
            DomainMask::ChannelBoxes(vec![([5, 6, 7, 8], 2..=10), ([1, 2, 3, 4], 0..=19)])
        );
    }

    /// The per-channel support holds each box, inclusive and clipped to the
    /// image, on its channels only, plane after plane, each x-major.
    #[test]
    fn channel_boxes_hold_their_pixels_on_their_channels() {
        let [nx, ny, channels] = [6_usize, 4, 3];
        let support = channel_box_support(
            &[([4, 1, 9, 2], 1..=1), ([-3, -3, 0, 0], 0..=2)],
            [nx, ny],
            channels,
        );
        assert_eq!(support.len(), nx * ny * channels);
        let set = |channel: usize, x: usize, y: usize| support[channel * nx * ny + x * ny + y];
        for channel in 0..channels {
            for x in 0..nx {
                for y in 0..ny {
                    let first = channel == 1 && (4..=5).contains(&x) && (1..=2).contains(&y);
                    let corner = x == 0 && y == 0;
                    assert_eq!(set(channel, x, y), first || corner, "{channel} {x} {y}");
                }
            }
        }
    }

    #[test]
    fn an_empty_region_file_is_zero_support_but_an_absent_mask_is_full_plane() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("empty.crtf");
        std::fs::write(&path, "#CRTF\n# no regions\n").unwrap();
        let explicit = crate::request::tests::request(serde_json::json!({"mask": path}));
        assert!(matches!(mask_of(&explicit), DomainMask::Boxes(boxes) if boxes.is_empty()));
        let default = crate::request::tests::request(serde_json::json!({}));
        assert_eq!(mask_of(&default), DomainMask::FullPlane);
        let empty_argument = crate::request::tests::request(serde_json::json!({"mask": []}));
        assert_eq!(mask_of(&empty_argument), DomainMask::FullPlane);
        let automatic = crate::request::tests::request(
            serde_json::json!({"usemask": "auto-multithresh", "mask": path}),
        );
        assert!(matches!(
            mask_of(&automatic),
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
        assert!(matches!(mask_of(&continuation), DomainMask::Boxes(boxes) if boxes.is_empty()));
        continuation.niter = 5;
        assert!(continuation.check_tclean_output_prefixes().is_err());
    }
}
