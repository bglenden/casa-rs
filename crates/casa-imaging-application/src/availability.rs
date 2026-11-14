// SPDX-License-Identifier: LGPL-3.0-or-later
//! The one availability gate: what the installed
//! implementation cannot run, checked once on the compiled problem, the
//! run's backend and its host, before any phase.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, DensityCells, FrequencyFrame, InstrumentResponse, PolarizationCoordinate,
    ProductKind, ReconstructionAlgorithm, ReconstructionBasis, RequiredCapability,
    SpectralFrameAnchor, SpectralKernel, WeightingScheme,
};
use casa_imaging_operator::GridPrecision;
use casa_imaging_runtime::HostResources;
use casa_imaging_runtime::pass::BackendChoice;

use crate::BackendRequest;

/// One reason the installed implementation cannot run a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unsupported {
    /// MEM is verified only for a single standard Stokes I continuum domain.
    MemScope,
    /// The major-cycle pass has no implementation of a capability the
    /// problem requires.
    Capability(RequiredCapability),
    /// A Taylor basis reconstructs one image domain: the Taylor products
    /// have no outlier fields.
    MultiDomainTaylorBasis,
    /// A Taylor basis of Stokes planes other than I alone grids with the
    /// standard kernel set, where every plane takes the first correlation's
    /// PSF as CASA's do; the mosaic, W- and AW-projection sets wait for
    /// #1112.
    PolarizedTaylorKernelSet,
    /// The Metal backend needs a Metal device, which the host lacks.
    NoMetalDevice,
    /// Metal accumulates its grids in `f32` (ADR-0016 decision 6).
    F64GridsOnMetal,
    /// Metal grids the standard and mosaic kernel sets (#787); W and AW
    /// wait for their gate R2 rows (#653).
    KernelSetOnMetal,
    /// Facets image a constant-basis (MFS) image; CASA also facets Taylor
    /// images, which wait for a CASA comparison, and a cube's density rule
    /// would see each facet's rotated uvw.
    FacetedBasis,
    /// Facets grid with the standard or W-projection kernel set; AW and
    /// mosaic facets remain unsupported (#1063, #1070).
    FacetedKernelSet,
    /// W-facet modes outside the verified scalar continuum scope (#1063).
    FacetedWMode,
    /// Multi-channel AW cubes use the verified interpolation, frame
    /// and weighting combinations (#1121).
    MultiChannelWindowAwCube,
    /// AW projection grids a square image of square cells: its native
    /// convolution functions and their cache are laid out on one axis.
    NonSquareAwProjection,
    /// The AW catalog keys its cells on the first MeasurementSet's
    /// frequencies and first cross-correlation sample; a list of them waits
    /// for cells over all of them (#980).
    SeveralMeasurementSetsAwProjection,
}

impl fmt::Display for Unsupported {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemScope => formatter.write_str(
                "MEM currently supports one standard, natural-weighted Stokes I MFS domain (#1060)",
            ),
            Self::Capability(capability) => write!(
                formatter,
                "the major-cycle pass has no implementation of {capability:?}"
            ),
            Self::MultiDomainTaylorBasis => {
                formatter.write_str("a Taylor basis reconstructs one image domain, no outliers")
            }
            Self::PolarizedTaylorKernelSet => formatter.write_str(
                "deconvolver='mtmfs' images Stokes planes other than I alone with the standard \
                 gridder only; the mosaic, W- and AW-projection gridders wait for #1112",
            ),
            Self::NoMetalDevice => {
                formatter.write_str("the Metal backend needs a unified-memory Metal 3 device")
            }
            Self::F64GridsOnMetal => formatter.write_str("the Metal backend grids in f32 only"),
            Self::KernelSetOnMetal => formatter.write_str(
                "the Metal backend grids the standard and mosaic kernel sets only; W projection \
                 and AW projection run on the CPU",
            ),
            Self::FacetedBasis => {
                formatter.write_str("facets image a single-term MFS image (specmode mfs; #1063)")
            }
            Self::FacetedKernelSet => {
                formatter.write_str("facets grid with standard or W projection only (#1063)")
            }
            Self::FacetedWMode => formatter.write_str(
                "W-projection facets need Stokes I, Högbom or dirty MFS, natural or uniform \
                 weighting, explicit W planes and one image domain (#1063)",
            ),
            Self::MultiChannelWindowAwCube => formatter.write_str(
                "AW projection of a multi-channel window supports the interpolation, \
                 frame and weighting combinations documented for tclean; this combination \
                 is tracked in #1121",
            ),
            Self::NonSquareAwProjection => formatter.write_str(
                "AW projection images a square imsize of square cells; the standard, W and \
                 mosaic gridders image any other",
            ),
            Self::SeveralMeasurementSetsAwProjection => formatter.write_str(
                "the awproject gridder images one MeasurementSet; the standard, wproject and \
                 mosaic gridders image a list of them (#980)",
            ),
        }
    }
}

/// Why a compiled problem cannot run on this build and host; every reason
/// in deterministic order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImplementationUnavailable {
    unsupported: Vec<Unsupported>,
}

impl ImplementationUnavailable {
    /// Every unsupported requirement.
    #[must_use]
    pub fn unsupported(&self) -> &[Unsupported] {
        &self.unsupported
    }
}

impl fmt::Display for ImplementationUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the installed imaging implementation cannot run this request: ")?;
        for (index, reason) in self.unsupported.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{reason}")?;
        }
        Ok(())
    }
}

impl Error for ImplementationUnavailable {}

/// The device `requested` resolves to for `problem` at `precision` on
/// `host`. `cpu` and `gpu` are taken as asked ([`check`] refuses a `gpu`
/// request the device cannot run). `auto` takes the GPU where [`check`]
/// admits the problem there and the GPU route has been compared with the
/// CPU's end to end (`gpu_checked`), and the CPU otherwise: off macOS,
/// without a device, for the W and AW kernel sets, for an explicit
/// `gridprecision = f64`, and for outlier fields, polarizations other than
/// Stokes I and visibility writes. A mosaic goes to the GPU since its
/// Metal laws landed (#787; the owner kept `auto`'s rule, 2026-10-19).
#[must_use]
pub fn resolve_backend(
    problem: &CompiledProblem,
    requested: BackendRequest,
    precision: Option<GridPrecision>,
    host: &HostResources,
) -> BackendChoice {
    match requested {
        BackendRequest::Cpu => BackendChoice::Cpu,
        BackendRequest::Gpu => BackendChoice::Metal,
        BackendRequest::Auto
            if gpu_checked(problem)
                && check(problem, BackendChoice::Metal, precision, host).is_ok() =>
        {
            BackendChoice::Metal
        }
        BackendRequest::Auto => BackendChoice::Cpu,
    }
}

/// Whether the T1 Metal laws compare `problem`'s GPU route with the CPU's:
/// one image domain, with or without facets, in any basis, imaging Stokes I
/// and writing no visibilities. Outlier fields, other polarizations and
/// `MODEL_DATA` or `CORRECTED_DATA` written from GPU predictions run on the
/// GPU when asked for, but `auto` keeps them on the CPU until a Metal law
/// covers them (review of #723).
fn gpu_checked(problem: &CompiledProblem) -> bool {
    problem.geometry().domains().len() == 1
        && problem.reconstruction().polarization().coordinates()
            == [PolarizationCoordinate::StokesI]
        && problem
            .observation_transaction()
            .write_set()
            .visibility_columns()
            .is_empty()
}

/// Require the installed implementation to run `problem` on `backend` at
/// `precision` (`None`: `f64` on the CPU, `f32` on Metal) on `host`.
pub fn check(
    problem: &CompiledProblem,
    backend: BackendChoice,
    precision: Option<GridPrecision>,
    host: &HostResources,
) -> Result<(), ImplementationUnavailable> {
    let mut unsupported = problem
        .required_capabilities()
        .iter()
        .copied()
        .filter(|capability| !supports_capability(*capability))
        .map(Unsupported::Capability)
        .collect::<Vec<_>>();
    if matches!(
        problem.reconstruction().algorithm(),
        ReconstructionAlgorithm::Mem
    ) && (!matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::Constant
    ) || problem.reconstruction().polarization().coordinates()
        != [PolarizationCoordinate::StokesI]
        || problem.geometry().domains().len() != 1
        || problem
            .required_capabilities()
            .contains(&RequiredCapability::FacetedGeometry)
        || !crate::imaging::standard_kernel_set(problem)
        || problem
            .science()
            .measurement_equation()
            .instrument_response()
            != InstrumentResponse::Scalar
        || !matches!(problem.weighting().scheme(), WeightingScheme::Natural)
        || problem.observation().sources().len() != 1
        || !problem
            .observation_transaction()
            .write_set()
            .visibility_columns()
            .is_empty())
    {
        unsupported.push(Unsupported::MemScope);
    }
    if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::Taylor { .. }
    ) && problem.geometry().domains().len() != 1
    {
        unsupported.push(Unsupported::MultiDomainTaylorBasis);
    }
    if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::Taylor { .. }
    ) && problem.reconstruction().polarization().coordinates()
        != [PolarizationCoordinate::StokesI]
        && !crate::imaging::standard_kernel_set(problem)
    {
        unsupported.push(Unsupported::PolarizedTaylorKernelSet);
    }
    if problem
        .required_capabilities()
        .contains(&RequiredCapability::FacetedGeometry)
    {
        if !matches!(
            problem.reconstruction().basis(),
            ReconstructionBasis::Constant
        ) {
            unsupported.push(Unsupported::FacetedBasis);
        }
        let equation = problem.science().measurement_equation();
        if !crate::imaging::standard_kernel_set(problem) && equation.w_projection().is_none() {
            unsupported.push(Unsupported::FacetedKernelSet);
        }
        if let Some(w) = equation.w_projection()
            && matches!(
                problem.reconstruction().basis(),
                ReconstructionBasis::Constant
            )
            && (equation.instrument_response() != InstrumentResponse::Scalar
                || problem.reconstruction().polarization().coordinates()
                    != [PolarizationCoordinate::StokesI]
                || !matches!(
                    problem.reconstruction().algorithm(),
                    ReconstructionAlgorithm::Dirty | ReconstructionAlgorithm::Hogbom
                )
                || !matches!(
                    problem.weighting().scheme(),
                    WeightingScheme::Natural | WeightingScheme::Uniform
                )
                || matches!(problem.weighting().density_cells(), DensityCells::Box(_))
                || w.planes().is_none_or(|planes| planes.get() < 2)
                || problem.geometry().domains().len() != 1)
        {
            unsupported.push(Unsupported::FacetedWMode);
        }
    }
    if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::ChannelLocal { .. }
    ) && problem
        .required_capabilities()
        .contains(&RequiredCapability::AwProjection)
        && problem
            .observation_transaction()
            .read_set()
            .sources()
            .iter()
            .flat_map(|source| source.selection().spectral_windows())
            .any(|window| window.channel_indices().len() > 1)
        && !aw_multichannel_sampling_supported(
            problem.science().spectral().sampling().kernel(),
            problem.geometry().spectral().source_frame(),
            problem.geometry().spectral().output_frame(),
            matches!(
                problem.geometry().spectral().anchor(),
                SpectralFrameAnchor::Conversion { .. }
            ),
            problem.weighting().scheme(),
        )
    {
        unsupported.push(Unsupported::MultiChannelWindowAwCube);
    }
    if problem
        .required_capabilities()
        .contains(&RequiredCapability::AwProjection)
        && problem.geometry().domains().iter().any(|domain| {
            let [nx, ny] = domain.shape().pixels();
            let [dx, dy] = domain.direction().increment_rad();
            nx != ny || dx.abs() != dy.abs()
        })
    {
        unsupported.push(Unsupported::NonSquareAwProjection);
    }
    if problem.observation().sources().len() > 1 && !crate::imaging::multi_store_kernel_set(problem)
    {
        unsupported.push(Unsupported::SeveralMeasurementSetsAwProjection);
    }
    if backend == BackendChoice::Metal {
        if !host.metal {
            unsupported.push(Unsupported::NoMetalDevice);
        }
        if precision == Some(GridPrecision::F64) {
            unsupported.push(Unsupported::F64GridsOnMetal);
        }
        if !crate::imaging::metal_kernel_set(problem) {
            unsupported.push(Unsupported::KernelSetOnMetal);
        }
    }
    unsupported.sort_unstable();
    unsupported.dedup();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(ImplementationUnavailable { unsupported })
    }
}

/// Scientific capabilities of the major-cycle pass. The
/// primary-beam-corrected spectral index and full-Mueller responses have no
/// implementation.
const fn supports_capability(capability: RequiredCapability) -> bool {
    matches!(
        capability,
        RequiredCapability::Polarization(_)
            | RequiredCapability::FacetedGeometry
            | RequiredCapability::WProjection
            | RequiredCapability::AwProjection
            | RequiredCapability::PrimaryBeamResponse
            | RequiredCapability::SpectralFrameTransform
            | RequiredCapability::SpectralResampling
            | RequiredCapability::CommonBeamSpectralCoupling
            | RequiredCapability::SequentialContinuumTransform
            | RequiredCapability::ConstantBasis
            | RequiredCapability::MultiDomainGeometry
            | RequiredCapability::TaylorBasis
            | RequiredCapability::ChannelLocalBasis
            | RequiredCapability::DirtyReconstruction
            | RequiredCapability::HogbomReconstruction
            | RequiredCapability::ClarkReconstruction
            | RequiredCapability::MemReconstruction
            | RequiredCapability::MultiscaleReconstruction
            | RequiredCapability::MtmfsReconstruction
            | RequiredCapability::NaturalWeighting
            | RequiredCapability::UniformWeighting
            | RequiredCapability::BriggsWeighting
            | RequiredCapability::BriggsBandwidthTaperWeighting
            | RequiredCapability::BriggsAbsoluteWeighting
            | RequiredCapability::RadialWeighting
            | RequiredCapability::UvTaper
            | RequiredCapability::UnitResponseNormalization
            | RequiredCapability::FlatNoiseNormalization
            | RequiredCapability::FlatSkyNormalization
            | RequiredCapability::PbSquareNormalization
            | RequiredCapability::Product(
                ProductKind::Psf
                    | ProductKind::Residual
                    | ProductKind::Model
                    | ProductKind::RestoredImage
                    | ProductKind::SumWeights
                    | ProductKind::Weight
                    | ProductKind::Sensitivity
                    | ProductKind::Mask
                    | ProductKind::Beam
                    | ProductKind::PrimaryBeam
                    | ProductKind::PbCorrectedImage
                    | ProductKind::TaylorTerms
                    | ProductKind::SpectralIndex
                    | ProductKind::SpectralIndexError
                    | ProductKind::SpectralCurvature
            )
    )
}

/// Nearest uses native channels. Linear keeps the interpolated plane and
/// frequency together through the shared resampler (#1121).
fn aw_multichannel_sampling_supported(
    kernel: SpectralKernel,
    source: FrequencyFrame,
    output: FrequencyFrame,
    tracked: bool,
    weighting: WeightingScheme,
) -> bool {
    use FrequencyFrame::{Barycentric, Lsrk, Rest, Topocentric};

    let common_weighting = matches!(
        weighting,
        WeightingScheme::Natural | WeightingScheme::Briggs { .. } | WeightingScheme::Uniform
    );
    match kernel {
        SpectralKernel::Nearest => {
            output != Rest || (source == Topocentric && weighting == WeightingScheme::Natural)
        }
        SpectralKernel::Cubic => match (source, output) {
            (Topocentric, Topocentric) => tracked && weighting == WeightingScheme::Natural,
            (Lsrk | Topocentric | Barycentric, Lsrk) | (Lsrk, Topocentric | Barycentric) => {
                common_weighting
            }
            _ => false,
        },
        SpectralKernel::Linear => match (source, output) {
            (Lsrk, Lsrk) => true,
            (Lsrk | Topocentric | Barycentric, Lsrk | Topocentric | Barycentric) => {
                common_weighting
            }
            _ => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_channel_aw_rest_accepts_natural_nearest_from_topo() {
        use FrequencyFrame::{Barycentric, Lsrk, Rest, Topocentric};

        for source in [Topocentric, Lsrk, Barycentric, Rest] {
            for weighting in [
                WeightingScheme::Natural,
                WeightingScheme::Briggs { robust: 0.5 },
                WeightingScheme::Uniform,
            ] {
                assert_eq!(
                    aw_multichannel_sampling_supported(
                        SpectralKernel::Nearest,
                        source,
                        Rest,
                        true,
                        weighting
                    ),
                    source == Topocentric && weighting == WeightingScheme::Natural,
                    "REST from {source:?} with {weighting:?}",
                );
            }
        }
    }

    #[test]
    fn multi_channel_aw_accepts_verified_linear_frame_pairs() {
        let lsrk = FrequencyFrame::Lsrk;
        let topo = FrequencyFrame::Topocentric;
        let bary = FrequencyFrame::Barycentric;
        let natural = WeightingScheme::Natural;
        for frame in [
            lsrk,
            topo,
            FrequencyFrame::Barycentric,
            FrequencyFrame::Rest,
        ] {
            assert_eq!(
                aw_multichannel_sampling_supported(
                    SpectralKernel::Nearest,
                    lsrk,
                    frame,
                    true,
                    natural
                ),
                frame != FrequencyFrame::Rest,
            );
            for (weighting, converted) in [
                (natural, true),
                (WeightingScheme::Briggs { robust: 0.5 }, true),
                (WeightingScheme::Uniform, true),
                (WeightingScheme::Radial, false),
                (
                    WeightingScheme::BriggsAbsolute {
                        robust: 0.5,
                        noise_jy: 1.0,
                    },
                    false,
                ),
                (WeightingScheme::BriggsBandwidthTaper { robust: 0.5 }, false),
            ] {
                assert_eq!(
                    aw_multichannel_sampling_supported(
                        SpectralKernel::Linear,
                        frame,
                        lsrk,
                        true,
                        weighting
                    ),
                    frame == lsrk
                        || (matches!(
                            frame,
                            FrequencyFrame::Topocentric | FrequencyFrame::Barycentric
                        ) && converted),
                    "linear from {frame:?} with {weighting:?}",
                );
                for (source, expected) in [
                    (
                        lsrk,
                        frame == lsrk || ((frame == topo || frame == bary) && converted),
                    ),
                    (
                        topo,
                        converted && (frame == lsrk || frame == topo || frame == bary),
                    ),
                    (
                        bary,
                        converted && (frame == lsrk || frame == topo || frame == bary),
                    ),
                    (FrequencyFrame::Rest, false),
                ] {
                    assert_eq!(
                        aw_multichannel_sampling_supported(
                            SpectralKernel::Linear,
                            source,
                            frame,
                            true,
                            weighting
                        ),
                        expected,
                        "linear from {source:?} to {frame:?} with {weighting:?}",
                    );
                }
            }
            assert!(!aw_multichannel_sampling_supported(
                SpectralKernel::Identity,
                lsrk,
                frame,
                true,
                natural
            ));
        }
    }

    #[test]
    fn multi_channel_aw_cubic_accepts_verified_frame_pairs() {
        use FrequencyFrame::{Barycentric, Lsrk, Rest, Topocentric};

        for source in [Lsrk, Topocentric, Barycentric, Rest] {
            for output in [Lsrk, Topocentric, Barycentric, Rest] {
                for (weighting, supported) in [
                    (WeightingScheme::Natural, true),
                    (WeightingScheme::Briggs { robust: 0.5 }, true),
                    (WeightingScheme::Uniform, true),
                    (WeightingScheme::Radial, false),
                ] {
                    assert_eq!(
                        aw_multichannel_sampling_supported(
                            SpectralKernel::Cubic,
                            source,
                            output,
                            true,
                            weighting
                        ),
                        supported
                            && if source == Lsrk {
                                matches!(output, Lsrk | Topocentric | Barycentric)
                            } else {
                                ((source == Topocentric || source == Barycentric) && output == Lsrk)
                                    || (source == Topocentric
                                        && output == Topocentric
                                        && weighting == WeightingScheme::Natural)
                            },
                        "cubic from {source:?} to {output:?} with {weighting:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn multi_channel_aw_cubic_refuses_an_untracked_topo_axis() {
        assert!(!aw_multichannel_sampling_supported(
            SpectralKernel::Cubic,
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            false,
            WeightingScheme::Natural,
        ));
    }

    #[test]
    fn the_pass_runs_every_convolution_function_set_and_publication_beam() {
        for capability in [
            RequiredCapability::WProjection,
            RequiredCapability::AwProjection,
            RequiredCapability::PrimaryBeamResponse,
            RequiredCapability::Product(ProductKind::Weight),
            RequiredCapability::Product(ProductKind::Sensitivity),
            RequiredCapability::Product(ProductKind::PrimaryBeam),
            RequiredCapability::Product(ProductKind::PbCorrectedImage),
            RequiredCapability::Polarization(PolarizationCoordinate::StokesQ),
            RequiredCapability::Polarization(PolarizationCoordinate::CircularRl),
            RequiredCapability::FacetedGeometry,
            RequiredCapability::UvTaper,
        ] {
            assert!(supports_capability(capability), "{capability:?}");
        }
    }

    #[test]
    fn full_mueller_and_the_corrected_spectral_index_wait() {
        for capability in [
            RequiredCapability::FullMuellerResponse,
            RequiredCapability::Product(ProductKind::PbCorrectedSpectralIndex),
        ] {
            assert!(!supports_capability(capability), "{capability:?}");
        }
    }
}
