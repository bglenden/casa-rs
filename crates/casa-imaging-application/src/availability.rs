// SPDX-License-Identifier: LGPL-3.0-or-later
//! The one availability gate (plan section 5.8): what the installed
//! implementation cannot run, checked once on the compiled problem, the
//! run's backend and its host, before any phase.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, PolarizationCoordinate, ProductKind, ReconstructionBasis, RequiredCapability,
};
use casa_imaging_operator::GridPrecision;
use casa_imaging_runtime::HostResources;
use casa_imaging_runtime::pass::BackendChoice;

use crate::BackendRequest;

/// One reason the installed implementation cannot run a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unsupported {
    /// The major-cycle pass has no implementation of a capability the
    /// problem requires.
    Capability(RequiredCapability),
    /// A Taylor basis reconstructs Stokes I only.
    PolarizedTaylorBasis,
    /// A Taylor basis reconstructs one image domain: the Taylor products
    /// have no outlier fields.
    MultiDomainTaylorBasis,
    /// The Metal backend needs a Metal device, which the host lacks.
    NoMetalDevice,
    /// Metal accumulates its grids in `f32` (plan decision D2).
    F64GridsOnMetal,
    /// Metal grids the standard and mosaic kernel sets (#787); W and AW
    /// wait for their gate R2 rows (#653).
    KernelSetOnMetal,
    /// Facets image a constant-basis (MFS) image; CASA also facets Taylor
    /// images, which wait for a CASA comparison, and a cube's density rule
    /// would see each facet's rotated uvw.
    FacetedBasis,
    /// Facets grid with the standard kernel set. CASA refuses AW and mosaic
    /// facets; W-projection facets wait for a per-facet W set.
    FacetedKernelSet,
    /// AW projection images a continuum or MT-MFS image. A cube would keep
    /// each channel's weight image as its plane's sensitivity, as a mosaic
    /// cube does, but its per-plane weights have no CASA comparison yet
    /// (#719).
    CubeAwProjection,
    /// AW projection grids a square image of square cells: its native
    /// convolution functions and their cache are laid out on one axis.
    NonSquareAwProjection,
}

impl fmt::Display for Unsupported {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capability(capability) => write!(
                formatter,
                "the major-cycle pass has no implementation of {capability:?}"
            ),
            Self::PolarizedTaylorBasis => {
                formatter.write_str("a Taylor basis reconstructs Stokes I only")
            }
            Self::MultiDomainTaylorBasis => {
                formatter.write_str("a Taylor basis reconstructs one image domain, no outliers")
            }
            Self::NoMetalDevice => {
                formatter.write_str("the Metal backend needs a unified-memory Metal 3 device")
            }
            Self::F64GridsOnMetal => formatter.write_str("the Metal backend grids in f32 only"),
            Self::KernelSetOnMetal => formatter.write_str(
                "the Metal backend grids the standard and mosaic kernel sets only; W projection \
                 and AW projection run on the CPU",
            ),
            Self::FacetedBasis => {
                formatter.write_str("facets image a single-term MFS image (specmode mfs)")
            }
            Self::FacetedKernelSet => {
                formatter.write_str("facets grid with the standard gridder only")
            }
            Self::CubeAwProjection => formatter.write_str(
                "AW projection images specmode mfs; a cube waits for its per-plane weight check \
                 (#719)",
            ),
            Self::NonSquareAwProjection => formatter.write_str(
                "AW projection images a square imsize of square cells; the standard, W and \
                 mosaic gridders image any other",
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
/// `precision` (`None` for plan decision D2's rule) on `host`.
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
        problem.reconstruction().basis(),
        ReconstructionBasis::Taylor { .. }
    ) {
        if problem.reconstruction().polarization().coordinates()
            != [PolarizationCoordinate::StokesI]
        {
            unsupported.push(Unsupported::PolarizedTaylorBasis);
        }
        if problem.geometry().domains().len() != 1 {
            unsupported.push(Unsupported::MultiDomainTaylorBasis);
        }
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
        if !crate::imaging::standard_kernel_set(problem) {
            unsupported.push(Unsupported::FacetedKernelSet);
        }
    }
    if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::ChannelLocal { .. }
    ) && problem
        .required_capabilities()
        .contains(&RequiredCapability::AwProjection)
    {
        unsupported.push(Unsupported::CubeAwProjection);
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
            | RequiredCapability::MultiscaleReconstruction
            | RequiredCapability::MtmfsReconstruction
            | RequiredCapability::NaturalWeighting
            | RequiredCapability::UniformWeighting
            | RequiredCapability::BriggsWeighting
            | RequiredCapability::BriggsBandwidthTaperWeighting
            | RequiredCapability::UvTaper
            | RequiredCapability::UnitResponseNormalization
            | RequiredCapability::FlatNoiseNormalization
            | RequiredCapability::FlatSkyNormalization
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
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
