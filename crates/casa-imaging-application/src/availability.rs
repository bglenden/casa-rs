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

/// One reason the installed implementation cannot run a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unsupported {
    /// The major-cycle pass has no implementation of a capability the
    /// problem requires.
    Capability(RequiredCapability),
    /// A Taylor basis reconstructs Stokes I only.
    PolarizedTaylorBasis,
    /// The Metal backend needs a Metal device, which the host lacks.
    NoMetalDevice,
    /// Metal accumulates its grids in `f32` (plan decision D2).
    F64GridsOnMetal,
    /// Metal grids the standard kernel set; W, mosaic and AW wait for gate
    /// R2 (#653).
    KernelSetOnMetal,
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
            Self::NoMetalDevice => {
                formatter.write_str("the Metal backend needs a unified-memory Metal 3 device")
            }
            Self::F64GridsOnMetal => formatter.write_str("the Metal backend grids in f32 only"),
            Self::KernelSetOnMetal => formatter.write_str(
                "the Metal backend grids the standard kernel set only; W projection, mosaic and \
                 AW projection run on the CPU",
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
        ReconstructionBasis::Taylor { .. } | ReconstructionBasis::TaylorViaChannelMajor { .. }
    ) && problem.reconstruction().polarization().coordinates()
        != [PolarizationCoordinate::StokesI]
    {
        unsupported.push(Unsupported::PolarizedTaylorBasis);
    }
    if backend == BackendChoice::Metal {
        if !host.metal {
            unsupported.push(Unsupported::NoMetalDevice);
        }
        if precision == Some(GridPrecision::F64) {
            unsupported.push(Unsupported::F64GridsOnMetal);
        }
        if !crate::imaging::standard_kernel_set(problem) {
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

/// Scientific capabilities of the major-cycle pass. Faceted geometry has no
/// pass implementation (#664); the primary-beam-corrected spectral index
/// has none.
const fn supports_capability(capability: RequiredCapability) -> bool {
    matches!(
        capability,
        RequiredCapability::Polarization(_)
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
        ] {
            assert!(supports_capability(capability), "{capability:?}");
        }
    }

    #[test]
    fn faceting_full_mueller_and_the_corrected_spectral_index_wait() {
        for capability in [
            RequiredCapability::FacetedGeometry,
            RequiredCapability::FullMuellerResponse,
            RequiredCapability::Product(ProductKind::PbCorrectedSpectralIndex),
        ] {
            assert!(!supports_capability(capability), "{capability:?}");
        }
    }
}
