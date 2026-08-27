// SPDX-License-Identifier: LGPL-3.0-or-later
//! Installed imaging implementation availability at the application boundary.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, ImageDomainRole, InstrumentResponse, ModelStateIdentity, PhaseCentreLaw,
    PolarizationCoordinate, ProductKind, RequiredCapability,
};

/// A task-surface requirement not represented by [`CompiledProblem`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskRequirement {
    /// Spectral-cube task surface.
    SpectralCube,
    /// Cubedata task surface.
    SpectralCubedata,
    /// Mosaic gridder request.
    MosaicGridder,
    /// W-projection gridder request.
    WProjection,
    /// A/W-projection gridder request.
    AwProjection,
    /// Facet or outlier-file request.
    FacetsOutliers,
    /// Auto-multithreshold masking request.
    Automasking,
    /// Standalone CLEAN-mask product request.
    MaskProduct,
    /// Initial model supplied by the caller.
    StartModel,
    /// `MODEL_DATA` persistence request.
    ModelColumnWrite,
    /// Serial CPU execution selected explicitly.
    SerialCpu,
    /// Automatic execution selection.
    ExecutionAuto,
    /// Fixed-tile CPU execution override.
    FixedTileCpu,
    /// Metal gridding override.
    MetalGridder,
    /// Metal row-run gridding override.
    MetalRowRunGridder,
    /// Grouped Metal row-run gridding override.
    MetalRowRunGroupedGridder,
    /// Automatic FFT selection.
    FftAuto,
    /// RustFFT override.
    RustFft,
    /// Accelerate FFT override.
    Accelerate,
    /// FFTW override.
    Fftw,
    /// Metal MPSGraph FFT override.
    MetalMpsGraph,
    /// Task controls outside the installed spectral-cycle contract.
    UnsupportedControls,
}

/// One typed requirement not implemented by the installed imaging build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnsupportedRequirement {
    /// A compiler-derived capability has no installed implementation.
    Capability(RequiredCapability),
    /// A task-only requirement has no installed implementation.
    Task(TaskRequirement),
    /// The implementation requires exactly one observation source.
    SingleObservationSource,
    /// The implementation requires exactly one main image domain.
    SingleMainImageDomain,
    /// The implementation requires one facet.
    SingleFacet,
    /// The implementation requires a fixed phase centre.
    FixedPhaseCentre,
    /// The implementation does not accept an initial model.
    EmptyInitialModel,
    /// The implementation does not write `MODEL_DATA`.
    NoModelColumnWrite,
    /// The implementation requires a scalar measurement equation.
    ScalarInstrumentResponse,
}

/// Typed fail-closed result returned before physical planning or execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImplementationUnavailable {
    unsupported: Vec<UnsupportedRequirement>,
}

impl ImplementationUnavailable {
    /// Return every unsupported requirement in deterministic order.
    #[must_use]
    pub fn unsupported(&self) -> &[UnsupportedRequirement] {
        &self.unsupported
    }
}

impl fmt::Display for ImplementationUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "imaging request requires {} unsupported installed-implementation contract item(s)",
            self.unsupported.len()
        )
    }
}

impl Error for ImplementationUnavailable {}

/// Require the compiled problem and task-only constraints to be supported by
/// the implementation installed in this build.
pub fn validate_installed_implementation(
    problem: &CompiledProblem,
    task_requirements: impl IntoIterator<Item = TaskRequirement>,
) -> Result<(), ImplementationUnavailable> {
    let mut unsupported = problem
        .required_capabilities()
        .iter()
        .copied()
        .filter(|capability| !supports_capability(*capability))
        .map(UnsupportedRequirement::Capability)
        .collect::<Vec<_>>();

    unsupported.extend(
        task_requirements
            .into_iter()
            .filter(|requirement| !supports_task(*requirement))
            .map(UnsupportedRequirement::Task),
    );

    if problem.inputs().observation_snapshot().sources().len() != 1 {
        unsupported.push(UnsupportedRequirement::SingleObservationSource);
    }
    if problem.geometry().domains().len() != 1
        || *problem.geometry().domains()[0].role() != ImageDomainRole::Main
    {
        unsupported.push(UnsupportedRequirement::SingleMainImageDomain);
    }
    if problem
        .geometry()
        .domains()
        .iter()
        .any(|domain| domain.facets().len() != 1)
    {
        unsupported.push(UnsupportedRequirement::SingleFacet);
    }
    if !matches!(
        problem.geometry().centres().phase_tracking(),
        PhaseCentreLaw::Fixed(_)
    ) {
        unsupported.push(UnsupportedRequirement::FixedPhaseCentre);
    }
    if !matches!(problem.inputs().model(), ModelStateIdentity::Empty) {
        unsupported.push(UnsupportedRequirement::EmptyInitialModel);
    }
    if problem
        .science()
        .measurement_equation()
        .instrument_response()
        != InstrumentResponse::Scalar
    {
        unsupported.push(UnsupportedRequirement::ScalarInstrumentResponse);
    }

    unsupported.sort_unstable();
    unsupported.dedup();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(ImplementationUnavailable { unsupported })
    }
}

const fn supports_task(requirement: TaskRequirement) -> bool {
    matches!(
        requirement,
        TaskRequirement::Automasking
            | TaskRequirement::MaskProduct
            | TaskRequirement::ModelColumnWrite
            | TaskRequirement::SerialCpu
            | TaskRequirement::RustFft
    )
}

const fn supports_capability(capability: RequiredCapability) -> bool {
    matches!(
        capability,
        RequiredCapability::Polarization(PolarizationCoordinate::StokesI)
            | RequiredCapability::SpectralFrameTransform
            | RequiredCapability::ConstantBasis
            | RequiredCapability::DirtyReconstruction
            | RequiredCapability::HogbomReconstruction
            | RequiredCapability::ClarkReconstruction
            | RequiredCapability::MultiscaleReconstruction
            | RequiredCapability::NaturalWeighting
            | RequiredCapability::UniformWeighting
            | RequiredCapability::BriggsWeighting
            | RequiredCapability::BriggsBandwidthTaperWeighting
            | RequiredCapability::UnitResponseNormalization
            | RequiredCapability::FlatNoiseNormalization
            | RequiredCapability::FlatSkyNormalization
            | RequiredCapability::Product(ProductKind::Psf)
            | RequiredCapability::Product(ProductKind::Residual)
            | RequiredCapability::Product(ProductKind::Model)
            | RequiredCapability::Product(ProductKind::RestoredImage)
            | RequiredCapability::Product(ProductKind::SumWeights)
            | RequiredCapability::Product(ProductKind::Mask)
    )
}
